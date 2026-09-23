using Microsoft.AspNetCore.Http;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Clients.Usenet.Contexts;
using NzbWebDAV.Config;
using NzbWebDAV.Database.Models;
using NzbWebDAV.Exceptions;
using NzbWebDAV.Services;
using NzbWebDAV.Streams;
using NzbWebDAV.Tests.Clients.Usenet;

namespace NzbWebDAV.Tests.Middlewares;

public sealed partial class UnprovenMissingArticleTests
{
    [Fact]
    public async Task ScopedMissWithoutWalkEvidence_DoesNotAuthorizeRepair()
    {
        var segmentId = NewSegmentId();
        var probe = await RunReadAsync(() => Task.FromException(new UsenetArticleNotFoundException(segmentId)));

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal(0, probe.StreamingFailures);
        AssertNotSeededForFailFast(segmentId);
    }

    [Theory]
    [InlineData("no-cache, no-store", null)]
    [InlineData("no-cache", null)]
    [InlineData("no-store", null)]
    [InlineData(null, "no-cache")]
    public async Task FreshRead_RevalidatesCachedAndGroupedMissesInsteadOfHoldingForever(string? cacheControl, string? pragma)
    {
        var segmentId = NewSegmentId();
        using var cache = new ArticleMissNegativeCache(new ConfigManager());
        cache.MarkMissing(ArticleMissNegativeCache.BuildKey(segmentId, "first.example", "shared", ArticleMissNegativeCache.ArticleMissOperation.Body));
        var first = MissingProviderClient();
        var second = MissingProviderClient();
        using var client = new MultiProviderNntpClient(
            [MultiProviderNntpClientTests.CreateProvider(first, host: "first.example", storageGroup: "shared"),
             MultiProviderNntpClientTests.CreateProvider(second, host: "second.example", storageGroup: "shared")],
            articleMissCache: cache);

        for (var attempt = 0; attempt < 3; attempt++)
        {
            var probe = await RunReadAsync(
                () => client.DecodedBodyAsync(segmentId, CancellationToken.None),
                cacheControl: cacheControl, pragma: pragma);
            Assert.Equal(StatusCodes.Status404NotFound, probe.StatusCode);
            Assert.Equal(1, probe.StreamingFailures);
        }
        Assert.Equal(3, first.SingularRequests);
        Assert.Equal(3, second.SingularRequests);
        Assert.Equal(0, cache.Hits);
    }

    [Fact]
    public async Task FreshRead_RecoversAnArticleHiddenByAStaleNegativeCache()
    {
        var segmentId = NewSegmentId();
        using var cache = new ArticleMissNegativeCache(new ConfigManager());
        cache.MarkMissing(ArticleMissNegativeCache.BuildKey(segmentId, "serving.example", "", ArticleMissNegativeCache.ArticleMissOperation.Body));
        var serving = ServingProviderClient();
        using var client = new MultiProviderNntpClient(
            [MultiProviderNntpClientTests.CreateProvider(serving, host: "serving.example")], articleMissCache: cache);

        var probe = await RunReadAsync(async () =>
        {
            var response = await client.DecodedBodyAsync(segmentId, CancellationToken.None);
            await response.Stream!.DisposeAsync();
        }, cacheControl: "no-cache, no-store");

        Assert.Equal(StatusCodes.Status200OK, probe.StatusCode);
        Assert.Equal(1, serving.SingularRequests);
        Assert.Equal(0, probe.StreamingFailures);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public async Task FreshRead_BypassesHealthHolesAndPlaybackFailFast(int bufferSize)
    {
        var segmentId = NewSegmentId();
        var path = $"/content/{Guid.NewGuid():N}.mkv";
        for (var index = 0; index < GapFillLimits.MaxConsecutiveZeroFills; index++)
            PlaybackHoleTracker.RecordHole(path, segmentId, new UsenetArticleNotFoundException(segmentId));
        Assert.True(PlaybackHoleTracker.ShouldFailFast(path, out _));
        var first = MissingProviderClient();
        var second = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(first, host: "first.example"),
            MultiProviderNntpClientTests.CreateProvider(second, host: "second.example"));

        var probe = await RunReadAsync(async () =>
        {
            await using var stream = MultiSegmentStream.Create(
                new[] { segmentId }.AsMemory(), client, bufferSize, 8,
                failFastOnFirstSegment: true, usePipelinedBodyRequests: false,
                CancellationToken.None, fileName: path, exactSegmentSizes: new long[] { 8 },
                knownMissingSegmentIndices: new HashSet<int> { 0 });
            await stream.ReadAsync(new byte[8], CancellationToken.None);
        }, cacheControl: "no-cache, no-store");

        Assert.Equal(StatusCodes.Status404NotFound, probe.StatusCode);
        Assert.Equal(1, first.SingularRequests);
        Assert.Equal(1, second.SingularRequests);
        Assert.Equal(1, probe.StreamingFailures);
    }

    [Fact]
    public async Task FreshRead_DoesNotReuseSharedStreamEvidence()
    {
        var config = new ConfigManager();
        config.UpdateValues([
            new ConfigItem { ConfigName = ConfigKeys.UsenetSharedStreamsEnabled, ConfigValue = "true" },
            new ConfigItem { ConfigName = ConfigKeys.UsenetSharedStreamsRingMb, ConfigValue = "4" },
        ]);
        await using var registry = new SharedStreamRegistry(config, new ConcurrentReadTracker());
        var segmentId = NewSegmentId();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(MissingProviderClient()),
            MultiProviderNntpClientTests.CreateProvider(MissingProviderClient(), host: "second.example"));
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = new PrefetchedMissSource(gate, client, segmentId);
        var original = await registry.TryAttachAsync(
            "/fresh.mkv", 0, null, source.FileSize, source, NoFallback, CancellationToken.None);
        Assert.NotNull(original);
        await using var originalStream = original.Stream;
        var context = new DefaultHttpContext();
        context.Request.Headers.CacheControl = "no-cache, no-store";
        using var scope = ProviderReadEvidence.BeginRequest(context);

        var attached = await registry.TryAttachAsync(
            "/fresh.mkv", 0, null, source.FileSize, source, NoFallback, CancellationToken.None);

        Assert.Null(attached);
        Assert.Equal(1, registry.LiveEntryCount);
        var originalProbe = await RunReadAsync(async () =>
        {
            gate.SetResult();
            await originalStream.ReadAsync(new byte[8], CancellationToken.None);
        });
        Assert.Equal(StatusCodes.Status404NotFound, originalProbe.StatusCode);
        Assert.Equal(1, originalProbe.StreamingFailures);
    }
}
