using Microsoft.AspNetCore.Http;
using NWebDav.Server.Stores;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Config;
using NzbWebDAV.Exceptions;
using NzbWebDAV.Middlewares;
using NzbWebDAV.Services;
using NzbWebDAV.Services.StreamTrace;
using NzbWebDAV.Streams;
using NzbWebDAV.Tests.Clients.Usenet;
using NzbWebDAV.Tests.Fakes;
using NzbWebDAV.Tests.Streams;
using NzbWebDAV.WebDav.Base;
using NzbWebDAV.Models;

namespace NzbWebDAV.Tests.WebDav;

[Collection(nameof(PlaybackHoleTrackerCollection))]
public sealed class FreshWalkOffsetTests : IDisposable
{
    private const string PresentId = "present@test";
    private const string MissingId = "missing@test";

    public FreshWalkOffsetTests() => PlaybackHoleTracker.ResetForTests();

    public void Dispose() => PlaybackHoleTracker.ResetForTests();

    [Fact]
    public void FreshWalk_IgnoresEarlierPlaybackHoleFailFast()
    {
        var path = $"/view/fresh-{Guid.NewGuid():N}.mkv";
        var miss = new UsenetArticleNotFoundException(MissingId);
        PlaybackHoleTracker.RecordHole(path, "one@test", miss);
        PlaybackHoleTracker.RecordHole(path, "two@test", miss);
        PlaybackHoleTracker.RecordHole(path, "three@test", miss);
        Assert.True(PlaybackHoleTracker.ShouldFailFast(path, out _));

        var context = new DefaultHttpContext();
        context.Request.Headers.CacheControl = "no-cache";
        using var freshWalk = FreshWalkRequest.BeginRequest(context);

        Assert.False(PlaybackHoleTracker.ShouldFailFast(path, out _));
    }

    [Theory]
    [InlineData(null, null, false, false, StatusCodes.Status206PartialContent)]
    [InlineData("no-cache", null, false, false, StatusCodes.Status404NotFound)]
    [InlineData("no-store", null, false, false, StatusCodes.Status404NotFound)]
    [InlineData(null, "no-cache", false, false, StatusCodes.Status404NotFound)]
    [InlineData("no-cache, no-store", null, false, false, StatusCodes.Status404NotFound)]
    [InlineData("no-cache", null, true, true, StatusCodes.Status206PartialContent)]
    public async Task RangeAtNonzeroOffset_UsesFreshStateAndPreservesPlaybackGapFill(
        string? cacheControl,
        string? pragma,
        bool knownMissing,
        bool targetPresent,
        int expectedStatus)
    {
        var fileName = $"/content/fresh-{Guid.NewGuid():N}.mkv";
        var segments = new Dictionary<string, byte[]> { [PresentId] = "abcdefgh"u8.ToArray() };
        if (targetPresent) segments[MissingId] = "ijklmnop"u8.ToArray();
        var segmentRanges = new Dictionary<string, LongRange>
        {
            [PresentId] = new(0, 8),
            [MissingId] = new(8, 16),
        };
        using var client = new MultiProviderNntpClient(
        [
            MultiProviderNntpClientTests.CreateProvider(
                new FakeNntpClient(segments, useCachedYencStreams: true, segmentRanges: segmentRanges),
                "a.example", maxConnections: 2),
            MultiProviderNntpClientTests.CreateProvider(
                new FakeNntpClient(segments, useCachedYencStreams: true, segmentRanges: segmentRanges),
                "b.example", maxConnections: 2),
        ]);
        var config = new ConfigManager();
        var failures = new StreamingFailureTracker();
        using var sharedStreams = new SharedStreamRegistry(config, new ConcurrentReadTracker());
        using var body = new MemoryStream();
        var context = new DefaultHttpContext();
        context.Request.Method = HttpMethods.Get;
        context.Request.Scheme = "http";
        context.Request.Host = new HostString("localhost");
        context.Request.Path = $"/.ids/{Guid.NewGuid():N}";
        context.Request.Headers.Range = "bytes=8-15";
        if (cacheControl is not null)
            context.Request.Headers.CacheControl = cacheControl;
        if (pragma is not null)
            context.Request.Headers.Pragma = pragma;
        context.Response.Body = body;

        var item = new NzbFileStoreItem(fileName, client, knownMissing ? new HashSet<int> { 1 } : null);
        var handler = new GetAndHeadHandlerPatch(
            new SingleItemStore(item),
            config,
            new ProviderUsageTracker(),
            new ActiveReadRegistry(),
            new ConcurrentReadTracker(),
            new StreamTraceBuffer(100, enabled: false),
            failures,
            sharedStreams);
        Exception? observedException = null;
        var exceptionMiddleware = new ExceptionMiddleware(
            async request =>
            {
                try
                {
                    await handler.HandleRequestAsync(request);
                }
                catch (Exception exception)
                {
                    observedException = exception;
                    throw;
                }
            },
            config,
            failures);

        await exceptionMiddleware.InvokeAsync(context);

        Assert.True(expectedStatus == context.Response.StatusCode, observedException?.ToString());
        if (expectedStatus == StatusCodes.Status206PartialContent)
        {
            Assert.Equal(targetPresent ? "ijklmnop"u8.ToArray() : new byte[8], body.ToArray());
            Assert.Equal(!targetPresent && !knownMissing,
                PlaybackHoleTracker.IsKnownMissingSegment(fileName, MissingId));
        }
        else
        {
            Assert.Empty(body.ToArray());
            Assert.False(PlaybackHoleTracker.IsKnownMissingSegment(fileName, MissingId));
        }
    }

    private sealed class NzbFileStoreItem(
        string fileName,
        MultiProviderNntpClient client,
        HashSet<int>? knownMissingSegmentIndices = null) : BaseStoreReadonlyItem
    {
        public override string Name => "fresh.mkv";
        public override string UniqueKey => fileName;
        public override long FileSize => 16;
        public override DateTime CreatedAt => DateTime.UnixEpoch;

        public override Task<Stream> GetReadableStreamAsync(CancellationToken cancellationToken)
        {
            Stream stream = new NzbFileStream(
                [PresentId, MissingId],
                FileSize,
                client,
                articleBufferSize: 0,
                segmentByteRanges: [new LongRange(0, 8), new LongRange(8, 16)],
                usePipelinedBodyRequests: false,
                fileName: fileName,
                knownMissingSegmentIndices: knownMissingSegmentIndices,
                readBudgetOverride: 8);
            return Task.FromResult(stream);
        }
    }

    private sealed class SingleItemStore(IStoreItem item) : IStore
    {
        public Task<IStoreItem?> GetItemAsync(string path, CancellationToken cancellationToken)
            => Task.FromResult<IStoreItem?>(item);

        public Task<IStoreItem?> GetItemAsync(Uri uri, CancellationToken cancellationToken)
            => Task.FromResult<IStoreItem?>(item);

        public Task<IStoreCollection?> GetCollectionAsync(Uri uri, CancellationToken cancellationToken)
            => Task.FromResult<IStoreCollection?>(null);
    }
}
