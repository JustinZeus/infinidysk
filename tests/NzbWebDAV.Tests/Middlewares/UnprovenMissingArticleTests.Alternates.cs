using Microsoft.AspNetCore.Http;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Config;
using NzbWebDAV.Exceptions;
using NzbWebDAV.Streams;
using NzbWebDAV.Tests.Clients.Usenet;

namespace NzbWebDAV.Tests.Middlewares;

/// <summary>
/// A segment with alternate article ids is only lost when every id that could serve it is
/// proven gone. An alternate whose walk passed over a provider keeps the failure retryable,
/// whichever article's exception reaches the HTTP boundary.
/// </summary>
public sealed partial class UnprovenMissingArticleTests
{
    [Theory]
    [InlineData(0, false, false)]
    [InlineData(2, false, false)]
    [InlineData(2, true, false)]
    [InlineData(0, false, true)]
    [InlineData(2, true, true)]
    public async Task AlternateArticle_MustBeProvenBeforeThePrimaryMissIsTerminal(
        int articleBufferSize, bool pipelined, bool alternateProven)
    {
        var segmentId = NewSegmentId();
        var alternateId = NewSegmentId();
        using var cache = new ArticleMissNegativeCache(new ConfigManager());
        cache.MarkMissing(ArticleMissNegativeCache.BuildKey(
            alternateId, "first.example", "", ArticleMissNegativeCache.ArticleMissOperation.Body));
        var first = MissingProviderClient();
        using var client = new MultiProviderNntpClient(
            [
                MultiProviderNntpClientTests.CreateProvider(first, host: "first.example"),
                MultiProviderNntpClientTests.CreateProvider(MissingProviderClient(), host: "second.example"),
            ],
            articleMissCache: alternateProven ? null : cache);

        var probe = await RunReadAsync(() => ReadToEndAsync(MultiSegmentStream.Create(
            new[] { segmentId }.AsMemory(), client, articleBufferSize, PlaybackSegmentSize,
            failFastOnFirstSegment: true, usePipelinedBodyRequests: pipelined,
            CancellationToken.None, fileName: $"/content/{Guid.NewGuid():N}.mkv",
            segmentFallbacks: [[alternateId]], exactSegmentSizes: new long[] { PlaybackSegmentSize })));

        Assert.True(first.SingularRequests + first.BatchRequests > 0);
        if (alternateProven)
        {
            Assert.Equal(StatusCodes.Status404NotFound, probe.StatusCode);
            Assert.Equal(1, probe.StreamingFailures);
            return;
        }

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal("5", probe.RetryAfter);
        Assert.Equal(0, probe.StreamingFailures);
        AssertNotSeededForFailFast(segmentId);
        var warning = Assert.Single(probe.Logs, IsUnprovenWarning);
        Assert.Contains($"alternate article {alternateId}", warning.RenderMessage(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task CorruptAlternateFromAnIncompleteWalk_KeepsAProvenPrimaryMissRetryable()
    {
        var segmentId = NewSegmentId();
        var alternateId = NewSegmentId();
        using var cache = new ArticleMissNegativeCache(new ConfigManager());
        cache.MarkMissing(ArticleMissNegativeCache.BuildKey(
            alternateId, "first.example", "", ArticleMissNegativeCache.ArticleMissOperation.Body));
        var second = new MultiProviderNntpClientTests.ScriptedNntpClient
        {
            BatchResponseCode = 430,
            SingularException = id => id == alternateId ? null : new UsenetArticleNotFoundException(id),
            SuccessStream = _ => new ScriptedYencBody([], CrcMismatch()),
        };
        using var client = new MultiProviderNntpClient(
            [
                MultiProviderNntpClientTests.CreateProvider(MissingProviderClient(), host: "first.example"),
                MultiProviderNntpClientTests.CreateProvider(second, host: "second.example"),
            ],
            articleMissCache: cache);

        var probe = await RunReadAsync(() => ReadToEndAsync(MultiSegmentStream.Create(
            new[] { segmentId }.AsMemory(), client, articleBufferSize: 0, PlaybackSegmentSize,
            failFastOnFirstSegment: true, usePipelinedBodyRequests: false,
            CancellationToken.None, fileName: $"/content/{Guid.NewGuid():N}.mkv",
            segmentFallbacks: [[alternateId]], exactSegmentSizes: new long[] { PlaybackSegmentSize })));

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal(0, probe.StreamingFailures);
        AssertNotSeededForFailFast(segmentId);
    }

    /// <summary>
    /// A seek probe that cannot place the offset reports one missing article. That report
    /// must be an unproven one whenever any probed id was unproven, not simply the last.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SeekProbe_CarriesAnUnprovenMissAmongThePrimaryAndItsAlternates(bool primaryProven)
    {
        var intactId = NewSegmentId();
        var segmentId = NewSegmentId();
        var alternateId = NewSegmentId();
        using var cache = new ArticleMissNegativeCache(new ConfigManager());
        cache.MarkMissing(ArticleMissNegativeCache.BuildKey(
            segmentId, "first.example", "", ArticleMissNegativeCache.ArticleMissOperation.Body));
        using var client = new MultiProviderNntpClient(
            [
                MultiProviderNntpClientTests.CreateProvider(MissingProviderClient(), host: "first.example"),
                MultiProviderNntpClientTests.CreateProvider(MissingProviderClient(), host: "second.example"),
            ],
            articleMissCache: primaryProven ? null : cache);

        var probe = await RunReadAsync(async () =>
        {
            await using var stream = new NzbFileStream(
                [intactId, segmentId],
                2 * PlaybackSegmentSize,
                client,
                articleBufferSize: 0,
                usePipelinedBodyRequests: false,
                fileName: $"/content/{Guid.NewGuid():N}.mkv",
                segmentFallbacks: [[], [alternateId]],
                readBudgetOverride: PlaybackSegmentSize);
            stream.Seek(PlaybackSegmentSize, SeekOrigin.Begin);
            await stream.ReadAsync(new byte[PlaybackSegmentSize]);
        });

        Assert.Equal(
            primaryProven ? StatusCodes.Status404NotFound : StatusCodes.Status503ServiceUnavailable,
            probe.StatusCode);
        Assert.Equal(primaryProven ? 1 : 0, probe.StreamingFailures);
    }
}
