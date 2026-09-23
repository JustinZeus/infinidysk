using Microsoft.AspNetCore.Http;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Clients.Usenet.Contexts;
using NzbWebDAV.Config;
using NzbWebDAV.Database.Models;
using NzbWebDAV.Exceptions;
using NzbWebDAV.Models;
using NzbWebDAV.Tests.Clients.Usenet;

namespace NzbWebDAV.Tests.Middlewares;

public sealed partial class UnprovenMissingArticleTests
{
    [Theory]
    [InlineData("stream")]
    [InlineData("batch")]
    [InlineData("pipelined-body")]
    [InlineData("pipelined-article")]
    [InlineData("stat")]
    public async Task CompleteWalkOnEachReadPath_PreservesNotFound(string operation)
    {
        var segmentId = NewSegmentId();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(MissingProviderClient(), host: "first.example"),
            MultiProviderNntpClientTests.CreateProvider(MissingProviderClient(), host: "second.example"));

        var probe = await RunReadAsync(() => ReadMissingAsync(client, segmentId, operation));

        Assert.Equal(StatusCodes.Status404NotFound, probe.StatusCode);
        Assert.Equal(1, probe.StreamingFailures);
    }

    [Fact]
    public async Task PrimaryOnlyStatSweep_DoesNotProveAnUnqueriedBackupMissing()
    {
        var segmentId = NewSegmentId();
        var backup = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                new MultiProviderNntpClientTests.ScriptedNntpClient { BatchResponseCode = 430, StatResponseCode = 430 },
                host: "first.example"),
            MultiProviderNntpClientTests.CreateProvider(backup, host: "backup.example", providerType: ProviderType.BackupOnly));

        var probe = await RunReadAsync(() => ReadMissingAsync(client, segmentId, "pipelined-stat"));

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal(0, backup.SingularRequests);
        Assert.Equal(0, probe.StreamingFailures);
        AssertNotSeededForFailFast(segmentId);
    }

    [Theory]
    [InlineData("body", "circuit")]
    [InlineData("stream", "circuit")]
    [InlineData("batch", "circuit")]
    [InlineData("body", "admission")]
    [InlineData("stream", "admission")]
    [InlineData("batch", "admission")]
    [InlineData("body", "transport")]
    [InlineData("stream", "transport")]
    [InlineData("batch", "transport")]
    public async Task InconclusiveProviderThenMissing_AnswersRetryable(string operation, string failure)
    {
        var segmentId = NewSegmentId();
        Exception Fail() => failure switch
        {
            "circuit" => new CircuitAdmissionRejectedException(),
            "admission" => new ProviderTransferAdmissionTimeoutException("first.example", TimeSpan.FromMilliseconds(1)),
            _ => new IOException("provider unavailable"),
        };
        var first = new MultiProviderNntpClientTests.ScriptedNntpClient
        {
            BatchResponseCode = 430,
            SingularException = _ => Fail(),
            FaultBatchResponsesWith = Fail,
        };
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(first, host: "first.example"),
            MultiProviderNntpClientTests.CreateProvider(
                MissingProviderClient(), host: "backup.example", providerType: ProviderType.BackupOnly));

        var probe = await RunReadAsync(() => ReadMissingAsync(client, segmentId, operation));

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal(0, probe.StreamingFailures);
        AssertNotSeededForFailFast(segmentId);
    }

    [Theory]
    [InlineData("circuit")]
    [InlineData("admission")]
    [InlineData("transport")]
    public async Task BatchSetupFailureThenMissing_PreservesOmittedProvider(string failure)
    {
        var segmentId = NewSegmentId();
        var first = new MultiProviderNntpClientTests.ScriptedNntpClient
        {
            BatchResponseCode = 430,
            BatchException = _ => failure switch
            {
                "circuit" => new CircuitAdmissionRejectedException(),
                "admission" => new ProviderTransferAdmissionTimeoutException("first.example", TimeSpan.FromMilliseconds(1)),
                _ => new IOException("batch setup unavailable"),
            },
        };
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(first, host: "first.example"),
            MultiProviderNntpClientTests.CreateProvider(
                MissingProviderClient(), host: "backup.example", providerType: ProviderType.BackupOnly));

        var probe = await RunReadAsync(() => ReadMissingAsync(client, segmentId, "batch"));

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal(0, probe.StreamingFailures);
        AssertNotSeededForFailFast(segmentId);
    }

    [Theory]
    [InlineData("stat")]
    [InlineData("pipelined-stat")]
    [InlineData("pipelined-body")]
    [InlineData("pipelined-article")]
    public async Task PipelineOrMetadataMiss_PreservesSelectionOmission(string operation)
    {
        var segmentId = NewSegmentId();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                MissingProviderClient(), host: "open.example", circuitBreaker: OpenBreaker("open.example")),
            MultiProviderNntpClientTests.CreateProvider(MissingProviderClient(), host: "healthy.example"));

        var probe = await RunReadAsync(() => ReadMissingAsync(client, segmentId, operation));

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal(0, probe.StreamingFailures);
        AssertNotSeededForFailFast(segmentId);
    }

    [Theory]
    [InlineData("body")]
    [InlineData("stream")]
    [InlineData("batch")]
    public async Task RetiredProviderGeneration_NeverClaimsMissing(string operation)
    {
        var segmentId = NewSegmentId();
        var retired = new MultiProviderNntpClientTests.ScriptedNntpClient
        {
            BatchResponseCode = 430,
            SingularException = _ => new NntpClientRetiredException("generation retired", new ObjectDisposedException("pool")),
            FaultBatchResponsesWith = () => new NntpClientRetiredException("generation retired", new ObjectDisposedException("pool")),
        };
        var backup = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(retired, host: "retired.example"),
            MultiProviderNntpClientTests.CreateProvider(backup, host: "backup.example", providerType: ProviderType.BackupOnly));

        var probe = await RunReadAsync(() => ReadMissingAsync(client, segmentId, operation));

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal(0, backup.SingularRequests);
        Assert.Equal(0, probe.StreamingFailures);
        AssertNotSeededForFailFast(segmentId);
    }

    [Theory]
    [InlineData("body")]
    [InlineData("batch")]
    public async Task StorageGroupSkip_DoesNotProveEveryProviderMissing(string operation)
    {
        var segmentId = NewSegmentId();
        var sibling = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(MissingProviderClient(), host: "first.example", storageGroup: "shared"),
            MultiProviderNntpClientTests.CreateProvider(sibling, host: "sibling.example", storageGroup: "shared"));

        var probe = await RunReadAsync(() => ReadMissingAsync(client, segmentId, operation));

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal(0, sibling.SingularRequests);
        Assert.Equal(0, probe.StreamingFailures);
        AssertNotSeededForFailFast(segmentId);
    }

    [Fact]
    public async Task CachedProviderMiss_DoesNotProveCurrentFullWalk()
    {
        var segmentId = NewSegmentId();
        using var cache = new ArticleMissNegativeCache(new ConfigManager());
        cache.MarkMissing(ArticleMissNegativeCache.BuildKey(segmentId, "cached.example", "", ArticleMissNegativeCache.ArticleMissOperation.Body));
        var cached = MissingProviderClient();
        using var client = new MultiProviderNntpClient(
            [MultiProviderNntpClientTests.CreateProvider(cached, host: "cached.example"),
             MultiProviderNntpClientTests.CreateProvider(MissingProviderClient(), host: "backup.example")],
            articleMissCache: cache);

        var probe = await RunReadAsync(() => ReadMissingAsync(client, segmentId, "body"));

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal(0, cached.SingularRequests);
        Assert.Equal(0, probe.StreamingFailures);
        AssertNotSeededForFailFast(segmentId);
    }

    private static async Task ReadMissingAsync(MultiProviderNntpClient client, string segmentId, string operation)
    {
        switch (operation)
        {
            case "body":
                await client.DecodedBodyAsync(segmentId, CancellationToken.None);
                break;
            case "stream":
                await client.DecodedBodyAsync(segmentId, null, CancellationToken.None);
                break;
            case "batch":
                var batch = await client.DecodedBodiesAsync([segmentId], null, CancellationToken.None);
                try { await batch.Responses[0]; }
                finally { await batch.Completion; }
                break;
            case "stat":
                await client.StatAsync(segmentId, CancellationToken.None);
                break;
            case "pipelined-stat":
                await foreach (var result in client.StatsPipelinedAsync([segmentId], 1, CancellationToken.None))
                    if (!result.Exists) throw new UsenetArticleNotFoundException(segmentId);
                break;
            case "pipelined-body":
                await foreach (var result in client.DecodedBodiesPipelinedAsync([segmentId], 1, CancellationToken.None))
                    if (!result.Found) throw new UsenetArticleNotFoundException(segmentId);
                break;
            case "pipelined-article":
                await foreach (var result in client.DecodedArticlesPipelinedAsync([segmentId], 1, CancellationToken.None))
                    if (!result.Found) throw new UsenetArticleNotFoundException(segmentId);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(operation));
        }
    }
}
