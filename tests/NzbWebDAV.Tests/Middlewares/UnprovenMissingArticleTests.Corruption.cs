using Microsoft.AspNetCore.Http;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Config;
using NzbWebDAV.Exceptions;
using NzbWebDAV.Streams;
using NzbWebDAV.Tests.Clients.Usenet;
using NzbWebDAV.WebDav.Base;
using Serilog.Events;
using UsenetSharp.Models;
using UsenetSharp.Streams;

namespace NzbWebDAV.Tests.Middlewares;

/// <summary>
/// A provider walk stops at the first provider that answers, so a corrupt copy is only
/// terminal when that walk passed over no enabled provider on the way: a provider it
/// skipped or could not reach may still hold an intact copy.
/// </summary>
public sealed partial class UnprovenMissingArticleTests
{
    private const string UnprovenCorruptionTemplate = "is not confirmed unrecoverable";
    private const string ProvenCorruptionTemplate = "has corrupt articles";

    [Theory]
    [InlineData(0, false, null)]
    [InlineData(2, false, null)]
    [InlineData(2, true, null)]
    [InlineData(0, false, "no-cache, no-store")]
    public async Task OpenCircuitOmission_CorruptCopyAnswersRetryableWithoutRepair(
        int articleBufferSize, bool pipelined, string? cacheControl)
    {
        using var reports = await RepairReportCapture.CreateAsync();
        var segmentId = NewSegmentId();
        var path = $"/content/{Guid.NewGuid():N}.mkv";
        var skipped = ServingProviderClient();
        var corrupt = CorruptProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                skipped, host: "open.example", circuitBreaker: OpenBreaker("open.example")),
            MultiProviderNntpClientTests.CreateProvider(corrupt, host: "healthy.example"));

        var probe = await RunReadAsync(
            () => ReadToEndAsync(CreatePlaybackStream(
                client, [segmentId], articleBufferSize, pipelined, path, failFastOnFirstSegment: true)),
            cacheControl: cacheControl);

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal("5", probe.RetryAfter);
        Assert.False(probe.Aborted);
        Assert.Equal(0, skipped.SingularRequests + skipped.BatchRequests);
        Assert.True(corrupt.SingularRequests + corrupt.BatchRequests > 0);
        Assert.Equal(0, probe.StreamingFailures);
        Assert.Empty(ReportsFor(reports, path, segmentId));
        var warning = Assert.Single(probe.Logs, IsUnprovenCorruptionWarning);
        Assert.Contains("circuit breaker was open", warning.RenderMessage(), StringComparison.Ordinal);
        Assert.DoesNotContain(probe.Logs, IsProvenCorruptionWarning);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public async Task EveryEarlierProviderMissed_CorruptCopyKeepsNotFoundAndRepair(int articleBufferSize)
    {
        using var reports = await RepairReportCapture.CreateAsync();
        var segmentId = NewSegmentId();
        var path = $"/content/{Guid.NewGuid():N}.mkv";
        var missing = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(missing, host: "first.example"),
            MultiProviderNntpClientTests.CreateProvider(CorruptProviderClient(), host: "second.example"));

        var probe = await RunReadAsync(() => ReadToEndAsync(CreatePlaybackStream(
            client, [segmentId], articleBufferSize, pipelined: false, path, failFastOnFirstSegment: true)));

        Assert.Equal(StatusCodes.Status404NotFound, probe.StatusCode);
        Assert.True(missing.SingularRequests > 0);
        Assert.Equal(1, probe.StreamingFailures);
        Assert.Contains(ReportsFor(reports, path, segmentId), report => report.IsCorruption);
        Assert.Contains(probe.Logs, IsProvenCorruptionWarning);
        Assert.DoesNotContain(probe.Logs, IsUnprovenCorruptionWarning);
    }

    [Fact]
    public async Task InconclusiveProviderAhead_CorruptCopyAnswersRetryable()
    {
        var segmentId = NewSegmentId();
        var failing = new MultiProviderNntpClientTests.ScriptedNntpClient
        {
            BatchResponseCode = 430,
            SingularException = _ => new IOException("transport interrupted"),
        };
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(failing, host: "first.example"),
            MultiProviderNntpClientTests.CreateProvider(CorruptProviderClient(), host: "second.example"));

        var probe = await RunReadAsync(() => ReadToEndAsync(CreatePlaybackStream(
            client, [segmentId], articleBufferSize: 0, pipelined: false,
            $"/content/{Guid.NewGuid():N}.mkv", failFastOnFirstSegment: true)));

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.True(failing.SingularRequests > 0);
        Assert.Equal(0, probe.StreamingFailures);
        Assert.Single(probe.Logs, IsUnprovenCorruptionWarning);
    }

    [Fact]
    public async Task CachedMissAhead_IsNotProofUntilAFreshReadAsksAgain()
    {
        var segmentId = NewSegmentId();
        using var cache = new ArticleMissNegativeCache(new ConfigManager());
        cache.MarkMissing(ArticleMissNegativeCache.BuildKey(
            segmentId, "first.example", "", ArticleMissNegativeCache.ArticleMissOperation.Body));
        var missing = MissingProviderClient();
        using var client = new MultiProviderNntpClient(
            [
                MultiProviderNntpClientTests.CreateProvider(missing, host: "first.example"),
                MultiProviderNntpClientTests.CreateProvider(CorruptProviderClient(), host: "second.example"),
            ],
            articleMissCache: cache);

        var cached = await RunReadAsync(() => ReadToEndAsync(CreatePlaybackStream(
            client, [segmentId], articleBufferSize: 0, pipelined: false,
            $"/content/{Guid.NewGuid():N}.mkv", failFastOnFirstSegment: true)));
        var requestsWhileCached = missing.SingularRequests;
        var fresh = await RunReadAsync(
            () => ReadToEndAsync(CreatePlaybackStream(
                client, [segmentId], articleBufferSize: 0, pipelined: false,
                $"/content/{Guid.NewGuid():N}.mkv", failFastOnFirstSegment: true)),
            cacheControl: "no-cache, no-store");

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, cached.StatusCode);
        Assert.Equal(0, cached.StreamingFailures);
        Assert.Equal(0, requestsWhileCached);
        Assert.Equal(StatusCodes.Status404NotFound, fresh.StatusCode);
        Assert.True(missing.SingularRequests > 0);
        Assert.Equal(1, fresh.StreamingFailures);
    }

    /// <summary>
    /// The detached pump serves and validates the body on its own flow, so the verdict of
    /// the walk that served the corrupt copy reaches the reader only through the failure
    /// the pump publishes.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SharedPumpCorruptCopy_DeliversTheServingWalkVerdictToTheReader(bool omitted)
    {
        var segmentId = NewSegmentId();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                first, host: "first.example", circuitBreaker: omitted ? OpenBreaker("first.example") : null),
            MultiProviderNntpClientTests.CreateProvider(CorruptProviderClient(), host: "second.example"));
        await using var entry = new SharedStreamEntry(
            $"/content/{Guid.NewGuid():N}.mkv", 0, 8, 64,
            TimeSpan.FromSeconds(10), CancellationToken.None, chunkSize: 8, leadBytes: 8);
        entry.BindAndStart(new DetachedStreamLease
        {
            Stream = new WalkThenDrainBodyStream(gate, client, segmentId),
            Ownership = NullAsyncDisposable.Instance,
            ContentIdentity = new SharedContentIdentity(segmentId, null, 8),
        });
        var reader = entry.TryAttach(0, NoFallback, out var missReason);
        Assert.True(reader is not null, $"attach missed: {missReason}");

        var probe = await RunReadAsync(async () =>
        {
            var read = reader!.ReadAsync(new byte[8], CancellationToken.None).AsTask();
            gate.SetResult();
            await read;
        });
        await reader!.DisposeAsync();

        Assert.Equal(
            omitted ? StatusCodes.Status503ServiceUnavailable : StatusCodes.Status404NotFound,
            probe.StatusCode);
        Assert.Equal(omitted ? 0 : 1, probe.StreamingFailures);
        Assert.Equal(omitted ? 0 : 1, first.SingularRequests);
    }

    [Theory]
    [InlineData(0, true)]
    [InlineData(2, true)]
    [InlineData(0, false)]
    [InlineData(2, false)]
    public async Task CorruptCopyInPlayback_StillZeroFillsAndReportsOnlyWhenProven(
        int articleBufferSize, bool omitted)
    {
        using var reports = await RepairReportCapture.CreateAsync();
        var segmentId = NewSegmentId();
        var path = $"/content/{Guid.NewGuid():N}.mkv";
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                omitted ? ServingProviderClient() : MissingProviderClient(),
                host: "first.example",
                circuitBreaker: omitted ? OpenBreaker("first.example") : null),
            MultiProviderNntpClientTests.CreateProvider(CorruptProviderClient(), host: "second.example"));

        var result = await ReadPlaybackAsync(
            client, [segmentId], articleBufferSize, pipelined: false, path, failFastOnFirstSegment: false);

        Assert.Equal(StatusCodes.Status200OK, result.Probe.StatusCode);
        Assert.Equal(new byte[PlaybackSegmentSize], result.Content);
        Assert.Equal(0, result.Probe.StreamingFailures);
        Assert.Equal(!omitted, PlaybackHoleTracker.IsKnownMissingSegment(path, segmentId));
        Assert.Equal(omitted ? 0 : 1, ReportsFor(reports, path, segmentId).Count(report => report.IsCorruption));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CorruptionAfterBytesWereSent_AbortsAndRepairsOnlyWhenProven(bool omitted)
    {
        using var reports = await RepairReportCapture.CreateAsync();
        var intactId = NewSegmentId();
        var corruptId = NewSegmentId();
        var path = $"/content/{Guid.NewGuid():N}.mkv";
        var serving = new MultiProviderNntpClientTests.ScriptedNntpClient
        {
            BatchResponseCode = (int)UsenetResponseType.ArticleRetrievedBodyFollows,
            SuccessStream = id => new ScriptedYencBody(
                new byte[PlaybackSegmentSize], id == corruptId ? CrcMismatch() : null),
        };
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                omitted ? ServingProviderClient() : MissingProviderClient(),
                host: "first.example",
                circuitBreaker: omitted ? OpenBreaker("first.example") : null),
            MultiProviderNntpClientTests.CreateProvider(serving, host: "second.example"));

        var probe = await RunReadAsync(
            () => ReadToEndAsync(CreatePlaybackStream(
                client, [intactId, corruptId], articleBufferSize: 0, pipelined: false,
                path, failFastOnFirstSegment: true)),
            responseStarted: true);

        Assert.True(probe.Aborted);
        Assert.Equal(omitted ? 0 : 1, probe.StreamingFailures);
        Assert.Equal(omitted ? 0 : 1, ReportsFor(reports, path, corruptId).Count(report => report.IsCorruption));
    }

    [Fact]
    public async Task ScopedCorruptCopyWithoutServingWalkEvidence_DoesNotAuthorizeRepair()
    {
        var segmentId = NewSegmentId();

        var probe = await RunReadAsync(() => Task.FromException(
            new UsenetCorruptArticleException(segmentId, "second.example", CrcMismatch())));

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal(0, probe.StreamingFailures);
    }

    [Fact]
    public async Task WithoutAReadEvidenceScope_TheCorruptCopyKeepsItsCurrentVerdict()
    {
        var segmentId = NewSegmentId();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                ServingProviderClient(), host: "open.example", circuitBreaker: OpenBreaker("open.example")),
            MultiProviderNntpClientTests.CreateProvider(CorruptProviderClient(), host: "healthy.example"));

        var probe = await RunReadAsync(
            () => ReadToEndAsync(CreatePlaybackStream(
                client, [segmentId], articleBufferSize: 0, pipelined: false,
                $"/content/{Guid.NewGuid():N}.mkv", failFastOnFirstSegment: true)),
            withReadEvidence: false);

        Assert.Equal(StatusCodes.Status404NotFound, probe.StatusCode);
        Assert.Equal(1, probe.StreamingFailures);
    }

    private static async Task ReadToEndAsync(Stream stream)
    {
        await using (stream)
            await stream.CopyToAsync(Stream.Null);
    }

    private static MultiProviderNntpClientTests.ScriptedNntpClient CorruptProviderClient() => new()
    {
        BatchResponseCode = (int)UsenetResponseType.ArticleRetrievedBodyFollows,
        SuccessStream = _ => new ScriptedYencBody([], CrcMismatch()),
    };

    private static InvalidDataException CrcMismatch() =>
        new("The decoded yEnc CRC32 was afccdc56, but the trailer expected a8e2a630.");

    private static bool IsUnprovenCorruptionWarning(LogEvent logEvent) =>
        logEvent.Level == LogEventLevel.Warning
        && logEvent.MessageTemplate.Text.Contains(UnprovenCorruptionTemplate, StringComparison.Ordinal);

    private static bool IsProvenCorruptionWarning(LogEvent logEvent) =>
        logEvent.MessageTemplate.Text.Contains(ProvenCorruptionTemplate, StringComparison.Ordinal);

    /// <summary>Decoded body bytes, then either a clean end or a failed yEnc trailer.</summary>
    private sealed class ScriptedYencBody(byte[] bytes, InvalidDataException? trailerFailure) : YencStream(Null)
    {
        private int _position;

        public override ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_position < bytes.Length)
            {
                var count = Math.Min(buffer.Length, bytes.Length - _position);
                bytes.AsMemory(_position, count).CopyTo(buffer);
                _position += count;
                return ValueTask.FromResult(count);
            }

            return trailerFailure is null
                ? ValueTask.FromResult(0)
                : ValueTask.FromException<int>(trailerFailure);
        }
    }

    /// <summary>
    /// Stands in for the stream a shared pump owns: a real provider walk on the pump's own
    /// flow, then the body drain that surfaces the corrupt copy.
    /// </summary>
    private sealed class WalkThenDrainBodyStream(
        TaskCompletionSource gate,
        MultiProviderNntpClient client,
        string segmentId) : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => 0;
            set => throw new NotSupportedException();
        }

        public override async Task<int> ReadAsync(
            byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            await gate.Task.WaitAsync(cancellationToken);
            var response = await client.DecodedBodyAsync(segmentId, cancellationToken);
            await using var body = response.Stream!;
            await body.CopyToAsync(Null, cancellationToken);
            throw new InvalidOperationException("The corrupt body above must have thrown.");
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            ReadAsync(buffer, offset, count, CancellationToken.None).GetAwaiter().GetResult();

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
