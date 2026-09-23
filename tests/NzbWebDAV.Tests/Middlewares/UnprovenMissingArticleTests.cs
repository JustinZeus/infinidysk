using Microsoft.AspNetCore.Http;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Clients.Usenet.Connections;
using NzbWebDAV.Clients.Usenet.Contexts;
using NzbWebDAV.Database.Models;
using NzbWebDAV.Exceptions;
using NzbWebDAV.Middlewares;
using NzbWebDAV.Models;
using NzbWebDAV.Services;
using NzbWebDAV.Services.Metrics;
using NzbWebDAV.Streams;
using NzbWebDAV.Tests.Clients.Usenet;
using NzbWebDAV.Tests.Database;
using NzbWebDAV.Tests.TestUtils;
using NzbWebDAV.WebDav.Base;
using Serilog;
using Serilog.Core;
using Serilog.Events;
using UsenetSharp.Models;

namespace NzbWebDAV.Tests.Middlewares;

/// <summary>
/// A definitive miss only proves an article is gone when every provider the operator
/// left enabled was actually asked. These drive the real provider walk and the real HTTP
/// error boundary, so every assertion is a status code, a provider contact, or a repair
/// side effect rather than internal bookkeeping.
/// </summary>
[Collection(nameof(ConfigPathCollection))]
public sealed partial class UnprovenMissingArticleTests
{
    private const string UnprovenTemplate = "could not be confirmed missing";
    private const string ProvenTemplate = "has missing articles";

    [Theory]
    [InlineData(null)]
    [InlineData("no-cache, no-store")]
    public async Task OpenCircuitOmission_AnswersRetryableAndSkipsRepair(string? cacheControl)
    {
        var segmentId = NewSegmentId();
        var skipped = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                skipped, host: "open.example", circuitBreaker: OpenBreaker("open.example")),
            MultiProviderNntpClientTests.CreateProvider(
                MissingProviderClient(), host: "healthy.example"));

        var probe = await RunReadAsync(
            () => client.DecodedBodyAsync(segmentId, CancellationToken.None), cacheControl: cacheControl);

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.False(probe.Aborted);
        Assert.Equal(0, skipped.SingularRequests);
        Assert.Equal(0, probe.StreamingFailures);
        AssertNotSeededForFailFast(segmentId);
        var warning = Assert.Single(probe.Logs, IsUnprovenWarning);
        Assert.Contains("circuit breaker was open", warning.RenderMessage(), StringComparison.Ordinal);
        Assert.DoesNotContain(probe.Logs, IsProvenMissError);
    }

    [Fact]
    public async Task ByteLimitOmission_AnswersRetryableAndSkipsRepair()
    {
        var segmentId = NewSegmentId();
        var exhausted = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                exhausted, host: "block.example", byteLimit: 1_000, bytesUsedOffset: 1_000),
            MultiProviderNntpClientTests.CreateProvider(
                MissingProviderClient(), host: "healthy.example"),
            new ProviderBytesTracker());

        var probe = await RunReadAsync(
            () => client.DecodedBodyAsync(segmentId, CancellationToken.None));

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal(0, exhausted.SingularRequests);
        Assert.Equal(0, probe.StreamingFailures);
        AssertNotSeededForFailFast(segmentId);
        var warning = Assert.Single(probe.Logs, IsUnprovenWarning);
        Assert.Contains("byte limit", warning.RenderMessage(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task EveryEnabledProviderMisses_KeepsTheNotFoundAndSchedulesRepair()
    {
        var segmentId = NewSegmentId();
        var first = MissingProviderClient();
        var second = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(first, host: "a.example"),
            MultiProviderNntpClientTests.CreateProvider(second, host: "b.example"));

        var probe = await RunReadAsync(
            () => client.DecodedBodyAsync(segmentId, CancellationToken.None));

        Assert.Equal(StatusCodes.Status404NotFound, probe.StatusCode);
        Assert.Equal(1, first.SingularRequests);
        Assert.Equal(1, second.SingularRequests);
        Assert.Equal(1, probe.StreamingFailures);
        Assert.Throws<UsenetArticleNotFoundException>(
            () => HealthCheckService.CheckCachedMissingSegmentIds([segmentId]));
        Assert.DoesNotContain(probe.Logs, IsUnprovenWarning);
    }

    [Fact]
    public async Task EveryProviderOpen_RejectsAdmissionWithoutClaimingMissing()
    {
        var segmentId = NewSegmentId();
        var first = MissingProviderClient();
        var second = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                first, host: "a.example", circuitBreaker: OpenBreaker("a.example")),
            MultiProviderNntpClientTests.CreateProvider(
                second, host: "b.example", circuitBreaker: OpenBreaker("b.example")));

        var probe = await RunReadAsync(
            () => client.DecodedBodyAsync(segmentId, CancellationToken.None));

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal(0, first.SingularRequests);
        Assert.Equal(0, second.SingularRequests);
        Assert.Equal(0, probe.StreamingFailures);
        AssertNotSeededForFailFast(segmentId);
    }

    [Fact]
    public async Task DisabledProviderIsNotAnOmission_KeepsTheNotFound()
    {
        var segmentId = NewSegmentId();
        var disabled = MissingProviderClient();
        var enabled = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                disabled, host: "off.example", providerType: ProviderType.Disabled),
            MultiProviderNntpClientTests.CreateProvider(enabled, host: "on.example"));

        var probe = await RunReadAsync(
            () => client.DecodedBodyAsync(segmentId, CancellationToken.None));

        Assert.Equal(StatusCodes.Status404NotFound, probe.StatusCode);
        Assert.Equal(0, disabled.SingularRequests);
        Assert.Equal(1, enabled.SingularRequests);
        Assert.Equal(1, probe.StreamingFailures);
    }

    [Fact]
    public async Task OpenCircuitOmission_AnswersRetryableOnTheBatchPath()
    {
        var segmentId = NewSegmentId();
        var skipped = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                skipped, host: "open.example", circuitBreaker: OpenBreaker("open.example")),
            MultiProviderNntpClientTests.CreateProvider(
                MissingProviderClient(), host: "healthy.example"));

        var probe = await RunReadAsync(async () =>
        {
            var batch = await client.DecodedBodiesAsync(
                [segmentId], onConnectionReadyAgain: null, CancellationToken.None);
            await batch.Responses[0];
        });

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal(0, skipped.BatchRequests);
        Assert.Equal(0, skipped.SingularRequests);
        Assert.Equal(0, probe.StreamingFailures);
    }

    [Fact]
    public async Task OpenCircuitOmission_CompletesTheStreamingCallbackExactlyOnce()
    {
        var segmentId = NewSegmentId();
        var callbacks = 0;
        var skipped = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                skipped, host: "open.example", circuitBreaker: OpenBreaker("open.example")),
            MultiProviderNntpClientTests.CreateProvider(
                MissingProviderClient(), host: "healthy.example"));

        var probe = await RunReadAsync(() => client.DecodedBodyAsync(
            segmentId,
            (_, _) => Interlocked.Increment(ref callbacks),
            CancellationToken.None));

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal(1, callbacks);
        Assert.Equal(0, skipped.SingularRequests);
    }

    [Fact]
    public async Task OpenCircuitOmission_StillFallsBackToTheProviderHoldingTheArticle()
    {
        var segmentId = NewSegmentId();
        var skipped = MissingProviderClient();
        var serving = ServingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                skipped, host: "open.example", circuitBreaker: OpenBreaker("open.example")),
            MultiProviderNntpClientTests.CreateProvider(serving, host: "healthy.example"));

        UsenetDecodedBodyResponse? served = null;
        var probe = await RunReadAsync(async () =>
            served = await client.DecodedBodyAsync(segmentId, CancellationToken.None));

        Assert.Equal(StatusCodes.Status200OK, probe.StatusCode);
        Assert.Equal(
            UsenetResponseType.ArticleRetrievedBodyFollows,
            Assert.IsType<UsenetDecodedBodyResponse>(served).ResponseType);
        Assert.Equal(1, serving.SingularRequests);
        Assert.Equal(0, skipped.SingularRequests);
        Assert.Equal(0, probe.StreamingFailures);
        Assert.DoesNotContain(probe.Logs, IsUnprovenWarning);
    }

    [Fact]
    public async Task UnprovenMissAfterTheResponseStarted_AbortsAndStillSkipsRepair()
    {
        var segmentId = NewSegmentId();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                MissingProviderClient(), host: "open.example",
                circuitBreaker: OpenBreaker("open.example")),
            MultiProviderNntpClientTests.CreateProvider(
                MissingProviderClient(), host: "healthy.example"));

        var probe = await RunReadAsync(
            () => client.DecodedBodyAsync(segmentId, CancellationToken.None),
            responseStarted: true);

        Assert.True(probe.Aborted);
        Assert.Equal(0, probe.StreamingFailures);
        AssertNotSeededForFailFast(segmentId);
    }

    [Fact]
    public async Task WithoutAReadEvidenceScope_TheMissKeepsItsCurrentVerdict()
    {
        var segmentId = NewSegmentId();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                MissingProviderClient(), host: "open.example",
                circuitBreaker: OpenBreaker("open.example")),
            MultiProviderNntpClientTests.CreateProvider(
                MissingProviderClient(), host: "healthy.example"));

        var probe = await RunReadAsync(
            () => client.DecodedBodyAsync(segmentId, CancellationToken.None),
            withReadEvidence: false);

        Assert.Equal(StatusCodes.Status404NotFound, probe.StatusCode);
        Assert.Equal(1, probe.StreamingFailures);
    }

    /// <summary>
    /// The default read path attaches to a shared upstream whose pump runs with
    /// execution-context flow suppressed, so the walk it performs can only reach the
    /// attached reader's request through the failure the pump publishes.
    /// </summary>
    [Fact]
    public async Task SharedPumpOmission_AnswersRetryableForTheAttachedReader()
    {
        var segmentId = NewSegmentId();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var skipped = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                skipped, host: "open.example", circuitBreaker: OpenBreaker("open.example")),
            MultiProviderNntpClientTests.CreateProvider(
                MissingProviderClient(), host: "healthy.example"));

        await using var entry = new SharedStreamEntry(
            $"/content/{Guid.NewGuid():N}.mkv",
            0,
            8,
            64,
            TimeSpan.FromSeconds(10),
            CancellationToken.None,
            chunkSize: 8,
            leadBytes: 8);
        entry.BindAndStart(new DetachedStreamLease
        {
            Stream = new WalkThenSurfaceMissStream(gate, client, segmentId),
            Ownership = NullAsyncDisposable.Instance,
            ContentIdentity = new SharedContentIdentity("evidence-test", null, 8),
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

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal(0, skipped.SingularRequests);
        Assert.Equal(0, probe.StreamingFailures);
        AssertNotSeededForFailFast(segmentId);
        Assert.Single(probe.Logs, IsUnprovenWarning);
    }

    private sealed record ReadProbe(
        int StatusCode,
        bool Aborted,
        int StreamingFailures,
        IReadOnlyList<LogEvent> Logs,
        string RetryAfter);

    /// <summary>
    /// Runs <paramref name="read"/> inside the same read-evidence scope the WebDAV read
    /// boundaries open, behind the real <see cref="ExceptionMiddleware"/>.
    /// </summary>
    private static async Task<ReadProbe> RunReadAsync(
        Func<Task> read,
        bool responseStarted = false,
        bool withReadEvidence = true,
        string? cacheControl = null,
        string? pragma = null)
    {
        var lifetime = new ExceptionMiddlewareTests.TestHttpRequestLifetimeFeature();
        var context = ExceptionMiddlewareTests.CreateDavItemContext(responseStarted, lifetime);
        context.Request.Headers.CacheControl = cacheControl;
        context.Request.Headers.Pragma = pragma;
        var davItem = Assert.IsType<DavItem>(context.Items["DavItem"]);
        var failureTracker = new StreamingFailureTracker();
        var middleware = new ExceptionMiddleware(
            async ctx =>
            {
                if (!withReadEvidence)
                {
                    await read();
                    return;
                }

                using var readEvidence = ProviderReadEvidence.BeginRequest(ctx);
                await read();
            },
            ExceptionMiddlewareTests.CreateRepairEnabledConfig(),
            failureTracker);

        var logs = await CaptureLogsAsync(() => middleware.InvokeAsync(context));
        return new ReadProbe(
            context.Response.StatusCode,
            lifetime.Aborted,
            failureTracker.GetFailureCount(davItem.Id),
            logs,
            context.Response.Headers.RetryAfter.ToString());
    }

    private static MultiProviderNntpClient TwoProviders(
        MultiConnectionNntpClient first,
        MultiConnectionNntpClient second,
        ProviderBytesTracker? bytesTracker = null) =>
        new([first, second], bytesTracker: bytesTracker);

    private static MultiProviderNntpClientTests.ScriptedNntpClient MissingProviderClient() => new()
    {
        BatchResponseCode = 430,
        SingularResponseCode = 430,
        SingularException = id => new UsenetArticleNotFoundException(id),
    };

    private static MultiProviderNntpClientTests.ScriptedNntpClient ServingProviderClient() => new()
    {
        BatchResponseCode = (int)UsenetResponseType.ArticleRetrievedBodyFollows,
    };

    /// <summary>Trips a breaker and leaves it inside its cooldown, fully open.</summary>
    private static ProviderCircuitBreaker OpenBreaker(string host)
    {
        var breaker = new ProviderCircuitBreaker(host);
        breaker.RecordFailure();
        breaker.RecordFailure();
        breaker.RecordFailure();
        return breaker;
    }

    private static string NewSegmentId() => $"{Guid.NewGuid():N}@unproven.test";

    /// <summary>Throws when the segment reached the queue's fail-fast cache.</summary>
    private static void AssertNotSeededForFailFast(string segmentId) =>
        HealthCheckService.CheckCachedMissingSegmentIds([segmentId]);

    private static Task<Stream> NoFallback(long offset, CancellationToken cancellationToken) =>
        throw new InvalidOperationException($"Private fallback must not run at offset {offset}.");

    private static bool IsUnprovenWarning(LogEvent logEvent) =>
        logEvent.Level == LogEventLevel.Warning
        && logEvent.MessageTemplate.Text.Contains(UnprovenTemplate, StringComparison.Ordinal);

    private static bool IsProvenMissError(LogEvent logEvent) =>
        logEvent.MessageTemplate.Text.Contains(ProvenTemplate, StringComparison.Ordinal);

    private static async Task<IReadOnlyList<LogEvent>> CaptureLogsAsync(Func<Task> action)
    {
        var sink = new CollectingSink();
        var previous = Log.Logger;
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.Sink(sink)
            .CreateLogger();
        try
        {
            await action();
        }
        finally
        {
            Log.Logger = previous;
        }

        return sink.Events;
    }

    private sealed class CollectingSink : ILogEventSink
    {
        private readonly List<LogEvent> _events = [];

        public IReadOnlyList<LogEvent> Events
        {
            get
            {
                lock (_events) return _events.ToArray();
            }
        }

        public void Emit(LogEvent logEvent)
        {
            lock (_events) _events.Add(logEvent);
        }
    }

    /// <summary>
    /// Stands in for the MultiSegmentStream the shared pump normally owns: it runs a real
    /// provider walk on the pump's own flow and surfaces the miss that walk produced. The
    /// gate holds the pump parked until a reader has attached.
    /// </summary>
    private sealed class WalkThenSurfaceMissStream(
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
            await client.DecodedBodyAsync(segmentId, cancellationToken);
            throw new InvalidOperationException("The provider walk above must have thrown.");
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
