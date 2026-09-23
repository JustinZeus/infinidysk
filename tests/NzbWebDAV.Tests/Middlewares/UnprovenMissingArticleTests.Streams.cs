using Microsoft.AspNetCore.Http;
using System.Collections.Concurrent;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Config;
using NzbWebDAV.Database.Models;
using NzbWebDAV.Exceptions;
using NzbWebDAV.Services;
using NzbWebDAV.Streams;
using NzbWebDAV.Tests.Clients.Usenet;
using NzbWebDAV.WebDav.Base;

namespace NzbWebDAV.Tests.Middlewares;

public sealed partial class UnprovenMissingArticleTests
{
    [Theory]
    [InlineData(0, false)]
    [InlineData(2, false)]
    [InlineData(2, true)]
    public async Task IncompleteMissWithFallbackIds_NeverGapFillsOrRepairs(int bufferSize, bool pipelined)
    {
        var segmentId = NewSegmentId();
        var fallbackId = NewSegmentId();
        var requested = new ConcurrentBag<string>();
        var answering = new MultiProviderNntpClientTests.ScriptedNntpClient
        {
            BatchResponseCode = 430,
            SingularException = id =>
            {
                requested.Add(id);
                return new UsenetArticleNotFoundException(id);
            },
        };
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                MissingProviderClient(), host: "open.example", circuitBreaker: OpenBreaker("open.example")),
            MultiProviderNntpClientTests.CreateProvider(answering, host: "healthy.example"));

        var probe = await RunReadAsync(async () =>
        {
            await using var stream = MultiSegmentStream.Create(
                new[] { segmentId }.AsMemory(), client, bufferSize, 8,
                failFastOnFirstSegment: false, usePipelinedBodyRequests: pipelined,
                CancellationToken.None, fileName: segmentId,
                segmentFallbacks: [[fallbackId]], exactSegmentSizes: new long[] { 8 });
            await stream.ReadAsync(new byte[8], CancellationToken.None);
        });

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal(0, probe.StreamingFailures);
        Assert.Contains(fallbackId, requested);
        AssertNotSeededForFailFast(segmentId);
        AssertNotSeededForFailFast(fallbackId);
    }

    [Fact]
    public async Task SharedProducerStartedBeforePump_PreservesEvidenceForBothReaders()
    {
        var segmentId = NewSegmentId();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                MissingProviderClient(), host: "open.example", circuitBreaker: OpenBreaker("open.example")),
            MultiProviderNntpClientTests.CreateProvider(MissingProviderClient(), host: "healthy.example"));
        var config = new ConfigManager();
        config.UpdateValues([
            new ConfigItem { ConfigName = ConfigKeys.UsenetSharedStreamsEnabled, ConfigValue = "true" },
            new ConfigItem { ConfigName = ConfigKeys.UsenetSharedStreamsRingMb, ConfigValue = "4" },
        ]);
        await using var registry = new SharedStreamRegistry(config, new ConcurrentReadTracker());
        var source = new PrefetchedMissSource(gate, client, segmentId);
        var first = await registry.TryAttachAsync(
            "/prefetched.mkv", 0, null, source.FileSize, source, NoFallback, CancellationToken.None);
        var second = await registry.TryAttachAsync(
            "/prefetched.mkv", 0, null, source.FileSize, source, NoFallback, CancellationToken.None);
        Assert.NotNull(first);
        Assert.NotNull(second);
        await using var firstStream = first.Stream;
        await using var secondStream = second.Stream;

        var firstProbe = await RunReadAsync(async () =>
        {
            gate.SetResult();
            await firstStream.ReadAsync(new byte[8], CancellationToken.None);
        });
        var secondProbe = await RunReadAsync(async () =>
            await secondStream.ReadAsync(new byte[8], CancellationToken.None));

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, firstProbe.StatusCode);
        Assert.Equal(StatusCodes.Status503ServiceUnavailable, secondProbe.StatusCode);
        Assert.Equal(0, firstProbe.StreamingFailures);
        Assert.Equal(0, secondProbe.StreamingFailures);
        AssertNotSeededForFailFast(segmentId);
    }

    private sealed class PrefetchedMissSource(
        TaskCompletionSource gate, MultiProviderNntpClient client, string segmentId) : IDetachedStreamSource
    {
        public long FileSize => 8;
        public SharedContentIdentity ContentIdentity => new(segmentId, null, FileSize);
        public Task<DetachedStreamLease> GetDetachedReadableStreamAsync(CancellationToken cancellationToken) =>
            Task.FromResult(new DetachedStreamLease
            {
                Stream = new PrefetchedMissStream(gate, client, segmentId),
                Ownership = NullAsyncDisposable.Instance,
                ContentIdentity = ContentIdentity,
            });
    }

    private sealed class PrefetchedMissStream : Stream
    {
        private readonly Task _prefetch;

        public PrefetchedMissStream(TaskCompletionSource gate, MultiProviderNntpClient client, string segmentId)
        {
            _prefetch = FetchAsync();
            async Task FetchAsync()
            {
                await gate.Task;
                await client.DecodedBodyAsync(segmentId, CancellationToken.None);
            }
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => 8;
        public override long Position { get => 0; set => throw new NotSupportedException(); }
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await _prefetch.WaitAsync(cancellationToken);
            return 0;
        }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override void Flush() => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
