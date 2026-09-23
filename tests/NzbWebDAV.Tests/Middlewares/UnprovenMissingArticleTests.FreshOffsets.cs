using Microsoft.AspNetCore.Http;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Database.Models;
using NzbWebDAV.Exceptions;
using NzbWebDAV.Middlewares;
using NzbWebDAV.Models;
using NzbWebDAV.Services;
using NzbWebDAV.Services.StreamTrace;
using NzbWebDAV.Streams;
using NzbWebDAV.Tests.Clients.Usenet;
using NzbWebDAV.Tests.Fakes;
using NzbWebDAV.WebDav.Base;

namespace NzbWebDAV.Tests.Middlewares;

/// <summary>
/// A read that asks for fresh provider evidence (Cache-Control no-cache or no-store, or
/// Pragma no-cache) is a probe. The segment its range starts in fails instead of being
/// gap-filled, at any offset and through every seek path, and that failure gets the same
/// coverage verdict as an offset-zero read. Playback reads keep filling the gap.
/// </summary>
public sealed partial class UnprovenMissingArticleTests
{
    private const int OffsetSegmentBytes = 8;

    public enum OffsetRead
    {
        Playback,
        FreshComplete,
        FreshProviderSkipped,
    }

    [Theory]
    [InlineData(0, OffsetRead.Playback)]
    [InlineData(0, OffsetRead.FreshComplete)]
    [InlineData(0, OffsetRead.FreshProviderSkipped)]
    [InlineData(2, OffsetRead.Playback)]
    [InlineData(2, OffsetRead.FreshComplete)]
    [InlineData(2, OffsetRead.FreshProviderSkipped)]
    public async Task ExactIndexedOffset_FailsFastOnlyForAFreshRead(int articleBufferSize, OffsetRead mode)
    {
        var file = new OffsetFile(targetIndex: 1);
        using var client = file.CreateClient(mode);

        var result = await ReadAtOffsetAsync(mode, OffsetSegmentBytes, OffsetSegmentBytes, () => new NzbFileStream(
            file.SegmentIds, file.Length, client, articleBufferSize,
            segmentByteRanges: file.Ranges, usePipelinedBodyRequests: false,
            fileName: file.Path, readBudgetOverride: OffsetSegmentBytes));

        AssertOffsetRead(mode, result);
    }

    /// <summary>
    /// Offset zero of an archive member starts inside its volume, after the RAR headers, so
    /// the part's first segment is read at a nonzero volume offset.
    /// </summary>
    [Theory]
    [InlineData(0, 0, OffsetRead.Playback)]
    [InlineData(0, 0, OffsetRead.FreshComplete)]
    [InlineData(0, 0, OffsetRead.FreshProviderSkipped)]
    [InlineData(4, 1, OffsetRead.Playback)]
    [InlineData(4, 1, OffsetRead.FreshComplete)]
    [InlineData(4, 1, OffsetRead.FreshProviderSkipped)]
    public async Task MultipartPartOffset_FailsFastOnlyForAFreshRead(long memberOffset, int targetIndex, OffsetRead mode)
    {
        var file = new OffsetFile(targetIndex);
        using var client = file.CreateClient(mode);

        var result = await ReadAtOffsetAsync(mode, memberOffset, 4, () => new DavMultipartFileStream(
            file.CreateMultipart(), client, articleBufferSize: 0, resolver: null,
            usePipelinedBodyRequests: false, fileName: file.Path));

        AssertOffsetRead(mode, result);
    }

    /// <summary>
    /// Without imported ranges a short range seeks by header probes. The probe finds the
    /// target, then the body read that positions on it misses. The buffered head knows the
    /// probed length, so playback can fill it; an unbuffered head of unknown length already
    /// fails upstream.
    /// </summary>
    [Theory]
    [InlineData(OffsetRead.Playback)]
    [InlineData(OffsetRead.FreshComplete)]
    [InlineData(OffsetRead.FreshProviderSkipped)]
    public async Task LegacyProbedOffset_FailsFastOnlyForAFreshRead(OffsetRead mode)
    {
        var file = new OffsetFile(targetIndex: 1);
        using var client = file.CreateClient(mode, targetServedOnBody: body => body == 1);

        var result = await ReadAtOffsetAsync(mode, OffsetSegmentBytes, OffsetSegmentBytes, () => new NzbFileStream(
            file.SegmentIds, file.Length, client, articleBufferSize: 2,
            usePipelinedBodyRequests: false, fileName: file.Path,
            readBudgetOverride: OffsetSegmentBytes));

        AssertOffsetRead(mode, result);
    }

    /// <summary>
    /// A read without a short budget first tries the buffered fast seek. When that body
    /// fetch fails it falls back to the probed seek, which must apply the same rule.
    /// </summary>
    [Theory]
    [InlineData(OffsetRead.Playback)]
    [InlineData(OffsetRead.FreshComplete)]
    [InlineData(OffsetRead.FreshProviderSkipped)]
    public async Task BufferedSeekOffset_FailsFastOnlyForAFreshRead(OffsetRead mode)
    {
        var file = new OffsetFile(targetIndex: 1);
        using var client = file.CreateClient(mode, targetServedOnBody: body => body == 2);

        var result = await ReadAtOffsetAsync(mode, OffsetSegmentBytes, OffsetSegmentBytes, () => new NzbFileStream(
            file.SegmentIds, file.Length, client, articleBufferSize: 2,
            usePipelinedBodyRequests: false, fileName: file.Path));

        AssertOffsetRead(mode, result);
    }

    [Theory]
    [InlineData(OffsetRead.Playback)]
    [InlineData(OffsetRead.FreshComplete)]
    [InlineData(OffsetRead.FreshProviderSkipped)]
    public async Task ProductionEndpoint_RangeAtAnOffsetFailsFastOnlyForAFreshProbe(OffsetRead mode)
    {
        var file = new OffsetFile(targetIndex: 1);
        using var client = file.CreateClient(mode);
        var davItem = file.CreateItem(file.Length);

        var probe = await RunRangeRequestAsync(
            davItem,
            () => new NzbFileStream(
                file.SegmentIds, file.Length, client, articleBufferSize: 0,
                segmentByteRanges: file.Ranges, usePipelinedBodyRequests: false, fileName: davItem.Path),
            $"bytes={OffsetSegmentBytes}-{2 * OffsetSegmentBytes - 1}",
            mode);

        AssertRangeProbe(mode, probe, $"bytes {OffsetSegmentBytes}-{2 * OffsetSegmentBytes - 1}/{file.Length}");
    }

    [Theory]
    [InlineData(OffsetRead.Playback)]
    [InlineData(OffsetRead.FreshComplete)]
    [InlineData(OffsetRead.FreshProviderSkipped)]
    public async Task ProductionEndpoint_MultipartMemberStartFailsFastOnlyForAFreshProbe(OffsetRead mode)
    {
        var file = new OffsetFile(targetIndex: 0);
        using var client = file.CreateClient(mode);
        var davItem = file.CreateItem(OffsetFile.MemberLength);

        var probe = await RunRangeRequestAsync(
            davItem,
            () => new DavMultipartFileStream(
                file.CreateMultipart(), client, articleBufferSize: 0, resolver: null,
                usePipelinedBodyRequests: false, fileName: davItem.Path),
            "bytes=0-3",
            mode);

        AssertRangeProbe(mode, probe, $"bytes 0-3/{OffsetFile.MemberLength}");
    }

    private static async Task<OffsetReadResult> ReadAtOffsetAsync(
        OffsetRead mode, long offset, int length, Func<Stream> open)
    {
        var content = new byte[length];
        Array.Fill(content, (byte)0x7f);
        var probe = await RunReadAsync(async () =>
        {
            await using var stream = open();
            stream.Seek(offset, SeekOrigin.Begin);
            await stream.ReadExactlyAsync(content);
        }, cacheControl: mode == OffsetRead.Playback ? null : "no-cache, no-store");
        return new OffsetReadResult(probe, content);
    }

    private static void AssertOffsetRead(OffsetRead mode, OffsetReadResult result)
    {
        switch (mode)
        {
            case OffsetRead.Playback:
                Assert.Equal(StatusCodes.Status200OK, result.Probe.StatusCode);
                Assert.Equal(new byte[result.Content.Length], result.Content);
                Assert.Equal(0, result.Probe.StreamingFailures);
                break;
            case OffsetRead.FreshComplete:
                Assert.Equal(StatusCodes.Status404NotFound, result.Probe.StatusCode);
                Assert.Equal(1, result.Probe.StreamingFailures);
                break;
            default:
                Assert.Equal(StatusCodes.Status503ServiceUnavailable, result.Probe.StatusCode);
                Assert.Equal("5", result.Probe.RetryAfter);
                Assert.Equal(0, result.Probe.StreamingFailures);
                break;
        }
    }

    private static void AssertRangeProbe(OffsetRead mode, RangeProbe probe, string contentRange)
    {
        switch (mode)
        {
            case OffsetRead.Playback:
                Assert.Equal(StatusCodes.Status206PartialContent, probe.StatusCode);
                Assert.Equal(contentRange, probe.ContentRange);
                Assert.Equal(new byte[probe.Body.Length], probe.Body);
                Assert.NotEmpty(probe.Body);
                Assert.Equal(0, probe.StreamingFailures);
                break;
            case OffsetRead.FreshComplete:
                Assert.Equal(StatusCodes.Status404NotFound, probe.StatusCode);
                Assert.Empty(probe.RetryAfter);
                Assert.Empty(probe.Body);
                Assert.Equal(1, probe.StreamingFailures);
                break;
            default:
                Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
                Assert.Equal("5", probe.RetryAfter);
                Assert.Empty(probe.Body);
                Assert.Equal(0, probe.StreamingFailures);
                break;
        }
    }

    /// <summary>
    /// The real WebDAV GET handler behind the real middleware, with the exact request
    /// headers the conductarr media-health probe sends for a fresh read.
    /// </summary>
    private static async Task<RangeProbe> RunRangeRequestAsync(
        DavItem davItem, Func<Stream> open, string range, OffsetRead mode)
    {
        var config = CreateProductionEndpointConfig();
        var request = new DefaultHttpContext();
        request.Request.Method = HttpMethods.Get;
        request.Request.Scheme = "http";
        request.Request.Host = new HostString("localhost");
        request.Request.Path = davItem.Path;
        request.Request.Headers.Range = range;
        if (mode != OffsetRead.Playback)
        {
            request.Request.Headers.CacheControl = "no-cache, no-store";
            request.Request.Headers.Pragma = "no-cache";
        }

        using var responseBody = new MemoryStream();
        request.Response.Body = responseBody;
        var failures = new StreamingFailureTracker();
        var concurrentReads = new ConcurrentReadTracker();
        using var sharedStreams = new SharedStreamRegistry(config, concurrentReads);
        var handler = new GetAndHeadHandlerPatch(
            new SingleItemStore(new OpenedStreamStoreItem(request, davItem, open)),
            config,
            new ProviderUsageTracker(),
            new ActiveReadRegistry(),
            concurrentReads,
            new StreamTraceBuffer(100, enabled: false),
            failures,
            sharedStreams);
        var middleware = new ExceptionMiddleware(
            async context => { await handler.HandleRequestAsync(context); },
            config,
            failures);

        await middleware.InvokeAsync(request);

        return new RangeProbe(
            request.Response.StatusCode,
            request.Response.Headers.RetryAfter.ToString(),
            request.Response.Headers.ContentRange.ToString(),
            responseBody.ToArray(),
            failures.GetFailureCount(davItem.Id));
    }

    private sealed record OffsetReadResult(ReadProbe Probe, byte[] Content);

    private sealed record RangeProbe(
        int StatusCode,
        string RetryAfter,
        string ContentRange,
        byte[] Body,
        int StreamingFailures);

    /// <summary>
    /// Three 8-byte segments of a 24-byte file with exact imported ranges. As an archive
    /// member the file starts 4 bytes into the volume, where the RAR headers would sit.
    /// </summary>
    private sealed class OffsetFile(int targetIndex)
    {
        private const int MemberStart = 4;
        public const long MemberLength = 3 * OffsetSegmentBytes - MemberStart;
        private int _targetBodies;

        public string[] SegmentIds { get; } = [NewSegmentId(), NewSegmentId(), NewSegmentId()];
        public long Length => SegmentIds.Length * OffsetSegmentBytes;
        public string Path { get; } = $"/content/{Guid.NewGuid():N}.mkv";

        public LongRange[] Ranges =>
            SegmentIds.Select((_, index) => LongRange.FromStartAndSize(index * OffsetSegmentBytes, OffsetSegmentBytes))
                .ToArray();

        private string Target => SegmentIds[targetIndex];

        /// <summary>
        /// Two reachable providers that lack the target, or a first provider that serves
        /// only the scripted bodies of it. In the skipped mode an open-circuit provider that
        /// holds every article comes first and is never asked.
        /// </summary>
        public MultiProviderNntpClient CreateClient(OffsetRead mode, Func<int, bool>? targetServedOnBody = null)
        {
            var providers = new List<MultiConnectionNntpClient>();
            if (mode == OffsetRead.FreshProviderSkipped)
            {
                providers.Add(MultiProviderNntpClientTests.CreateProvider(
                    Serving(includeTarget: true), host: "open.example", circuitBreaker: OpenBreaker("open.example")));
            }

            providers.Add(MultiProviderNntpClientTests.CreateProvider(
                Serving(includeTarget: targetServedOnBody is not null, targetServedOnBody), host: "first.example"));
            providers.Add(MultiProviderNntpClientTests.CreateProvider(
                Serving(includeTarget: false), host: "second.example"));
            return new MultiProviderNntpClient(providers);
        }

        public DavMultipartFile CreateMultipart() => new()
        {
            Id = Guid.NewGuid(),
            Metadata = new DavMultipartFile.Meta
            {
                ExpectedFileSize = MemberLength,
                FileParts =
                [
                    new DavMultipartFile.FilePart
                    {
                        SegmentIds = SegmentIds,
                        SegmentIdByteRange = new LongRange(0, Length),
                        FilePartByteRange = new LongRange(MemberStart, Length),
                        SegmentByteRanges = Ranges,
                        SegmentByteRangesTrusted = true,
                    },
                ],
            },
        };

        public DavItem CreateItem(long fileSize) => DavItem.New(
            Guid.NewGuid(),
            DavItem.ContentFolder,
            $"{Guid.NewGuid():N}.mkv",
            fileSize,
            DavItem.ItemType.UsenetFile,
            DavItem.ItemSubType.MultipartFile,
            null,
            null,
            null,
            null);

        private FakeNntpClient Serving(bool includeTarget, Func<int, bool>? targetServedOnBody = null)
        {
            var ranges = Ranges;
            var segments = SegmentIds
                .Select((id, index) => (Id: id, Bytes: Enumerable.Repeat((byte)(0x10 + index), OffsetSegmentBytes).ToArray()))
                .Where(segment => includeTarget || segment.Id != Target)
                .ToDictionary(segment => segment.Id, segment => segment.Bytes, StringComparer.Ordinal);
            var rangeById = SegmentIds
                .Select((id, index) => (Id: id, Range: ranges[index]))
                .ToDictionary(segment => segment.Id, segment => segment.Range, StringComparer.Ordinal);
            Func<string, byte[], Stream>? bodies = null;
            if (targetServedOnBody is { } served)
            {
                bodies = (id, bytes) => id != Target || served(Interlocked.Increment(ref _targetBodies))
                    ? new MemoryStream(bytes, writable: false)
                    : throw new UsenetArticleNotFoundException(id, "430 No such article");
            }

            return new FakeNntpClient(
                segments,
                useCachedYencStreams: true,
                segmentRanges: rangeById,
                decodedStreamFactory: bodies);
        }
    }

    private sealed class OpenedStreamStoreItem(HttpContext context, DavItem davItem, Func<Stream> open)
        : BaseStoreReadonlyItem
    {
        public override string Name => davItem.Name;
        public override string UniqueKey => davItem.Id.ToString();
        public override long FileSize => davItem.FileSize!.Value;
        public override DateTime CreatedAt => davItem.CreatedAt;

        public override Task<Stream> GetReadableStreamAsync(CancellationToken cancellationToken)
        {
            context.Items["DavItem"] = davItem;
            return Task.FromResult(open());
        }
    }
}
