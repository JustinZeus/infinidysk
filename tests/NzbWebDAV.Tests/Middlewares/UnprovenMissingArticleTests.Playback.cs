using System.Collections.Concurrent;
using Microsoft.AspNetCore.Http;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Config;
using NzbWebDAV.Database.Models;
using NzbWebDAV.Exceptions;
using NzbWebDAV.Models;
using NzbWebDAV.Services.Repair;
using NzbWebDAV.Streams;
using NzbWebDAV.Tests.Clients.Usenet;

namespace NzbWebDAV.Tests.Middlewares;

public sealed partial class UnprovenMissingArticleTests
{
    private const int PlaybackSegmentSize = 8;

    [Theory]
    [InlineData(0, false)]
    [InlineData(2, false)]
    [InlineData(2, true)]
    public async Task ProvenMiss_ZeroFillsAndReportsUpstream(int articleBufferSize, bool pipelined)
    {
        using var reports = await RepairReportCapture.CreateAsync();
        var segmentId = NewSegmentId();
        var path = $"/content/{Guid.NewGuid():N}.mkv";
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(MissingProviderClient(), host: "first.example"),
            MultiProviderNntpClientTests.CreateProvider(MissingProviderClient(), host: "second.example"));

        var result = await ReadPlaybackAsync(
            client, [segmentId], articleBufferSize, pipelined, path, failFastOnFirstSegment: false);

        Assert.Equal(StatusCodes.Status200OK, result.Probe.StatusCode);
        Assert.Equal(new byte[PlaybackSegmentSize], result.Content);
        Assert.True(PlaybackHoleTracker.IsKnownMissingSegment(path, segmentId));
        Assert.Equal(1, ReportsFor(reports, path, segmentId).Count(report => !report.IsCorruption));
    }

    [Theory]
    [InlineData(0, false)]
    [InlineData(2, false)]
    [InlineData(2, true)]
    public async Task KnownMissingIndex_ZeroFillsWithoutWalkingAndReportsUpstream(
        int articleBufferSize, bool pipelined)
    {
        using var reports = await RepairReportCapture.CreateAsync();
        var segmentId = NewSegmentId();
        var path = $"/content/{Guid.NewGuid():N}.mkv";
        var first = MissingProviderClient();
        var second = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(first, host: "first.example"),
            MultiProviderNntpClientTests.CreateProvider(second, host: "second.example"));

        var result = await ReadPlaybackAsync(
            client,
            [segmentId],
            articleBufferSize,
            pipelined,
            path,
            failFastOnFirstSegment: false,
            knownMissingSegmentIndices: new HashSet<int> { 0 });

        Assert.Equal(StatusCodes.Status200OK, result.Probe.StatusCode);
        Assert.Equal(new byte[PlaybackSegmentSize], result.Content);
        Assert.Equal(0, first.SingularRequests + first.BatchRequests + second.SingularRequests + second.BatchRequests);
        Assert.True(PlaybackHoleTracker.IsKnownMissingSegment(path, segmentId));
        Assert.Equal(1, ReportsFor(reports, path, segmentId).Count(report => !report.IsCorruption));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public async Task CachedMiss_StillZeroFillsWithoutRepeatingUnprovenSideEffects(int articleBufferSize)
    {
        using var reports = await RepairReportCapture.CreateAsync();
        var segmentId = NewSegmentId();
        var firstPath = $"/content/{Guid.NewGuid():N}.mkv";
        var secondPath = $"/content/{Guid.NewGuid():N}.mkv";
        using var cache = new ArticleMissNegativeCache(new ConfigManager());
        var first = MissingProviderClient();
        var second = MissingProviderClient();
        using var client = new MultiProviderNntpClient(
            [
                MultiProviderNntpClientTests.CreateProvider(first, host: "first.example"),
                MultiProviderNntpClientTests.CreateProvider(second, host: "second.example"),
            ],
            articleMissCache: cache);

        var initial = await ReadPlaybackAsync(
            client, [segmentId], articleBufferSize, pipelined: false, firstPath,
            failFastOnFirstSegment: false);
        var requestsAfterInitial = (
            first.SingularRequests + first.BatchRequests,
            second.SingularRequests + second.BatchRequests);

        var cached = await ReadPlaybackAsync(
            client, [segmentId], articleBufferSize, pipelined: false, secondPath,
            failFastOnFirstSegment: false);

        Assert.Equal(StatusCodes.Status200OK, initial.Probe.StatusCode);
        Assert.Equal(new byte[PlaybackSegmentSize], initial.Content);
        Assert.True(requestsAfterInitial.Item1 > 0);
        Assert.True(requestsAfterInitial.Item2 > 0);
        Assert.Equal(StatusCodes.Status200OK, cached.Probe.StatusCode);
        Assert.Equal(new byte[PlaybackSegmentSize], cached.Content);
        Assert.Equal(requestsAfterInitial, (
            first.SingularRequests + first.BatchRequests,
            second.SingularRequests + second.BatchRequests));
        Assert.True(PlaybackHoleTracker.IsKnownMissingSegment(firstPath, segmentId));
        Assert.False(PlaybackHoleTracker.IsKnownMissingSegment(secondPath, segmentId));
        Assert.Equal(1, ReportsFor(reports, firstPath, segmentId).Count(report => !report.IsCorruption));
        Assert.Empty(ReportsFor(reports, secondPath, segmentId));
    }

    [Fact]
    public async Task CompleteRewalk_ReplacesAnEarlierIncompleteVerdictForTheSameArticle()
    {
        using var reports = await RepairReportCapture.CreateAsync();
        var segmentId = NewSegmentId();
        var path = $"/content/{Guid.NewGuid():N}.mkv";
        var completeWalk = false;
        var first = new MultiProviderNntpClientTests.ScriptedNntpClient
        {
            BatchResponseCode = 430,
            SingularException = id => completeWalk
                ? new UsenetArticleNotFoundException(id)
                : new IOException("transport interrupted"),
        };
        var second = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(first, host: "first.example"),
            MultiProviderNntpClientTests.CreateProvider(second, host: "second.example"));
        byte[] firstReadContent = [];
        var requestsBeforeRewalk = (First: 0, Second: 0);

        var probe = await RunReadAsync(async () =>
        {
            await using (var incomplete = CreatePlaybackStream(
                client, [segmentId], articleBufferSize: 0, pipelined: false,
                path, failFastOnFirstSegment: false))
            {
                using var output = new MemoryStream();
                await incomplete.CopyToAsync(output);
                firstReadContent = output.ToArray();
            }

            requestsBeforeRewalk = (first.SingularRequests, second.SingularRequests);
            completeWalk = true;
            await using var complete = CreatePlaybackStream(
                client, [segmentId], articleBufferSize: 0, pipelined: false,
                path, failFastOnFirstSegment: true);
            await complete.CopyToAsync(Stream.Null);
        }, cacheControl: "no-cache, no-store");

        Assert.Equal(StatusCodes.Status404NotFound, probe.StatusCode);
        Assert.Equal(new byte[PlaybackSegmentSize], firstReadContent);
        Assert.True(requestsBeforeRewalk.First > 0 && requestsBeforeRewalk.Second > 0);
        Assert.True(first.SingularRequests > requestsBeforeRewalk.First);
        Assert.True(second.SingularRequests > requestsBeforeRewalk.Second);
        Assert.False(PlaybackHoleTracker.IsKnownMissingSegment(path, segmentId));
        Assert.Empty(ReportsFor(reports, path, segmentId));
    }

    [Fact]
    public async Task ProvenMissForOneArticle_DoesNotProveAnUnwalkedMissForAnother()
    {
        using var reports = await RepairReportCapture.CreateAsync();
        var provenSegmentId = NewSegmentId();
        var unwalkedSegmentId = NewSegmentId();
        var path = $"/content/{Guid.NewGuid():N}.mkv";
        var first = MissingProviderClient();
        var second = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(first, host: "first.example"),
            MultiProviderNntpClientTests.CreateProvider(second, host: "second.example"));
        byte[] content = [];

        var probe = await RunReadAsync(async () =>
        {
            await using (var stream = CreatePlaybackStream(
                client, [provenSegmentId], articleBufferSize: 0, pipelined: false,
                path, failFastOnFirstSegment: false))
            {
                using var output = new MemoryStream();
                await stream.CopyToAsync(output);
                content = output.ToArray();
            }

            throw new UsenetArticleNotFoundException(unwalkedSegmentId);
        });

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal(new byte[PlaybackSegmentSize], content);
        Assert.Equal(1, first.SingularRequests);
        Assert.Equal(1, second.SingularRequests);
        Assert.True(PlaybackHoleTracker.IsKnownMissingSegment(path, provenSegmentId));
        Assert.False(PlaybackHoleTracker.IsKnownMissingSegment(path, unwalkedSegmentId));
        Assert.Equal(1, ReportsFor(reports, path, provenSegmentId).Count(report => !report.IsCorruption));
        Assert.Empty(ReportsFor(reports, path, unwalkedSegmentId));
        AssertNotSeededForFailFast(unwalkedSegmentId);
        Assert.Equal(0, probe.StreamingFailures);
    }

    private static async Task<PlaybackReadResult> ReadPlaybackAsync(
        MultiProviderNntpClient client,
        string[] segmentIds,
        int articleBufferSize,
        bool pipelined,
        string path,
        bool failFastOnFirstSegment,
        IReadOnlySet<int>? knownMissingSegmentIndices = null)
    {
        byte[] content = [];
        var probe = await RunReadAsync(async () =>
        {
            await using var stream = CreatePlaybackStream(
                client,
                segmentIds,
                articleBufferSize,
                pipelined,
                path,
                failFastOnFirstSegment,
                knownMissingSegmentIndices);
            using var output = new MemoryStream();
            await stream.CopyToAsync(output);
            content = output.ToArray();
        });
        return new PlaybackReadResult(probe, content);
    }

    private static Stream CreatePlaybackStream(
        MultiProviderNntpClient client,
        string[] segmentIds,
        int articleBufferSize,
        bool pipelined,
        string path,
        bool failFastOnFirstSegment,
        IReadOnlySet<int>? knownMissingSegmentIndices = null) =>
        MultiSegmentStream.Create(
            segmentIds.AsMemory(),
            client,
            articleBufferSize,
            estimatedSegmentSize: PlaybackSegmentSize,
            failFastOnFirstSegment: failFastOnFirstSegment,
            usePipelinedBodyRequests: pipelined,
            CancellationToken.None,
            fileName: path,
            exactSegmentSizes: Enumerable.Repeat((long)PlaybackSegmentSize, segmentIds.Length).ToArray(),
            knownMissingSegmentIndices: knownMissingSegmentIndices);

    private static IEnumerable<(string Path, string SegmentId, bool IsCorruption)> ReportsFor(
        RepairReportCapture reports, string path, string segmentId) =>
        reports.Reports.Where(report => report.Path == path && report.SegmentId == segmentId);

    private sealed record PlaybackReadResult(ReadProbe Probe, byte[] Content);

    private sealed class RepairReportCapture : IDisposable
    {
        private readonly string _directory;
        private readonly Par2RepairService _service;
        private readonly Par2RepairTriggerSink? _previousSink;
        private readonly ConcurrentBag<(string Path, string SegmentId, bool IsCorruption)>? _previousReports;

        private RepairReportCapture(
            string directory,
            Par2RepairService service,
            Par2RepairTriggerSink? previousSink,
            ConcurrentBag<(string Path, string SegmentId, bool IsCorruption)>? previousReports,
            ConcurrentBag<(string Path, string SegmentId, bool IsCorruption)> reports)
        {
            _directory = directory;
            _service = service;
            _previousSink = previousSink;
            _previousReports = previousReports;
            Reports = reports;
        }

        public ConcurrentBag<(string Path, string SegmentId, bool IsCorruption)> Reports { get; }

        public static async Task<RepairReportCapture> CreateAsync()
        {
            var previousSink = Par2RepairTriggerSink.Current;
            var previousReports = Par2RepairTriggerSink.TestReports;
            var reports = new ConcurrentBag<(string Path, string SegmentId, bool IsCorruption)>();
            var directory = Path.Join(
                Path.GetTempPath(), "infinidysk-unproven-playback-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);

            var config = new ConfigManager();
            config.UpdateValues([
                new ConfigItem { ConfigName = ConfigKeys.RepairEnable, ConfigValue = "true" },
            ]);
            var store = new RepairPatchStore(directory, 1024 * 1024);
            await store.EnsureCatalogLoadedAsync(CancellationToken.None);
            var service = new Par2RepairService(config, null!, store);
            Par2RepairTriggerSink.Current = new Par2RepairTriggerSink(service);
            Par2RepairTriggerSink.TestReports = reports;
            return new RepairReportCapture(directory, service, previousSink, previousReports, reports);
        }

        public void Dispose()
        {
            Par2RepairTriggerSink.Current = _previousSink;
            Par2RepairTriggerSink.TestReports = _previousReports;
            _service.Dispose();
            if (Directory.Exists(_directory))
                Directory.Delete(_directory, recursive: true);
        }
    }
}
