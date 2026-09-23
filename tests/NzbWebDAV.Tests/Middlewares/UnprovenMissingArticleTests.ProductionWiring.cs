using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.Extensions.DependencyInjection;
using NWebDav.Server.Stores;
using NzbWebDAV.Api.Controllers.GetWebdavItem;
using NzbWebDAV.Clients.Usenet;
using NzbWebDAV.Config;
using NzbWebDAV.Database;
using NzbWebDAV.Database.Interceptors;
using NzbWebDAV.Database.MigrationHelpers;
using NzbWebDAV.Database.Models;
using NzbWebDAV.Middlewares;
using NzbWebDAV.Models;
using NzbWebDAV.Services;
using NzbWebDAV.Services.StreamTrace;
using NzbWebDAV.Streams;
using NzbWebDAV.Tests.Clients.Usenet;
using NzbWebDAV.WebDav;
using NzbWebDAV.WebDav.Base;

namespace NzbWebDAV.Tests.Middlewares;

public sealed partial class UnprovenMissingArticleTests
{
    private const long ProductionEndpointSegmentBytes = 8;
    private const string ViewApiKey = "production-wiring-test-key";

    [Theory]
    [InlineData("webdav")]
    [InlineData("view")]
    public async Task ProductionEndpoint_OpenCircuitOmissionReturnsRetryableWithoutRepair(string endpoint)
    {
        using var reports = await RepairReportCapture.CreateAsync();
        var segmentId = NewSegmentId();
        var davItem = NewProductionEndpointItem();
        var skipped = MissingProviderClient();
        var reached = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(
                skipped, host: "open.example", circuitBreaker: OpenBreaker("open.example")),
            MultiProviderNntpClientTests.CreateProvider(reached, host: "reachable.example"));

        var probe = await RunProductionEndpointAsync(endpoint, client, davItem, segmentId);

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, probe.StatusCode);
        Assert.Equal("5", probe.RetryAfter);
        Assert.Equal(0, probe.StreamingFailures);
        Assert.Equal(0, skipped.BatchRequests + skipped.SingularRequests);
        Assert.True(reached.BatchRequests + reached.SingularRequests > 0);
        AssertNotSeededForFailFast(segmentId);
        Assert.False(PlaybackHoleTracker.IsKnownMissingSegment(davItem.Path, segmentId));
        Assert.False(PlaybackHoleTracker.ShouldFailFast(davItem.Path, out _));
        Assert.Empty(ReportsFor(reports, davItem.Path, segmentId));
    }

    [Theory]
    [InlineData("webdav")]
    [InlineData("view")]
    public async Task ProductionEndpoint_CompleteAllProviderMissRemainsNotFound(string endpoint)
    {
        var segmentId = NewSegmentId();
        var davItem = NewProductionEndpointItem();
        var first = MissingProviderClient();
        var second = MissingProviderClient();
        using var client = TwoProviders(
            MultiProviderNntpClientTests.CreateProvider(first, host: "first.example"),
            MultiProviderNntpClientTests.CreateProvider(second, host: "second.example"));

        var probe = await RunProductionEndpointAsync(endpoint, client, davItem, segmentId);

        Assert.Equal(StatusCodes.Status404NotFound, probe.StatusCode);
        Assert.True(first.BatchRequests + first.SingularRequests > 0);
        Assert.True(second.BatchRequests + second.SingularRequests > 0);
    }

    private static async Task<EndpointProbe> RunProductionEndpointAsync(
        string endpoint,
        MultiProviderNntpClient client,
        DavItem davItem,
        string segmentId) => endpoint switch
    {
        "webdav" => await RunGetAndHeadHandlerAsync(client, davItem, segmentId),
        "view" => await RunViewControllerAsync(client, davItem, segmentId),
        _ => throw new ArgumentOutOfRangeException(nameof(endpoint), endpoint, "Unknown production endpoint."),
    };

    private static async Task<EndpointProbe> RunGetAndHeadHandlerAsync(
        MultiProviderNntpClient client,
        DavItem davItem,
        string segmentId)
    {
        var config = CreateProductionEndpointConfig();
        var request = NewProductionEndpointContext(davItem, isView: false);
        using var responseBody = new MemoryStream();
        request.Response.Body = responseBody;
        var failures = new StreamingFailureTracker();
        var concurrentReads = new ConcurrentReadTracker();
        using var sharedStreams = new SharedStreamRegistry(config, concurrentReads);
        var handler = new GetAndHeadHandlerPatch(
            new SingleItemStore(new ProviderMissStoreItem(request, davItem, client, segmentId)),
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

        return new EndpointProbe(
            request.Response.StatusCode,
            request.Response.Headers["Retry-After"].ToString(),
            failures.GetFailureCount(davItem.Id));
    }

    private static async Task<EndpointProbe> RunViewControllerAsync(
        MultiProviderNntpClient client,
        DavItem davItem,
        string segmentId)
    {
        var previousApiKey = Environment.GetEnvironmentVariable("FRONTEND_BACKEND_API_KEY");
        Environment.SetEnvironmentVariable("FRONTEND_BACKEND_API_KEY", ViewApiKey);
        try
        {
            await using var connection = new SqliteConnection("Data Source=:memory:");
            await connection.OpenAsync();
            var options = new DbContextOptionsBuilder<DavDatabaseContext>()
                .UseSqlite(connection)
                .AddInterceptors(new SqliteForeignKeyEnabler())
                .ReplaceService<
                    IMigrationsSqlGenerator,
                    SqliteMigrationsSqlGenerator<SqliteMigrationsSqlGenerator>>()
                .Options;
            await using var database = new DavDatabaseContext(options);
            await database.Database.MigrateAsync();
            database.Items.Add(davItem);
            database.MultipartFiles.Add(new DavMultipartFile
            {
                Id = davItem.Id,
                Metadata = new DavMultipartFile.Meta
                {
                    ExpectedFileSize = ProductionEndpointSegmentBytes,
                    FileParts =
                    [
                        new DavMultipartFile.FilePart
                        {
                            SegmentIds = [segmentId],
                            SegmentIdByteRange = new LongRange(0, ProductionEndpointSegmentBytes),
                            FilePartByteRange = new LongRange(0, ProductionEndpointSegmentBytes),
                            SegmentByteRanges = [new LongRange(0, ProductionEndpointSegmentBytes)],
                            SegmentByteRangesTrusted = true,
                        },
                    ],
                },
            });
            await database.SaveChangesAsync();

            var config = CreateProductionEndpointConfig();
            var request = NewProductionEndpointContext(davItem, isView: true);
            using var responseBody = new MemoryStream();
            request.Response.Body = responseBody;
            using var requestServices = new ServiceCollection().BuildServiceProvider();
            request.RequestServices = requestServices;
            var concurrentReads = new ConcurrentReadTracker();
            using var sharedStreams = new SharedStreamRegistry(config, concurrentReads);
            var failures = new StreamingFailureTracker();
            var store = new DatabaseStore(
                new HttpContextAccessor { HttpContext = request },
                new DavDatabaseClient(database),
                config,
                new UsenetStreamingClient(client),
                null!,
                null!,
                null!,
                null!);
            var controller = new GetWebdavItemController(
                store,
                config,
                new ProviderUsageTracker(),
                new ActiveReadRegistry(),
                concurrentReads,
                new CandidateNegativeCache(config),
                new StreamTraceBuffer(100, enabled: false),
                sharedStreams);
            controller.ControllerContext = new ControllerContext { HttpContext = request };
            var middleware = new ExceptionMiddleware(
                _ => controller.HandleRequest(),
                config,
                failures);

            await middleware.InvokeAsync(request);

            return new EndpointProbe(
                request.Response.StatusCode,
                request.Response.Headers["Retry-After"].ToString(),
                failures.GetFailureCount(davItem.Id));
        }
        finally
        {
            Environment.SetEnvironmentVariable("FRONTEND_BACKEND_API_KEY", previousApiKey);
        }
    }

    private static DefaultHttpContext NewProductionEndpointContext(DavItem davItem, bool isView)
    {
        var request = new DefaultHttpContext();
        request.Request.Method = HttpMethods.Get;
        request.Request.Scheme = "http";
        request.Request.Host = new HostString("localhost");
        request.Request.Path = isView ? $"/view{davItem.Path}" : davItem.Path;
        request.Request.Headers.Range = $"bytes=0-{ProductionEndpointSegmentBytes - 1}";
        request.Request.Headers.CacheControl = "no-cache, no-store";
        request.Request.Headers.Pragma = "no-cache";
        if (isView)
        {
            var itemPath = davItem.Path.TrimStart('/');
            var downloadKey = GetWebdavItemRequest.GenerateDownloadKey(ViewApiKey, itemPath);
            request.Request.QueryString = new QueryString(
                $"?downloadKey={Uri.EscapeDataString(downloadKey)}");
        }

        return request;
    }

    private static ConfigManager CreateProductionEndpointConfig()
    {
        var config = ExceptionMiddlewareTests.CreateRepairEnabledConfig();
        config.UpdateValues(
        [
            new ConfigItem
            {
                ConfigName = ConfigKeys.UsenetSharedStreamsEnabled,
                ConfigValue = "false",
            },
        ]);
        return config;
    }

    private static DavItem NewProductionEndpointItem() => DavItem.New(
        Guid.NewGuid(),
        DavItem.ContentFolder,
        "wiring-test.mkv",
        ProductionEndpointSegmentBytes,
        DavItem.ItemType.UsenetFile,
        DavItem.ItemSubType.MultipartFile,
        null,
        null,
        null,
        null);

    private sealed record EndpointProbe(int StatusCode, string RetryAfter, int StreamingFailures);

    private sealed class ProviderMissStoreItem(
        HttpContext context,
        DavItem davItem,
        MultiProviderNntpClient client,
        string segmentId) : BaseStoreReadonlyItem
    {
        public override string Name => davItem.Name;
        public override string UniqueKey => davItem.Id.ToString();
        public override long FileSize => ProductionEndpointSegmentBytes;
        public override DateTime CreatedAt => davItem.CreatedAt;

        public override Task<Stream> GetReadableStreamAsync(CancellationToken cancellationToken)
        {
            context.Items["DavItem"] = davItem;
            return Task.FromResult<Stream>(MultiSegmentStream.Create(
                new[] { segmentId }.AsMemory(),
                client,
                articleBufferSize: 0,
                estimatedSegmentSize: ProductionEndpointSegmentBytes,
                failFastOnFirstSegment: true,
                usePipelinedBodyRequests: false,
                cancellationToken,
                fileName: davItem.Path,
                exactSegmentSizes: new long[] { ProductionEndpointSegmentBytes }));
        }
    }

    private sealed class SingleItemStore(IStoreItem item) : IStore
    {
        public Task<IStoreItem?> GetItemAsync(string path, CancellationToken cancellationToken) =>
            Task.FromResult<IStoreItem?>(item);

        public Task<IStoreItem?> GetItemAsync(Uri uri, CancellationToken cancellationToken) =>
            Task.FromResult<IStoreItem?>(item);

        public Task<IStoreCollection?> GetCollectionAsync(Uri uri, CancellationToken cancellationToken) =>
            Task.FromResult<IStoreCollection?>(null);
    }
}
