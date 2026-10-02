using Microsoft.AspNetCore.Http;
using NzbWebDAV.Middlewares;
using NzbWebDAV.Tests.TestUtils;
using Serilog;
using Serilog.Core;
using Serilog.Events;

namespace NzbWebDAV.Tests.Middlewares;

[Collection(nameof(GlobalLoggerCollection))]
public sealed class CircuitAdmissionFailureLogTests
{
    [Theory]
    [InlineData("/.ids/movie-id", "GET", true)]
    [InlineData("/content/movie.mkv", "GET", false)]
    [InlineData("/.ids/movie-id", "HEAD", false)]
    public async Task CircuitAdmissionRejection_OnlyLogsIdsGetFailure(
        string path,
        string method,
        bool shouldLog)
    {
        var events = await CaptureLogsAsync(async () =>
        {
            var context = new DefaultHttpContext();
            context.Request.Path = path;
            context.Request.Method = method;
            context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
            context.Items[ExceptionMiddleware.CircuitAdmissionRejectedKey] = true;

            var middleware = new WebDavObservabilityMiddleware(_ => Task.CompletedTask);
            await middleware.InvokeAsync(context);
        });

        var genericFailure = events.Where(log =>
            log.MessageTemplate.Text.StartsWith("WebDAV request failed.", StringComparison.Ordinal)).ToArray();
        if (shouldLog)
        {
            var log = Assert.Single(genericFailure);
            Assert.Contains("GET", log.RenderMessage(), StringComparison.Ordinal);
            Assert.Contains("/.ids/movie-id", log.RenderMessage(), StringComparison.Ordinal);
            Assert.Contains("503", log.RenderMessage(), StringComparison.Ordinal);
        }
        else
        {
            Assert.Empty(genericFailure);
        }
    }

    private static async Task<IReadOnlyList<LogEvent>> CaptureLogsAsync(Func<Task> action)
    {
        var sink = new CollectingSink();
        var previous = Log.Logger;
        using var logger = new LoggerConfiguration()
            .MinimumLevel.Verbose()
            .WriteTo.Sink(sink)
            .CreateLogger();
        Log.Logger = logger;
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
}
