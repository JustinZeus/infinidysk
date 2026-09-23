using System.Runtime.ExceptionServices;
using Microsoft.AspNetCore.Http;
using Microsoft.Net.Http.Headers;
using NzbWebDAV.Exceptions;
using NzbWebDAV.Extensions;

namespace NzbWebDAV.Clients.Usenet.Contexts;

/// <summary>Coverage of the enabled providers before an article walk starts.</summary>
internal readonly record struct ProviderSelectionCoverage(
    bool OmittedForByteLimit,
    bool OmittedForOpenCircuit,
    bool OmittedForOtherReason = false)
{
    public bool IsComplete => !OmittedForByteLimit && !OmittedForOpenCircuit && !OmittedForOtherReason;
}

/// <summary>Conservative miss evidence owned by one request or shared upstream stream.</summary>
internal sealed class ProviderReadEvidence
{
    private const string ItemKey = "ProviderReadEvidence";
    private const string ExceptionDataKey = "ProviderReadEvidence.IncompleteReason";
    private static readonly AsyncLocal<ProviderReadEvidence?> CurrentLocal = new();
    private int _incomplete;
    private int _recorded;
    private int _byteLimitOmitted;
    private int _openCircuitOmitted;
    private bool _requiresFreshWalk;

    internal static ProviderReadEvidence? Current => CurrentLocal.Value;
    internal static bool RequiresFreshWalk => Current?._requiresFreshWalk == true;

    public static Scope BeginRequest(HttpContext context)
    {
        CacheControlHeaderValue.TryParse(context.Request.Headers.CacheControl.ToString(), out var cacheControl);
        var evidence = new ProviderReadEvidence
        {
            _requiresFreshWalk = cacheControl is { NoCache: true } or { NoStore: true }
                || context.Request.Headers.GetCommaSeparatedValues("Pragma")
                    .Contains("no-cache", StringComparer.OrdinalIgnoreCase),
        };
        return evidence.BeginScope(context.Items);
    }

    /// <summary>Bind the same evidence during producer construction and later detached reads.</summary>
    public Scope BeginScope(IDictionary<object, object?>? requestItems = null)
    {
        var previous = CurrentLocal.Value;
        CurrentLocal.Value = this;
        if (requestItems is not null) requestItems[ItemKey] = this;
        return new Scope(previous);
    }

    public bool HasTerminalWalk => Volatile.Read(ref _recorded) != 0;
    public bool IsComplete => HasTerminalWalk && Volatile.Read(ref _incomplete) == 0;

    public string IncompleteReason =>
        (Volatile.Read(ref _byteLimitOmitted) > 0, Volatile.Read(ref _openCircuitOmitted) > 0) switch
        {
            (true, true) => "providers were skipped for an exhausted byte limit and an open circuit breaker",
            (true, false) => "a provider was skipped because it reached its byte limit",
            (false, true) => "a provider was skipped because its circuit breaker was open",
            _ => "a provider walk ended without a definitive miss from every enabled provider",
        };

    internal void RecordTerminalWalk(bool pureDefinitiveMiss, ProviderSelectionCoverage coverage)
    {
        if (coverage.OmittedForByteLimit) Volatile.Write(ref _byteLimitOmitted, 1);
        if (coverage.OmittedForOpenCircuit) Volatile.Write(ref _openCircuitOmitted, 1);
        if (!pureDefinitiveMiss || !coverage.IsComplete) Volatile.Write(ref _incomplete, 1);
        Volatile.Write(ref _recorded, 1);
    }

    /// <summary>Carry detached producer evidence on the failure delivered to every reader.</summary>
    internal void TagFailure(Exception exception)
    {
        if (!IsComplete)
            exception.Data[ExceptionDataKey] = IncompleteReason;
        else if (exception.Data[ExceptionDataKey] is null)
            exception.Data[ExceptionDataKey] = true;
    }

    internal static string? IncompleteReasonFrom(Exception exception)
    {
        for (Exception? current = exception; current is not null; current = current.InnerException)
            if (current.Data[ExceptionDataKey] is string reason) return reason;
        return null;
    }

    internal static bool HasCompleteFailureEvidence(Exception exception)
    {
        for (Exception? current = exception; current is not null; current = current.InnerException)
            if (current.Data[ExceptionDataKey] is true) return true;
        return false;
    }

    /// <summary>Do not turn an incomplete miss into a gap-fill repair or playback-hole cache entry.</summary>
    internal static void ThrowIfIncomplete(Exception exception)
    {
        if (!exception.TryGetCausingException<UsenetArticleNotFoundException>(out _)) return;
        Current?.TagFailure(exception);
        if (IncompleteReasonFrom(exception) is not null)
            ExceptionDispatchInfo.Capture(exception).Throw();
    }

    public static ProviderReadEvidence? FromRequestItems(IDictionary<object, object?> requestItems) =>
        requestItems.TryGetValue(ItemKey, out var value) ? value as ProviderReadEvidence : null;

    internal readonly struct Scope(ProviderReadEvidence? previous) : IDisposable
    {
        public void Dispose() => CurrentLocal.Value = previous;
    }
}
