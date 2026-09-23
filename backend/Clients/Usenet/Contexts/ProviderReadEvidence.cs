using System.Collections.Concurrent;
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

/// <summary>
/// Coverage of the walk that returned an article body. Providers ordered after the one
/// that served are not asked; that is the provider order, not a coverage gap.
/// </summary>
internal readonly record struct ServedWalkCoverage(
    bool OnlyMissesBeforeServer,
    ProviderSelectionCoverage Selection)
{
    public bool IsComplete => OnlyMissesBeforeServer && Selection.IsComplete;
}

/// <summary>Article-local terminal evidence owned by one request or shared upstream stream.</summary>
internal sealed class ProviderReadEvidence
{
    private const string ItemKey = "ProviderReadEvidence";
    private const string UnobservedMiss = "a provider walk ended without a definitive miss from every enabled provider";
    private const string UnobservedCorruption = "no walk in this read served that copy";
    private const string IncompleteServe = "an enabled provider ahead of it was skipped or did not answer";
    private static readonly AsyncLocal<ProviderReadEvidence?> CurrentLocal = new();
    private ConcurrentDictionary<string, string?>? _terminalWalks;
    private ConcurrentDictionary<(string SegmentId, string ProviderKey), string?>? _corruptServes;
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

    internal void RecordTerminalWalk(string? segmentId, bool pureDefinitiveMiss, ProviderSelectionCoverage coverage)
    {
        if (segmentId is null) return;
        RecordVerdict(segmentId, pureDefinitiveMiss && coverage.IsComplete ? null : OmissionReason(coverage, UnobservedMiss));
    }

    /// <summary>A served body failed yEnc validation; keep how complete the walk that served it was.</summary>
    internal void RecordCorruptServe(string segmentId, string providerKey, ServedWalkCoverage served) =>
        RecordCorruptVerdict(
            (segmentId, providerKey),
            served.IsComplete ? null : OmissionReason(served.Selection, IncompleteServe));

    private static string OmissionReason(ProviderSelectionCoverage coverage, string otherwise) =>
        (coverage.OmittedForByteLimit, coverage.OmittedForOpenCircuit) switch
        {
            (true, true) => "providers were skipped for an exhausted byte limit and an open circuit breaker",
            (true, false) => "a provider was skipped because it reached its byte limit",
            (false, true) => "a provider was skipped because its circuit breaker was open",
            _ => otherwise,
        };

    private void RecordVerdict(string segmentId, string? reason) =>
        LazyInitializer.EnsureInitialized(ref _terminalWalks,
            static () => new ConcurrentDictionary<string, string?>(StringComparer.Ordinal))[segmentId] = reason;

    private string? ReasonFor(string segmentId) =>
        Volatile.Read(ref _terminalWalks) is { } walks && walks.TryGetValue(segmentId, out var reason)
            ? reason
            : UnobservedMiss;

    private void RecordCorruptVerdict((string, string) serve, string? reason) =>
        LazyInitializer.EnsureInitialized(ref _corruptServes,
            static () => new ConcurrentDictionary<(string, string), string?>())[serve] = reason;

    private string? CorruptReasonFor((string, string) serve) =>
        Volatile.Read(ref _corruptServes) is { } serves && serves.TryGetValue(serve, out var reason)
            ? reason
            : UnobservedCorruption;

    /// <summary>Only the shared delivery boundary may import another producer's proof.</summary>
    internal void AdoptFailure(ProviderReadEvidence producer, Exception exception)
    {
        if (exception.TryGetCausingException(out UsenetArticleNotFoundException? missing))
            RecordVerdict(missing!.SegmentId, producer.ReasonFor(missing.SegmentId));
        if (exception.TryGetCausingException(out UsenetCorruptArticleException? corrupt))
        {
            var serve = (corrupt!.SegmentId, corrupt.ProviderKey);
            RecordCorruptVerdict(serve, producer.CorruptReasonFor(serve));
        }
    }

    internal string? IncompleteReasonFrom(Exception exception) =>
        exception.TryGetCausingException(out UsenetArticleNotFoundException? missing)
            ? ReasonFor(missing!.SegmentId)
            : null;

    /// <summary>
    /// A corrupt copy is terminal only when the walk that served it could not have reached
    /// another copy: every provider ahead of the server missed and none was skipped.
    /// </summary>
    internal string? IncompleteCorruptionReasonFrom(Exception exception) =>
        exception.TryGetCausingException(out UsenetCorruptArticleException? corrupt)
            ? CorruptReasonFor((corrupt!.SegmentId, corrupt.ProviderKey))
            : null;

    /// <summary>Gap-fill remains playable; only unproven repair/cache reports are suppressed.</summary>
    internal static bool IsUnproven(Exception exception) =>
        Current is { } evidence
        && (evidence.IncompleteReasonFrom(exception) ?? evidence.IncompleteCorruptionReasonFrom(exception)) is not null;

    public static ProviderReadEvidence? FromRequestItems(IDictionary<object, object?> requestItems) =>
        requestItems.TryGetValue(ItemKey, out var value) ? value as ProviderReadEvidence : null;

    internal readonly struct Scope(ProviderReadEvidence? previous) : IDisposable
    {
        public void Dispose() => CurrentLocal.Value = previous;
    }
}
