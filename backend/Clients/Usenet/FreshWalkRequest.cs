using Microsoft.AspNetCore.Http;
using Microsoft.Net.Http.Headers;

namespace NzbWebDAV.Clients.Usenet;

/// <summary>Scopes provider-fresh reads to one WebDAV request.</summary>
internal static class FreshWalkRequest
{
    private static readonly AsyncLocal<bool> CurrentLocal = new();

    internal static bool RequiresFreshWalk => CurrentLocal.Value;

    internal static Scope BeginRequest(HttpContext context)
    {
        var previous = CurrentLocal.Value;
        CurrentLocal.Value = IsFreshWalkRequest(context.Request.Headers);
        return new Scope(previous);
    }

    private static bool IsFreshWalkRequest(IHeaderDictionary headers)
    {
        var cacheControl = headers.CacheControl;
        if (cacheControl.Count > 0
            && CacheControlHeaderValue.TryParse(cacheControl.ToString(), out var parsed)
            && (parsed.NoCache || parsed.NoStore))
        {
            return true;
        }

        if (!headers.TryGetValue("Pragma", out var pragma) || pragma.Count == 0)
            return false;

        return headers.GetCommaSeparatedValues("Pragma")
            .Contains("no-cache", StringComparer.OrdinalIgnoreCase);
    }

    internal readonly struct Scope(bool previous) : IDisposable
    {
        public void Dispose() => CurrentLocal.Value = previous;
    }
}
