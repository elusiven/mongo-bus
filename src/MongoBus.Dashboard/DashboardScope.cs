using System.Security.Claims;

namespace MongoBus.Dashboard;

/// <summary>
/// Decides whether a user was granted the dashboard scope. OAuth 2.0 access tokens carry their scopes as one
/// space-separated value, in a <c>scope</c> claim or, from Microsoft Entra ID, an <c>scp</c> claim that JwtBearer
/// maps to a longer claim type by default.
/// </summary>
internal static class DashboardScope
{
    private static readonly string[] ScopeClaimTypes =
    [
        "scope",
        "scp",
        "http://schemas.microsoft.com/identity/claims/scope"
    ];

    public static bool IsGrantedTo(ClaimsPrincipal user) =>
        user.Claims
            .Where(claim => ScopeClaimTypes.Contains(claim.Type, StringComparer.Ordinal))
            .SelectMany(claim => claim.Value.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            .Contains(MongoBusDashboardOptions.DefaultScope, StringComparer.Ordinal);
}
