using System.Net;
using System.Security.Claims;
using System.Text.Encodings.Web;
using FluentAssertions;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoBus.DependencyInjection;
using MongoBus.Tests;
using Xunit;

namespace MongoBus.Dashboard.Tests;

[Collection("Mongo collection")]
public class DashboardScopePolicyTests(MongoDbFixture fixture)
{
    // JwtBearer maps Microsoft Entra ID's "scp" claim to this type unless MapInboundClaims is turned off.
    private const string MappedEntraScopeClaimType = "http://schemas.microsoft.com/identity/claims/scope";

    [Theory]
    [InlineData("scope", "mongobus:dashboard")]
    [InlineData("scope", "openid mongobus:dashboard profile")]
    [InlineData("scp", "mongobus:dashboard")]
    [InlineData(MappedEntraScopeClaimType, "User.Read mongobus:dashboard")]
    public async Task Default_Policy_Should_Allow_A_Token_Granted_The_Dashboard_Scope(string claimType, string claimValue)
    {
        await using var dashboard = await StartDashboardForUserWithAsync(new Claim(claimType, claimValue));

        var response = await dashboard.Client.GetAsync("/mongobus/api/sagas");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Theory]
    [InlineData("scope", "openid profile")]
    [InlineData("scope", "mongobus:dashboard:admin")]
    [InlineData("roles", "mongobus:dashboard")]
    public async Task Default_Policy_Should_Reject_A_Token_Without_The_Dashboard_Scope(string claimType, string claimValue)
    {
        await using var dashboard = await StartDashboardForUserWithAsync(new Claim(claimType, claimValue));

        var response = await dashboard.Client.GetAsync("/mongobus/api/sagas");

        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
    }

    private async Task<DashboardUnderTest> StartDashboardForUserWithAsync(Claim claim)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Services.AddRouting();
        builder.Services.AddAuthentication(FixedClaimsAuthHandler.SchemeName)
            .AddScheme<FixedClaimsAuthOptions, FixedClaimsAuthHandler>(
                FixedClaimsAuthHandler.SchemeName,
                options => options.Claims = [claim]);
        builder.Services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "dashboard_scope_" + Guid.NewGuid().ToString("N");
        });
        builder.Services.AddMongoBusDashboard();

        var app = builder.Build();
        app.UseRouting();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapMongoBusDashboard();
        await app.StartAsync();

        return new DashboardUnderTest(app, new HttpClient { BaseAddress = new Uri(app.Urls.First()) });
    }

    private sealed class DashboardUnderTest(WebApplication app, HttpClient client) : IAsyncDisposable
    {
        public HttpClient Client { get; } = client;

        public async ValueTask DisposeAsync()
        {
            Client.Dispose();
            await app.StopAsync();
            await app.DisposeAsync();
        }
    }

    public sealed class FixedClaimsAuthOptions : AuthenticationSchemeOptions
    {
        public IReadOnlyList<Claim> Claims { get; set; } = [];
    }

    /// <summary>Authenticates every request as a user holding the configured claims.</summary>
    public sealed class FixedClaimsAuthHandler(
        IOptionsMonitor<FixedClaimsAuthOptions> options,
        ILoggerFactory logger,
        UrlEncoder encoder)
        : AuthenticationHandler<FixedClaimsAuthOptions>(options, logger, encoder)
    {
        public const string SchemeName = "FixedClaims";

        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            var principal = new ClaimsPrincipal(new ClaimsIdentity(Options.Claims, SchemeName));
            return Task.FromResult(AuthenticateResult.Success(new AuthenticationTicket(principal, SchemeName)));
        }
    }
}
