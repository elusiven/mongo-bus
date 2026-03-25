using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using MongoBus.Dashboard;
using MongoBus.DependencyInjection;
using MongoBus.Tests;
using Xunit;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using System.Net;
using FluentAssertions;

namespace MongoBus.Dashboard.Tests;

[Collection("Mongo collection")]
public class DashboardIntegrationTests(MongoDbFixture fixture)
{
    [Fact]
    public async Task Dashboard_Ui_ShouldBeAccessible()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddRouting();
        builder.Services.AddMongoBus(opt => {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "dashboard_ui_test";
        });
        builder.Services.AddMongoBusDashboard();
        
        var app = builder.Build();
        app.UseStaticFiles();
        app.UseRouting();
        app.MapMongoBusDashboard();

        await app.StartAsync();
        try
        {
            using var handler = new HttpClientHandler { AllowAutoRedirect = false };
            using var client = new HttpClient(handler) { BaseAddress = new Uri(app.Urls.First()) };
            
            // Check redirect from /mongobus to /mongobus/index.html
            var response = await client.GetAsync("/mongobus");
            response.StatusCode.Should().Be(HttpStatusCode.Redirect);
            
            // Check index.html exists
            var indexResponse = await client.GetAsync("/mongobus/index.html");
            indexResponse.StatusCode.Should().Be(HttpStatusCode.OK);
            var content = await indexResponse.Content.ReadAsStringAsync();
            content.Should().Contain("MongoBus Monitoring Dashboard");
        }
        finally
        {
            await app.StopAsync();
        }
    }

    [Fact]
    public async Task Dashboard_Api_ShouldBeAccessible()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddRouting();
        builder.Services.AddMongoBus(opt => {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "dashboard_api_test";
        });
        builder.Services.AddMongoBusDashboard();

        var app = builder.Build();
        app.UseRouting();
        app.MapMongoBusDashboard();

        await app.StartAsync();
        try
        {
            using var client = new HttpClient { BaseAddress = new Uri(app.Urls.First()) };
            var response = await client.GetAsync("/mongobus/api/stats");
            response.StatusCode.Should().Be(HttpStatusCode.OK);
        }
        finally
        {
            await app.StopAsync();
        }
    }

    [Fact]
    public async Task Dashboard_SagaApi_ShouldBeAccessible()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddRouting();
        builder.Services.AddMongoBus(opt => {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "dashboard_saga_api_test";
        });
        builder.Services.AddMongoBusDashboard();

        var app = builder.Build();
        app.UseRouting();
        app.MapMongoBusDashboard();

        await app.StartAsync();
        try
        {
            using var client = new HttpClient { BaseAddress = new Uri(app.Urls.First()) };
            var response = await client.GetAsync("/mongobus/api/sagas");
            response.StatusCode.Should().Be(HttpStatusCode.OK);
        }
        finally
        {
            await app.StopAsync();
        }
    }

    [Fact]
    public async Task Dashboard_WithPolicy_RejectsUnauthenticated()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddRouting();
        builder.Services.AddAuthentication("Test")
            .AddScheme<AuthenticationSchemeOptions, TestAuthHandler>("Test", _ => { });
        builder.Services.AddAuthorization(auth =>
        {
            auth.AddPolicy("DashboardPolicy", policy =>
                policy.RequireClaim("scope", "mongobus:dashboard"));
        });
        builder.Services.AddMongoBus(opt => {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "dashboard_auth_reject_test";
        });
        builder.Services.AddMongoBusDashboard(opt =>
        {
            opt.AuthorizationPolicy = "DashboardPolicy";
        });

        var app = builder.Build();
        app.UseRouting();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapMongoBusDashboard();

        await app.StartAsync();
        try
        {
            using var client = new HttpClient { BaseAddress = new Uri(app.Urls.First()) };

            // API should be forbidden without the required claim
            var apiResponse = await client.GetAsync("/mongobus/api/stats");
            apiResponse.StatusCode.Should().Be(HttpStatusCode.Forbidden);

            // UI should also be forbidden
            var uiResponse = await client.GetAsync("/mongobus/index.html");
            uiResponse.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        }
        finally
        {
            await app.StopAsync();
        }
    }

    [Fact]
    public async Task Dashboard_WithoutPolicy_RemainsOpen()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddRouting();
        builder.Services.AddMongoBus(opt => {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "dashboard_no_auth_test";
        });
        // No AuthorizationPolicy configured
        builder.Services.AddMongoBusDashboard();

        var app = builder.Build();
        app.UseRouting();
        app.MapMongoBusDashboard();

        await app.StartAsync();
        try
        {
            using var client = new HttpClient { BaseAddress = new Uri(app.Urls.First()) };
            var response = await client.GetAsync("/mongobus/api/stats");
            response.StatusCode.Should().Be(HttpStatusCode.OK);
        }
        finally
        {
            await app.StopAsync();
        }
    }
}

/// <summary>
/// Minimal test authentication handler that authenticates all requests
/// but without any claims (so policy checks will fail).
/// </summary>
internal sealed class TestAuthHandler(
    Microsoft.Extensions.Options.IOptionsMonitor<AuthenticationSchemeOptions> options,
    Microsoft.Extensions.Logging.ILoggerFactory logger,
    System.Text.Encodings.Web.UrlEncoder encoder)
    : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        // Authenticate successfully but with no claims
        var identity = new System.Security.Claims.ClaimsIdentity("Test");
        var principal = new System.Security.Claims.ClaimsPrincipal(identity);
        var ticket = new AuthenticationTicket(principal, "Test");
        return Task.FromResult(AuthenticateResult.Success(ticket));
    }
}
