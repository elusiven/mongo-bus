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
    public async Task Dashboard_Ui_ShouldBeAccessible_WhenPolicyDisabled()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddRouting();
        builder.Services.AddMongoBus(opt => {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "dashboard_ui_test";
        });
        builder.Services.AddMongoBusDashboard(opt => opt.AuthorizationPolicy = null);

        var app = builder.Build();
        app.UseStaticFiles();
        app.UseRouting();
        app.MapMongoBusDashboard();

        await app.StartAsync();
        try
        {
            using var handler = new HttpClientHandler { AllowAutoRedirect = false };
            using var client = new HttpClient(handler) { BaseAddress = new Uri(app.Urls.First()) };

            var response = await client.GetAsync("/mongobus");
            response.StatusCode.Should().Be(HttpStatusCode.Redirect);

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
    public async Task Dashboard_Api_ShouldBeAccessible_WhenPolicyDisabled()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddRouting();
        builder.Services.AddMongoBus(opt => {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "dashboard_api_test";
        });
        builder.Services.AddMongoBusDashboard(opt => opt.AuthorizationPolicy = null);

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
    public async Task Dashboard_SagaApi_ShouldBeAccessible_WhenPolicyDisabled()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddRouting();
        builder.Services.AddMongoBus(opt => {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "dashboard_saga_api_test";
        });
        builder.Services.AddMongoBusDashboard(opt => opt.AuthorizationPolicy = null);

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
    public async Task Dashboard_DefaultPolicy_RejectsWithoutScope()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Environment.EnvironmentName = "Production"; // Ensure not Development
        builder.Services.AddRouting();
        builder.Services.AddAuthentication("Test")
            .AddScheme<AuthenticationSchemeOptions, TestAuthHandler>("Test", _ => { });
        builder.Services.AddMongoBus(opt => {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "dashboard_default_policy_test";
        });
        // Default policy: requires scope=mongobus:dashboard
        builder.Services.AddMongoBusDashboard();

        var app = builder.Build();
        app.UseRouting();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapMongoBusDashboard();

        await app.StartAsync();
        try
        {
            using var client = new HttpClient { BaseAddress = new Uri(app.Urls.First()) };

            var apiResponse = await client.GetAsync("/mongobus/api/stats");
            apiResponse.StatusCode.Should().Be(HttpStatusCode.Forbidden);

            var uiResponse = await client.GetAsync("/mongobus/index.html");
            uiResponse.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        }
        finally
        {
            await app.StopAsync();
        }
    }

    [Fact]
    public async Task Dashboard_DisabledPolicy_ForDevelopment()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddRouting();
        builder.Services.AddMongoBus(opt => {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "dashboard_dev_env_test";
        });
        // In development, disable the policy by setting it to null
        builder.Services.AddMongoBusDashboard(opt => opt.AuthorizationPolicy = null);

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
    public async Task Dashboard_CustomPolicy_RejectsWithoutClaim()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Environment.EnvironmentName = "Production";
        builder.Services.AddRouting();
        builder.Services.AddAuthentication("Test")
            .AddScheme<AuthenticationSchemeOptions, TestAuthHandler>("Test", _ => { });
        builder.Services.AddAuthorization(auth =>
        {
            auth.AddPolicy("CustomDashboard", policy =>
                policy.RequireClaim("scope", "custom:dashboard:view"));
        });
        builder.Services.AddMongoBus(opt => {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "dashboard_custom_policy_test";
        });
        builder.Services.AddMongoBusDashboard(opt =>
            opt.AuthorizationPolicy = "CustomDashboard");

        var app = builder.Build();
        app.UseRouting();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapMongoBusDashboard();

        await app.StartAsync();
        try
        {
            using var client = new HttpClient { BaseAddress = new Uri(app.Urls.First()) };
            var response = await client.GetAsync("/mongobus/api/stats");
            response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        }
        finally
        {
            await app.StopAsync();
        }
    }

    [Fact]
    public async Task Dashboard_NullPolicy_DashboardIsOpen()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Environment.EnvironmentName = "Production";
        builder.Services.AddRouting();
        builder.Services.AddMongoBus(opt => {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "dashboard_null_policy_test";
        });
        builder.Services.AddMongoBusDashboard(opt => opt.AuthorizationPolicy = null);

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
        var identity = new System.Security.Claims.ClaimsIdentity("Test");
        var principal = new System.Security.Claims.ClaimsPrincipal(identity);
        var ticket = new AuthenticationTicket(principal, "Test");
        return Task.FromResult(AuthenticateResult.Success(ticket));
    }
}
