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
}
