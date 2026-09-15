using System.Text.RegularExpressions;
using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using MongoBus.DependencyInjection;
using MongoBus.Tests;
using Xunit;

namespace MongoBus.Dashboard.Tests;

[Collection("Mongo collection")]
public partial class DashboardPageIntegrityTests(MongoDbFixture fixture)
{
    [Fact]
    public async Task Dashboard_Page_Should_Pin_Every_Externally_Hosted_Resource_With_Subresource_Integrity()
    {
        var page = await GetDashboardPageAsync();

        var externalResourceTags = ExternalResourceTag().Matches(page).Select(match => match.Value).ToList();

        externalResourceTags.Should().NotBeEmpty("the page loads Bootstrap from a CDN");
        externalResourceTags.Should().AllSatisfy(tag =>
        {
            tag.Should().MatchRegex("integrity=\"sha384-[A-Za-z0-9+/=]+\"", "a compromised CDN must not be able to run code in the dashboard");
            tag.Should().Contain("crossorigin=\"anonymous\"");
        });
    }

    private async Task<string> GetDashboardPageAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Services.AddRouting();
        builder.Services.AddMongoBus(opt =>
        {
            opt.ConnectionString = fixture.ConnectionString;
            opt.DatabaseName = "dashboard_page_integrity";
        });
        builder.Services.AddMongoBusDashboard(opt => opt.AuthorizationPolicy = null);

        await using var app = builder.Build();
        app.UseRouting();
        app.MapMongoBusDashboard();
        await app.StartAsync();
        try
        {
            using var client = new HttpClient { BaseAddress = new Uri(app.Urls.First()) };
            return await client.GetStringAsync("/mongobus/index.html");
        }
        finally
        {
            await app.StopAsync();
        }
    }

    [GeneratedRegex("<(script|link)\\b[^>]*\\b(src|href)=\"https?://[^\"]+\"[^>]*>", RegexOptions.IgnoreCase)]
    private static partial Regex ExternalResourceTag();
}
