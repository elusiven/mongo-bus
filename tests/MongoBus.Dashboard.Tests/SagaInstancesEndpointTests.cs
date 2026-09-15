using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using MongoBus.DependencyInjection;
using MongoBus.Tests;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Dashboard.Tests;

[Collection("Mongo collection")]
public class SagaInstancesEndpointTests(MongoDbFixture fixture)
{
    private const string SagaCollection = "bus_saga_order-state";

    [Fact]
    public async Task Saga_Instances_Endpoint_Should_Return_A_Summary_Of_Each_Instance()
    {
        var databaseName = await SeedSagaAsync(OrderInstance("order-1", "Submitted", version: 3));
        await using var dashboard = await DashboardApp.StartAsync(fixture.ConnectionString, databaseName);

        var response = await dashboard.Client.GetAsync($"/mongobus/api/sagas/{SagaCollection}/instances");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var instance = (await response.Content.ReadFromJsonAsync<JsonElement>()).EnumerateArray().Single();
        instance.GetProperty("correlationId").GetString().Should().Be("order-1");
        instance.GetProperty("currentState").GetString().Should().Be("Submitted");
        instance.GetProperty("version").GetInt32().Should().Be(3);
    }

    [Fact]
    public async Task Saga_Instances_Endpoint_Should_Not_Expose_The_Rest_Of_The_Saga_State()
    {
        var instanceWithCardNumber = OrderInstance("order-2", "Paid", version: 1);
        instanceWithCardNumber["CardNumber"] = "4111111111111111";
        var databaseName = await SeedSagaAsync(instanceWithCardNumber);
        await using var dashboard = await DashboardApp.StartAsync(fixture.ConnectionString, databaseName);

        var body = await dashboard.Client.GetStringAsync($"/mongobus/api/sagas/{SagaCollection}/instances");

        body.Should().NotContain("4111111111111111", "the dashboard only needs the fields it displays");
    }

    [Theory]
    [InlineData("take=0")]
    [InlineData("take=201")]
    [InlineData("skip=-1")]
    public async Task Saga_Instances_Endpoint_Should_Reject_Paging_Outside_Its_Limits(string query)
    {
        var databaseName = await SeedSagaAsync(OrderInstance("order-3", "Submitted", version: 1));
        await using var dashboard = await DashboardApp.StartAsync(fixture.ConnectionString, databaseName);

        var response = await dashboard.Client.GetAsync($"/mongobus/api/sagas/{SagaCollection}/instances?{query}");

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    private async Task<string> SeedSagaAsync(BsonDocument instance)
    {
        var databaseName = "dashboard_saga_instances_" + Guid.NewGuid().ToString("N");
        await new MongoClient(fixture.ConnectionString)
            .GetDatabase(databaseName)
            .GetCollection<BsonDocument>(SagaCollection)
            .InsertOneAsync(instance);
        return databaseName;
    }

    private static BsonDocument OrderInstance(string correlationId, string state, int version) => new()
    {
        ["CorrelationId"] = correlationId,
        ["CurrentState"] = state,
        ["Version"] = version,
        ["CreatedUtc"] = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc),
        ["LastModifiedUtc"] = new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Utc)
    };

    private sealed class DashboardApp(WebApplication app, HttpClient client) : IAsyncDisposable
    {
        public HttpClient Client { get; } = client;

        public static async Task<DashboardApp> StartAsync(string connectionString, string databaseName)
        {
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseUrls("http://127.0.0.1:0");
            builder.Services.AddRouting();
            builder.Services.AddMongoBus(opt =>
            {
                opt.ConnectionString = connectionString;
                opt.DatabaseName = databaseName;
            });
            builder.Services.AddMongoBusDashboard(opt => opt.AuthorizationPolicy = null);

            var app = builder.Build();
            app.UseRouting();
            app.MapMongoBusDashboard();
            await app.StartAsync();

            return new DashboardApp(app, new HttpClient { BaseAddress = new Uri(app.Urls.First()) });
        }

        public async ValueTask DisposeAsync()
        {
            Client.Dispose();
            await app.StopAsync();
            await app.DisposeAsync();
        }
    }
}
