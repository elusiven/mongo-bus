using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using MongoBus.Dashboard.Services;

namespace MongoBus.Dashboard;

public sealed class MongoBusDashboardOptions
{
    /// <summary>
    /// The name of an authorization policy to apply to all dashboard routes.
    /// Defaults to "MongoBusDashboard" which requires the claim scope=mongobus:dashboard.
    /// Set to null to explicitly disable authorization (open dashboard).
    /// Override with your own policy name for custom authorization logic.
    /// </summary>
    public string? AuthorizationPolicy { get; set; } = DefaultPolicyName;

    internal const string DefaultPolicyName = "MongoBusDashboard";
    internal const string DefaultScope = "mongobus:dashboard";
}

public static class MongoBusDashboardExtensions
{
    public static IServiceCollection AddMongoBusDashboard(this IServiceCollection services, Action<MongoBusDashboardOptions>? configure = null)
    {
        var options = new MongoBusDashboardOptions();
        configure?.Invoke(options);
        services.AddSingleton(options);
        services.AddScoped<IMongoBusMonitoringService, MongoBusMonitoringService>();

        // Register the default authorization policy if the user hasn't overridden or disabled it
        if (options.AuthorizationPolicy == MongoBusDashboardOptions.DefaultPolicyName)
        {
            services.AddAuthorization(auth =>
                auth.AddPolicy(MongoBusDashboardOptions.DefaultPolicyName, policy =>
                    policy.RequireClaim("scope", MongoBusDashboardOptions.DefaultScope)));
        }

        return services;
    }

    public static IEndpointRouteBuilder MapMongoBusDashboard(this IEndpointRouteBuilder endpoints, string pattern = "/mongobus")
    {
        pattern = pattern.TrimEnd('/');

        var options = endpoints.ServiceProvider.GetService<MongoBusDashboardOptions>();
        var assembly = typeof(MongoBusDashboardExtensions).Assembly;
        var fileProvider = new ManifestEmbeddedFileProvider(assembly, "wwwroot");

        // API Endpoints
        var statsEndpoint = endpoints.MapGet($"{pattern}/api/stats", async (IMongoBusMonitoringService monitoring, CancellationToken ct) =>
        {
            return Results.Ok(await monitoring.GetStatsAsync(ct));
        });

        var sagasEndpoint = endpoints.MapGet($"{pattern}/api/sagas", async (IMongoBusMonitoringService monitoring, CancellationToken ct) =>
        {
            return Results.Ok(await monitoring.GetSagaCollectionsAsync(ct));
        });

        var sagaStatsEndpoint = endpoints.MapGet($"{pattern}/api/sagas/{{collection}}/stats", async (string collection, IMongoBusMonitoringService monitoring, CancellationToken ct) =>
        {
            return Results.Ok(await monitoring.GetSagaStatsAsync(collection, ct));
        });

        var sagaInstancesEndpoint = endpoints.MapGet($"{pattern}/api/sagas/{{collection}}/instances", async (string collection, string? state, int? skip, int? take, IMongoBusMonitoringService monitoring, CancellationToken ct) =>
        {
            return Results.Ok(await monitoring.GetSagaInstancesAsync(collection, state, skip ?? 0, take ?? 50, ct));
        });

        var sagaHistoryEndpoint = endpoints.MapGet($"{pattern}/api/sagas/{{collection}}/history/{{correlationId}}", async (string collection, string correlationId, IMongoBusMonitoringService monitoring, CancellationToken ct) =>
        {
            return Results.Ok(await monitoring.GetSagaHistoryAsync(collection, correlationId, ct));
        });

        // Dashboard UI (Static Files)
        var uiEndpoint = endpoints.MapGet($"{pattern}/{{*path}}", async (string? path, HttpContext context) =>
        {
            if (string.IsNullOrEmpty(path) || path == "/")
            {
                context.Response.Redirect($"{context.Request.PathBase}{pattern}/index.html");
                return;
            }

            var fileInfo = fileProvider.GetFileInfo(path);
            if (!fileInfo.Exists)
            {
                context.Response.StatusCode = 404;
                return;
            }

            var contentType = path.EndsWith(".html") ? "text/html" :
                              path.EndsWith(".js") ? "application/javascript" :
                              path.EndsWith(".css") ? "text/css" :
                              "application/octet-stream";

            context.Response.ContentType = contentType;
            await context.Response.SendFileAsync(fileInfo);
        });

        // Apply authorization policy if configured
        if (!string.IsNullOrEmpty(options?.AuthorizationPolicy))
        {
            var policy = options.AuthorizationPolicy;
            statsEndpoint.RequireAuthorization(policy);
            sagasEndpoint.RequireAuthorization(policy);
            sagaStatsEndpoint.RequireAuthorization(policy);
            sagaInstancesEndpoint.RequireAuthorization(policy);
            sagaHistoryEndpoint.RequireAuthorization(policy);
            uiEndpoint.RequireAuthorization(policy);
        }

        return endpoints;
    }
}
