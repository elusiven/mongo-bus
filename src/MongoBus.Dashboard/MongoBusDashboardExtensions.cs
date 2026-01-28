using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using MongoBus.Dashboard.Services;

namespace MongoBus.Dashboard;

public static class MongoBusDashboardExtensions
{
    public static IServiceCollection AddMongoBusDashboard(this IServiceCollection services)
    {
        services.AddScoped<IMongoBusMonitoringService, MongoBusMonitoringService>();
        return services;
    }

    public static IEndpointRouteBuilder MapMongoBusDashboard(this IEndpointRouteBuilder endpoints, string pattern = "/mongobus")
    {
        pattern = pattern.TrimEnd('/');

        var assembly = typeof(MongoBusDashboardExtensions).Assembly;
        var fileProvider = new ManifestEmbeddedFileProvider(assembly, "wwwroot");

        // API Endpoint
        endpoints.MapGet($"{pattern}/api/stats", async (IMongoBusMonitoringService monitoring, CancellationToken ct) =>
        {
            return Results.Ok(await monitoring.GetStatsAsync(ct));
        });

        // Dashboard UI (Static Files)
        endpoints.MapGet($"{pattern}/{{*path}}", async (string? path, HttpContext context) =>
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

        return endpoints;
    }
}
