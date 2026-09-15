using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoBus.DependencyInjection;
using MongoDB.Driver;

namespace MongoBus.Tests;

/// <summary>
/// A MongoBus whose hosted services have been started, as a host would start them.
/// Disposing stops them again. Services that started before a failing one are stopped too,
/// so a failed start does not leave background loops running against the shared server.
/// </summary>
internal sealed class RunningBus : IAsyncDisposable
{
    private readonly IReadOnlyList<IHostedService> _hostedServices;

    private RunningBus(IServiceProvider services, IReadOnlyList<IHostedService> hostedServices)
    {
        Services = services;
        _hostedServices = hostedServices;
    }

    public IServiceProvider Services { get; }

    public IMongoDatabase Database => Services.GetRequiredService<IMongoDatabase>();

    public static async Task<RunningBus> StartAsync(
        string connectionString,
        Action<MongoBusOptions>? configureOptions = null,
        Action<IServiceCollection>? registerServices = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMongoBus(opt =>
        {
            opt.ConnectionString = connectionString;
            opt.DatabaseName = "bus_" + Guid.NewGuid().ToString("N");
            configureOptions?.Invoke(opt);
        });
        registerServices?.Invoke(services);

        var provider = services.BuildServiceProvider();
        var started = new List<IHostedService>();
        try
        {
            foreach (var hostedService in provider.GetServices<IHostedService>())
            {
                await hostedService.StartAsync(CancellationToken.None);
                started.Add(hostedService);
            }
        }
        catch
        {
            await StopAllAsync(started);
            throw;
        }

        return new RunningBus(provider, started);
    }

    public async ValueTask DisposeAsync() => await StopAllAsync(_hostedServices);

    private static async Task StopAllAsync(IEnumerable<IHostedService> hostedServices)
    {
        foreach (var hostedService in hostedServices.Reverse())
            await hostedService.StopAsync(CancellationToken.None);
    }
}
