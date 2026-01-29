using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.ClaimCheck;

namespace MongoBus.DependencyInjection;

public static class AzureBlobServiceCollectionExtensions
{
    public static IServiceCollection AddMongoBusClaimCheckAzureBlob(this IServiceCollection services, Action<AzureBlobClaimCheckOptions> configure)
    {
        var options = new AzureBlobClaimCheckOptions();
        configure(options);
        services.AddSingleton(options);
        services.AddSingleton<IClaimCheckProvider, AzureBlobClaimCheckProvider>();
        return services;
    }
}
