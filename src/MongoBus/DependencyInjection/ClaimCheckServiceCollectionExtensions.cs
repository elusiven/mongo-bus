using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.ClaimCheck;

namespace MongoBus.DependencyInjection;

public static class ClaimCheckServiceCollectionExtensions
{
    public static IServiceCollection AddMongoBusClaimCheckProvider<TProvider>(this IServiceCollection services)
        where TProvider : class, IClaimCheckProvider
    {
        services.AddSingleton<IClaimCheckProvider, TProvider>();
        return services;
    }

    public static IServiceCollection AddMongoBusClaimCheckAzureBlob(this IServiceCollection services, Action<AzureBlobClaimCheckOptions> configure)
    {
        var options = new AzureBlobClaimCheckOptions();
        configure(options);
        services.AddSingleton(options);
        services.AddSingleton<IClaimCheckProvider, AzureBlobClaimCheckProvider>();
        return services;
    }

    public static IServiceCollection AddMongoBusClaimCheckS3(this IServiceCollection services, Action<S3ClaimCheckOptions> configure)
    {
        var options = new S3ClaimCheckOptions();
        configure(options);
        services.AddSingleton(options);
        services.AddSingleton<IClaimCheckProvider, S3ClaimCheckProvider>();
        return services;
    }

    public static IServiceCollection AddMongoBusClaimCheckWasabi(this IServiceCollection services, Action<S3ClaimCheckOptions> configure)
    {
        return services.AddMongoBusClaimCheckS3(opt =>
        {
            opt.ProviderName = "wasabi";
            configure(opt);
        });
    }
}
