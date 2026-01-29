using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.ClaimCheck;

namespace MongoBus.DependencyInjection;

public static class ClaimCheckServiceCollectionExtensions
{
    public static IServiceCollection AddMongoBusClaimCheckProvider<TProvider>(this IServiceCollection services)
        where TProvider : class, IClaimCheckProvider
    {
        return AddClaimCheckProvider<TProvider>(services);
    }

    public static IServiceCollection AddMongoBusClaimCheckAzureBlob(this IServiceCollection services, Action<AzureBlobClaimCheckOptions> configure)
    {
        var options = BuildOptions(configure);
        services.AddSingleton(options);
        return AddClaimCheckProvider<AzureBlobClaimCheckProvider>(services);
    }

    public static IServiceCollection AddMongoBusClaimCheckS3(this IServiceCollection services, Action<S3ClaimCheckOptions> configure)
    {
        var options = BuildOptions(configure);
        services.AddSingleton(options);
        return AddClaimCheckProvider<S3ClaimCheckProvider>(services);
    }

    public static IServiceCollection AddMongoBusClaimCheckWasabi(this IServiceCollection services, Action<S3ClaimCheckOptions> configure)
    {
        return services.AddMongoBusClaimCheckS3(opt =>
        {
            opt.ProviderName = "wasabi";
            configure(opt);
        });
    }

    private static IServiceCollection AddClaimCheckProvider<TProvider>(IServiceCollection services)
        where TProvider : class, IClaimCheckProvider
    {
        services.AddSingleton<IClaimCheckProvider, TProvider>();
        return services;
    }

    private static TOptions BuildOptions<TOptions>(Action<TOptions> configure) where TOptions : new()
    {
        var options = new TOptions();
        configure(options);
        return options;
    }
}
