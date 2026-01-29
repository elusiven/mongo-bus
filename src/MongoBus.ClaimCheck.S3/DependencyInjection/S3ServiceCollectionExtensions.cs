using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.ClaimCheck;

namespace MongoBus.DependencyInjection;

public static class S3ServiceCollectionExtensions
{
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
