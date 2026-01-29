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
}
