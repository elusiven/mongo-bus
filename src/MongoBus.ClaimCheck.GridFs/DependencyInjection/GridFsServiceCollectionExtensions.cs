using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.ClaimCheck;
using MongoDB.Driver;

namespace MongoBus.DependencyInjection;

public static class GridFsServiceCollectionExtensions
{
    public static IServiceCollection AddMongoBusGridFsClaimCheck(this IServiceCollection services, string bucketName = "claimcheck")
    {
        services.AddSingleton<IClaimCheckProvider>(sp =>
            new MongoGridFsClaimCheckProvider(sp.GetRequiredService<IMongoDatabase>(), bucketName));
        return services;
    }
}
