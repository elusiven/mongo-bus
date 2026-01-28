using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.Internal;
using MongoDB.Driver;

namespace MongoBus.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddMongoBus(this IServiceCollection services, Action<MongoBusOptions> configure)
    {
        var opt = new MongoBusOptions();
        configure(opt);

        services.AddSingleton(opt);
        services.AddSingleton<IMongoClient>(_ => new MongoClient(opt.ConnectionString));
        services.AddSingleton(sp => sp.GetRequiredService<IMongoClient>().GetDatabase(opt.DatabaseName));

        services.AddSingleton<IMessageBus, MongoMessageBus>();
        services.AddSingleton<ITopologyManager, MongoBindingRegistry>();
        services.AddSingleton<IMessageDispatcher, MongoMessageDispatcher>();
        services.AddSingleton<IMessagePump, MongoMessagePump>();
        services.AddSingleton<ICloudEventEnveloper, CloudEventEnveloper>();
        services.AddSingleton<ICloudEventSerializer, CloudEventSerializer>();

        services.AddHostedService<MongoBusIndexesHostedService>();
        services.AddHostedService<MongoBusRuntime>();

        return services;
    }

    public static IServiceCollection AddMongoBusConsumer<TConsumer, TMessage, TDefinition>(this IServiceCollection services)
        where TConsumer : class, IMessageHandler<TMessage>
        where TDefinition : ConsumerDefinition<TConsumer, TMessage>
    {
        services.AddScoped<TConsumer>();
        services.AddScoped<IMessageHandler<TMessage>>(sp => sp.GetRequiredService<TConsumer>());
        services.AddSingleton<IConsumerDefinition, TDefinition>();
        return services;
    }

    public static IServiceCollection AddMongoBusPublishInterceptor<T>(this IServiceCollection services)
        where T : class, IPublishInterceptor
    {
        services.AddSingleton<IPublishInterceptor, T>();
        return services;
    }

    public static IServiceCollection AddMongoBusConsumeInterceptor<T>(this IServiceCollection services)
        where T : class, IConsumeInterceptor
    {
        services.AddScoped<IConsumeInterceptor, T>();
        return services;
    }
}
