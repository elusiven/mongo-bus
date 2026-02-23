using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.ClaimCheck;
using MongoBus.Internal;
using MongoBus.Internal.ClaimCheck;
using MongoDB.Driver;

namespace MongoBus.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddMongoBus(this IServiceCollection services, Action<MongoBusOptions> configure)
    {
        var opt = BuildOptions(configure);

        RegisterCoreServices(services, opt);
        RegisterHostedServices(services, opt);

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

    public static IServiceCollection AddMongoBusBatchConsumer<TConsumer, TMessage, TDefinition>(this IServiceCollection services)
        where TConsumer : class, IBatchMessageHandler<TMessage>
        where TDefinition : BatchConsumerDefinition<TConsumer, TMessage>
    {
        services.AddScoped<TConsumer>();
        services.AddScoped<IBatchMessageHandler<TMessage>>(sp => sp.GetRequiredService<TConsumer>());
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

    public static IServiceCollection AddMongoBusBatchConsumeInterceptor<T>(this IServiceCollection services)
        where T : class, IBatchConsumeInterceptor
    {
        services.AddScoped<IBatchConsumeInterceptor, T>();
        return services;
    }

    public static IServiceCollection AddMongoBusPublishObserver<T>(this IServiceCollection services)
        where T : class, IPublishObserver
    {
        services.AddSingleton<IPublishObserver, T>();
        return services;
    }

    public static IServiceCollection AddMongoBusConsumeObserver<T>(this IServiceCollection services)
        where T : class, IConsumeObserver
    {
        services.AddSingleton<IConsumeObserver, T>();
        return services;
    }

    public static IServiceCollection AddMongoBusBatchObserver<T>(this IServiceCollection services)
        where T : class, IBatchObserver
    {
        services.AddSingleton<IBatchObserver, T>();
        return services;
    }

    public static IServiceCollection AddMongoBusBatchMetricsObserver<T>(this IServiceCollection services)
        where T : class, IBatchMetricsObserver
    {
        services.AddSingleton<IBatchObserver, T>();
        return services;
    }

    public static IServiceCollection AddMongoBusInMemoryClaimCheck(this IServiceCollection services)
    {
        services.AddSingleton<IClaimCheckProvider, InMemoryClaimCheckProvider>();
        return services;
    }

    private static MongoBusOptions BuildOptions(Action<MongoBusOptions> configure)
    {
        var opt = new MongoBusOptions();
        configure(opt);
        return opt;
    }

    private static void RegisterCoreServices(IServiceCollection services, MongoBusOptions opt)
    {
        services.AddSingleton(opt);
        services.AddSingleton<IMongoClient>(_ => new MongoClient(opt.ConnectionString));
        services.AddSingleton(sp => sp.GetRequiredService<IMongoClient>().GetDatabase(opt.DatabaseName));

        services.AddSingleton<IMessageBus, MongoMessageBus>();
        services.AddSingleton<ITransactionalMessageBus, MongoTransactionalMessageBus>();
        services.AddSingleton<ITopologyManager, MongoBindingRegistry>();
        services.AddSingleton<IMessageDispatcher, MongoMessageDispatcher>();
        services.AddSingleton<IBatchMessageDispatcher, MongoBatchMessageDispatcher>();
        services.AddSingleton<IMessagePump, MongoMessagePump>();
        services.AddSingleton<ICloudEventEnveloper, CloudEventEnveloper>();
        services.AddSingleton<ICloudEventSerializer, CloudEventSerializer>();
        services.AddSingleton<IClaimCheckDataSerializer, ClaimCheckDataSerializer>();
        services.AddSingleton<IClaimCheckProviderResolver, ClaimCheckProviderResolver>();
        services.AddSingleton<IClaimCheckCompressor, GZipClaimCheckCompressor>();
        services.AddSingleton<IClaimCheckCompressorProvider, ClaimCheckCompressorProvider>();
        services.AddSingleton<IClaimCheckManager, ClaimCheckManager>();

        services.AddSingleton<IPublishObserver, OpenTelemetryPublishObserver>();
        services.AddSingleton<IConsumeObserver, OpenTelemetryConsumeObserver>();
        services.AddSingleton<IBatchObserver, OpenTelemetryBatchObserver>();
    }

    private static void RegisterHostedServices(IServiceCollection services, MongoBusOptions options)
    {
        services.AddSingleton<MongoBusValidationHostedService>();
        services.AddHostedService(sp => sp.GetRequiredService<MongoBusValidationHostedService>());
        services.AddHostedService<MongoBusIndexesHostedService>();
        services.AddHostedService<MongoBusRuntime>();
        if (options.Outbox.Enabled)
        {
            services.AddHostedService<MongoOutboxRelayService>();
        }
        services.AddHostedService<ClaimCheckCleanupService>();
    }
}
