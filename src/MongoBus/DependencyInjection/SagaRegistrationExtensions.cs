using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoBus.Abstractions;
using MongoBus.Abstractions.Saga;
using MongoBus.Internal.Saga;
using MongoBus.Models.Saga;
using MongoBus.Utils;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace MongoBus.DependencyInjection;

public static class SagaRegistrationExtensions
{
    public static IServiceCollection AddMongoBusSaga<TStateMachine, TInstance>(
        this IServiceCollection services,
        Action<SagaOptions>? configure = null)
        where TStateMachine : MongoBusStateMachine<TInstance>, new()
        where TInstance : class, ISagaInstance, new()
    {
        var options = new SagaOptions();
        configure?.Invoke(options);

        var stateMachine = new TStateMachine();

        if (!BsonClassMap.IsClassMapRegistered(typeof(TInstance)))
        {
            BsonClassMap.RegisterClassMap<TInstance>(cm =>
            {
                cm.AutoMap();
                cm.SetIgnoreExtraElements(true);
            });
        }

        services.AddSingleton(stateMachine);
        services.AddSingleton<MongoBusStateMachine<TInstance>>(stateMachine);
        services.AddSingleton(options);

        var endpointName = options.EndpointName
            ?? EndpointNameHelper.FromConsumerType(typeof(TStateMachine));

        var collectionName = $"bus_saga_{EndpointNameHelper.FromConsumerType(typeof(TInstance))}";

        services.AddSingleton<ISagaRepository<TInstance>>(sp =>
        {
            var db = sp.GetRequiredService<IMongoDatabase>();
            var collection = db.GetCollection<TInstance>(collectionName);
            return new MongoSagaRepository<TInstance>(collection);
        });

        if (options.DefaultPartitionCount > 0)
        {
            services.AddSingleton(new SagaPartitioner(options.DefaultPartitionCount));
        }

        if (options.HistoryEnabled)
        {
            services.AddSingleton(sp =>
                new SagaHistoryWriter<TInstance>(sp.GetRequiredService<IMongoDatabase>()));
        }

        foreach (var registration in stateMachine.GetEventRegistrations())
        {
            var typeId = registration.Key;
            var reg = registration.Value;
            var messageType = reg.MessageType;

            RegisterSagaEventHandler(services, typeof(TStateMachine), typeof(TInstance), messageType, typeId, endpointName, options);
        }

        if (options.SagaTimeout > TimeSpan.Zero)
        {
            services.AddHostedService(sp => new SagaTimeoutService<TInstance>(
                sp.GetRequiredService<IMongoDatabase>(),
                options,
                sp.GetService<SagaHistoryWriter<TInstance>>(),
                sp.GetRequiredService<ILogger<SagaTimeoutService<TInstance>>>()));
        }

        services.AddHostedService(sp => new SagaIndexesHostedService<TInstance>(
            sp.GetRequiredService<IMongoDatabase>(),
            collectionName,
            options,
            sp.GetRequiredService<ILogger<SagaIndexesHostedService<TInstance>>>()));

        return services;
    }

    private static void RegisterSagaEventHandler(
        IServiceCollection services,
        Type stateMachineType,
        Type instanceType,
        Type messageType,
        string typeId,
        string endpointName,
        SagaOptions options)
    {
        var handlerType = typeof(SagaEventHandler<,>).MakeGenericType(instanceType, messageType);
        var handlerInterface = typeof(IMessageHandler<>).MakeGenericType(messageType);

        services.AddScoped(handlerInterface, sp =>
        {
            var sm = sp.GetRequiredService(stateMachineType);
            var repoType = typeof(ISagaRepository<>).MakeGenericType(instanceType);
            var repo = sp.GetRequiredService(repoType);
            var bus = sp.GetRequiredService<IMessageBus>();
            var partitioner = sp.GetService<SagaPartitioner>();
            var historyWriterType = typeof(SagaHistoryWriter<>).MakeGenericType(instanceType);
            var historyWriter = sp.GetService(historyWriterType);
            var logger = sp.GetRequiredService<ILoggerFactory>().CreateLogger(handlerType);

            return Activator.CreateInstance(handlerType, sm, repo, bus, partitioner, historyWriter, logger)!;
        });

        services.AddScoped(handlerType, sp => sp.GetRequiredService(handlerInterface));

        services.AddSingleton<IConsumerDefinition>(new SagaConsumerDefinition(
            typeId, messageType, handlerType, endpointName, options));
    }
}

internal sealed class SagaConsumerDefinition(
    string typeId,
    Type messageType,
    Type consumerType,
    string endpointName,
    SagaOptions options)
    : IConsumerDefinition
{
    public string TypeId => typeId;
    public Type MessageType => messageType;
    public Type ConsumerType => consumerType;
    public string EndpointName => endpointName;
    public int ConcurrencyLimit => options.ConcurrencyLimit;
    public int PrefetchCount => options.PrefetchCount;
    public TimeSpan LockTime => options.LockTime;
    public int MaxAttempts => options.MaxAttempts;
    public bool IdempotencyEnabled => options.IdempotencyEnabled;
}
