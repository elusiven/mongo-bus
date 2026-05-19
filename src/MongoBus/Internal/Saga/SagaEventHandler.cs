using Microsoft.Extensions.Logging;
using MongoBus.Abstractions;
using MongoBus.Abstractions.Saga;
using MongoBus.DependencyInjection;
using MongoBus.Models;
using MongoBus.Models.Saga;
using MongoDB.Driver;

namespace MongoBus.Internal.Saga;

internal sealed class SagaEventHandler<TInstance, TMessage>(
    MongoBusStateMachine<TInstance> stateMachine,
    ISagaRepository<TInstance> repository,
    IMessageBus bus,
    SagaPartitioner? partitioner,
    SagaHistoryWriter<TInstance>? historyWriter,
    SagaOptions options,
    IMongoClient? mongoClient,
    ITransactionalMessageBus? transactionalBus,
    MongoTransactionCapability? transactionCapability,
    ILogger logger)
    : IMessageHandler<TMessage>
    where TInstance : class, ISagaInstance, new()
{
    private static int _fallbackWarningLogged;

    public async Task HandleAsync(TMessage message, ConsumeContext context, CancellationToken ct)
    {
        var registration = stateMachine.GetEventRegistrations()[GetTypeId(context)];

        var correlationId = ResolveCorrelationId(registration, context);
        if (string.IsNullOrEmpty(correlationId))
        {
            throw new InvalidOperationException(
                $"Cannot correlate event '{typeof(TMessage).Name}' to a saga instance. " +
                "No correlation ID could be resolved from the consume context or custom expression.");
        }

        IDisposable? partitionLock = null;
        if (partitioner != null)
        {
            var partitionConfig = registration.Partition;
            var partitionKey = partitionConfig?.KeySelector(context) ?? correlationId;
            partitionLock = await partitioner.AcquireAsync(partitionKey, ct);
        }

        try
        {
            await ProcessEventAsync(message, context, correlationId, registration, ct);
        }
        finally
        {
            partitionLock?.Dispose();
        }
    }

    private async Task ProcessEventAsync(
        TMessage message,
        ConsumeContext context,
        string correlationId,
        SagaEventRegistration registration,
        CancellationToken ct)
    {
        TInstance? instance;
        if (registration.CorrelateByProperty != null)
        {
            var propertyValue = registration.CorrelateByProperty.MessageValueSelector(context);
            instance = await repository.FindByPropertyNameAsync(
                registration.CorrelateByProperty.InstancePropertyName, propertyValue, ct);
        }
        else
        {
            instance = await repository.FindAsync(correlationId, ct);
        }

        var isNew = false;

        if (instance == null)
        {
            // Check if this event can create a new instance (Initial state)
            var initialBehavior = stateMachine.GetBehavior<TMessage>(stateMachine.Initial.Name);
            if (initialBehavior == null)
            {
                await HandleMissingInstanceAsync(registration, context, ct);
                return;
            }

            instance = new TInstance
            {
                CorrelationId = correlationId,
                CurrentState = stateMachine.Initial.Name,
                Version = 0,
                CreatedUtc = DateTime.UtcNow,
                LastModifiedUtc = DateTime.UtcNow
            };
            isNew = true;
        }

        // Check if the event is ignored in the current state
        if (stateMachine.IsIgnored<TMessage>(instance.CurrentState))
        {
            logger.LogDebug(
                "Event '{EventType}' ignored in state '{State}' for saga {CorrelationId}",
                typeof(TMessage).Name, instance.CurrentState, correlationId);
            return;
        }

        // Get behavior for current state
        var behavior = stateMachine.GetBehavior<TMessage>(instance.CurrentState);
        if (behavior == null)
        {
            logger.LogWarning(
                "No behavior defined for event '{EventType}' in state '{State}' for saga {CorrelationId}",
                typeof(TMessage).Name, instance.CurrentState, correlationId);
            return;
        }

        var useTransactional = options.UseOutbox
            && await ShouldUseTransactionalPathAsync(ct);

        if (useTransactional)
        {
            await ProcessTransactionalAsync(message, context, correlationId, instance, behavior, isNew, ct);
        }
        else
        {
            await ProcessDirectAsync(message, context, correlationId, instance, behavior, isNew, bus, ct);
        }
    }

    private async Task ProcessDirectAsync(
        TMessage message,
        ConsumeContext context,
        string correlationId,
        TInstance instance,
        IReadOnlyList<ISagaActivity<TInstance, TMessage>> behavior,
        bool isNew,
        IMessageBus activityBus,
        CancellationToken ct)
    {
        var sagaContext = new SagaConsumeContext<TInstance, TMessage>(
            instance, message, context, activityBus, ct);

        var previousVersion = instance.Version;
        var previousState = instance.CurrentState;
        instance.LastModifiedUtc = DateTime.UtcNow;

        foreach (var activity in behavior)
            await activity.ExecuteAsync(sagaContext);

        await ApplyCompositeEventsAsync(instance, context, activityBus, ct);

        instance.Version++;
        if (isNew)
            await repository.InsertAsync(instance, ct);
        else
            await repository.UpdateAsync(instance, previousVersion, ct);

        if (historyWriter != null)
        {
            await historyWriter.WriteAsync(
                correlationId, previousState, instance.CurrentState,
                GetTypeId(context), context, instance.Version, ct);
        }

        if (stateMachine.IsCompleted(instance))
        {
            logger.LogDebug("Saga {CorrelationId} completed, deleting instance", correlationId);
            await repository.DeleteAsync(correlationId, ct);
        }
    }

    private async Task ProcessTransactionalAsync(
        TMessage message,
        ConsumeContext context,
        string correlationId,
        TInstance instance,
        IReadOnlyList<ISagaActivity<TInstance, TMessage>> behavior,
        bool isNew,
        CancellationToken ct)
    {
        // mongoClient and transactionalBus are non-null when useTransactional is true
        // (verified at registration: UseOutbox requires both to be available).
        var buffered = new BufferedSagaMessageBus();
        var sagaContext = new SagaConsumeContext<TInstance, TMessage>(
            instance, message, context, buffered, ct);

        var previousVersion = instance.Version;
        var previousState = instance.CurrentState;
        instance.LastModifiedUtc = DateTime.UtcNow;

        foreach (var activity in behavior)
            await activity.ExecuteAsync(sagaContext);

        await ApplyCompositeEventsAsync(instance, context, buffered, ct);

        instance.Version++;

        using var session = await mongoClient!.StartSessionAsync(cancellationToken: ct);
        session.StartTransaction();
        try
        {
            if (isNew)
                await repository.InsertAsync(instance, session, ct);
            else
                await repository.UpdateAsync(instance, previousVersion, session, ct);

            if (historyWriter != null)
            {
                await historyWriter.WriteAsync(
                    correlationId, previousState, instance.CurrentState,
                    GetTypeId(context), context, instance.Version, session, ct);
            }

            if (stateMachine.IsCompleted(instance))
            {
                logger.LogDebug("Saga {CorrelationId} completed, deleting instance", correlationId);
                await repository.DeleteAsync(correlationId, session, ct);
            }

            await buffered.FlushAsync(transactionalBus!, session, ct);

            await session.CommitTransactionAsync(ct);
        }
        catch
        {
            await session.AbortTransactionAsync(CancellationToken.None);
            throw;
        }
    }

    private async Task ApplyCompositeEventsAsync(
        TInstance instance,
        ConsumeContext context,
        IMessageBus activityBus,
        CancellationToken ct)
    {
        var typeId = GetTypeId(context);
        foreach (var composite in stateMachine.GetCompositeEvents())
        {
            var bitIndex = composite.RequiredEventTypeIds.IndexOf(typeId);
            if (bitIndex < 0) continue;

            var flagsProp = typeof(TInstance).GetProperty(composite.FlagsPropertyName);
            if (flagsProp == null) continue;

            var currentFlags = (int)flagsProp.GetValue(instance)!;
            currentFlags |= (1 << bitIndex);
            flagsProp.SetValue(instance, currentFlags);

            if (currentFlags == composite.RequiredBitmask)
            {
                var compositeBehavior = stateMachine.GetCompositeBehavior(composite.EventName);
                if (compositeBehavior != null)
                {
                    logger.LogDebug(
                        "Composite event '{EventName}' satisfied for saga {CorrelationId}",
                        composite.EventName, instance.CorrelationId);
                    await compositeBehavior(instance, activityBus, context, ct);
                }
            }
        }
    }

    private async Task<bool> ShouldUseTransactionalPathAsync(CancellationToken ct)
    {
        if (mongoClient is null || transactionalBus is null || transactionCapability is null)
        {
            // Misconfigured: UseOutbox=true but DI didn't wire the transactional pieces.
            // Throw a clear, non-retryable error so it surfaces in tests and on first run.
            throw new InvalidOperationException(
                "SagaOptions.UseOutbox is enabled but the transactional message bus is not " +
                "registered. Ensure MongoBusOptions.Outbox.Enabled = true on AddMongoBus().");
        }

        var supported = await transactionCapability.IsSupportedAsync(ct);
        if (supported)
            return true;

        if (options.AllowFallbackWhenTransactionsUnsupported)
        {
            if (Interlocked.Exchange(ref _fallbackWarningLogged, 1) == 0)
            {
                logger.LogWarning(
                    "SagaOptions.UseOutbox is enabled but the connected MongoDB deployment " +
                    "does not support transactions. Falling back to direct publish — saga " +
                    "publishes will not be atomic with the saga write. This warning is " +
                    "logged once per saga type.");
            }
            return false;
        }

        throw new InvalidOperationException(
            "SagaOptions.UseOutbox is enabled but the connected MongoDB deployment does not " +
            "support transactions (requires a replica set or sharded cluster). Set " +
            "SagaOptions.AllowFallbackWhenTransactionsUnsupported = true to degrade gracefully " +
            "in development environments.");
    }

    private static string ResolveCorrelationId(SagaEventRegistration registration, ConsumeContext context)
    {
        if (registration.CorrelateByIdExpression != null)
            return registration.CorrelateByIdExpression(context);

        if (registration.SelectIdExpression != null)
            return registration.SelectIdExpression(context);

        return context.CorrelationId ?? string.Empty;
    }

    private static string GetTypeId(ConsumeContext context) => context.TypeId;

    private async Task HandleMissingInstanceAsync(
        SagaEventRegistration registration,
        ConsumeContext context,
        CancellationToken ct)
    {
        var behavior = registration.MissingInstanceBehavior;

        if (behavior == null || behavior.Action == MissingInstanceAction.Fault)
        {
            throw new InvalidOperationException(
                $"No saga instance found for correlation ID and no initial behavior defined " +
                $"for event '{typeof(TMessage).Name}'.");
        }

        if (behavior.Action == MissingInstanceAction.Discard)
        {
            logger.LogDebug(
                "Discarding event '{EventType}' - no matching saga instance",
                typeof(TMessage).Name);
            return;
        }

        if (behavior.Action == MissingInstanceAction.Execute && behavior.AsyncHandler != null)
        {
            await behavior.AsyncHandler(context);
        }
    }
}
