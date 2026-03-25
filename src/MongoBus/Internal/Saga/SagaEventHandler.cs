using Microsoft.Extensions.Logging;
using MongoBus.Abstractions;
using MongoBus.Abstractions.Saga;
using MongoBus.Models;
using MongoBus.Models.Saga;

namespace MongoBus.Internal.Saga;

internal sealed class SagaEventHandler<TInstance, TMessage>(
    MongoBusStateMachine<TInstance> stateMachine,
    ISagaRepository<TInstance> repository,
    IMessageBus bus,
    SagaPartitioner? partitioner,
    ILogger logger)
    : IMessageHandler<TMessage>
    where TInstance : class, ISagaInstance, new()
{
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

        // Execute behavior chain
        var sagaContext = new SagaConsumeContext<TInstance, TMessage>(
            instance, message, context, bus, ct);

        var previousVersion = instance.Version;
        instance.LastModifiedUtc = DateTime.UtcNow;

        foreach (var activity in behavior)
            await activity.ExecuteAsync(sagaContext);

        // Check composite events
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
                        composite.EventName, correlationId);
                    await compositeBehavior(instance, bus, context, ct);
                }
            }
        }

        // Persist
        instance.Version++;
        if (isNew)
            await repository.InsertAsync(instance, ct);
        else
            await repository.UpdateAsync(instance, previousVersion, ct);

        // Auto-purge if completed
        if (stateMachine.IsCompleted(instance))
        {
            logger.LogDebug("Saga {CorrelationId} completed, deleting instance", correlationId);
            await repository.DeleteAsync(correlationId, ct);
        }
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
