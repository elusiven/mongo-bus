namespace MongoBus.Models.Saga;

/// <summary>
/// Represents a typed event in a saga state machine, bound to a specific message type.
/// </summary>
public sealed class SagaEvent<TMessage>
{
    public string Name { get; }
    public string TypeId { get; }

    internal EventCorrelation<TMessage> Correlation { get; set; }

    internal SagaEvent(string name, string typeId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(typeId);
        Name = name;
        TypeId = typeId;
        Correlation = new EventCorrelation<TMessage>();
    }

    public override string ToString() => $"{Name} ({TypeId})";
}

/// <summary>
/// Represents a non-generic event in a saga state machine (used for composite events).
/// </summary>
public sealed class SagaEvent
{
    public string Name { get; }

    internal SagaEvent(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        Name = name;
    }

    public override string ToString() => Name;
}

/// <summary>
/// Holds correlation configuration for a saga event.
/// </summary>
public sealed class EventCorrelation<TMessage>
{
    /// <summary>
    /// Expression to extract the correlation ID from the consume context.
    /// If null, the default CloudEvent correlationId is used.
    /// </summary>
    internal Func<MongoBus.Models.ConsumeContext, string>? CorrelateByIdExpression { get; set; }

    /// <summary>
    /// Expression to match an instance property to a message value (for query-based correlation).
    /// </summary>
    internal CorrelateByConfig? CorrelateByProperty { get; set; }

    /// <summary>
    /// Expression to generate a new correlation ID for initial events.
    /// </summary>
    internal Func<MongoBus.Models.ConsumeContext, string>? SelectIdExpression { get; set; }

    /// <summary>
    /// Missing instance behavior configuration.
    /// </summary>
    internal MissingInstanceBehavior? MissingInstanceBehavior { get; set; }

    /// <summary>
    /// Per-event partition configuration.
    /// </summary>
    internal PartitionConfig? Partition { get; set; }
}

/// <summary>
/// Configuration for property-based correlation.
/// </summary>
public sealed class CorrelateByConfig
{
    internal string InstancePropertyName { get; set; } = default!;
    internal Func<MongoBus.Models.ConsumeContext, string> MessageValueSelector { get; set; } = default!;
}

/// <summary>
/// Configuration for missing instance behavior.
/// </summary>
public sealed class MissingInstanceBehavior
{
    internal MissingInstanceAction Action { get; set; }
    internal Func<MongoBus.Models.ConsumeContext, Task>? AsyncHandler { get; set; }
}

/// <summary>
/// The action to take when an event is received but no matching saga instance exists.
/// </summary>
public enum MissingInstanceAction
{
    Fault,
    Discard,
    Execute
}

/// <summary>
/// Per-event partition configuration.
/// </summary>
public sealed class PartitionConfig
{
    internal int PartitionCount { get; set; }
    internal Func<MongoBus.Models.ConsumeContext, string> KeySelector { get; set; } = default!;
}
