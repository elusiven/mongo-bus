namespace MongoBus.Models.Saga;

/// <summary>
/// Represents a schedule declaration on a saga state machine.
/// Schedules use delayed delivery to deliver timeout messages after a configured delay.
/// </summary>
public sealed class SagaSchedule<TInstance, TTimeout>
    where TInstance : class, MongoBus.Abstractions.Saga.ISagaInstance
{
    public string Name { get; }
    public string TypeId { get; }
    public TimeSpan Delay { get; internal set; }

    /// <summary>
    /// The event that is raised when the scheduled message is received.
    /// </summary>
    public SagaEvent<TTimeout> Received { get; internal set; } = default!;

    internal SagaSchedule(string name, string typeId, TimeSpan delay)
    {
        Name = name;
        TypeId = typeId;
        Delay = delay;
    }
}
