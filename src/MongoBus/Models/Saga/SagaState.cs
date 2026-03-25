namespace MongoBus.Models.Saga;

/// <summary>
/// Represents a named state in a saga state machine.
/// States are automatically initialized by the MongoBusStateMachine base class.
/// </summary>
public sealed class SagaState
{
    public string Name { get; }

    internal SagaState(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        Name = name;
    }

    public override string ToString() => Name;

    public override bool Equals(object? obj) => obj is SagaState other && Name == other.Name;

    public override int GetHashCode() => Name.GetHashCode();
}
