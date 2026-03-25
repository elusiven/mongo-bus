namespace MongoBus.Abstractions.Saga;

/// <summary>
/// Base interface for saga state machine instances.
/// All saga state classes must implement this interface.
/// </summary>
public interface ISagaInstance
{
    /// <summary>
    /// Unique identifier for this saga instance, used for event correlation.
    /// Uses string to maintain consistency with CloudEvents correlation.
    /// </summary>
    string CorrelationId { get; set; }

    /// <summary>
    /// The current state name of this saga instance.
    /// </summary>
    string CurrentState { get; set; }

    /// <summary>
    /// Optimistic concurrency version. Incremented on every state transition.
    /// Used by the repository to detect concurrent modifications.
    /// </summary>
    int Version { get; set; }

    /// <summary>
    /// Timestamp when this saga instance was created.
    /// </summary>
    DateTime CreatedUtc { get; set; }

    /// <summary>
    /// Timestamp when this saga instance was last modified.
    /// Used for TTL-based cleanup of stuck sagas.
    /// </summary>
    DateTime LastModifiedUtc { get; set; }
}
