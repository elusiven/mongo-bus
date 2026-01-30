using MongoBus.Abstractions;
using MongoBus.Models;

namespace MongoBus.Internal;

internal sealed record DispatchRegistration(
    string EndpointId,
    string TypeId,
    Type MessageClrType,
    Type HandlerInterface,
    Func<object, object, ConsumeContext, CancellationToken, Task> HandlerDelegate);

internal sealed record BatchDispatchRegistration(
    string EndpointId,
    string TypeId,
    Type MessageClrType,
    Type HandlerInterface,
    IBatchGroupingStrategy GroupingStrategy,
    BatchFailureMode FailureMode,
    Func<object, object, BatchConsumeContext, CancellationToken, Task> HandlerDelegate);

internal sealed record EndpointRuntimeConfig(
    string EndpointId,
    int Concurrency,
    int Prefetch,
    TimeSpan LockTime,
    int MaxAttempts,
    bool IdempotencyEnabled,
    IReadOnlyCollection<string> TypeIds);

internal sealed record BatchRuntimeConfig(
    string EndpointId,
    string TypeId,
    int Concurrency,
    TimeSpan LockTime,
    int MaxAttempts,
    bool IdempotencyEnabled,
    BatchConsumerOptions Options);