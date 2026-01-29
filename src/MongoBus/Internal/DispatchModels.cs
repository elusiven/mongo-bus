using MongoBus.Models;

namespace MongoBus.Internal;

internal sealed record DispatchRegistration(
    string EndpointId,
    string TypeId,
    Type MessageClrType,
    Type HandlerInterface,
    Func<object, object, ConsumeContext, CancellationToken, Task> HandlerDelegate);

internal sealed record EndpointRuntimeConfig(
    string EndpointId,
    int Concurrency,
    int Prefetch,
    TimeSpan LockTime,
    int MaxAttempts,
    bool IdempotencyEnabled);