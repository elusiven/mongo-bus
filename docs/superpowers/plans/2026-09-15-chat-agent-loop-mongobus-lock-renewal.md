# Consumer Lock Renewal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A single-message consumer can opt in to having its message lock renewed for as long as its handler runs, so a long handler keeps its message and is dispatched once, and a crashed consumer's message is redelivered after its lock lapses.

**Architecture:** `ConsumerDefinition.RenewLock` (default `false`) flows into the endpoint's runtime config. A renewing endpoint's fetch loop gives each lock its own `LockOwner` (`{pumpId}:{lockId}`), so a copy the same pump re-locked while an older copy waited in the channel is distinguishable. Before dispatch the worker re-claims the copy and holds a `MessageLockLease` that extends `LockedUntilUtc` every `LockTime / 3`, guarded on `LockOwner`. The lease cancels the handler's token when the lock is found taken, and a watchdog cancels it `LockTime / 6` before the last confirmed expiry whether or not renewals complete. Outcome writes already match `_id` and `LockOwner` on `main` (PR #26), and a cancelled handler's lock is released through #26's release path.

**Tech Stack:** .NET 10, MongoDB.Driver 3.11.2, xUnit v3, FluentAssertions 8, Testcontainers.MongoDb (`mongo:6.0`, `enableTestCommands=1`).

**Spec:** GenCAD `docs/superpowers/specs/2026-09-15-chat-agent-loop-design.md`, section "mongo-bus prerequisite (its own spec, in that repository)" — on GenCAD branch `feat/chat-agent-loop` (commits 49396a8, 5799242), local path `/home/elusiven/projects/gencad/docs/superpowers/specs/2026-09-15-chat-agent-loop-design.md`. The spec text: "While a handler runs, the runtime extends `LockedUntilUtc` every `LockTime / 3`, guarded on `LockOwner`. If an extension finds the lock taken, the handler's cancellation token is cancelled. Renewal is opt-in per consumer definition. The exact API is decided in the mongo-bus spec; it ships as a `+semver:minor` release (3.1.0)." The spec's PR 0 delivery text asks for Testcontainers tests that "a handler that outlives `LockTime` keeps its message; a competing consumer cannot take a renewed message; when renewal stops, the message is redelivered after the lock lapses; renewing a lock already lost cancels the handler." This plan is where the API is decided.

**Base:** `origin/main` at `5b2f2e0` (Merge pull request #29). Line references below are against that commit. PRs #26 (`InboxOutcomeWriter`), #27 (`BuildConsumeContext(InboxMessage)`), #28 and #29 (saga) are on it.

## Global Constraints

- Files touched:
  - `src/MongoBus/Abstractions/IConsumerDefinition.cs` — the opt-in must be on the definition contract; added as a default interface member so existing implementers (including `SagaConsumerDefinition`) keep compiling.
  - `src/MongoBus/Abstractions/ConsumerDefinition.cs` — the base class consumers override; needs a `virtual RenewLock`.
  - `src/MongoBus/Internal/DispatchModels.cs` — `EndpointRuntimeConfig` must carry the endpoint's renewal setting to the fetch and worker loops.
  - `src/MongoBus/Internal/DispatchRegistrationBuilder.cs` — builds and merges `EndpointRuntimeConfig`; must set `RenewLock` when any definition on the endpoint opts in.
  - `src/MongoBus/Internal/MongoBusConfigValidator.cs` — renewal every `LockTime / 3` needs a floor on `LockTime`, or a tiny lock time floods the inbox with updates.
  - `src/MongoBus/Internal/MongoBusRuntime.cs` — a renewing endpoint's fetch loop must give each lock its own owner, and its worker loop must hold a lease around dispatch.
  - `src/MongoBus/Internal/MessageLockRenewer.cs` (new) — the owner-guarded lock extension query and lease acquisition.
  - `src/MongoBus/Internal/MessageLockLease.cs` (new) — periodic renewal, the watchdog and the lock-lost signal for one dispatch.
  - `tests/MongoBus.Tests/LockRenewalConfigTests.cs` (new) — default, endpoint merge and validation.
  - `tests/MongoBus.Tests/InboxLocks.cs` (new) — test helper shared by the two test classes that seize a message's lock.
  - `tests/MongoBus.Tests/MessageLockRenewerTests.cs` (new) — extension query, lease, watchdog and crash redelivery against MongoDB.
  - `tests/MongoBus.Tests/LockRenewalTests.cs` (new) — lock owners and runtime renewal end to end, on the existing `RunningBus` helper.
  - `README.md` — documents `RenewLock` in the consumer definition example.
  - `src/MongoBus/README.md` — the README packed into the NuGet package (`MongoBus.csproj` `PackageReadmeFile`); the package page must mention the new option.
  - `clients/python/README.md` — its ".NET parity" bullet says .NET consumers do not renew, which this change makes false.
- Out of scope (follow-up, not this PR):
  - Lock renewal for batch consumers (`BatchWorkerLoopAsync`, `MongoBatchMessageDispatcher`) and a `SagaOptions.RenewLock`; GenCAD needs neither.
  - Guarding the idempotent-skip write in `MongoBusRuntime.TrySkipIdempotentAsync` on `LockOwner`.
  - Non-renewing endpoints keep one owner per pump and their pre-existing behaviour: a message whose lock lapses while it waits in the channel can be dispatched twice, and since #26 a handler re-locked by a different replica has its outcome discarded. Lowering the default prefetch (held by the peer session until this PR merges) addresses the first.
  - A lease-lost cancellation is reported through #26's release path, which logs "Stopped while handling message …; releasing it for redelivery." at Information; the lease's own warning is the lock-lost signal. A dedicated report is a follow-up.
  - A lease tick that runs after the dispatcher recorded the outcome but before the lease is disposed finds no owned message and logs a warning; the wording is neutral for that reason.
  - Whether MongoDB.Driver 3.11.2 aborts an in-flight socket read when its token is cancelled is unverified. If it does not, a hung renewal can delay releasing the worker until the driver returns; the watchdog still cancels the handler on time.
  - `ConcurrencyTests.MessageShouldNotBeProcessedByMultipleWorkersSimultaneously` accepts 1 or 2 starts; tightening it is a separate decision.
  - Sharing a single `failCommand` test helper between `ConsumerResilienceTests` and `MessageLockRenewerTests`.
- Dependencies added: none.
- Verification commands (from `.github/workflows/ci.yml`, run at the repository root):
  - `dotnet restore`
  - `dotnet build --no-restore -c Release`
  - `dotnet test --no-build -c Release`
- Risk and rollback:
  - Consumers that do not set `RenewLock` behave exactly as on `main`: same lock owner, same fetch and dispatch path.
  - For renewing endpoints `LockOwner` changes from the pump id to `{pumpId}:{ObjectId}` per lock. Nothing in the repository reads `LockOwner` beyond the lock and outcome writes (checked: dashboard, samples, claim-check providers; the Python client issues its own ids).
  - A renewing consumer writes one `UpdateOne` per in-flight message every `LockTime / 3`; `LockTime` under 1 second is rejected at startup when `RenewLock` is set.
  - When renewals do not succeed, the handler is cancelled `LockTime / 6` before the last confirmed expiry, so it stops before another consumer can take the message; a handler that ignores its token can still overlap a redelivery.
  - Merging to `main` publishes MongoBus 3.1.0 (currently 3.0.x) to nuget.org and GitHub Packages (`deploy.yml`). PRs land as merge commits. Rollback: `git revert -m 1 <merge sha>` on `main`, which publishes the next patch version, and unlist 3.1.0 on nuget.org. No consumer uses 3.1.0 yet — GenCAD pins 1.2.0 until its own upgrade PR.
- Commit message prefix: `chat-agent-loop: `. Task 1's commit message and the PR title both end with `+semver:minor`, so GitVersion bumps the minor version however the merge commit message is written.

## Design

**Chosen:**
1. Opt-in per consumer: `bool RenewLock` on `IConsumerDefinition` as a default interface member returning `false`, overridden via `public virtual bool RenewLock => false;` on `ConsumerDefinition<TConsumer, TMessage>`. An endpoint renews when any of its definitions opts in, matching how `IdempotencyEnabled` merges. `LockTime` must be at least 1 second when `RenewLock` is set.
2. A renewing endpoint's fetch loop gives each lock a unique owner, `{pumpId}:{ObjectId.GenerateNewId()}`. The lock filter matches any pending message whose lock lapsed, including copies the same pump already queued, so a per-endpoint owner cannot tell two copies apart. Non-renewing endpoints keep the pump id as owner.
3. Renewal starts at dispatch. Before dispatch the worker extends the lock once, guarded on the copy's owner; if that fails, the message was re-locked while waiting and this copy is skipped. From then on a `MessageLockLease` extends it every `LockTime / 3` until dispatch returns.
4. The lease's deadline is a watchdog: after the claim and after each successful renewal, `LockLost` is scheduled to cancel `LockTime / 6` before the confirmed expiry, timed from before the write. A renewal that finds the lock taken cancels `LockLost` at once; a renewal that throws is logged and retried on the next interval, and the watchdog decides when to give up. Each attempt runs on a token linked to `LockLost`, and renewal stops once `LockLost` is cancelled.
5. The extension query lives in a new internal `MessageLockRenewer` built by `MongoBusRuntime` from the inbox collection it already holds, keeping the public `IMessagePump` unchanged.
6. Outcome writes are not changed: `InboxOutcomeWriter` (PR #26, `5300957`) already matches `_id` and `LockOwner`, and `MongoMessageDispatcher` releases the lock without an attempt when the handler's token was cancelled — which is the token the lease cancels.

**Rejected:**
- *Renewal for every consumer.* Changes write volume and behaviour for all users of the library; the spec asks for opt-in.
- *A manual `ConsumeContext.RenewLockAsync()`.* Changes a public record and relies on every handler remembering to call it during long waits.
- *A `TryExtendLockAsync` member on `IMessagePump`.* Adds a member to a public interface, breaking custom pump implementations.
- *Per-lock owners for every endpoint.* With #26's owner-matched outcome writes, a non-renewing handler that outlives `LockTime` is re-locked under a new owner before it finishes, every copy's write misses, and the message is redelivered forever without ever being dead-lettered.
- *Renewing from fetch time, covering the channel wait.* Needs a per-pump registry of channel-held messages; per-lock owners plus a re-claim at dispatch give the same single dispatch with far less code.
- *Retrying failed renewals until the confirmed expiry is reached, checked only between attempts.* A slow failure lets the check run up to one interval after expiry, when another replica may already be running the job.
- *Only a longer `LockTime`.* Delays redelivery after a crash by the whole lock time, which the GenCAD spec rejected.

---

### Task 1: `RenewLock` on consumer definitions, endpoint configs and validation

**Files:**
- Modify: `src/MongoBus/Abstractions/IConsumerDefinition.cs`
- Modify: `src/MongoBus/Abstractions/ConsumerDefinition.cs`
- Modify: `src/MongoBus/Internal/DispatchModels.cs:23-30`
- Modify: `src/MongoBus/Internal/DispatchRegistrationBuilder.cs:47-57,105-113`
- Modify: `src/MongoBus/Internal/MongoBusConfigValidator.cs:51-70`
- Test: `tests/MongoBus.Tests/LockRenewalConfigTests.cs`
- Also commit: `docs/superpowers/plans/2026-09-15-chat-agent-loop-mongobus-lock-renewal.md` (this plan, not yet committed)

**Interfaces:**
- Consumes: nothing.
- Produces: `bool IConsumerDefinition.RenewLock { get; }` (default `false`); `public virtual bool RenewLock` on `ConsumerDefinition<TConsumer, TMessage>`; `EndpointRuntimeConfig(string EndpointId, int Concurrency, int Prefetch, TimeSpan LockTime, int MaxAttempts, bool IdempotencyEnabled, bool RenewLock, IReadOnlyCollection<string> TypeIds)`.

- [ ] **Step 1: Write the failing test**

Create `tests/MongoBus.Tests/LockRenewalConfigTests.cs`:

```csharp
using FluentAssertions;
using MongoBus.Abstractions;
using MongoBus.Internal;
using MongoBus.Models;
using Xunit;

namespace MongoBus.Tests;

public class LockRenewalConfigTests
{
    public sealed class RenewedMessage { }

    public sealed class PlainMessage { }

    public sealed class RenewedHandler : IMessageHandler<RenewedMessage>
    {
        public Task HandleAsync(RenewedMessage message, ConsumeContext context, CancellationToken ct) => Task.CompletedTask;
    }

    public sealed class PlainHandler : IMessageHandler<PlainMessage>
    {
        public Task HandleAsync(PlainMessage message, ConsumeContext context, CancellationToken ct) => Task.CompletedTask;
    }

    public sealed class RenewedDefinition : ConsumerDefinition<RenewedHandler, RenewedMessage>
    {
        public override string TypeId => "renewal.config.renewed";
        public override string EndpointName => "renewal-config-endpoint";
        public override bool RenewLock => true;
    }

    public sealed class PlainDefinition : ConsumerDefinition<PlainHandler, PlainMessage>
    {
        public override string TypeId => "renewal.config.plain";
        public override string EndpointName => "renewal-config-endpoint";
    }

    public sealed class ShortRenewedDefinition : ConsumerDefinition<RenewedHandler, RenewedMessage>
    {
        public override string TypeId => "renewal.config.short";
        public override TimeSpan LockTime => TimeSpan.FromMilliseconds(500);
        public override bool RenewLock => true;
    }

    public sealed class OneSecondRenewedDefinition : ConsumerDefinition<RenewedHandler, RenewedMessage>
    {
        public override string TypeId => "renewal.config.one-second";
        public override TimeSpan LockTime => TimeSpan.FromSeconds(1);
        public override bool RenewLock => true;
    }

    [Fact]
    public void ConsumerDefinition_DoesNotRenewLocksByDefault()
    {
        new PlainDefinition().RenewLock.Should().BeFalse();
    }

    [Fact]
    public void Endpoint_RenewsLocks_WhenAnyDefinitionOnItOptsIn()
    {
        var configs = DispatchRegistrationBuilder.BuildEndpointConfigs(
            new IConsumerDefinition[] { new PlainDefinition(), new RenewedDefinition() });

        configs["renewal-config-endpoint"].RenewLock.Should().BeTrue();
    }

    [Fact]
    public void Endpoint_DoesNotRenewLocks_WhenNoDefinitionOptsIn()
    {
        var configs = DispatchRegistrationBuilder.BuildEndpointConfigs(
            new IConsumerDefinition[] { new PlainDefinition() });

        configs["renewal-config-endpoint"].RenewLock.Should().BeFalse();
    }

    [Fact]
    public void RenewingDefinition_WithLockTimeUnderOneSecond_IsRejected()
    {
        var validate = () => MongoBusConfigValidator.ValidateDefinitions(
            new IConsumerDefinition[] { new ShortRenewedDefinition() });

        validate.Should().Throw<InvalidOperationException>().WithMessage("*RenewLock*");
    }

    [Fact]
    public void RenewingDefinition_WithOneSecondLockTime_IsAccepted()
    {
        var validate = () => MongoBusConfigValidator.ValidateDefinitions(
            new IConsumerDefinition[] { new OneSecondRenewedDefinition() });

        validate.Should().NotThrow();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet build tests/MongoBus.Tests -c Release`
Expected: FAIL — `CS0115: 'LockRenewalConfigTests.RenewedDefinition.RenewLock': no suitable method found to override` and `CS1061` for `EndpointRuntimeConfig.RenewLock`.

- [ ] **Step 3: Write minimal implementation**

`src/MongoBus/Abstractions/IConsumerDefinition.cs` — add after `bool IdempotencyEnabled { get; }`:

```csharp
    /// <summary>
    /// When true, the lock on a message is extended for as long as its handler runs, and the handler's
    /// cancellation token is cancelled if the lock is lost. Applies to single-message consumers; batch
    /// consumers do not renew. Requires a <see cref="LockTime"/> of at least one second.
    /// </summary>
    bool RenewLock => false;
```

`src/MongoBus/Abstractions/ConsumerDefinition.cs` — add after `public virtual bool IdempotencyEnabled => false;`:

```csharp
    public virtual bool RenewLock => false;
```

`src/MongoBus/Internal/DispatchModels.cs` — `EndpointRuntimeConfig` becomes:

```csharp
internal sealed record EndpointRuntimeConfig(
    string EndpointId,
    int Concurrency,
    int Prefetch,
    TimeSpan LockTime,
    int MaxAttempts,
    bool IdempotencyEnabled,
    bool RenewLock,
    IReadOnlyCollection<string> TypeIds);
```

`src/MongoBus/Internal/DispatchRegistrationBuilder.cs` — in the merge inside `BuildEndpointConfigs`, after the `IdempotencyEnabled` line:

```csharp
                RenewLock = existing.RenewLock || def.RenewLock,
```

and `CreateEndpointConfig` becomes:

```csharp
    private static EndpointRuntimeConfig CreateEndpointConfig(IConsumerDefinition def) =>
        new(
            def.EndpointName,
            Math.Max(1, def.ConcurrencyLimit),
            Math.Max(def.PrefetchCount, def.ConcurrencyLimit),
            def.LockTime,
            def.MaxAttempts,
            def.IdempotencyEnabled,
            def.RenewLock,
            new[] { def.TypeId });
```

`src/MongoBus/Internal/MongoBusConfigValidator.cs` — add a field at the top of the class:

```csharp
    private static readonly TimeSpan MinimumRenewedLockTime = TimeSpan.FromSeconds(1);
```

and in `ValidateDefinition`, directly after the `def.LockTime <= TimeSpan.Zero` check:

```csharp
        if (def.RenewLock && def.LockTime < MinimumRenewedLockTime)
            throw new InvalidOperationException(
                $"Consumer '{def.ConsumerType.Name}' LockTime must be at least 1 second when RenewLock is enabled.");
```

- [ ] **Step 4: Run test to verify it passes**

Run: `dotnet build -c Release && dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.LockRenewalConfigTests"`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add src/MongoBus/Abstractions/IConsumerDefinition.cs src/MongoBus/Abstractions/ConsumerDefinition.cs src/MongoBus/Internal/DispatchModels.cs src/MongoBus/Internal/DispatchRegistrationBuilder.cs src/MongoBus/Internal/MongoBusConfigValidator.cs tests/MongoBus.Tests/LockRenewalConfigTests.cs docs/superpowers/plans/2026-09-15-chat-agent-loop-mongobus-lock-renewal.md
git commit -m "chat-agent-loop: let consumer definitions opt in to lock renewal +semver:minor"
```

### Task 2: A renewing endpoint gives each lock its own owner

**Files:**
- Modify: `src/MongoBus/Internal/MongoBusRuntime.cs:1-9,99-102`
- Test: `tests/MongoBus.Tests/LockRenewalTests.cs`

**Interfaces:**
- Consumes: `EndpointRuntimeConfig.RenewLock` (Task 1); `RunningBus.StartAsync(string connectionString, Action<MongoBusOptions>? configureOptions, Action<IServiceCollection>? registerServices)` (existing test helper).
- Produces: in `LockRenewalTests`, the private helpers `StartBusAsync(string databaseName, Action<IServiceCollection> registerConsumer)` → `Task<RunningBus>`, `PublisherOf(RunningBus bus)` → `IMessageBus`, `InboxOf(RunningBus bus)` → `IMongoCollection<InboxMessage>`, `NewDatabaseName()`, `NewSignal()` → `TaskCompletionSource`, `WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)`; used again in Task 5. Renewing endpoints' lock owners have the form `{pumpId}:{ObjectId}`.

- [ ] **Step 1: Write the failing test**

Create `tests/MongoBus.Tests/LockRenewalTests.cs`:

```csharp
using System.Collections.Concurrent;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Infrastructure;
using MongoBus.Internal;
using MongoBus.Models;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class LockRenewalTests(MongoDbFixture fixture)
{
    public sealed class HeldMessage
    {
        public string Name { get; set; } = "";
    }

    public sealed class HeldHandler : IMessageHandler<HeldMessage>
    {
        public static ConcurrentQueue<string> Started = new();
        public static TaskCompletionSource Release = NewSignal();

        public static void Reset()
        {
            Started = new ConcurrentQueue<string>();
            Release = NewSignal();
        }

        public async Task HandleAsync(HeldMessage message, ConsumeContext context, CancellationToken ct)
        {
            Started.Enqueue(message.Name);
            await Release.Task.WaitAsync(ct);
        }
    }

    public sealed class HeldDefinition : ConsumerDefinition<HeldHandler, HeldMessage>
    {
        public override string TypeId => "renewal.held";
        public override int ConcurrencyLimit => 2;
        public override TimeSpan LockTime => TimeSpan.FromSeconds(30);
        public override bool RenewLock => true;
    }

    [Fact]
    public async Task EachLockTakenByARenewingEndpoint_HasItsOwnOwner()
    {
        HeldHandler.Reset();
        await using var bus = await StartBusAsync(
            NewDatabaseName(),
            services => services.AddMongoBusConsumer<HeldHandler, HeldMessage, HeldDefinition>());

        await PublisherOf(bus).PublishAsync("renewal.held", new HeldMessage { Name = "a" }, "test-source");
        await PublisherOf(bus).PublishAsync("renewal.held", new HeldMessage { Name = "b" }, "test-source");
        await WaitUntilAsync(() => Task.FromResult(HeldHandler.Started.Count == 2), TimeSpan.FromSeconds(10));

        var owners = await InboxOf(bus).Find(x => x.TypeId == "renewal.held").Project(x => x.LockOwner).ToListAsync();
        HeldHandler.Release.TrySetResult();

        owners.Should().HaveCount(2).And.NotContainNulls().And.OnlyHaveUniqueItems();
    }

    public sealed class SlowPlainMessage { }

    public sealed class SlowPlainHandler : IMessageHandler<SlowPlainMessage>
    {
        public static int Starts;

        public async Task HandleAsync(SlowPlainMessage message, ConsumeContext context, CancellationToken ct)
        {
            Interlocked.Increment(ref Starts);
            await Task.Delay(TimeSpan.FromSeconds(2.5), ct);
        }
    }

    public sealed class SlowPlainDefinition : ConsumerDefinition<SlowPlainHandler, SlowPlainMessage>
    {
        public override string TypeId => "renewal.slow-plain";
        public override TimeSpan LockTime => TimeSpan.FromSeconds(1);
    }

    /// <summary>
    /// Guards the opt-in: a consumer that does not renew keeps one owner per pump, so the first of its overlapping
    /// copies to finish still records the outcome. Per-lock owners here would redeliver the message forever.
    /// </summary>
    [Fact]
    public async Task NonRenewingHandlerOutlivingLockTime_StillGetsItsMessageProcessed()
    {
        Interlocked.Exchange(ref SlowPlainHandler.Starts, 0);
        await using var bus = await StartBusAsync(
            NewDatabaseName(),
            services => services.AddMongoBusConsumer<SlowPlainHandler, SlowPlainMessage, SlowPlainDefinition>());

        await PublisherOf(bus).PublishAsync("renewal.slow-plain", new SlowPlainMessage(), "test-source");
        await WaitUntilAsync(
            async () => await InboxOf(bus).CountDocumentsAsync(x => x.TypeId == "renewal.slow-plain" && x.Status == InboxStatus.Processed) == 1,
            TimeSpan.FromSeconds(15));

        SlowPlainHandler.Starts.Should().BeLessThanOrEqualTo(5);
    }

    private Task<RunningBus> StartBusAsync(string databaseName, Action<IServiceCollection> registerConsumer) =>
        RunningBus.StartAsync(fixture.ConnectionString, options => options.DatabaseName = databaseName, registerConsumer);

    private static IMessageBus PublisherOf(RunningBus bus) => bus.Services.GetRequiredService<IMessageBus>();

    private static IMongoCollection<InboxMessage> InboxOf(RunningBus bus) =>
        bus.Database.GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);

    private static string NewDatabaseName() => "lock_renewal_" + Guid.NewGuid().ToString("N");

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("The condition was not met in time.");
            await Task.Delay(50);
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet build -c Release && dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.LockRenewalTests"`
Expected: `EachLockTakenByARenewingEndpoint_HasItsOwnOwner` FAILS — both owners equal the endpoint's pump id, so `OnlyHaveUniqueItems` fails. `NonRenewingHandlerOutlivingLockTime_StillGetsItsMessageProcessed` PASSES: it is a characterization test of `main`'s behaviour that must stay green after the change.

- [ ] **Step 3: Write minimal implementation**

In `src/MongoBus/Internal/MongoBusRuntime.cs` add `using MongoDB.Bson;`. In `FetchLoopAsync` the local function at line 102 becomes:

```csharp
        Task<InboxMessage?> LockNext() => _pump.TryLockOneAsync(cfg.EndpointId, cfg.TypeIds, cfg.LockTime, LockOwnerFor(cfg, pumpId), ct);
```

and add below `FetchLoopAsync`:

```csharp
    /// <summary>
    /// A renewing endpoint gives each lock its own owner: its fetch loop can re-lock a message whose lock lapsed while an
    /// older copy still waits in the channel, and distinct owners let the dispatch-time re-claim skip the stale copy.
    /// Other endpoints keep the pump id, so the first of their overlapping copies to finish still records the outcome.
    /// </summary>
    private static string LockOwnerFor(EndpointRuntimeConfig cfg, string pumpId) =>
        cfg.RenewLock ? $"{pumpId}:{ObjectId.GenerateNewId()}" : pumpId;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `dotnet build -c Release && dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.LockRenewalTests|FullyQualifiedName~MongoBus.Tests.ConcurrencyTests|FullyQualifiedName~MongoBus.Tests.ConsumerResilienceTests|FullyQualifiedName~MongoBus.Tests.ConsumerStateWriteTests"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/MongoBus/Internal/MongoBusRuntime.cs tests/MongoBus.Tests/LockRenewalTests.cs
git commit -m "chat-agent-loop: give each lock a renewing endpoint takes its own owner"
```

### Task 3: Owner-guarded lock extension (`MessageLockRenewer.TryExtendAsync`)

**Files:**
- Create: `src/MongoBus/Internal/MessageLockRenewer.cs`
- Create: `tests/MongoBus.Tests/InboxLocks.cs`
- Test: `tests/MongoBus.Tests/MessageLockRenewerTests.cs`

**Interfaces:**
- Consumes: `InboxMessage` (`Id`, `LockOwner`, `Status`, `LockedUntilUtc`), `InboxStatus.Pending`, `MongoBusConstants.InboxCollectionName`.
- Produces: `internal sealed class MessageLockRenewer(IMongoCollection<InboxMessage> inbox, ILogger log)` with `Task<bool> TryExtendAsync(InboxMessage message, TimeSpan lockTime, CancellationToken ct)`. Test helper `InboxLocks.OtherOwner` (`"another-consumer"`) and `InboxLocks.TakeLockAsync(IMongoCollection<InboxMessage> inbox, ObjectId messageId)`. In `MessageLockRenewerTests`: `NewDatabaseName()`, `InboxIn(string databaseName, string? applicationName = null)`, `NewRenewer(...)`, `InsertLockedAsync(...)`, `ReloadAsync(...)`, used again in Task 4.

- [ ] **Step 1: Write the failing test**

Create `tests/MongoBus.Tests/InboxLocks.cs`:

```csharp
using MongoBus.Infrastructure;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoBus.Tests;

internal static class InboxLocks
{
    public const string OtherOwner = "another-consumer";

    /// <summary>Seizes a message's lock the way a competing consumer would after the lock expired.</summary>
    public static Task TakeLockAsync(IMongoCollection<InboxMessage> inbox, ObjectId messageId) =>
        inbox.UpdateOneAsync(
            x => x.Id == messageId,
            Builders<InboxMessage>.Update
                .Set(x => x.LockOwner, OtherOwner)
                .Set(x => x.LockedUntilUtc, DateTime.UtcNow.AddMinutes(5)));
}
```

Create `tests/MongoBus.Tests/MessageLockRenewerTests.cs`:

```csharp
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using MongoBus.Infrastructure;
using MongoBus.Internal;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoBus.Tests;

[Collection("Mongo collection")]
public class MessageLockRenewerTests(MongoDbFixture fixture)
{
    private const string ThisConsumer = "this-consumer";
    private const string EndpointId = "lock-renewer-endpoint";

    [Fact]
    public async Task TryExtend_MovesTheLockForward_WhenThisConsumerStillOwnsIt()
    {
        var inbox = InboxIn(NewDatabaseName());
        var message = await InsertLockedAsync(inbox, TimeSpan.FromSeconds(1));

        var extended = await NewRenewer(inbox).TryExtendAsync(message, TimeSpan.FromSeconds(30), CancellationToken.None);

        extended.Should().BeTrue();
        (await ReloadAsync(inbox, message)).LockedUntilUtc.Should().BeAfter(DateTime.UtcNow.AddSeconds(25));
    }

    [Fact]
    public async Task TryExtend_LeavesTheLockAlone_WhenAnotherConsumerTookIt()
    {
        var inbox = InboxIn(NewDatabaseName());
        var message = await InsertLockedAsync(inbox, TimeSpan.FromSeconds(1));
        await InboxLocks.TakeLockAsync(inbox, message.Id);

        var extended = await NewRenewer(inbox).TryExtendAsync(message, TimeSpan.FromSeconds(30), CancellationToken.None);

        extended.Should().BeFalse();
        (await ReloadAsync(inbox, message)).LockOwner.Should().Be(InboxLocks.OtherOwner);
    }

    [Fact]
    public async Task TryExtend_ReportsFailure_WhenTheMessageIsNoLongerPending()
    {
        var inbox = InboxIn(NewDatabaseName());
        var message = await InsertLockedAsync(inbox, TimeSpan.FromSeconds(1));
        await inbox.UpdateOneAsync(
            x => x.Id == message.Id,
            Builders<InboxMessage>.Update.Set(x => x.Status, InboxStatus.Processed));

        var extended = await NewRenewer(inbox).TryExtendAsync(message, TimeSpan.FromSeconds(30), CancellationToken.None);

        extended.Should().BeFalse();
    }

    private static string NewDatabaseName() => "lock_renewer_" + Guid.NewGuid().ToString("N");

    private IMongoCollection<InboxMessage> InboxIn(string databaseName, string? applicationName = null) =>
        new MongoClient(new MongoUrlBuilder(fixture.ConnectionString) { ApplicationName = applicationName }.ToString())
            .GetDatabase(databaseName)
            .GetCollection<InboxMessage>(MongoBusConstants.InboxCollectionName);

    private static MessageLockRenewer NewRenewer(IMongoCollection<InboxMessage> inbox) =>
        new(inbox, NullLogger.Instance);

    private static async Task<InboxMessage> InsertLockedAsync(IMongoCollection<InboxMessage> inbox, TimeSpan lockTime)
    {
        var now = DateTime.UtcNow;
        var message = new InboxMessage
        {
            Id = ObjectId.GenerateNewId(),
            EndpointId = EndpointId,
            Topic = "lock.renewer",
            TypeId = "lock.renewer",
            PayloadJson = "{}",
            CreatedUtc = now,
            VisibleUtc = now,
            LockOwner = ThisConsumer,
            LockedUntilUtc = now.Add(lockTime),
            Status = InboxStatus.Pending
        };
        await inbox.InsertOneAsync(message);
        return message;
    }

    private static Task<InboxMessage> ReloadAsync(IMongoCollection<InboxMessage> inbox, InboxMessage message) =>
        inbox.Find(x => x.Id == message.Id).SingleAsync();
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet build tests/MongoBus.Tests -c Release`
Expected: FAIL — `CS0246: The type or namespace name 'MessageLockRenewer' could not be found`.

- [ ] **Step 3: Write minimal implementation**

Create `src/MongoBus/Internal/MessageLockRenewer.cs`:

```csharp
using Microsoft.Extensions.Logging;
using MongoBus.Infrastructure;
using MongoDB.Driver;

namespace MongoBus.Internal;

internal sealed class MessageLockRenewer(IMongoCollection<InboxMessage> inbox, ILogger log)
{
    /// <summary>
    /// Pushes the lock's expiry forward, but only while this delivery still owns the pending message.
    /// Returns false when the message was re-locked or is no longer pending.
    /// </summary>
    public async Task<bool> TryExtendAsync(InboxMessage message, TimeSpan lockTime, CancellationToken ct)
    {
        var result = await inbox.UpdateOneAsync(
            x => x.Id == message.Id && x.LockOwner == message.LockOwner && x.Status == InboxStatus.Pending,
            Builders<InboxMessage>.Update.Set(x => x.LockedUntilUtc, DateTime.UtcNow.Add(lockTime)),
            cancellationToken: ct);

        return result.MatchedCount == 1;
    }
}
```

(`log` is read by Task 4's `TryAcquireLeaseAsync`; it is in the constructor now so the tests' construction does not change between tasks. Until Task 4 the compiler reports it as unread — a warning, not an error, in this repository.)

- [ ] **Step 4: Run test to verify it passes**

Run: `dotnet build -c Release && dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.MessageLockRenewerTests"`
Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

```bash
git add src/MongoBus/Internal/MessageLockRenewer.cs tests/MongoBus.Tests/InboxLocks.cs tests/MongoBus.Tests/MessageLockRenewerTests.cs
git commit -m "chat-agent-loop: extend a message lock only while its delivery owns it"
```

### Task 4: `MessageLockLease` — renewal, watchdog and the lock-lost signal

**Files:**
- Create: `src/MongoBus/Internal/MessageLockLease.cs`
- Modify: `src/MongoBus/Internal/MessageLockRenewer.cs`
- Test: `tests/MongoBus.Tests/MessageLockRenewerTests.cs`

**Interfaces:**
- Consumes: `MessageLockRenewer.TryExtendAsync` and the `MessageLockRenewerTests` helpers (Task 3).
- Produces: `Task<MessageLockLease?> MessageLockRenewer.TryAcquireLeaseAsync(InboxMessage message, TimeSpan lockTime, CancellationToken ct)`; `internal sealed class MessageLockLease : IAsyncDisposable` with `CancellationToken LockLost { get; }`.

- [ ] **Step 1: Write the failing tests**

Add to `MessageLockRenewerTests`:

```csharp
    private static readonly TimeSpan LeaseLockTime = TimeSpan.FromSeconds(3);

    [Fact]
    public async Task Lease_KeepsTheLockAlive_ForLongerThanLockTime()
    {
        var inbox = InboxIn(NewDatabaseName());
        var message = await InsertLockedAsync(inbox, LeaseLockTime);

        await using var lease = await NewRenewer(inbox).TryAcquireLeaseAsync(message, LeaseLockTime, CancellationToken.None);
        await Task.Delay(TimeSpan.FromSeconds(7));

        lease.Should().NotBeNull();
        lease!.LockLost.IsCancellationRequested.Should().BeFalse();
        (await ReloadAsync(inbox, message)).LockedUntilUtc.Should().BeAfter(DateTime.UtcNow);
    }

    [Fact]
    public async Task Lease_SignalsLockLost_WhenAnotherConsumerTakesTheLock()
    {
        var inbox = InboxIn(NewDatabaseName());
        var message = await InsertLockedAsync(inbox, LeaseLockTime);

        await using var lease = await NewRenewer(inbox).TryAcquireLeaseAsync(message, LeaseLockTime, CancellationToken.None);
        await InboxLocks.TakeLockAsync(inbox, message.Id);
        var signalled = await WaitForCancellationAsync(lease!.LockLost, TimeSpan.FromSeconds(5));

        signalled.Should().BeTrue();
        (await ReloadAsync(inbox, message)).LockOwner.Should().Be(InboxLocks.OtherOwner);
    }

    [Fact]
    public async Task TryAcquireLease_ReturnsNoLease_WhenTheLockIsAlreadyTaken()
    {
        var inbox = InboxIn(NewDatabaseName());
        var message = await InsertLockedAsync(inbox, TimeSpan.FromSeconds(30));
        await InboxLocks.TakeLockAsync(inbox, message.Id);

        var lease = await NewRenewer(inbox).TryAcquireLeaseAsync(message, TimeSpan.FromSeconds(30), CancellationToken.None);

        lease.Should().BeNull();
    }

    [Fact]
    public async Task MessageWhoseRenewalStoppedWithoutARelease_IsLockableByAnotherPumpOnlyAfterItsLockLapses()
    {
        var databaseName = NewDatabaseName();
        var inbox = InboxIn(databaseName);
        var message = await InsertLockedAsync(inbox, LeaseLockTime);
        var pump = new MongoMessagePump(new MongoClient(fixture.ConnectionString).GetDatabase(databaseName));

        var lease = await NewRenewer(inbox).TryAcquireLeaseAsync(message, LeaseLockTime, CancellationToken.None);
        await Task.Delay(TimeSpan.FromSeconds(4));
        await lease!.DisposeAsync();

        var beforeLapse = await pump.TryLockOneAsync(EndpointId, LeaseLockTime, "another-pump", CancellationToken.None);
        await Task.Delay(LeaseLockTime);
        var afterLapse = await pump.TryLockOneAsync(EndpointId, LeaseLockTime, "another-pump", CancellationToken.None);

        beforeLapse.Should().BeNull();
        afterLapse.Should().NotBeNull();
        afterLapse!.Id.Should().Be(message.Id);
    }

    [Fact]
    public async Task Lease_KeepsTheLock_WhenOneRenewalFails()
    {
        var databaseName = NewDatabaseName();
        var applicationName = NewApplicationName();
        var inbox = InboxIn(databaseName, applicationName);
        var message = await InsertLockedAsync(inbox, LeaseLockTime);

        await using var lease = await NewRenewer(inbox).TryAcquireLeaseAsync(message, LeaseLockTime, CancellationToken.None);
        await using var failures = await UpdateFailures.InjectAsync(
            fixture.ConnectionString, applicationName, new BsonDocument("times", 1));
        await Task.Delay(TimeSpan.FromSeconds(6));

        lease!.LockLost.IsCancellationRequested.Should().BeFalse();
        (await ReloadAsync(InboxIn(databaseName), message)).LockedUntilUtc.Should().BeAfter(DateTime.UtcNow);
    }

    [Fact]
    public async Task Lease_SignalsLockLostBeforeTheLockExpires_WhenRenewalsKeepFailing()
    {
        var databaseName = NewDatabaseName();
        var applicationName = NewApplicationName();
        var inbox = InboxIn(databaseName, applicationName);
        var message = await InsertLockedAsync(inbox, LeaseLockTime);

        await using var lease = await NewRenewer(inbox).TryAcquireLeaseAsync(message, LeaseLockTime, CancellationToken.None);
        await using var failures = await UpdateFailures.InjectAsync(fixture.ConnectionString, applicationName, "alwaysOn");
        var expiry = (await ReloadAsync(InboxIn(databaseName), message)).LockedUntilUtc!.Value;

        (await CancellationTimeAsync(lease!.LockLost, TimeSpan.FromSeconds(8))).Should().BeBefore(expiry);
    }

    [Fact]
    public async Task Lease_SignalsLockLostBeforeTheLockExpires_WhenARenewalHangs()
    {
        var databaseName = NewDatabaseName();
        var applicationName = NewApplicationName();
        var inbox = InboxIn(databaseName, applicationName);
        var message = await InsertLockedAsync(inbox, LeaseLockTime);

        await using var lease = await NewRenewer(inbox).TryAcquireLeaseAsync(message, LeaseLockTime, CancellationToken.None);
        await using var failures = await UpdateFailures.InjectAsync(
            fixture.ConnectionString, applicationName, "alwaysOn", blockMilliseconds: 10_000);
        var expiry = (await ReloadAsync(InboxIn(databaseName), message)).LockedUntilUtc!.Value;

        (await CancellationTimeAsync(lease!.LockLost, TimeSpan.FromSeconds(8))).Should().BeBefore(expiry);
    }

    private static string NewApplicationName() => "lock-renewer-" + Guid.NewGuid().ToString("N");

    private static async Task<bool> WaitForCancellationAsync(CancellationToken token, TimeSpan timeout)
    {
        try
        {
            await Task.Delay(timeout, token);
            return false;
        }
        catch (OperationCanceledException)
        {
            return true;
        }
    }

    /// <summary>When <paramref name="token"/> was cancelled; throws <see cref="TimeoutException"/> if it was not.</summary>
    private static async Task<DateTime> CancellationTimeAsync(CancellationToken token, TimeSpan timeout)
    {
        var cancelledAt = new TaskCompletionSource<DateTime>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var registration = token.Register(() => cancelledAt.TrySetResult(DateTime.UtcNow));
        return await cancelledAt.Task.WaitAsync(timeout);
    }

    /// <summary>
    /// Makes MongoDB reject update commands from one application until disposed, optionally holding each rejected
    /// command's connection first. A plain command error is used, as in <c>ConsumerResilienceTests</c>, so the driver
    /// does not mark the server unknown.
    /// </summary>
    private sealed class UpdateFailures(IMongoDatabase admin) : IAsyncDisposable
    {
        private const int BadValue = 2;

        public static async Task<UpdateFailures> InjectAsync(
            string connectionString, string applicationName, BsonValue mode, int? blockMilliseconds = null)
        {
            var data = new BsonDocument
            {
                ["failCommands"] = new BsonArray { "update" },
                ["errorCode"] = BadValue,
                ["appName"] = applicationName
            };
            if (blockMilliseconds is { } block)
            {
                data["blockConnection"] = true;
                data["blockTimeMS"] = block;
            }

            var admin = new MongoClient(connectionString).GetDatabase("admin");
            await admin.RunCommandAsync<BsonDocument>(new BsonDocument
            {
                ["configureFailPoint"] = "failCommand",
                ["mode"] = mode,
                ["data"] = data
            });
            return new UpdateFailures(admin);
        }

        public async ValueTask DisposeAsync() =>
            await admin.RunCommandAsync<BsonDocument>(new BsonDocument
            {
                ["configureFailPoint"] = "failCommand",
                ["mode"] = "off"
            });
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet build tests/MongoBus.Tests -c Release`
Expected: FAIL — `CS1061: 'MessageLockRenewer' does not contain a definition for 'TryAcquireLeaseAsync'`.

- [ ] **Step 3: Write minimal implementation**

Create `src/MongoBus/Internal/MessageLockLease.cs`:

```csharp
using Microsoft.Extensions.Logging;
using MongoBus.Infrastructure;

namespace MongoBus.Internal;

/// <summary>
/// Keeps one delivery's lock on a message until disposed, renewing every third of the lock time, and signals
/// <see cref="LockLost"/> when the lock was taken or can no longer be counted on.
/// </summary>
internal sealed class MessageLockLease : IAsyncDisposable
{
    private readonly CancellationTokenSource _lockLost = new();
    private readonly CancellationTokenSource _stopRenewing;
    private readonly Task _renewing;

    private MessageLockLease(
        MessageLockRenewer renewer, InboxMessage message, TimeSpan lockTime, DateTime claimedAt, ILogger log, CancellationToken ct)
    {
        _stopRenewing = CancellationTokenSource.CreateLinkedTokenSource(ct);
        GiveUpBeforeExpiry(claimedAt, lockTime);
        _renewing = RenewUntilStoppedAsync(renewer, message, lockTime, log, _stopRenewing.Token);
    }

    /// <summary>Cancelled once this delivery can no longer count on holding the lock; work on the message should stop.</summary>
    public CancellationToken LockLost => _lockLost.Token;

    /// <param name="claimedAt">When the claim that confirmed the lock was sent; the first deadline counts from here.</param>
    public static MessageLockLease StartRenewing(
        MessageLockRenewer renewer, InboxMessage message, TimeSpan lockTime, DateTime claimedAt, ILogger log, CancellationToken ct) =>
        new(renewer, message, lockTime, claimedAt, log, ct);

    /// <summary>
    /// Schedules <see cref="LockLost"/> a sixth of the lock time before the confirmed expiry. It fires whether or not a
    /// renewal is still in flight, so a slow or hanging write cannot keep the handler running into the moment another
    /// consumer may take the message; one failed renewal in between is still tolerated.
    /// </summary>
    private void GiveUpBeforeExpiry(DateTime extendedAt, TimeSpan lockTime)
    {
        var deadline = extendedAt.Add(lockTime - lockTime / 6);
        var remaining = deadline - DateTime.UtcNow;
        _lockLost.CancelAfter(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
    }

    private async Task RenewUntilStoppedAsync(
        MessageLockRenewer renewer, InboxMessage message, TimeSpan lockTime, ILogger log, CancellationToken stop)
    {
        var interval = lockTime / 3;
        while (!stop.IsCancellationRequested && !_lockLost.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, stop);

                var attemptedAt = DateTime.UtcNow;
                using var attempt = CancellationTokenSource.CreateLinkedTokenSource(stop, _lockLost.Token);
                if (await renewer.TryExtendAsync(message, lockTime, attempt.Token))
                {
                    GiveUpBeforeExpiry(attemptedAt, lockTime);
                    continue;
                }

                log.LogWarning(
                    "Delivery no longer holds the lock on message {MessageId} on endpoint {EndpointId}; cancelling its dispatch.",
                    message.Id, message.EndpointId);
                await _lockLost.CancelAsync();
                return;
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested || _lockLost.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                log.LogWarning(
                    ex,
                    "Could not renew the lock on message {MessageId}; its dispatch is cancelled if the lock nears expiry first.",
                    message.Id);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _stopRenewing.CancelAsync();
        await _renewing;
        _stopRenewing.Dispose();
        _lockLost.Dispose();
    }
}
```

Add to `MessageLockRenewer`:

```csharp
    /// <summary>
    /// Re-claims the message and starts renewing its lock, or returns null when the message was re-locked while it
    /// waited to be dispatched.
    /// </summary>
    public async Task<MessageLockLease?> TryAcquireLeaseAsync(InboxMessage message, TimeSpan lockTime, CancellationToken ct)
    {
        var claimedAt = DateTime.UtcNow;
        return await TryExtendAsync(message, lockTime, ct)
            ? MessageLockLease.StartRenewing(this, message, lockTime, claimedAt, log, ct)
            : null;
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `dotnet build -c Release && dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.MessageLockRenewerTests"`
Expected: PASS, 10 tests.

- [ ] **Step 5: Commit**

```bash
git add src/MongoBus/Internal/MessageLockLease.cs src/MongoBus/Internal/MessageLockRenewer.cs tests/MongoBus.Tests/MessageLockRenewerTests.cs
git commit -m "chat-agent-loop: renew a message lock until released and give up before it expires"
```

### Task 5: Runtime renews locks around dispatch; documentation

**Files:**
- Modify: `src/MongoBus/Internal/MongoBusRuntime.cs:14-39,277-303`
- Modify: `README.md` (consumer definition example, section "Defining Messages and Handlers")
- Modify: `src/MongoBus/README.md` (before `## Batch consumers`)
- Modify: `clients/python/README.md:116-117`
- Test: `tests/MongoBus.Tests/LockRenewalTests.cs`

**Interfaces:**
- Consumes: `EndpointRuntimeConfig.RenewLock` (Task 1); per-lock owners and `LockRenewalTests` helpers (Task 2); `InboxLocks` (Task 3); `MessageLockRenewer.TryAcquireLeaseAsync`, `MessageLockLease.LockLost` (Task 4). From `main` (PR #26): `MongoMessageDispatcher.DispatchAsync` catches `OperationCanceledException` when its token is cancelled and releases the lock through `InboxOutcomeWriter`, matching `_id` and `LockOwner`, without using an attempt — a lease-lost cancellation takes this path.
- Produces: nothing later tasks depend on.

- [ ] **Step 1: Write the failing tests**

Add to `LockRenewalTests`:

```csharp
    public sealed class LongMessage { }

    public sealed class LongHandler : IMessageHandler<LongMessage>
    {
        public static int Starts;
        public static int Ends;

        public async Task HandleAsync(LongMessage message, ConsumeContext context, CancellationToken ct)
        {
            Interlocked.Increment(ref Starts);
            await Task.Delay(TimeSpan.FromSeconds(7), ct);
            Interlocked.Increment(ref Ends);
        }
    }

    public sealed class LongDefinition : ConsumerDefinition<LongHandler, LongMessage>
    {
        public override string TypeId => "renewal.long";
        public override TimeSpan LockTime => TimeSpan.FromSeconds(3);
        public override bool RenewLock => true;
    }

    [Fact]
    public async Task HandlerOutlivingLockTime_RunsOnce_AcrossCompetingConsumers()
    {
        Interlocked.Exchange(ref LongHandler.Starts, 0);
        Interlocked.Exchange(ref LongHandler.Ends, 0);
        var databaseName = NewDatabaseName();
        Action<IServiceCollection> registerConsumer =
            services => services.AddMongoBusConsumer<LongHandler, LongMessage, LongDefinition>();
        await using var first = await StartBusAsync(databaseName, registerConsumer);
        await using var second = await StartBusAsync(databaseName, registerConsumer);

        await PublisherOf(first).PublishAsync("renewal.long", new LongMessage(), "test-source");
        await WaitUntilAsync(() => Task.FromResult(LongHandler.Ends >= 1), TimeSpan.FromSeconds(20));
        await Task.Delay(TimeSpan.FromSeconds(4));

        LongHandler.Starts.Should().Be(1);
        (await InboxOf(first).Find(x => x.TypeId == "renewal.long").SingleAsync()).Status.Should().Be(InboxStatus.Processed);
    }

    public sealed class StolenMessage { }

    public sealed class StolenHandler : IMessageHandler<StolenMessage>
    {
        public static TaskCompletionSource Started = NewSignal();
        public static TaskCompletionSource Cancelled = NewSignal();

        public static void Reset()
        {
            Started = NewSignal();
            Cancelled = NewSignal();
        }

        public async Task HandleAsync(StolenMessage message, ConsumeContext context, CancellationToken ct)
        {
            Started.TrySetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
                Cancelled.TrySetResult();
                throw;
            }
        }
    }

    public sealed class StolenDefinition : ConsumerDefinition<StolenHandler, StolenMessage>
    {
        public override string TypeId => "renewal.stolen";
        public override TimeSpan LockTime => TimeSpan.FromSeconds(3);
        public override bool RenewLock => true;
    }

    [Fact]
    public async Task RenewingHandler_IsCancelled_WhenAnotherConsumerTakesItsLock()
    {
        StolenHandler.Reset();
        await using var bus = await StartBusAsync(
            NewDatabaseName(),
            services => services.AddMongoBusConsumer<StolenHandler, StolenMessage, StolenDefinition>());

        await PublisherOf(bus).PublishAsync("renewal.stolen", new StolenMessage(), "test-source");
        await StolenHandler.Started.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var message = await InboxOf(bus).Find(x => x.TypeId == "renewal.stolen").SingleAsync();
        await InboxLocks.TakeLockAsync(InboxOf(bus), message.Id);

        await StolenHandler.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromMilliseconds(500));

        var stored = await InboxOf(bus).Find(x => x.Id == message.Id).SingleAsync();
        stored.LockOwner.Should().Be(InboxLocks.OtherOwner);
        stored.Status.Should().Be(InboxStatus.Pending);
        stored.Attempt.Should().Be(message.Attempt);
    }

    public sealed class BacklogMessage
    {
        public string Name { get; set; } = "";
    }

    public sealed class BacklogHandler : IMessageHandler<BacklogMessage>
    {
        public static ConcurrentQueue<string> Handled = new();
        public static TaskCompletionSource ReleaseBlockers = NewSignal();

        public static void Reset()
        {
            Handled = new ConcurrentQueue<string>();
            ReleaseBlockers = NewSignal();
        }

        public async Task HandleAsync(BacklogMessage message, ConsumeContext context, CancellationToken ct)
        {
            Handled.Enqueue(message.Name);
            if (message.Name.StartsWith("blocker", StringComparison.Ordinal))
                await ReleaseBlockers.Task.WaitAsync(ct);
        }
    }

    public sealed class BacklogDefinition : ConsumerDefinition<BacklogHandler, BacklogMessage>
    {
        public override string TypeId => "renewal.backlog";
        public override int ConcurrencyLimit => 2;
        public override int PrefetchCount => 2;
        public override TimeSpan LockTime => TimeSpan.FromSeconds(3);
        public override bool RenewLock => true;
    }

    [Fact]
    public async Task WaitingMessageRelockedByItsOwnEndpoint_IsHandledOnce()
    {
        BacklogHandler.Reset();
        await using var bus = await StartBusAsync(
            NewDatabaseName(),
            services => services.AddMongoBusConsumer<BacklogHandler, BacklogMessage, BacklogDefinition>());
        var inbox = InboxOf(bus);

        await PublisherOf(bus).PublishAsync("renewal.backlog", new BacklogMessage { Name = "blocker-1" }, "test-source");
        await PublisherOf(bus).PublishAsync("renewal.backlog", new BacklogMessage { Name = "blocker-2" }, "test-source");
        await WaitUntilAsync(() => Task.FromResult(BacklogHandler.Handled.Count == 2), TimeSpan.FromSeconds(10));
        var blockerIds = await inbox.Find(x => x.TypeId == "renewal.backlog").Project(x => x.Id).ToListAsync();

        await PublisherOf(bus).PublishAsync("renewal.backlog", new BacklogMessage { Name = "waiting" }, "test-source");
        InboxMessage? firstLock = null;
        await WaitUntilAsync(
            async () => (firstLock = await inbox.Find(x => !blockerIds.Contains(x.Id) && x.LockOwner != null).FirstOrDefaultAsync()) != null,
            TimeSpan.FromSeconds(10));
        var waitingId = firstLock!.Id;
        var firstOwner = firstLock.LockOwner;
        await WaitUntilAsync(
            async () => await inbox.CountDocumentsAsync(x => x.Id == waitingId && x.LockOwner != firstOwner) == 1,
            TimeSpan.FromSeconds(10));

        BacklogHandler.ReleaseBlockers.TrySetResult();
        await WaitUntilAsync(() => Task.FromResult(BacklogHandler.Handled.Contains("waiting")), TimeSpan.FromSeconds(15));
        await Task.Delay(TimeSpan.FromSeconds(4));

        BacklogHandler.Handled.Count(name => name == "waiting").Should().Be(1);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet build -c Release && dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.LockRenewalTests"`
Expected: FAIL, 3 tests — `LongHandler.Starts` is 2 or more (the lock lapsed and was re-taken); `StolenHandler.Cancelled` times out (`TimeoutException`); in the backlog test the blockers' lapsed locks are re-locked first and fill the channel, so "waiting" is never re-locked and the owner-change wait throws `TimeoutException`.

- [ ] **Step 3: Write minimal implementation**

In `src/MongoBus/Internal/MongoBusRuntime.cs` add the field after `_definitions`:

```csharp
    private readonly MessageLockRenewer _lockRenewer;
```

and at the end of the constructor (after `_definitions = definitions.ToList();`):

```csharp
        _lockRenewer = new MessageLockRenewer(_inbox, log);
```

In `WorkerLoopAsync` replace `await _dispatcher.DispatchAsync(msg, ctx, ct);` (line 296) with:

```csharp
                await DispatchAsync(cfg, msg, ctx, ct);
```

and add below `WorkerLoopAsync`:

```csharp
    private async Task DispatchAsync(EndpointRuntimeConfig cfg, InboxMessage msg, ConsumeContext ctx, CancellationToken ct)
    {
        if (!cfg.RenewLock)
        {
            await _dispatcher.DispatchAsync(msg, ctx, ct);
            return;
        }

        await using var lease = await _lockRenewer.TryAcquireLeaseAsync(msg, cfg.LockTime, ct);
        if (lease is null)
        {
            _log.LogInformation(
                "Message {MessageId} on endpoint {Endpoint} was re-locked while waiting to be dispatched; skipping this copy.",
                msg.Id, cfg.EndpointId);
            return;
        }

        using var dispatchCancellation = CancellationTokenSource.CreateLinkedTokenSource(ct, lease.LockLost);
        await _dispatcher.DispatchAsync(msg, ctx, dispatchCancellation.Token);
    }
```

In `README.md`, in the `OrderCreatedDefinition` example under "Optional overrides", add after the `LockTime` line:

```csharp
    public override bool RenewLock => true; // Keep the lock while a long handler runs; its token is cancelled if the lock is lost
```

and directly after that code block, before `### Batch Consumers`, add:

```markdown
#### Long-running handlers

A handler that can outlive `LockTime` sets `RenewLock => true` (with a `LockTime` of at least one second). The lock is then extended every third of `LockTime` for as long as the handler runs, so no other consumer picks the message up, and if the consumer crashes the message is redelivered once the lock lapses. If the lock is lost — another consumer took the message, or renewals did not succeed before the lock neared expiry — the handler's `CancellationToken` is cancelled and the message is released for redelivery. Batch consumers do not renew locks.
```

In `src/MongoBus/README.md`, directly before `## Batch consumers`, add:

````markdown
## Long-running handlers

```csharp
public class MyDefinition : ConsumerDefinition<MyHandler, MyMessage>
{
    public override string TypeId => "my.message";
    public override TimeSpan LockTime => TimeSpan.FromMinutes(1);
    public override bool RenewLock => true; // lock kept while the handler runs; its token is cancelled if the lock is lost
}
```

````

In `clients/python/README.md` replace the two lines

```markdown
- **.NET parity:** .NET consumers set a fixed `LockTime` and do not renew; keep their
  `LockTime` above the longest handler.
```

with

```markdown
- **.NET parity:** .NET consumers renew their lock only when their definition sets
  `RenewLock` (MongoBus 3.1.0 and later); otherwise keep their `LockTime` above the longest handler.
```

- [ ] **Step 4: Run test to verify it passes**

Run: `dotnet build -c Release && dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.LockRenewalTests"`
Expected: PASS, 5 tests (2 from Task 2, 3 from this task).

- [ ] **Step 5: Commit**

```bash
git add src/MongoBus/Internal/MongoBusRuntime.cs README.md src/MongoBus/README.md clients/python/README.md tests/MongoBus.Tests/LockRenewalTests.cs
git commit -m "chat-agent-loop: renew locks around dispatch for consumers that opt in"
```

## Verification

Run at the repository root on `ef7d2fd` — the five task commits rebased onto `origin/main` `9cad9e2` (PR #50) — on 2026-09-15:

- `dotnet restore` → "All projects are up-to-date for restore.", exit 0.
- `dotnet build --no-restore -c Release` → 0 Error(s), 345 Warning(s). A clean rebuild (`dotnet build --no-restore --no-incremental -c Release`) gives the same counts, and none of the warnings is reported in a file this branch touches.
- `dotnet test --no-build -c Release`:
  - Run 1: MongoBus.Dashboard.Tests 37/37 passed. MongoBus.Tests 283/284 (3 m 37 s) — `ConcurrencyTests.MessageShouldNotBeProcessedByMultipleWorkersSimultaneously` failed.
  - Run 2, same build (`dotnet test tests/MongoBus.Tests --no-build -c Release`): MongoBus.Tests 284/284 passed (3 m 38 s).
  - About the run-1 failure: the test drives a consumer that does not set `RenewLock`, whose lock owner and dispatch path are unchanged from `main` (`git diff origin/main..HEAD -- src/MongoBus/Internal/MongoBusRuntime.cs`: `LockOwnerFor` returns the pump id and `DispatchAsync` calls the dispatcher directly when `RenewLock` is false). It passed 3 of 3 runs on its own and has no failures in the repository's CI history. It asserts 1–2 handler starts for a 1-second lock and a 2-second handler, which a slow outcome write under full-suite load can exceed. Reported to the session maintaining the test suite, which is rewriting that test on a separate test-only branch; not changed in this PR.
- Per-task runs by the implementers: `LockRenewalConfigTests` 5/5; `LockRenewalTests` 5/5 in two consecutive runs; `MessageLockRenewerTests` 10/10 in three consecutive runs.

After the review fixes, run again at the repository root on `5a39b23` — the ten branch commits rebased onto `origin/main` `2181640` (PR #51, which rewrote `ConcurrencyTests`) — on 2026-09-16:

- `dotnet restore` → "All projects are up-to-date for restore."
- `dotnet build --no-restore --no-incremental -c Release` → 0 Error(s), 345 Warning(s), unchanged from the base.
- `dotnet test --no-build -c Release` → MongoBus.Dashboard.Tests 37/37 passed; **MongoBus.Tests 287/287 passed** (3 m 57 s).
- Fix-cycle runs by the implementers: Fix 1's new shutdown test passed both runs and the Step 4 filter 23/23; Fix 2 proved both lock-taken tests fail when the owner-mismatch cancellation is removed, then 16/16 twice; Fix 3 proved the double-logging guard fails without its check, then 18/18; Fix 4 proved both give-up tests fail when the deadline is moved past the budget (3.52 s and 3.50 s against a 3.0 s budget), then passed 6 consecutive runs and `MessageLockRenewerTests` 12/12.
- The two intermittent failures seen during this work are both accounted for: `ConcurrencyTests.MessageShouldNotBeProcessedByMultipleWorkersSimultaneously` was load-sensitive on a path this branch does not change and has since been rewritten on `main`; the lease give-up tests compared a monotonic deadline against a wall-clock expiry on a machine whose clock steps forward 1.41-1.81 s about every 33.8 s (WSL2 with `systemd-timesyncd`), which Fix 4 removes by measuring the watchdog's budget with a `Stopwatch`.

Baseline before any change, at `8d04a75`: `dotnet build --no-restore -c Release` 0 errors / 282 warnings; `dotnet test --no-build -c Release` MongoBus.Tests 225/225, MongoBus.Dashboard.Tests 24/24. (At `dfb91e2`, `SagaPartitionerTests.AcquireAsync_DifferentKeys_CanRunConcurrently` failed 2 of 2 local runs because `SagaPartitioner` partitions on the per-process-randomized `string.GetHashCode()`; PR #29 on `5b2f2e0` addresses that test.)

## Review notes

### Plan review pass 1 (deep-reviewer, lens plan) — "Changes requested"

Every finding was checked against `origin/main` before it was applied.

Applied:
- **[High] Same-pump re-locks defeat the re-claim.** Verified: one pump id per endpoint, and the lock filter matches any pending message with a lapsed lock, including copies the same pump queued. Fix: per-lock owners (Task 2, since narrowed to renewing endpoints by pass 2) and `WaitingMessageRelockedByItsOwnEndpoint_IsHandledOnce` (Task 5).
- **[Medium] Stale base.** Verified: `main` had moved past the plan's base. Fix: rebased the worktree, reuse `RunningBus`, dropped the bindings wait.
- **[Medium] Missing ticket test for redelivery after renewal stops.** Fix: now `MessageWhoseRenewalStoppedWithoutARelease_IsLockableByAnotherPumpOnlyAfterItsLockLapses` (Task 4; see pass 2).
- **[Medium] Renewal errors never cancel.** Fix: now the watchdog (Task 4; see pass 2).
- **[Low] Misleading lock-lost wording.** Fix: neutral wording; out-of-scope notes describe the actual reporting path.
- **[Low] Timing-sensitive tests.** Fix: 3-second lock times with scaled waits.
- **[Low] No minimum `LockTime` with renewal.** Fix: Task 1 validator rule and tests.
- **[Low] Rollback and Python docs.** Verified merge commits and `clients/python/README.md:116`. Fix: `git revert -m 1`, `+semver:minor` in Task 1's commit and the PR title, Python parity bullet in Task 5.

Superseded by `main` since pass 1: the owner-guarded dispatcher writes (old Task 5) landed in PR #26, and the graceful-stop redelivery test no longer tests a lock lapse (PR #26 releases the lock on stop).

Not applied:
- **Suggested test "renewing handler ignores cancellation, keeps the new owner".** Outcome writes for renewing and non-renewing consumers go through the same `InboxOutcomeWriter` filter, which `ConsumerStateWriteTests` covers on `main`.
- **Earlier test "queued message whose lock another consumer took is not dispatched"** was dropped: the re-claim failing on an owner mismatch is the path `WaitingMessageRelockedByItsOwnEndpoint_IsHandledOnce` exercises.

### Plan review pass 2 (deep-reviewer, lens plan) — "Not ready"

Applied:
- **[High] Per-lock owners for non-renewing consumers redeliver forever.** Verified against `InboxOutcomeWriter` (matches `_id` and `LockOwner`) and the lock filter: a handler slower than `LockTime` is re-locked under a new owner before it finishes, so every copy's outcome write misses. Fix: `LockOwnerFor` keeps the pump id for non-renewing endpoints; `NonRenewingHandlerOutlivingLockTime_StillGetsItsMessageProcessed` guards it; the Rejected bullet and Risk text were corrected.
- **[High] The redelivery test tested #26's release, not a lapse.** Verified against `MongoMessageDispatcher.DispatchAsync`'s `OperationCanceledException` release path. Fix: the test now stops a lease without any release (as a crashed process would) and checks that another pump cannot lock the message before its lock lapses and can afterwards.
- **[Medium] Task 5 already on `main`.** Fix: the dispatcher task and `MongoMessageDispatcher.cs` were removed; Design item 6, out-of-scope and risk text now describe #26; line references moved to `5b2f2e0`.
- **[Medium] Late cancellation on failing renewals.** Fix: `GiveUpBeforeExpiry` schedules `LockLost` a sixth of the lock time before the confirmed expiry after the claim and every successful renewal, timed from before the write; attempts run on a token linked to `LockLost`; tests cover one tolerated failure, continuous failures and a hanging renewal (`blockConnection`), each asserting cancellation before the stored expiry.
- **[Low] Package README not updated.** Verified `MongoBus.csproj` packs `src/MongoBus/README.md`, a separate file with no consumer definition example. Fix: Task 5 adds a short "Long-running handlers" section there.
- **[Low] Wrong expected red-phase failure for the backlog test.** Fix: Step 2 now expects the owner-change wait to time out.
- **Suggested assertion** that a renewing handler whose lock is taken keeps `Attempt`: added to `RenewingHandler_IsCancelled_WhenAnotherConsumerTakesItsLock`.

Not applied:
- **Runtime-level crash test using a failpoint on bus A's writes.** While bus A keeps running, its own fetch loop (unaffected by an `update` failpoint) can re-lock the message after the lapse, so which delivery happens when is non-deterministic; the lease-plus-pump test models a crash exactly.
- **Assert that `OnMessageFailed` is not raised when a renewing handler's lock is taken.** Registering a consume observer in `RunningBus` adds setup this plan does not otherwise need; the unchanged `Attempt` already shows the failure path was not taken.

Plan review is capped at two passes (`/jira-task` Phase 3); these pass-2 revisions go to implementation without a third plan review and are re-checked by the Phase 5 deep reviews.

## Review fixes

Phase 5 deep reviews over `origin/main...3d1eefd`: run A (lens correctness) and run B (lens design), both "Acceptable with concerns — changes requested", no Critical or High findings. Both confirmed that consumers without `RenewLock` are unchanged from `main`. Merged list (duplicates keep the higher severity), each finding checked against the code before a decision:

| # | Severity | Finding | Source | Decision |
|---|---|---|---|---|
| M1 | Medium | Renewal stops when the host begins stopping, not when the lease is disposed: `MessageLockLease.cs:19` links `_stopRenewing` to the worker token passed at `MessageLockRenewer.cs:31`. A handler still finishing during a rolling deploy can let its lock lapse and the job run twice. | A1, B1 | Accepted — Fix 1 |
| M2 | Medium | The lock-taken cancellation tests (`MessageLockRenewerTests` lease test, `LockRenewalTests` runtime test) wait 5 s against a watchdog at 2.5 s, so they pass without the owner-mismatch branch (`MessageLockLease.cs:62-66`). | A2, B2 | Accepted — Fix 2 |
| M3 | Medium | A watchdog cancellation logs no Warning; operators see only the dispatcher's Information "Stopped while handling message…" line, identical to a normal shutdown. The "Could not renew" warning also omits `EndpointId`. | A3, B triage | Accepted — Fix 3 |
| M4 | Low | The crash-redelivery test's `Task.Delay(4 s)` lines up with the fourth renewal, leaving a millisecond margin. | B3 | Accepted — Fix 2 |
| M5 | Low | `README.md` says a lost lock releases the message for redelivery (false when another consumer took it), and neither the README nor the `RenewLock` XML doc says the flag applies to every consumer on the endpoint. | A5, B4, B5 | Accepted — Fix 1 (documentation steps) |
| M6 | Low | The private `DispatchAsync(cfg, …)` shares its name with the dispatcher's and hides the re-claim and skip. | B6 | Accepted — Fix 1 |
| M7 | Low | `MessageLockRenewer` builds `MessageLockLease`, which calls back into the renewer; the renewer carries a logger only to pass it on. | B7 | Follow-up (not this PR): behaviour-neutral restructuring; the fix wave stays on correctness. |
| M8 | Low | `MessageLockLease.DisposeAsync` awaits a renewal write the driver does not abort on cancellation, so a hung write holds the worker after the outcome is recorded. | A4 | Follow-up (not this PR): the outcome and the watchdog are unaffected, `InboxOutcomeWriter` has the same exposure, and the reviewer recommends a follow-up. |

Deferred per-task minors, triaged by both reviews:
- Folded into the fixes above: T4 lock-taken test through the watchdog (M2), T4 pump test delay (M4), T4 warning without `EndpointId` (M3), T5 renewal at shutdown (M1), T5 README wording (M5), T5 method name (M6).
- Moot: T1 and T2 doc comments written ahead of later tasks (all tasks ship in one PR); T4 "no test that the caller's token stops renewal" (M1 removes that coupling).
- Not needed before merge: T1 validator rejecting a hand-written batch definition with `RenewLock` and `LockTime` < 1 s; T2 `Starts <= 5` bound; T3 null-owner guard (callers are gated on `RenewLock`, owners always non-null); T3 return-value-only assertion; T4 expiry read after failpoint injection; T4 loop delay not waking on `LockLost`; T4 non-idempotent `DisposeAsync`; T5 timing margins (watch the first CI run).
- Follow-up (not this PR): T4 "Could not renew" logged after cancellation; T5 false "no longer holds the lock" warning in the window after the outcome write.
- Follow-up (not this PR, prioritise before GenCAD's PR 2 consumers): during every shutdown with a backlog, a renewing endpoint's workers drain messages still buffered in the channel (`ChannelReader.ReadAllAsync` may yield after cancellation) and claim each with the already-cancelled worker token; the claim throws `OperationCanceledException`, `WorkerLoopAsync` logs it at Error as "Unexpected error in WorkerLoop", and the message is neither dispatched nor released, so it is redelivered only after `LockTime`. Non-renewing endpoints dispatch these and release them through #26's path.
- Follow-up (not this PR): Fix 3's warning runs in a `CancellationToken` callback on the watchdog's timer thread — a logging provider that throws there has no caller to catch it, and callbacks run in reverse registration order, so the dispatcher's "Stopped while handling message…" line can precede the warning.

### Fix 1: The lease renews until it is disposed, not until the bus starts stopping

**Files:**
- Modify: `src/MongoBus/Internal/MessageLockLease.cs` (constructor, `StartRenewing`)
- Modify: `src/MongoBus/Internal/MessageLockRenewer.cs` (`TryAcquireLeaseAsync`)
- Modify: `src/MongoBus/Internal/MongoBusRuntime.cs` (`WorkerLoopAsync` call site, the private `DispatchAsync`)
- Modify: `src/MongoBus/Abstractions/IConsumerDefinition.cs` (`RenewLock` XML doc)
- Modify: `README.md` (`#### Long-running handlers` paragraph)
- Test: `tests/MongoBus.Tests/LockRenewalTests.cs`

**Interfaces:**
- Produces: `MessageLockLease.StartRenewing(MessageLockRenewer renewer, InboxMessage message, TimeSpan lockTime, DateTime claimedAt, ILogger log)` (no `CancellationToken`); private `MongoBusRuntime.DispatchUnderLeaseAsync(EndpointRuntimeConfig cfg, InboxMessage msg, ConsumeContext ctx, CancellationToken ct)`.

- [ ] **Step 1: Write the failing test**

Add to `LockRenewalTests`:

```csharp
    public sealed class FinishingMessage { }

    public sealed class FinishingHandler : IMessageHandler<FinishingMessage>
    {
        public static TaskCompletionSource Started = NewSignal();
        public static TaskCompletionSource Release = NewSignal();

        public static void Reset()
        {
            Started = NewSignal();
            Release = NewSignal();
        }

        public async Task HandleAsync(FinishingMessage message, ConsumeContext context, CancellationToken ct)
        {
            Started.TrySetResult();
            await Release.Task;
        }
    }

    public sealed class FinishingDefinition : ConsumerDefinition<FinishingHandler, FinishingMessage>
    {
        public override string TypeId => "renewal.finishing";
        public override TimeSpan LockTime => TimeSpan.FromSeconds(3);
        public override bool RenewLock => true;
    }

    [Fact]
    public async Task HandlerStillFinishingWhileTheBusStops_KeepsItsLockUntilItReturns()
    {
        FinishingHandler.Reset();
        var bus = await StartBusAsync(
            NewDatabaseName(),
            services => services.AddMongoBusConsumer<FinishingHandler, FinishingMessage, FinishingDefinition>());
        var inbox = InboxOf(bus);
        Task? stopping = null;
        InboxMessage locked;
        InboxMessage whileStopping;
        DateTime readAt;
        try
        {
            await PublisherOf(bus).PublishAsync("renewal.finishing", new FinishingMessage(), "test-source");
            await FinishingHandler.Started.Task.WaitAsync(TimeSpan.FromSeconds(10));
            locked = await inbox.Find(x => x.TypeId == "renewal.finishing").SingleAsync();

            stopping = bus.DisposeAsync().AsTask();
            await Task.Delay(TimeSpan.FromSeconds(5));
            readAt = DateTime.UtcNow;
            whileStopping = await inbox.Find(x => x.Id == locked.Id).SingleAsync();
        }
        finally
        {
            FinishingHandler.Release.TrySetResult();
            await (stopping ?? bus.DisposeAsync().AsTask()).WaitAsync(TimeSpan.FromSeconds(15));
        }

        whileStopping.LockOwner.Should().Be(locked.LockOwner);
        whileStopping.LockedUntilUtc.Should().BeAfter(readAt);
        (await inbox.Find(x => x.Id == locked.Id).SingleAsync()).Status.Should().Be(InboxStatus.Processed);
    }
```

(`RunningBus.DisposeAsync` stops the hosted services without disposing the service provider, so `inbox` stays usable after the bus stops.)

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet build -c Release && dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.LockRenewalTests.HandlerStillFinishingWhileTheBusStops_KeepsItsLockUntilItReturns"`
Expected: FAIL — `whileStopping.LockedUntilUtc` is before `readAt`, because renewal stopped when the bus began stopping and the 3-second lock lapsed during the 5-second wait.

- [ ] **Step 3: Write minimal implementation**

`src/MongoBus/Internal/MessageLockLease.cs` — the constructor and `StartRenewing` lose their `CancellationToken`:

```csharp
    private MessageLockLease(
        MessageLockRenewer renewer, InboxMessage message, TimeSpan lockTime, DateTime claimedAt, ILogger log)
    {
        _stopRenewing = new CancellationTokenSource();
        GiveUpBeforeExpiry(claimedAt, lockTime);
        _renewing = RenewUntilStoppedAsync(renewer, message, lockTime, log, _stopRenewing.Token);
    }
```

```csharp
    /// <param name="claimedAt">When the claim that confirmed the lock was sent; the first deadline counts from here.</param>
    /// <remarks>
    /// Renewal is not tied to the worker's stopping token: a handler still finishing while the bus stops must keep its
    /// lock until its dispatch returns and the lease is disposed, or another consumer could start the same message.
    /// </remarks>
    public static MessageLockLease StartRenewing(
        MessageLockRenewer renewer, InboxMessage message, TimeSpan lockTime, DateTime claimedAt, ILogger log) =>
        new(renewer, message, lockTime, claimedAt, log);
```

`src/MongoBus/Internal/MessageLockRenewer.cs` — `TryAcquireLeaseAsync` keeps `ct` for the claim only:

```csharp
    public async Task<MessageLockLease?> TryAcquireLeaseAsync(InboxMessage message, TimeSpan lockTime, CancellationToken ct)
    {
        var claimedAt = DateTime.UtcNow;
        return await TryExtendAsync(message, lockTime, ct)
            ? MessageLockLease.StartRenewing(this, message, lockTime, claimedAt, log)
            : null;
    }
```

`src/MongoBus/Internal/MongoBusRuntime.cs` — in `WorkerLoopAsync` replace `await DispatchAsync(cfg, msg, ctx, ct);` with:

```csharp
                if (cfg.RenewLock)
                    await DispatchUnderLeaseAsync(cfg, msg, ctx, ct);
                else
                    await _dispatcher.DispatchAsync(msg, ctx, ct);
```

and replace the private `DispatchAsync` method with:

```csharp
    private async Task DispatchUnderLeaseAsync(EndpointRuntimeConfig cfg, InboxMessage msg, ConsumeContext ctx, CancellationToken ct)
    {
        await using var lease = await _lockRenewer.TryAcquireLeaseAsync(msg, cfg.LockTime, ct);
        if (lease is null)
        {
            _log.LogInformation(
                "Message {MessageId} on endpoint {Endpoint} was re-locked while waiting to be dispatched; skipping this copy.",
                msg.Id, cfg.EndpointId);
            return;
        }

        using var dispatchCancellation = CancellationTokenSource.CreateLinkedTokenSource(ct, lease.LockLost);
        await _dispatcher.DispatchAsync(msg, ctx, dispatchCancellation.Token);
    }
```

`src/MongoBus/Abstractions/IConsumerDefinition.cs` — the `RenewLock` XML doc becomes:

```csharp
    /// <summary>
    /// When true, the lock on a message is extended for as long as its handler runs, and the handler's
    /// cancellation token is cancelled if the lock is lost. Applies to every single-message consumer on the same
    /// endpoint; batch consumers do not renew. Requires a <see cref="LockTime"/> of at least one second.
    /// </summary>
```

`README.md` — replace the `#### Long-running handlers` paragraph with:

```markdown
A handler that can outlive `LockTime` sets `RenewLock => true` (with a `LockTime` of at least one second). The lock is then extended every third of `LockTime` for as long as the handler runs — including while the bus is stopping — so no other consumer picks the message up, and if the consumer crashes the message is redelivered once the lock lapses. If the lock is lost — another consumer took the message, or renewals did not succeed before the lock neared expiry — the handler's `CancellationToken` is cancelled; stop promptly, because the message may already be running elsewhere. `RenewLock` applies to every consumer on the same endpoint. Batch consumers do not renew locks.
```

- [ ] **Step 4: Run test to verify it passes**

Run: `dotnet build -c Release && dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.LockRenewalTests|FullyQualifiedName~MongoBus.Tests.MessageLockRenewerTests|FullyQualifiedName~MongoBus.Tests.ConsumerStateWriteTests"`
Expected: PASS (LockRenewalTests 6, MessageLockRenewerTests 10, ConsumerStateWriteTests unchanged).

- [ ] **Step 5: Commit**

```bash
git add src/MongoBus/Internal/MessageLockLease.cs src/MongoBus/Internal/MessageLockRenewer.cs src/MongoBus/Internal/MongoBusRuntime.cs src/MongoBus/Abstractions/IConsumerDefinition.cs README.md tests/MongoBus.Tests/LockRenewalTests.cs
git commit -m "chat-agent-loop: keep renewing a lock until the dispatch returns, also while the bus stops"
```

### Fix 2: Lock-taken tests fail without the lock-taken branch; crash test off the renewal beat

**Files:**
- Test: `tests/MongoBus.Tests/MessageLockRenewerTests.cs` (`Lease_SignalsLockLost_WhenAnotherConsumerTakesTheLock`, `MessageWhoseRenewalStoppedWithoutARelease_IsLockableByAnotherPumpOnlyAfterItsLockLapses`)
- Test: `tests/MongoBus.Tests/LockRenewalTests.cs` (`StolenDefinition`)

**Interfaces:** none.

- [ ] **Step 1: Tighten the tests**

In `MessageLockRenewerTests`, `Lease_SignalsLockLost_WhenAnotherConsumerTakesTheLock` becomes (renewal at 3 s detects the taken lock; the watchdog would only fire at 7.5 s, after the 5-second wait):

```csharp
    [Fact]
    public async Task Lease_SignalsLockLost_WhenAnotherConsumerTakesTheLock()
    {
        var inbox = InboxIn(NewDatabaseName());
        var lockTime = TimeSpan.FromSeconds(9);
        var message = await InsertLockedAsync(inbox, lockTime);

        await using var lease = await NewRenewer(inbox).TryAcquireLeaseAsync(message, lockTime, CancellationToken.None);
        await InboxLocks.TakeLockAsync(inbox, message.Id);
        var signalled = await WaitForCancellationAsync(lease!.LockLost, TimeSpan.FromSeconds(5));

        signalled.Should().BeTrue();
        (await ReloadAsync(inbox, message)).LockOwner.Should().Be(InboxLocks.OtherOwner);
    }
```

In `MessageWhoseRenewalStoppedWithoutARelease_IsLockableByAnotherPumpOnlyAfterItsLockLapses`, stop depending on when renewals happen: read the expiry the stopped lease left behind and wait until just after it. The lines from `var lease = …` to `var afterLapse = …` become:

```csharp
        var lease = await NewRenewer(inbox).TryAcquireLeaseAsync(message, LeaseLockTime, CancellationToken.None);
        await Task.Delay(TimeSpan.FromSeconds(4));
        await lease!.DisposeAsync();
        var storedExpiry = (await ReloadAsync(inbox, message)).LockedUntilUtc!.Value;

        var beforeLapse = await pump.TryLockOneAsync(EndpointId, LeaseLockTime, "another-pump", CancellationToken.None);
        await Task.Delay(storedExpiry - DateTime.UtcNow + TimeSpan.FromMilliseconds(250));
        var afterLapse = await pump.TryLockOneAsync(EndpointId, LeaseLockTime, "another-pump", CancellationToken.None);
```

(If renewal wrongly continued after disposal, the stored expiry would keep moving and `afterLapse` would still be null.)

In `LockRenewalTests`, `StolenDefinition.LockTime` becomes `TimeSpan.FromSeconds(9)` (the test's 5-second wait for `Cancelled` then only passes through the lock-taken branch).

- [ ] **Step 2: Prove the tests now fail without the lock-taken branch**

Temporarily delete `await _lockLost.CancelAsync();` from the owner-mismatch branch in `src/MongoBus/Internal/MessageLockLease.cs` (keep the `return;`).
Run: `dotnet build -c Release && dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.MessageLockRenewerTests.Lease_SignalsLockLost_WhenAnotherConsumerTakesTheLock|FullyQualifiedName~MongoBus.Tests.LockRenewalTests.RenewingHandler_IsCancelled_WhenAnotherConsumerTakesItsLock"`
Expected: FAIL, both tests (`signalled` is false; `Cancelled` wait throws `TimeoutException`).
Restore the deleted line (`git diff src/MongoBus/Internal/MessageLockLease.cs` must be empty).

- [ ] **Step 3: Run the tightened tests to verify they pass**

Run: `dotnet build -c Release && dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.MessageLockRenewerTests|FullyQualifiedName~MongoBus.Tests.LockRenewalTests"`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/MongoBus.Tests/MessageLockRenewerTests.cs tests/MongoBus.Tests/LockRenewalTests.cs
git commit -m "chat-agent-loop: make the lock-taken tests fail without the lock-taken branch"
```

### Fix 3: The lease warns when it gives up before the lock expires

**Files:**
- Modify: `src/MongoBus/Internal/MessageLockLease.cs`
- Test: `tests/MongoBus.Tests/MessageLockRenewerTests.cs`

**Interfaces:** none new.

- [ ] **Step 1: Write the failing tests**

In `MessageLockRenewerTests` add `using System.Collections.Concurrent;` and `using Microsoft.Extensions.Logging;`, then add:

```csharp
    [Fact]
    public async Task Lease_WarnsThatItGaveUp_WhenRenewalsKeepFailing()
    {
        var databaseName = NewDatabaseName();
        var applicationName = NewApplicationName();
        var inbox = InboxIn(databaseName, applicationName);
        var message = await InsertLockedAsync(inbox, LeaseLockTime);
        var log = new RecordingLogger();

        await using var lease = await new MessageLockRenewer(inbox, log).TryAcquireLeaseAsync(message, LeaseLockTime, CancellationToken.None);
        await using var failures = await UpdateFailures.InjectAsync(fixture.ConnectionString, applicationName, "alwaysOn");
        await CancellationTimeAsync(lease!.LockLost, TimeSpan.FromSeconds(8));
        await Task.Delay(TimeSpan.FromMilliseconds(200));

        log.Warnings.Should().ContainSingle(warning => warning.Contains("neared expiry") && warning.Contains(message.Id.ToString()));
    }

    [Fact]
    public async Task Lease_WarnsOnlyThatTheLockWasTaken_WhenAnotherConsumerTakesIt()
    {
        var inbox = InboxIn(NewDatabaseName());
        var lockTime = TimeSpan.FromSeconds(9);
        var message = await InsertLockedAsync(inbox, lockTime);
        var log = new RecordingLogger();

        await using var lease = await new MessageLockRenewer(inbox, log).TryAcquireLeaseAsync(message, lockTime, CancellationToken.None);
        await InboxLocks.TakeLockAsync(inbox, message.Id);
        await CancellationTimeAsync(lease!.LockLost, TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromMilliseconds(200));

        log.Warnings.Should().ContainSingle().Which.Should().Contain("no longer holds the lock");
    }

    private sealed class RecordingLogger : ILogger
    {
        private readonly ConcurrentQueue<(LogLevel Level, string Message)> _entries = new();

        public IEnumerable<string> Warnings =>
            _entries.Where(entry => entry.Level == LogLevel.Warning).Select(entry => entry.Message);

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            _entries.Enqueue((logLevel, formatter(state, exception)));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet build -c Release && dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.MessageLockRenewerTests.Lease_Warns"`
Expected: `Lease_WarnsThatItGaveUp_WhenRenewalsKeepFailing` FAILS (no warning contains "neared expiry"; only "Could not renew" warnings are logged). `Lease_WarnsOnlyThatTheLockWasTaken_WhenAnotherConsumerTakesIt` PASSES — it guards against the watchdog warning also being logged on a lock-taken event, and Step 5 proves it can fail.

- [ ] **Step 3: Write minimal implementation**

In `src/MongoBus/Internal/MessageLockLease.cs` add the fields:

```csharp
    private readonly CancellationTokenRegistration _givingUpReport;
    private volatile bool _lockTakenReported;
```

register the report in the constructor before the watchdog is armed, so the constructor becomes:

```csharp
    private MessageLockLease(
        MessageLockRenewer renewer, InboxMessage message, TimeSpan lockTime, DateTime claimedAt, ILogger log)
    {
        _stopRenewing = new CancellationTokenSource();
        _givingUpReport = _lockLost.Token.Register(() => ReportGivingUp(message, log));
        GiveUpBeforeExpiry(claimedAt, lockTime);
        _renewing = RenewUntilStoppedAsync(renewer, message, lockTime, log, _stopRenewing.Token);
    }
```

and dispose the registration first in `DisposeAsync`, so a watchdog that fires while disposal waits on an in-flight renewal does not report a dispatch that already returned:

```csharp
    public async ValueTask DisposeAsync()
    {
        await _givingUpReport.DisposeAsync();
        await _stopRenewing.CancelAsync();
        await _renewing;
        _stopRenewing.Dispose();
        _lockLost.Dispose();
    }
```

and the method:

```csharp
    /// <summary>
    /// Runs when <see cref="LockLost"/> is cancelled. The lock-taken branch logs its own warning; any other cancellation
    /// comes from the watchdog, and without this warning it would look like an ordinary shutdown in the logs.
    /// </summary>
    private void ReportGivingUp(InboxMessage message, ILogger log)
    {
        if (_lockTakenReported)
            return;

        log.LogWarning(
            "The lock on message {MessageId} on endpoint {EndpointId} could not be renewed before it neared expiry; cancelling its dispatch.",
            message.Id, message.EndpointId);
    }
```

In the owner-mismatch branch of `RenewUntilStoppedAsync`, set the flag before logging:

```csharp
                _lockTakenReported = true;
                log.LogWarning(
                    "Delivery no longer holds the lock on message {MessageId} on endpoint {EndpointId}; cancelling its dispatch.",
                    message.Id, message.EndpointId);
                await _lockLost.CancelAsync();
                return;
```

and in the general `catch (Exception ex)` add the endpoint:

```csharp
                log.LogWarning(
                    ex,
                    "Could not renew the lock on message {MessageId} on endpoint {EndpointId}; its dispatch is cancelled if the lock nears expiry first.",
                    message.Id, message.EndpointId);
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet build -c Release && dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.MessageLockRenewerTests|FullyQualifiedName~MongoBus.Tests.LockRenewalTests"`
Expected: PASS (MessageLockRenewerTests 12, LockRenewalTests 6).

- [ ] **Step 5: Prove the lock-taken warning guard can fail**

Temporarily delete `if (_lockTakenReported) return;` from `ReportGivingUp`.
Run: `dotnet build -c Release && dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.MessageLockRenewerTests.Lease_WarnsOnlyThatTheLockWasTaken_WhenAnotherConsumerTakesIt"`
Expected: FAIL — two warnings ("no longer holds the lock" and "could not be renewed before it neared expiry").
Restore the deleted lines and re-run the Step 4 command: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/MongoBus/Internal/MessageLockLease.cs tests/MongoBus.Tests/MessageLockRenewerTests.cs
git commit -m "chat-agent-loop: warn when a lock lease gives up before the lock expires"
```

After the three fixes: rebase onto the current `origin/main` (it now carries PR #51, the `ConcurrencyTests` rewrite), re-run every command under Global Constraints "Verification commands" and record the results under `## Verification`.

### Fix 4: The give-up tests measure the watchdog's budget, not a wall-clock timestamp

**Why.** Post-fix verification failed once on `Lease_SignalsLockLostBeforeTheLockExpires_WhenARenewalHangs` ("Expected … to be before <22:41:21.773>, but found <22:41:22.6815325>", MongoBus.Tests 286/287), and a bisect plus instrumentation traced it to this machine's clock rather than to the library:

- A monitor sampling `CLOCK_REALTIME - CLOCK_MONOTONIC` every 5 ms recorded the wall clock stepping forward 1.41-1.81 s about every 33.8 s (`systemd-timesyncd`: 32 s poll, `Offset: +1.707 s`; the WSL2 VM clock runs ~4.7% slow, so each poll steps it).
- `CancelAfter` counts on the monotonic clock, and both instrumented failures sat exactly on logged steps: in one, a dedicated thread sleeping the same interval woke at monotonic 2501 ms of 2464 ms armed — on time — while the wall clock jumped 1.810 s, putting the observed cancellation 1.318 s past the stored `LockedUntilUtc`.
- Ruled out with data: callback ordering (the whole chain ran in under 7 ms) and thread-pool starvation (`ThreadCount` 4-5, `PendingWorkItemCount` 0 at every deadline).
- No commit introduced it: `8d70520` (Task 4) 0 failures in 12 runs, `114ed68` (Fix 1) 0 timing failures in 12, `95ae5ab` 3 in 12 — one rate, since the vulnerable window is about 7% per test (P(0 of 12) ≈ 0.15).

The tests therefore compare a monotonic deadline against a wall-clock timestamp, which a clock step invalidates. What the watchdog promises is a budget: give up before `LockTime - LockTime/6` of the lock's life is spent. Measuring that with a `Stopwatch` (monotonic, like `CancelAfter`) tests the real behaviour and is immune to steps. Production impact of the underlying clock sensitivity is bounded and accepted, see the follow-up entry above: at the 30-60 s lock times GenCAD will use, the margin is 5-10 s and absorbs a 1.8 s step; only the tests' 3 s lock is exposed.

**Files:**
- Test: `tests/MongoBus.Tests/MessageLockRenewerTests.cs` (`Lease_SignalsLockLostBeforeTheLockExpires_WhenRenewalsKeepFailing`, `Lease_SignalsLockLostBeforeTheLockExpires_WhenARenewalHangs`)

**Interfaces:** none.

- [ ] **Step 1: Measure the watchdog's budget**

Add `using System.Diagnostics;` to the test file. In both tests, start the stopwatch immediately before acquiring the lease:

```csharp
        var sinceClaim = Stopwatch.StartNew();
        await using var lease = await NewRenewer(inbox).TryAcquireLeaseAsync(message, LeaseLockTime, CancellationToken.None);
```

and replace the two closing statements

```csharp
        var expiry = (await ReloadAsync(InboxIn(databaseName), message)).LockedUntilUtc!.Value;

        (await CancellationTimeAsync(lease!.LockLost, TimeSpan.FromSeconds(8))).Should().BeBefore(expiry);
```

with

```csharp
        await CancellationTimeAsync(lease!.LockLost, TimeSpan.FromSeconds(8));

        sinceClaim.Elapsed.Should().BeLessThan(LeaseLockTime - LeaseLockTime / 6 + TimeSpan.FromMilliseconds(500));
```

(`CancellationTimeAsync` still throws `TimeoutException` if the lease never gives up, so the test keeps asserting that it does.)

- [ ] **Step 2: Prove the assertion still catches a late give-up**

Temporarily change `var deadline = extendedAt.Add(lockTime - lockTime / 6);` in `GiveUpBeforeExpiry` (`src/MongoBus/Internal/MessageLockLease.cs`) to `var deadline = extendedAt.Add(lockTime + lockTime / 6);`.
Run: `dotnet build -c Release && dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.MessageLockRenewerTests.Lease_SignalsLockLostBeforeTheLockExpires"`
Expected: FAIL, both tests — the lease gives up at about 3.5 s, past the 3.0 s budget the assertion allows.
Restore the line; `git diff src/MongoBus/Internal/MessageLockLease.cs` must be empty.

- [ ] **Step 3: Run the tests to verify they pass across clock steps**

Run the Step 2 command six times in a row (the ~34 s step cadence means several runs will span a step), then `dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.MessageLockRenewerTests"` once.
Expected: PASS each time (2 tests, then 12).

- [ ] **Step 4: Commit**

```bash
git add tests/MongoBus.Tests/MessageLockRenewerTests.cs docs/superpowers/plans/2026-09-15-chat-agent-loop-mongobus-lock-renewal.md
git commit -m "chat-agent-loop: measure the lease give-up against the watchdog budget"
```

### Fix-commit correctness review (deep-reviewer, lens correctness, scope `b6e373f..5a39b23`) — "Acceptable with concerns, merge with caution"

No Critical or Important findings. It traced the shutdown, cancellation, disposal and outcome-write paths and found no way for a lease that outlives the stopping token to extend or clobber another delivery's lock: `TryExtendAsync` filters on `_id + LockOwner + Status == Pending` and `LockOwnerFor` mints a per-lock owner for renewing endpoints only. Consumers without `RenewLock` are equivalent to `main`. Decisions:

- **[Medium] The give-up warning runs on the watchdog's timer thread with no catch frame above it.** Accepted — Fix 5. Every other `ILogger` call in the lease sits inside `RenewUntilStoppedAsync`'s `catch`; the registration added in Fix 3 is invoked from `CancelAfter`, where a throwing logging provider (disk full, a disposed exporter during shutdown, a failing scope enricher) has no handler and can take the process down, in exactly the degraded-MongoDB scenario the watchdog serves. The plan's earlier reason for deferring ("no other logging call in the library is guarded") does not hold, because no other call runs in a cancellation callback.
- **[Low] `HandlerStillFinishingWhileTheBusStops_KeepsItsLockUntilItReturns` compares a stored wall-clock expiry with a wall-clock read**, the pattern Fix 4 removed elsewhere, with a worst-case margin of about 0.2 s against this machine's 1.4-1.8 s clock steps. Accepted — Fix 6 (6-second lock, 8-second observation; `readAt` stays, because `locked.LockedUntilUtc` would stop failing when Fix 1 is reverted).
- **[Low] `Task.Delay(storedExpiry - DateTime.UtcNow + 250 ms)` can throw** instead of asserting. Accepted — Fix 6 (floor at zero).
- **[Low] The give-up budget is duplicated and its 500 ms tolerance equals `LeaseLockTime / 6`**, the whole margin under test. Accepted — Fix 6 (named `GiveUpBudget` and `SchedulingTolerance` constants, same values).
- **[Low] `_lockTakenReported` can be read before it is written**, producing one extra warning for a single event. Not fixed, as the reviewer recommends: `LockLost` is already cancelled and the dispatch already cancelled, the window needs a write returning `matchedCount == 0` at the same instant the timer fires, and it is not reproducible without instrumenting the lease. Follow-up: `Interlocked.Exchange` in both paths.
- Also noted and already on the follow-up list: `DisposeAsync` skips disposing its token sources if `await _renewing` rethrows and is not idempotent (M8), and Fix 1 slightly widens the microsecond-wide spurious "no longer holds the lock" warning after an outcome write.

### Fix 5: The give-up report cannot take the process down

**Files:**
- Modify: `src/MongoBus/Internal/MessageLockLease.cs` (`ReportGivingUp`)
- Test: `tests/MongoBus.Tests/MessageLockRenewerTests.cs`

**Interfaces:** none.

- [ ] **Step 1: Write the failing test**

Add to `MessageLockRenewerTests`:

```csharp
    [Fact]
    public async Task Lease_StillGivesUp_WhenTheLoggerThrows()
    {
        var databaseName = NewDatabaseName();
        var applicationName = NewApplicationName();
        var inbox = InboxIn(databaseName, applicationName);
        var message = await InsertLockedAsync(inbox, LeaseLockTime);

        await using var lease = await new MessageLockRenewer(inbox, new LoggerThatFailsOnGiveUp())
            .TryAcquireLeaseAsync(message, LeaseLockTime, CancellationToken.None);
        await using var failures = await UpdateFailures.InjectAsync(fixture.ConnectionString, applicationName, "alwaysOn");

        var signalled = await WaitForCancellationAsync(lease!.LockLost, TimeSpan.FromSeconds(8));

        signalled.Should().BeTrue();
    }

    /// <summary>Fails only on the give-up warning, so the renewal loop's own logging is unaffected.</summary>
    private sealed class LoggerThatFailsOnGiveUp : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (formatter(state, exception).Contains("neared expiry"))
                throw new InvalidOperationException("logging provider failed");
        }
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `dotnet build -c Release && dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.MessageLockRenewerTests.Lease_StillGivesUp_WhenTheLoggerThrows"`
Expected: FAIL. The exception is thrown inside a cancellation callback on the watchdog's timer thread, so it is unhandled: expect the run to report the test host crashing (or the test failing with `InvalidOperationException`), not a clean assertion failure. Record exactly what the runner printed.

- [ ] **Step 3: Write minimal implementation**

In `ReportGivingUp`, guard the single log call:

```csharp
        try
        {
            log.LogWarning(
                "The lock on message {MessageId} on endpoint {EndpointId} could not be renewed before it neared expiry; cancelling its dispatch.",
                message.Id, message.EndpointId);
        }
        catch (Exception)
        {
            // This runs in a cancellation callback on the watchdog's timer thread. No caller can catch a logging
            // provider that throws there, and an unhandled exception would end the process; losing the warning is
            // the lesser failure, and the dispatch is cancelled either way.
        }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `dotnet build -c Release && dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.MessageLockRenewerTests"`
Expected: PASS, 13 tests.

- [ ] **Step 5: Commit**

```bash
git add src/MongoBus/Internal/MessageLockLease.cs tests/MongoBus.Tests/MessageLockRenewerTests.cs docs/superpowers/plans/2026-09-15-chat-agent-loop-mongobus-lock-renewal.md
git commit -m "chat-agent-loop: keep a failing logger from ending the process when a lease gives up"
```

### Fix 6: Test robustness — clock margin, delay floor, named budget

**Files:**
- Test: `tests/MongoBus.Tests/LockRenewalTests.cs` (`FinishingDefinition`, `HandlerStillFinishingWhileTheBusStops_KeepsItsLockUntilItReturns`)
- Test: `tests/MongoBus.Tests/MessageLockRenewerTests.cs` (crash-redelivery test, both give-up tests)

**Interfaces:** none.

- [ ] **Step 1: Widen the shutdown test's margin**

In `LockRenewalTests`, `FinishingDefinition.LockTime` becomes `TimeSpan.FromSeconds(6)`, and in `HandlerStillFinishingWhileTheBusStops_KeepsItsLockUntilItReturns` the observation delay `await Task.Delay(TimeSpan.FromSeconds(5));` becomes `await Task.Delay(TimeSpan.FromSeconds(8));`. The wait still exceeds `LockTime`, so the test still fails without Fix 1, and the margin against a 1.8 s clock step grows from about 0.2 s to about 4 s. Do not replace `readAt` with `locked.LockedUntilUtc`: under the old behaviour one post-stop renewal could advance the expiry past that value, and the assertion would stop failing when Fix 1 is reverted.

- [ ] **Step 2: Floor the crash-redelivery delay**

In `MessageLockRenewerTests`, `MessageWhoseRenewalStoppedWithoutARelease_IsLockableByAnotherPumpOnlyAfterItsLockLapses`, replace

```csharp
        await Task.Delay(storedExpiry - DateTime.UtcNow + TimeSpan.FromMilliseconds(250));
```

with

```csharp
        var untilLapse = storedExpiry - DateTime.UtcNow + TimeSpan.FromMilliseconds(250);
        await Task.Delay(untilLapse > TimeSpan.Zero ? untilLapse : TimeSpan.Zero);
```

- [ ] **Step 3: Name the give-up budget**

In `MessageLockRenewerTests`, next to `LeaseLockTime`, add

```csharp
    private static readonly TimeSpan GiveUpBudget = LeaseLockTime - LeaseLockTime / 6;
    private static readonly TimeSpan SchedulingTolerance = TimeSpan.FromMilliseconds(500);
```

and in both `Lease_SignalsLockLostBeforeTheLockExpires_*` tests replace

```csharp
        sinceClaim.Elapsed.Should().BeLessThan(LeaseLockTime - LeaseLockTime / 6 + TimeSpan.FromMilliseconds(500));
```

with

```csharp
        sinceClaim.Elapsed.Should().BeLessThan(GiveUpBudget + SchedulingTolerance);
```

(Same values; the deadline and its tolerance are now named and defined once.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `dotnet build -c Release && dotnet test --no-build -c Release --filter "FullyQualifiedName~MongoBus.Tests.MessageLockRenewerTests|FullyQualifiedName~MongoBus.Tests.LockRenewalTests"`
Expected: PASS (MessageLockRenewerTests 13, LockRenewalTests 6). Run it twice.

- [ ] **Step 5: Commit**

```bash
git add tests/MongoBus.Tests/LockRenewalTests.cs tests/MongoBus.Tests/MessageLockRenewerTests.cs
git commit -m "chat-agent-loop: make the lease tests robust to clock steps and name the give-up budget"
```

After Fix 5 and Fix 6: re-run every command under Global Constraints "Verification commands", record the results under `## Verification`, and run one scoped correctness review over the new fix commits (second and final fix cycle).

### Review-fixes plan review (deep-reviewer, lens plan, single pass) — "Acceptable with concerns"

Decisions and Fixes 1-2 confirmed sound against the code; no rejected or deferred item blocks 3.1.0. Applied:
- [Medium] Fix 3's double-logging guard could not fail (`ContainSingle(predicate)` tolerates a second warning): it now asserts `ContainSingle()` over all warnings with a 9-second lock, and Step 5 proves it fails without the `_lockTakenReported` check; the other test is renamed `Lease_WarnsThatItGaveUp_WhenRenewalsKeepFailing` because it also logs "Could not renew" warnings.
- [Low] Fix 3's callback registration was discarded: it is kept in `_givingUpReport`, registered before the watchdog is armed and disposed first in `DisposeAsync`.
- [Low] The crash-redelivery test still depended on renewal timing: it now waits until the stored `LockedUntilUtc` instead of a fixed delay.
- [Low] The deferred pre-dispatch claim exception was described as a fault case: the follow-up now names the shutdown-drain trigger and its effects.
- Test hygiene: Fix 1's test releases the handler and stops the bus in `finally` for every failure after the bus starts.

Not applied:
- A narrow try/catch around the callback's `LogWarning` and the callback ordering relative to the dispatcher's log: recorded as a follow-up; no other logging call in the library is guarded, and the change would be the only swallowed exception in the lease.
- Replacing the 200 ms waits in Fix 3's tests with polling (optional in the review): the callback runs synchronously during cancellation, so 200 ms only absorbs continuation scheduling.
- A test that a disposed lease never reports giving up: disposing the registration first is a two-line change whose failure mode needs a hung renewal to reproduce; left to the correctness review of the fix commits.
