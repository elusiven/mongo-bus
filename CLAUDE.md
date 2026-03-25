# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MongoBus is a MongoDB-backed message bus library for .NET 10. It implements the **Inbox Pattern** with **CloudEvents** compliance for reliable, polyglot-compatible messaging. Published as NuGet packages.

## Build & Test Commands

```bash
# Build entire solution
dotnet build

# Run all tests (requires Docker — uses Testcontainers with real MongoDB)
dotnet test

# Run a single test class
dotnet test --filter "FullyQualifiedName~MongoBus.Tests.RetryTests"

# Run a single test method
dotnet test --filter "FullyQualifiedName~MongoBus.Tests.RetryTests.Should_Retry_Failed_Message"

# Build in Release mode (as CI does)
dotnet build -c Release

# Run the sample app
dotnet run --project samples/MongoBus.Sample
```

## Architecture

### Message Flow

Publisher → `MongoMessageBus` → Publish interceptors → `CloudEventEnveloper` → `CloudEventSerializer` → (optional) `ClaimCheckManager` → **bus_inbox** collection → `MongoBusRuntime` (background hosted service) → `MongoMessagePump` (atomic lock-and-fetch) → `MongoMessageDispatcher` → Consume interceptors → `IMessageHandler<T>`

### Key MongoDB Collections

- `bus_inbox` — Messages with status tracking (Pending/Processed/Dead), atomic locking for competing consumers
- `bus_bindings` — Topic-to-endpoint subscriptions (fan-out topology)
- `bus_outbox` — Transactional outbox for staged publishing
- `bus_saga_{type}` — Saga instance state (per state machine type)
- `bus_saga_history_{type}` — Saga transition audit log (when HistoryEnabled)

### Solution Structure

- `src/MongoBus/` — Core library: message bus, pump, dispatchers, claim-check, outbox relay, CloudEvents
- `src/MongoBus.Dashboard/` — Web monitoring dashboard (Svelte frontend)
- `src/MongoBus.ClaimCheck.{AzureBlob,GridFs,S3}/` — Pluggable claim-check storage providers
- `tests/MongoBus.Tests/` — Integration tests using xUnit + FluentAssertions + Testcontainers.MongoDb
- `samples/MongoBus.Sample/` — Complete working example

### Key Patterns

- **Competing consumers**: Atomic MongoDB locks via `LockedUntilUtc`/`LockOwner` fields
- **Retry**: Exponential backoff (`2^attempt` seconds), configurable max attempts, dead-lettering
- **Outbox**: `ITransactionalMessageBus` stages to outbox; `MongoOutboxRelayService` relays to inbox
- **Claim check**: Large messages offloaded to external storage (GridFS, Azure Blob, S3); threshold-based with optional GZip compression
- **Batch consumers**: Configurable batching by size/time with grouping strategies and backpressure
- **Fan-out**: One publish creates an inbox message per bound endpoint
- **Idempotency**: Per-consumer deduplication via CloudEvent ID
- **Correlation/Causation**: `BusContext` (AsyncLocal) propagates correlation IDs through nested publishes

### Saga State Machines

Sagas orchestrate long-running workflows via `MongoBusStateMachine<TInstance>`:
- **DSL**: `Initially()`, `During()`, `DuringAny()`, `When()`, `Ignore()`, `CompositeEvent()`
- **Activities**: Then, Publish, Send, Schedule, Request/Respond, TransitionTo, Finalize, If/IfElse/Switch, Catch — all with sync/async variants
- **Persistence**: `MongoSagaRepository` with optimistic concurrency (version field)
- **Event handler**: `SagaEventHandler<TInstance, TMessage>` implements `IMessageHandler<T>`, reusing the standard message pump
- **History**: Optional `SagaHistoryWriter` records every transition to `bus_saga_history_{type}`
- **Timeout**: `SagaTimeoutService` background service auto-expires stale sagas
- **Registration**: `AddMongoBusSaga<TStateMachine, TInstance>(opt => ...)` registers everything

Key files: `src/MongoBus/Abstractions/Saga/MongoBusStateMachine.cs`, `src/MongoBus/Internal/Saga/SagaEventHandler.cs`, `src/MongoBus/Internal/Saga/BehaviorBuilder.cs`

### Consumer Registration

Consumers are defined via `ConsumerDefinition<TConsumer, TMessage>` (single) or `BatchConsumerDefinition<TConsumer, TMessage>` (batch), registered through `AddMongoBusConsumer<>()` / `AddMongoBusBatchConsumer<>()` extension methods.

### Extensibility Points

- `IPublishInterceptor` / `IConsumeInterceptor` / `IBatchConsumeInterceptor` — Pipeline middleware
- `IPublishObserver` / `IConsumeObserver` / `IBatchObserver` — Telemetry/metrics hooks
- `IClaimCheckProvider` — Custom storage backends
- `IBatchGroupingStrategy` — Custom batch grouping logic

## Coding Guidelines (from AGENTS.txt)

- **SOLID principles and Clean Code are mandatory** — readability over cleverness
- **Testing priority**: Integration tests (with real MongoDB via Testcontainers) > E2E tests > Unit tests
- **Test naming**: Behavior-driven (e.g., `Should_Create_User_When_Input_Is_Valid`)
- **Test structure**: Arrange / Act / Assert
- Prefer real dependencies over mocks; unit tests only for complex algorithms or pure domain logic
- Clean Architecture: dependencies flow inward, infrastructure is replaceable

## Git Workflow

- **Never push directly to `main`** — always create a feature branch and open a PR via `gh pr create`
- Use `+semver:minor` for new features, `+semver:patch` for fixes/docs, `+semver:major` for breaking changes — append to PR title or commit message
- Branch naming: `feat/description`, `fix/description`, `test/description`, `docs/description`

## Versioning & CI

- GitVersion 5.x mainline mode — version derived from git history
- Semver bumps via commit messages: `+semver:major`, `+semver:minor`, `+semver:patch`
- GitHub Actions deploys to NuGet and GitHub Packages on push to `main`
