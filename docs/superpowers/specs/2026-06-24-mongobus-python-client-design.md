# MongoBus Python Client — Design

**Date:** 2026-06-24
**Status:** Approved (design); pending spec review
**Scope:** A minimal, wire-compatible Python publisher/consumer for MongoBus, published to PyPI as `mongo-bus`.

## 1. Goal

Provide a Python package that interoperates **as a full peer** with the .NET MongoBus over the same MongoDB database: it can publish messages that .NET consumers process, and consume messages that .NET (or Python) published. Compatibility is achieved by matching the existing on-the-wire format exactly — there is no shared binary, only the CloudEvents-over-MongoDB contract.

### In scope
- Publish with `bus_bindings` fan-out (one `bus_inbox` document per bound endpoint).
- Consume via the atomic lock-and-fetch pump (competing consumers).
- Exponential-backoff retry and dead-lettering.
- Per-endpoint idempotency (deduplication via CloudEvent id).
- Correlation/causation id propagation (contextvars).
- Binding registration helper (`bind`) with index creation.
- Both a synchronous (PyMongo) and an asynchronous (PyMongo async) API.

### Out of scope (deferred)
- Claim-check (large-payload offloading). A consumer that receives a
  `application/vnd.mongobus.claim-check+json` message raises a clear
  "claim-check not supported" error rather than mis-handling it.
- Transactional outbox.
- Sagas, batch consumers, interceptors/observers, dashboard.

## 2. Architecture

**Shared I/O-free core + thin sync/async shells.** All wire-format logic lives in
pure functions that take and return plain dicts and never touch MongoDB. The sync
and async shells call those builders and perform the actual driver I/O. This keeps
the compatibility-critical logic written and tested once; only the I/O glue is
duplicated.

### Package layout (`clients/python/`)
```
clients/python/
  pyproject.toml          # hatchling build; runtime dep: pymongo>=4.9
  README.md
  src/mongobus/
    __init__.py           # public exports + __version__
    constants.py          # collection names, status strings, defaults
    envelope.py           # CloudEvent envelope <-> JSON (pure)
    documents.py          # inbox-doc construction (pure)
    queries.py            # lock filter/update, dedup query, binding upsert (pure)
    retry.py              # backoff + dead-letter decision (pure)
    context.py            # correlation/causation contextvars + ConsumeContext
    errors.py             # exceptions
    _sync/bus.py, _sync/pump.py        # MongoBus (PyMongo)
    _async/bus.py, _async/pump.py      # AsyncMongoBus (PyMongo async)
  tests/
    test_envelope.py, test_queries.py, test_retry.py   # pure, no Mongo
    test_golden_compat.py                              # .NET-captured fixtures
    test_integration_sync.py, test_integration_async.py # testcontainers Mongo
    fixtures/                                           # golden bus_inbox docs/JSON
```

The four pure modules (`envelope`, `documents`, `queries`, `retry`) constitute the
entire wire-format contract. `_sync`/`_async` are thin shells over them.

### Distribution / import name
- PyPI distribution name: **`mongo-bus`**.
- Import package: **`mongobus`** (`from mongobus import MongoBus, AsyncMongoBus`).

## 3. Public API

```python
from mongobus import MongoBus            # sync; AsyncMongoBus for async

bus = MongoBus(uri="mongodb://...", database="appdb")

# Publish — mirrors .NET PublishAsync; returns number of inbox docs created.
bus.publish("OrderPlaced", {"orderId": "123", "total": 50},
            source="urn:python:orders")   # source default: urn:mongobus:unknown

# Bind a subscriber endpoint to a topic (unique Topic+EndpointId upsert).
bus.bind("OrderPlaced", endpoint_id="order-processor")

# Register a handler, then run the pump (blocking; async: await bus.run()).
@bus.consumer(endpoint_id="order-processor", type_id="OrderPlaced",
              max_attempts=10, idempotent=True)
def handle(ctx):
    ctx.data            # deserialized `data` dict
    ctx.cloud_event_id  # plus correlation_id / causation_id / attempt / ...
    # nested bus.publish(...) here inherits correlation, sets causation=ctx.cloud_event_id

bus.run()               # runs all registered endpoints
```

`publish` parameters mirror `IMessageBus.PublishAsync`:
`type_id`, `data`, `source=None`, `subject=None`, `id=None`, `time_utc=None`,
`deliver_at=None`, `correlation_id=None`, `causation_id=None`.

`ConsumeContext` exposes: `data`, `type_id`, `cloud_event_id`, `correlation_id`,
`causation_id`, `source`, `subject`, `attempt`, plus `envelope` (raw dict) and
`raw` (the inbox document).

## 4. Wire-format fidelity rules (non-negotiable)

Derived from the .NET source. These are the contract.

### `bus_inbox` document (top-level BSON, PascalCase)
`_id` (ObjectId, from `Id`), `EndpointId`, `Topic`, `TypeId`, `PayloadJson`,
`CreatedUtc`, `VisibleUtc`, `LockedUntilUtc`, `LockOwner`, `Attempt`, `Status`,
`ProcessedUtc`, `LastError`, `CorrelationId`, `CausationId`, `CloudEventId`.

- `Status` is a **string**: `"Pending"` / `"Processed"` / `"Dead"` (exact case).
- `PayloadJson` is **opaque JSON text** (the whole CloudEvent), not nested BSON.
- `Attempt` is Int32, starts at 0.
- All datetimes are **tz-aware UTC** BSON datetimes.
- Nullable fields serialize as BSON `null`.

### CloudEvent envelope (the `PayloadJson` content, camelCase, .NET-specific spellings)
`specVersion` (**capital V**) = `"1.0"`, `id`, `type`, `source`, `time`
(ISO-8601 UTC), `subject`, `dataContentType`, `correlationId`, `causationId`,
`traceParent`, `traceState`, `data`.

- **Null keys are omitted entirely** (`DefaultIgnoreCondition = WhenWritingNull`).
- CloudEvent `id` default = `uuid4().hex` (32-char dashless), denormalized into the
  top-level `CloudEventId`.
- These are **not** the CloudEvents-spec spellings (`specversion`,
  `datacontenttype`); they must match .NET's camelCased names exactly.

### `bus_bindings` document
`_id` (ObjectId), `Topic`, `EndpointId`. Unique compound index `(Topic, EndpointId)`.
Bind = upsert with filter `EndpointId == ep AND Topic == topic`, `SetOnInsert` both.

### Collection names
`bus_inbox`, `bus_bindings` (constants).

A `test_golden_compat.py` asserts Python parses .NET-produced fixtures and produces
a byte-equivalent envelope plus the expected BSON field set — the guard against
silent drift.

## 5. Consume pump (mirrors `MongoMessagePump`)

`find_one_and_update`, sort `VisibleUtc` ascending, `return_document=AFTER`.

- **Filter:** `EndpointId == ep AND Status == "Pending" AND VisibleUtc < now AND
  (LockedUntilUtc == None OR LockedUntilUtc < now)` [`+ TypeId in [...]` when filtered].
- **Update:** `set LockedUntilUtc = now + lock_duration`, `set LockOwner = pump_id`.
  The lock does **not** change `Status` (stays `"Pending"`).
- `pump_id = f"{gethostname()}:{uuid4().hex}:{endpoint_id}"`.
- `lock_duration` default **60s** (matches `ConsumerDefinition.LockTime`).
- Idle poll delay **50ms** when no message is available.

On claim:
1. **Idempotency skip** (when `idempotent`): see §6.
2. Parse `PayloadJson` → envelope → `data`.
3. Invoke handler.
4. **Success:** `set Status="Processed"`, `set ProcessedUtc=now`, clear
   `LockOwner`/`LockedUntilUtc`.
5. **Failure:** `next = Attempt + 1`.
   - If `next >= max_attempts` → `set Status="Dead"`, `set Attempt=next`,
     `set LastError=<concise>`, clear lock fields.
   - Else → `set Status="Pending"`, `set Attempt=next`,
     `set VisibleUtc = now + 2**next` seconds, `set LastError=<concise>`,
     clear lock fields.

`LastError` stores a concise message, not a stack trace. `max_attempts` default 10.

## 6. Idempotency (per-endpoint, default on)

Before invoking the handler, query `bus_inbox` for
`EndpointId == ep AND CloudEventId == ce_id AND Status == "Processed" AND _id != this._id`.
If any document exists, mark the current message `"Processed"` with
`LastError = "Skipped due to idempotency"` and skip the handler. Uses the existing
`(EndpointId, CloudEventId)` index. Toggle via `idempotent=True` on `@bus.consumer`.

## 7. Correlation / causation (contextvars)

`context.py` holds `_current_context: ContextVar[ConsumeContext | None]`.

- On publish: `correlation_id = arg or ctx.correlation_id or uuid4().hex`;
  `causation_id = arg or ctx.cloud_event_id`.
- The pump sets the contextvar around each handler call and resets it afterward, so
  nested publishes inside a handler link correctly across .NET↔Python — matching
  `BusContext` (AsyncLocal) semantics.

**To confirm during implementation:** `ConsumeContext.MessageId` in .NET is read as
the CloudEvent `id`; verify against `MongoBusRuntime.cs:286-293` so causation matches
exactly.

## 8. Testing strategy (TDD)

Development follows the red-green-refactor cycle. Each unit of behavior gets a
failing test first, the minimal implementation to pass, then refactoring.

- **Pure unit tests** (`test_envelope`, `test_queries`, `test_retry`) — the
  compatibility-critical logic. Fast, no Mongo. These drive the core modules.
- **Golden-fixture tests** (`test_golden_compat`) — cross-language byte
  compatibility against fixtures captured from the .NET implementation.
- **Integration tests** (`test_integration_sync`, `test_integration_async`) against
  real MongoDB via `testcontainers[mongodb]` + `pytest` / `pytest-asyncio`, mirroring
  the .NET Testcontainers approach (project convention: real dependencies over mocks).
- Behavior-driven test names (e.g. `test_should_dead_letter_after_max_attempts`),
  Arrange/Act/Assert structure.

### Coverage gate
- **≥95% line coverage**, enforced via `pytest-cov` with `--cov-fail-under=95` in CI.
  The pure core modules should approach 100%; integration tests cover the I/O shells.

## 9. CI / versioning / PyPI

A new GitHub Actions workflow, path-filtered to `clients/python/**`, independent of
the .NET GitVersion/NuGet pipeline:
- Run `pytest` with coverage gate (Docker available for testcontainers).
- Build sdist + wheel.
- Publish to PyPI on a `py-v*` git tag via trusted publishing (OIDC).

Python version is managed in `pyproject.toml` (manual or `setuptools-scm` with the
`py-v` tag prefix) so it never collides with the .NET semver.

## 10. Open items (flagged, non-blocking)

1. **PyPI name** — distribution name `mongo-bus` chosen; confirm availability before
   first publish.
2. **`ConsumeContext.MessageId` source** — confirm it is the CloudEvent `id`
   (`MongoBusRuntime.cs:286-293`) during implementation.
3. **Claim-check** — deferred; raise a clear error on claim-check payloads for now.

## 11. Process

- TDD throughout (red-green-refactor).
- After implementation: a deep code review (staff-engineer-level) of the new package.
