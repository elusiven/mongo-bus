# MongoBus Python Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Python package (`mongo-bus`, import `mongobus`) that interoperates as a full peer with the .NET MongoBus over the same MongoDB, publishing and consuming CloudEvents-over-MongoDB messages with idempotency, retry, and dead-lettering.

**Architecture:** A shared, I/O-free core of pure functions (envelope serialize/deserialize, inbox-document construction, lock filter/update builders, retry math) holds the entire wire-format contract. Thin synchronous (PyMongo) and asynchronous (PyMongo async) shells call those builders and perform the actual MongoDB I/O. The pure core is unit-tested without MongoDB; the shells are integration-tested against real MongoDB via Testcontainers.

**Tech Stack:** Python 3.10+, PyMongo 4.9+ (sync + native async), pytest, pytest-asyncio, pytest-cov, testcontainers[mongodb], hatchling (build).

## Global Constraints

- Distribution name `mongo-bus`; import package `mongobus`. Verbatim.
- Runtime dependency floor: `pymongo>=4.9` (native async lives in PyMongo 4.9+).
- Python floor: `>=3.10` (for `ContextVar`, `X | None` syntax).
- BSON top-level field names are **PascalCase**, exact: `EndpointId`, `Topic`, `TypeId`, `PayloadJson`, `CreatedUtc`, `VisibleUtc`, `LockedUntilUtc`, `LockOwner`, `Attempt`, `Status`, `ProcessedUtc`, `LastError`, `CorrelationId`, `CausationId`, `CloudEventId`; `_id` is the ObjectId.
- `Status` string values exact: `"Pending"`, `"Processed"`, `"Dead"`.
- CloudEvent JSON keys exact (camelCase, .NET spellings): `specVersion` (capital V) `= "1.0"`, `id`, `type`, `source`, `time`, `subject`, `dataContentType`, `correlationId`, `causationId`, `traceParent`, `traceState`, `data`. **Null keys omitted entirely.**
- CloudEvent `id` default = `uuid4().hex` (32-char dashless); denormalized into `CloudEventId`.
- All datetimes tz-aware UTC.
- Collection names exact: `bus_inbox`, `bus_bindings`.
- Defaults: `lock_duration = 60s`, idle poll = `50ms`, `max_attempts = 10`, retry backoff = `2 ** next_attempt` seconds.
- Test coverage gate: **≥95% line coverage** (`--cov-fail-under=95`).
- TDD throughout: red → green → refactor. Frequent commits.
- All commits use a feature branch (`feat/python-client`); never push to `main`.

---

### Task 1: Project scaffolding & constants

**Files:**
- Create: `clients/python/pyproject.toml`
- Create: `clients/python/README.md`
- Create: `clients/python/src/mongobus/__init__.py`
- Create: `clients/python/src/mongobus/constants.py`
- Create: `clients/python/tests/__init__.py`
- Test: `clients/python/tests/test_constants.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `mongobus.constants` module with module-level constants:
  - `INBOX_COLLECTION: str = "bus_inbox"`
  - `BINDINGS_COLLECTION: str = "bus_bindings"`
  - `STATUS_PENDING: str = "Pending"`, `STATUS_PROCESSED: str = "Processed"`, `STATUS_DEAD: str = "Dead"`
  - `SPEC_VERSION: str = "1.0"`
  - `DEFAULT_SOURCE: str = "urn:mongobus:unknown"`
  - `CLAIM_CHECK_CONTENT_TYPE: str = "application/vnd.mongobus.claim-check+json"`
  - `DEFAULT_LOCK_SECONDS: int = 60`
  - `DEFAULT_MAX_ATTEMPTS: int = 10`
  - `DEFAULT_POLL_SECONDS: float = 0.05`

- [ ] **Step 1: Write the failing test**

```python
# clients/python/tests/test_constants.py
from mongobus import constants


def test_collection_names_match_dotnet():
    assert constants.INBOX_COLLECTION == "bus_inbox"
    assert constants.BINDINGS_COLLECTION == "bus_bindings"


def test_status_values_match_dotnet():
    assert constants.STATUS_PENDING == "Pending"
    assert constants.STATUS_PROCESSED == "Processed"
    assert constants.STATUS_DEAD == "Dead"


def test_envelope_defaults_match_dotnet():
    assert constants.SPEC_VERSION == "1.0"
    assert constants.DEFAULT_SOURCE == "urn:mongobus:unknown"
    assert constants.CLAIM_CHECK_CONTENT_TYPE == "application/vnd.mongobus.claim-check+json"


def test_runtime_defaults():
    assert constants.DEFAULT_LOCK_SECONDS == 60
    assert constants.DEFAULT_MAX_ATTEMPTS == 10
    assert constants.DEFAULT_POLL_SECONDS == 0.05
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && python -m pytest tests/test_constants.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mongobus'`

- [ ] **Step 3: Create the build config**

```toml
# clients/python/pyproject.toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "mongo-bus"
version = "0.1.0"
description = "MongoDB-backed message bus client, wire-compatible with .NET MongoBus (CloudEvents over MongoDB)."
readme = "README.md"
requires-python = ">=3.10"
license = "MIT"
dependencies = ["pymongo>=4.9"]

[project.optional-dependencies]
test = ["pytest>=8", "pytest-asyncio>=0.24", "pytest-cov>=5", "testcontainers[mongodb]>=4"]

[tool.hatch.build.targets.wheel]
packages = ["src/mongobus"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]

[tool.coverage.run]
source = ["mongobus"]
branch = true
```

- [ ] **Step 4: Create the package files**

```python
# clients/python/src/mongobus/constants.py
INBOX_COLLECTION = "bus_inbox"
BINDINGS_COLLECTION = "bus_bindings"

STATUS_PENDING = "Pending"
STATUS_PROCESSED = "Processed"
STATUS_DEAD = "Dead"

SPEC_VERSION = "1.0"
DEFAULT_SOURCE = "urn:mongobus:unknown"
CLAIM_CHECK_CONTENT_TYPE = "application/vnd.mongobus.claim-check+json"

DEFAULT_LOCK_SECONDS = 60
DEFAULT_MAX_ATTEMPTS = 10
DEFAULT_POLL_SECONDS = 0.05
```

```python
# clients/python/src/mongobus/__init__.py
"""MongoBus Python client — wire-compatible with the .NET MongoBus implementation."""

__version__ = "0.1.0"
```

```python
# clients/python/tests/__init__.py
```

```markdown
<!-- clients/python/README.md -->
# mongo-bus (Python client)

Python publisher/consumer for [MongoBus](../../README.md), wire-compatible with the
.NET implementation. Publishes and consumes CloudEvents-over-MongoDB messages with
idempotency, retry, and dead-lettering.

```python
from mongobus import MongoBus

bus = MongoBus(uri="mongodb://localhost:27017", database="appdb")
bus.bind("OrderPlaced", endpoint_id="order-processor")
bus.publish("OrderPlaced", {"orderId": "123"})
```
```

Add the package to the test path. Create `clients/python/conftest.py`:

```python
# clients/python/conftest.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd clients/python && python -m pytest tests/test_constants.py -v`
Expected: PASS (4 passed)

- [ ] **Step 6: Commit**

```bash
git add clients/python
git commit -m "feat(python): scaffold mongo-bus package and constants +semver:minor"
```

---

### Task 2: Errors

**Files:**
- Create: `clients/python/src/mongobus/errors.py`
- Test: `clients/python/tests/test_errors.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `class MongoBusError(Exception)` — base.
  - `class ClaimCheckNotSupportedError(MongoBusError)` — raised when a consumer
    receives a claim-check payload.

- [ ] **Step 1: Write the failing test**

```python
# clients/python/tests/test_errors.py
import pytest

from mongobus.errors import MongoBusError, ClaimCheckNotSupportedError


def test_claim_check_error_is_a_mongobus_error():
    assert issubclass(ClaimCheckNotSupportedError, MongoBusError)


def test_claim_check_error_can_be_raised_with_message():
    with pytest.raises(MongoBusError, match="claim-check"):
        raise ClaimCheckNotSupportedError("claim-check not supported")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && python -m pytest tests/test_errors.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mongobus.errors'`

- [ ] **Step 3: Write minimal implementation**

```python
# clients/python/src/mongobus/errors.py
class MongoBusError(Exception):
    """Base class for all MongoBus client errors."""


class ClaimCheckNotSupportedError(MongoBusError):
    """Raised when a consumer receives a claim-check payload, which this client does not yet support."""
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && python -m pytest tests/test_errors.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/errors.py clients/python/tests/test_errors.py
git commit -m "feat(python): add error types +semver:patch"
```

---

### Task 3: CloudEvent envelope (serialize)

**Files:**
- Create: `clients/python/src/mongobus/envelope.py`
- Test: `clients/python/tests/test_envelope.py`

**Interfaces:**
- Consumes: `mongobus.constants` (`SPEC_VERSION`).
- Produces:
  - `def build_envelope(*, type_id: str, data, source: str, event_id: str, time_utc: datetime, subject: str | None = None, data_content_type: str | None = None, correlation_id: str | None = None, causation_id: str | None = None, trace_parent: str | None = None, trace_state: str | None = None) -> dict` — returns the envelope dict with **null keys omitted** and keys in .NET camelCase spellings.
  - `def serialize_envelope(envelope: dict) -> str` — `json.dumps` with `separators=(",", ":")` is NOT required; use default separators but ensure `time` is ISO-8601 UTC. Returns JSON text for `PayloadJson`.
  - `def new_event_id() -> str` — returns `uuid4().hex` (32-char dashless).

- [ ] **Step 1: Write the failing test**

```python
# clients/python/tests/test_envelope.py
import json
from datetime import datetime, timezone

from mongobus import envelope


def test_new_event_id_is_32_char_dashless_hex():
    eid = envelope.new_event_id()
    assert len(eid) == 32
    assert "-" not in eid
    int(eid, 16)  # parses as hex


def test_build_envelope_uses_dotnet_key_spellings():
    env = envelope.build_envelope(
        type_id="OrderPlaced",
        data={"orderId": "123"},
        source="urn:python:orders",
        event_id="abc",
        time_utc=datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc),
    )
    assert env["specVersion"] == "1.0"
    assert env["id"] == "abc"
    assert env["type"] == "OrderPlaced"
    assert env["source"] == "urn:python:orders"
    assert env["data"] == {"orderId": "123"}
    assert env["time"] == "2026-06-24T12:00:00+00:00"


def test_build_envelope_omits_null_keys():
    env = envelope.build_envelope(
        type_id="OrderPlaced",
        data={},
        source="s",
        event_id="abc",
        time_utc=datetime(2026, 6, 24, tzinfo=timezone.utc),
    )
    for absent in ("subject", "dataContentType", "correlationId",
                   "causationId", "traceParent", "traceState"):
        assert absent not in env


def test_build_envelope_includes_optional_keys_when_present():
    env = envelope.build_envelope(
        type_id="OrderPlaced",
        data={},
        source="s",
        event_id="abc",
        time_utc=datetime(2026, 6, 24, tzinfo=timezone.utc),
        subject="sub",
        data_content_type="application/json",
        correlation_id="corr",
        causation_id="cause",
        trace_parent="tp",
        trace_state="ts",
    )
    assert env["subject"] == "sub"
    assert env["dataContentType"] == "application/json"
    assert env["correlationId"] == "corr"
    assert env["causationId"] == "cause"
    assert env["traceParent"] == "tp"
    assert env["traceState"] == "ts"


def test_serialize_envelope_round_trips():
    env = envelope.build_envelope(
        type_id="OrderPlaced",
        data={"orderId": "123"},
        source="s",
        event_id="abc",
        time_utc=datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc),
    )
    text = envelope.serialize_envelope(env)
    assert json.loads(text)["data"]["orderId"] == "123"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && python -m pytest tests/test_envelope.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mongobus.envelope'`

- [ ] **Step 3: Write minimal implementation**

```python
# clients/python/src/mongobus/envelope.py
import json
import uuid
from datetime import datetime

from .constants import SPEC_VERSION


def new_event_id() -> str:
    return uuid.uuid4().hex


def build_envelope(
    *,
    type_id: str,
    data,
    source: str,
    event_id: str,
    time_utc: datetime,
    subject: str | None = None,
    data_content_type: str | None = None,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    trace_parent: str | None = None,
    trace_state: str | None = None,
) -> dict:
    envelope: dict = {
        "specVersion": SPEC_VERSION,
        "id": event_id,
        "type": type_id,
        "source": source,
        "time": time_utc.isoformat(),
    }
    optional = {
        "subject": subject,
        "dataContentType": data_content_type,
        "correlationId": correlation_id,
        "causationId": causation_id,
        "traceParent": trace_parent,
        "traceState": trace_state,
    }
    for key, value in optional.items():
        if value is not None:
            envelope[key] = value
    envelope["data"] = data
    return envelope


def serialize_envelope(envelope: dict) -> str:
    return json.dumps(envelope)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && python -m pytest tests/test_envelope.py -v`
Expected: PASS (5 passed)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/envelope.py clients/python/tests/test_envelope.py
git commit -m "feat(python): add CloudEvent envelope serialization +semver:minor"
```

---

### Task 4: CloudEvent envelope (parse) + claim-check guard

**Files:**
- Modify: `clients/python/src/mongobus/envelope.py`
- Modify: `clients/python/tests/test_envelope.py`

**Interfaces:**
- Consumes: `mongobus.constants` (`CLAIM_CHECK_CONTENT_TYPE`), `mongobus.errors`.
- Produces:
  - `def parse_envelope(payload_json: str) -> dict` — `json.loads` the stored
    `PayloadJson`. Raises `ClaimCheckNotSupportedError` when
    `dataContentType == CLAIM_CHECK_CONTENT_TYPE`.

- [ ] **Step 1: Write the failing test**

```python
# append to clients/python/tests/test_envelope.py
import pytest

from mongobus.errors import ClaimCheckNotSupportedError


def test_parse_envelope_reads_camelcase_keys():
    text = '{"specVersion":"1.0","id":"abc","type":"OrderPlaced","data":{"orderId":"123"}}'
    env = envelope.parse_envelope(text)
    assert env["id"] == "abc"
    assert env["data"]["orderId"] == "123"


def test_parse_envelope_rejects_claim_check_payloads():
    text = '{"id":"abc","dataContentType":"application/vnd.mongobus.claim-check+json","data":{}}'
    with pytest.raises(ClaimCheckNotSupportedError):
        envelope.parse_envelope(text)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && python -m pytest tests/test_envelope.py -k parse -v`
Expected: FAIL with `AttributeError: module 'mongobus.envelope' has no attribute 'parse_envelope'`

- [ ] **Step 3: Write minimal implementation**

```python
# append imports at top of clients/python/src/mongobus/envelope.py
from .constants import SPEC_VERSION, CLAIM_CHECK_CONTENT_TYPE
from .errors import ClaimCheckNotSupportedError
```

```python
# append function to clients/python/src/mongobus/envelope.py
def parse_envelope(payload_json: str) -> dict:
    envelope = json.loads(payload_json)
    if envelope.get("dataContentType") == CLAIM_CHECK_CONTENT_TYPE:
        raise ClaimCheckNotSupportedError(
            "Received a claim-check payload; the Python client does not support claim-check yet."
        )
    return envelope
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && python -m pytest tests/test_envelope.py -v`
Expected: PASS (7 passed)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/envelope.py clients/python/tests/test_envelope.py
git commit -m "feat(python): parse envelopes and reject claim-check payloads +semver:minor"
```

---

### Task 5: Inbox-document construction

**Files:**
- Create: `clients/python/src/mongobus/documents.py`
- Test: `clients/python/tests/test_documents.py`

**Interfaces:**
- Consumes: `mongobus.constants` (`STATUS_PENDING`).
- Produces:
  - `def build_inbox_document(*, endpoint_id: str, topic: str, type_id: str, payload_json: str, cloud_event_id: str, created_utc: datetime, visible_utc: datetime, correlation_id: str | None, causation_id: str | None) -> dict` — returns a dict with **PascalCase** keys, `Attempt = 0`, `Status = "Pending"`, lock/processed/error fields set to `None`. No `_id` (Mongo assigns it).

- [ ] **Step 1: Write the failing test**

```python
# clients/python/tests/test_documents.py
from datetime import datetime, timezone

from mongobus import documents


def _doc():
    now = datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc)
    return documents.build_inbox_document(
        endpoint_id="order-processor",
        topic="OrderPlaced",
        type_id="OrderPlaced",
        payload_json='{"id":"abc"}',
        cloud_event_id="abc",
        created_utc=now,
        visible_utc=now,
        correlation_id="corr",
        causation_id=None,
    )


def test_inbox_document_uses_pascalcase_keys():
    doc = _doc()
    assert doc["EndpointId"] == "order-processor"
    assert doc["Topic"] == "OrderPlaced"
    assert doc["TypeId"] == "OrderPlaced"
    assert doc["PayloadJson"] == '{"id":"abc"}'
    assert doc["CloudEventId"] == "abc"
    assert doc["CorrelationId"] == "corr"


def test_inbox_document_defaults_match_dotnet():
    doc = _doc()
    assert doc["Attempt"] == 0
    assert doc["Status"] == "Pending"
    assert doc["LockedUntilUtc"] is None
    assert doc["LockOwner"] is None
    assert doc["ProcessedUtc"] is None
    assert doc["LastError"] is None
    assert doc["CausationId"] is None


def test_inbox_document_has_no_id_field():
    assert "_id" not in _doc()
    assert "Id" not in _doc()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && python -m pytest tests/test_documents.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mongobus.documents'`

- [ ] **Step 3: Write minimal implementation**

```python
# clients/python/src/mongobus/documents.py
from datetime import datetime

from .constants import STATUS_PENDING


def build_inbox_document(
    *,
    endpoint_id: str,
    topic: str,
    type_id: str,
    payload_json: str,
    cloud_event_id: str,
    created_utc: datetime,
    visible_utc: datetime,
    correlation_id: str | None,
    causation_id: str | None,
) -> dict:
    return {
        "EndpointId": endpoint_id,
        "Topic": topic,
        "TypeId": type_id,
        "PayloadJson": payload_json,
        "CreatedUtc": created_utc,
        "VisibleUtc": visible_utc,
        "LockedUntilUtc": None,
        "LockOwner": None,
        "Attempt": 0,
        "Status": STATUS_PENDING,
        "ProcessedUtc": None,
        "LastError": None,
        "CorrelationId": correlation_id,
        "CausationId": causation_id,
        "CloudEventId": cloud_event_id,
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && python -m pytest tests/test_documents.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/documents.py clients/python/tests/test_documents.py
git commit -m "feat(python): add inbox-document construction +semver:minor"
```

---

### Task 6: Query builders (lock, dedup, binding, state transitions)

**Files:**
- Create: `clients/python/src/mongobus/queries.py`
- Test: `clients/python/tests/test_queries.py`

**Interfaces:**
- Consumes: `mongobus.constants`.
- Produces (all pure; return PyMongo filter/update dicts):
  - `def lock_filter(*, endpoint_id: str, now: datetime, type_ids: list[str] | None) -> dict`
  - `def lock_update(*, now: datetime, lock_seconds: int, pump_id: str) -> dict`
  - `def dedup_filter(*, endpoint_id: str, cloud_event_id: str, exclude_id) -> dict`
  - `def processed_update(*, now: datetime, last_error: str | None = None) -> dict`
  - `def retry_update(*, now: datetime, next_attempt: int, visible_utc: datetime, last_error: str) -> dict`
  - `def dead_update(*, next_attempt: int, last_error: str) -> dict`
  - `def binding_filter(*, topic: str, endpoint_id: str) -> dict`
  - `def binding_set_on_insert(*, topic: str, endpoint_id: str) -> dict`
  - `def bindings_for_topic_filter(*, topic: str) -> dict`

- [ ] **Step 1: Write the failing test**

```python
# clients/python/tests/test_queries.py
from datetime import datetime, timedelta, timezone

from mongobus import queries

NOW = datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc)


def test_lock_filter_without_type_ids():
    f = queries.lock_filter(endpoint_id="ep", now=NOW, type_ids=None)
    assert f["EndpointId"] == "ep"
    assert f["Status"] == "Pending"
    assert f["VisibleUtc"] == {"$lt": NOW}
    assert {"LockedUntilUtc": None} in f["$or"]
    assert {"LockedUntilUtc": {"$lt": NOW}} in f["$or"]
    assert "TypeId" not in f


def test_lock_filter_with_type_ids():
    f = queries.lock_filter(endpoint_id="ep", now=NOW, type_ids=["A", "B"])
    assert f["TypeId"] == {"$in": ["A", "B"]}


def test_lock_update_sets_lock_fields():
    u = queries.lock_update(now=NOW, lock_seconds=60, pump_id="host:guid:ep")
    assert u["$set"]["LockedUntilUtc"] == NOW + timedelta(seconds=60)
    assert u["$set"]["LockOwner"] == "host:guid:ep"


def test_dedup_filter_excludes_current_document():
    f = queries.dedup_filter(endpoint_id="ep", cloud_event_id="ce", exclude_id="OID")
    assert f["EndpointId"] == "ep"
    assert f["CloudEventId"] == "ce"
    assert f["Status"] == "Processed"
    assert f["_id"] == {"$ne": "OID"}


def test_processed_update_clears_lock():
    u = queries.processed_update(now=NOW)
    assert u["$set"]["Status"] == "Processed"
    assert u["$set"]["ProcessedUtc"] == NOW
    assert u["$set"]["LockOwner"] is None
    assert u["$set"]["LockedUntilUtc"] is None


def test_processed_update_with_idempotency_note():
    u = queries.processed_update(now=NOW, last_error="Skipped due to idempotency")
    assert u["$set"]["LastError"] == "Skipped due to idempotency"


def test_retry_update_sets_backoff_and_pending():
    visible = NOW + timedelta(seconds=8)
    u = queries.retry_update(now=NOW, next_attempt=3, visible_utc=visible, last_error="boom")
    assert u["$set"]["Status"] == "Pending"
    assert u["$set"]["Attempt"] == 3
    assert u["$set"]["VisibleUtc"] == visible
    assert u["$set"]["LastError"] == "boom"
    assert u["$set"]["LockOwner"] is None
    assert u["$set"]["LockedUntilUtc"] is None


def test_dead_update_sets_dead_status():
    u = queries.dead_update(next_attempt=10, last_error="boom")
    assert u["$set"]["Status"] == "Dead"
    assert u["$set"]["Attempt"] == 10
    assert u["$set"]["LastError"] == "boom"
    assert u["$set"]["LockOwner"] is None
    assert u["$set"]["LockedUntilUtc"] is None


def test_binding_filter_and_set_on_insert():
    assert queries.binding_filter(topic="T", endpoint_id="ep") == {"Topic": "T", "EndpointId": "ep"}
    assert queries.binding_set_on_insert(topic="T", endpoint_id="ep") == {
        "$setOnInsert": {"Topic": "T", "EndpointId": "ep"}
    }


def test_bindings_for_topic_filter():
    assert queries.bindings_for_topic_filter(topic="T") == {"Topic": "T"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && python -m pytest tests/test_queries.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mongobus.queries'`

- [ ] **Step 3: Write minimal implementation**

```python
# clients/python/src/mongobus/queries.py
from datetime import datetime, timedelta

from .constants import STATUS_PENDING, STATUS_PROCESSED, STATUS_DEAD


def lock_filter(*, endpoint_id: str, now: datetime, type_ids: list[str] | None) -> dict:
    f: dict = {
        "EndpointId": endpoint_id,
        "Status": STATUS_PENDING,
        "VisibleUtc": {"$lt": now},
        "$or": [{"LockedUntilUtc": None}, {"LockedUntilUtc": {"$lt": now}}],
    }
    if type_ids:
        f["TypeId"] = {"$in": list(type_ids)}
    return f


def lock_update(*, now: datetime, lock_seconds: int, pump_id: str) -> dict:
    return {
        "$set": {
            "LockedUntilUtc": now + timedelta(seconds=lock_seconds),
            "LockOwner": pump_id,
        }
    }


def dedup_filter(*, endpoint_id: str, cloud_event_id: str, exclude_id) -> dict:
    return {
        "EndpointId": endpoint_id,
        "CloudEventId": cloud_event_id,
        "Status": STATUS_PROCESSED,
        "_id": {"$ne": exclude_id},
    }


def processed_update(*, now: datetime, last_error: str | None = None) -> dict:
    fields = {
        "Status": STATUS_PROCESSED,
        "ProcessedUtc": now,
        "LockOwner": None,
        "LockedUntilUtc": None,
    }
    if last_error is not None:
        fields["LastError"] = last_error
    return {"$set": fields}


def retry_update(*, now: datetime, next_attempt: int, visible_utc: datetime, last_error: str) -> dict:
    return {
        "$set": {
            "Status": STATUS_PENDING,
            "Attempt": next_attempt,
            "VisibleUtc": visible_utc,
            "LastError": last_error,
            "LockOwner": None,
            "LockedUntilUtc": None,
        }
    }


def dead_update(*, next_attempt: int, last_error: str) -> dict:
    return {
        "$set": {
            "Status": STATUS_DEAD,
            "Attempt": next_attempt,
            "LastError": last_error,
            "LockOwner": None,
            "LockedUntilUtc": None,
        }
    }


def binding_filter(*, topic: str, endpoint_id: str) -> dict:
    return {"Topic": topic, "EndpointId": endpoint_id}


def binding_set_on_insert(*, topic: str, endpoint_id: str) -> dict:
    return {"$setOnInsert": {"Topic": topic, "EndpointId": endpoint_id}}


def bindings_for_topic_filter(*, topic: str) -> dict:
    return {"Topic": topic}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && python -m pytest tests/test_queries.py -v`
Expected: PASS (10 passed)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/queries.py clients/python/tests/test_queries.py
git commit -m "feat(python): add MongoDB query builders +semver:minor"
```

---

### Task 7: Retry decision (backoff + dead-letter)

**Files:**
- Create: `clients/python/src/mongobus/retry.py`
- Test: `clients/python/tests/test_retry.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `def backoff_seconds(next_attempt: int) -> int` — returns `2 ** next_attempt`.
  - `def should_dead_letter(*, next_attempt: int, max_attempts: int) -> bool` —
    returns `next_attempt >= max_attempts`.

- [ ] **Step 1: Write the failing test**

```python
# clients/python/tests/test_retry.py
from mongobus import retry


def test_backoff_is_exponential():
    assert retry.backoff_seconds(1) == 2
    assert retry.backoff_seconds(2) == 4
    assert retry.backoff_seconds(3) == 8


def test_should_dead_letter_at_or_above_max():
    assert retry.should_dead_letter(next_attempt=10, max_attempts=10) is True
    assert retry.should_dead_letter(next_attempt=11, max_attempts=10) is True


def test_should_not_dead_letter_below_max():
    assert retry.should_dead_letter(next_attempt=9, max_attempts=10) is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && python -m pytest tests/test_retry.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mongobus.retry'`

- [ ] **Step 3: Write minimal implementation**

```python
# clients/python/src/mongobus/retry.py
def backoff_seconds(next_attempt: int) -> int:
    return 2 ** next_attempt


def should_dead_letter(*, next_attempt: int, max_attempts: int) -> bool:
    return next_attempt >= max_attempts
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && python -m pytest tests/test_retry.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/retry.py clients/python/tests/test_retry.py
git commit -m "feat(python): add retry backoff and dead-letter decision +semver:minor"
```

---

### Task 8: ConsumeContext & correlation/causation contextvars

**Files:**
- Create: `clients/python/src/mongobus/context.py`
- Test: `clients/python/tests/test_context.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `@dataclass(frozen=True) class ConsumeContext` with fields: `data`, `type_id: str`,
    `cloud_event_id: str`, `correlation_id: str | None`, `causation_id: str | None`,
    `source: str | None`, `subject: str | None`, `attempt: int`, `envelope: dict`, `raw: dict`.
    Classmethod `from_message(envelope: dict, raw: dict) -> ConsumeContext`.
  - `def current_context() -> ConsumeContext | None`
  - `@contextmanager def use_context(ctx: ConsumeContext)` — sets/resets the contextvar.
  - `def resolve_correlation_id(explicit: str | None) -> str` — `explicit or
    current.correlation_id or uuid4().hex`.
  - `def resolve_causation_id(explicit: str | None) -> str | None` — `explicit or
    current.cloud_event_id`.

- [ ] **Step 1: Write the failing test**

```python
# clients/python/tests/test_context.py
from mongobus import context
from mongobus.context import ConsumeContext


def _ctx():
    env = {"id": "ce-1", "type": "OrderPlaced", "source": "s",
           "correlationId": "corr-1", "causationId": "cause-0", "subject": "sub",
           "data": {"orderId": "123"}}
    raw = {"Attempt": 2}
    return ConsumeContext.from_message(env, raw)


def test_from_message_maps_envelope_fields():
    ctx = _ctx()
    assert ctx.cloud_event_id == "ce-1"
    assert ctx.type_id == "OrderPlaced"
    assert ctx.correlation_id == "corr-1"
    assert ctx.causation_id == "cause-0"
    assert ctx.subject == "sub"
    assert ctx.source == "s"
    assert ctx.attempt == 2
    assert ctx.data == {"orderId": "123"}


def test_current_context_is_none_by_default():
    assert context.current_context() is None


def test_use_context_sets_and_resets():
    ctx = _ctx()
    with context.use_context(ctx):
        assert context.current_context() is ctx
    assert context.current_context() is None


def test_resolve_correlation_prefers_explicit():
    assert context.resolve_correlation_id("explicit") == "explicit"


def test_resolve_correlation_falls_back_to_context():
    with context.use_context(_ctx()):
        assert context.resolve_correlation_id(None) == "corr-1"


def test_resolve_correlation_generates_when_no_context():
    cid = context.resolve_correlation_id(None)
    assert len(cid) == 32


def test_resolve_causation_uses_context_cloud_event_id():
    with context.use_context(_ctx()):
        assert context.resolve_causation_id(None) == "ce-1"


def test_resolve_causation_is_none_without_context():
    assert context.resolve_causation_id(None) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && python -m pytest tests/test_context.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mongobus.context'`

- [ ] **Step 3: Write minimal implementation**

```python
# clients/python/src/mongobus/context.py
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass


@dataclass(frozen=True)
class ConsumeContext:
    data: object
    type_id: str
    cloud_event_id: str
    correlation_id: str | None
    causation_id: str | None
    source: str | None
    subject: str | None
    attempt: int
    envelope: dict
    raw: dict

    @classmethod
    def from_message(cls, envelope: dict, raw: dict) -> "ConsumeContext":
        return cls(
            data=envelope.get("data"),
            type_id=envelope.get("type"),
            cloud_event_id=envelope.get("id"),
            correlation_id=envelope.get("correlationId"),
            causation_id=envelope.get("causationId"),
            source=envelope.get("source"),
            subject=envelope.get("subject"),
            attempt=raw.get("Attempt", 0),
            envelope=envelope,
            raw=raw,
        )


_current: ContextVar[ConsumeContext | None] = ContextVar("mongobus_context", default=None)


def current_context() -> ConsumeContext | None:
    return _current.get()


@contextmanager
def use_context(ctx: ConsumeContext):
    token = _current.set(ctx)
    try:
        yield
    finally:
        _current.reset(token)


def resolve_correlation_id(explicit: str | None) -> str:
    if explicit is not None:
        return explicit
    ctx = _current.get()
    if ctx is not None and ctx.correlation_id is not None:
        return ctx.correlation_id
    return uuid.uuid4().hex


def resolve_causation_id(explicit: str | None) -> str | None:
    if explicit is not None:
        return explicit
    ctx = _current.get()
    return ctx.cloud_event_id if ctx is not None else None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && python -m pytest tests/test_context.py -v`
Expected: PASS (8 passed)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/context.py clients/python/tests/test_context.py
git commit -m "feat(python): add consume context and correlation propagation +semver:minor"
```

---

### Task 9: Pump id + handler dispatch helper (pure)

**Files:**
- Create: `clients/python/src/mongobus/dispatch.py`
- Test: `clients/python/tests/test_dispatch.py`

**Interfaces:**
- Consumes: `mongobus.{queries,retry,context,envelope}`.
- Produces:
  - `def build_pump_id(endpoint_id: str) -> str` — `f"{gethostname()}:{uuid4().hex}:{endpoint_id}"`.
  - `def plan_failure(*, attempt: int, max_attempts: int, now: datetime, error: str) -> dict` —
    returns the update dict: `dead_update` when `should_dead_letter`, else
    `retry_update` with `visible_utc = now + backoff_seconds(next_attempt)`.
    `next_attempt = attempt + 1`. This isolates the retry/dead branch for unit testing
    without MongoDB.

- [ ] **Step 1: Write the failing test**

```python
# clients/python/tests/test_dispatch.py
from datetime import datetime, timedelta, timezone

from mongobus import dispatch

NOW = datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc)


def test_pump_id_format_has_three_colon_parts():
    pid = dispatch.build_pump_id("order-processor")
    parts = pid.split(":")
    assert len(parts) == 3
    assert parts[2] == "order-processor"
    assert len(parts[1]) == 32  # uuid hex


def test_plan_failure_retries_below_max():
    update = dispatch.plan_failure(attempt=2, max_attempts=10, now=NOW, error="boom")
    assert update["$set"]["Status"] == "Pending"
    assert update["$set"]["Attempt"] == 3
    assert update["$set"]["VisibleUtc"] == NOW + timedelta(seconds=8)  # 2**3
    assert update["$set"]["LastError"] == "boom"


def test_plan_failure_dead_letters_at_max():
    update = dispatch.plan_failure(attempt=9, max_attempts=10, now=NOW, error="boom")
    assert update["$set"]["Status"] == "Dead"
    assert update["$set"]["Attempt"] == 10
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && python -m pytest tests/test_dispatch.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mongobus.dispatch'`

- [ ] **Step 3: Write minimal implementation**

```python
# clients/python/src/mongobus/dispatch.py
import socket
import uuid
from datetime import datetime, timedelta

from . import queries, retry


def build_pump_id(endpoint_id: str) -> str:
    return f"{socket.gethostname()}:{uuid.uuid4().hex}:{endpoint_id}"


def plan_failure(*, attempt: int, max_attempts: int, now: datetime, error: str) -> dict:
    next_attempt = attempt + 1
    if retry.should_dead_letter(next_attempt=next_attempt, max_attempts=max_attempts):
        return queries.dead_update(next_attempt=next_attempt, last_error=error)
    visible = now + timedelta(seconds=retry.backoff_seconds(next_attempt))
    return queries.retry_update(
        now=now, next_attempt=next_attempt, visible_utc=visible, last_error=error
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && python -m pytest tests/test_dispatch.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/dispatch.py clients/python/tests/test_dispatch.py
git commit -m "feat(python): add pump id and failure planning helpers +semver:minor"
```

---

### Task 10: Golden cross-language compatibility fixtures

**Files:**
- Create: `clients/python/tests/fixtures/dotnet_envelope.json`
- Create: `clients/python/tests/fixtures/dotnet_inbox_fields.json`
- Test: `clients/python/tests/test_golden_compat.py`

**Interfaces:**
- Consumes: `mongobus.{envelope,documents}`.
- Produces: regression guard. The fixtures capture the literal JSON keys / BSON field
  names from the .NET implementation (per the spec's Global Constraints), so any drift
  in Python serialization fails loudly.

- [ ] **Step 1: Create the fixtures**

```json
// clients/python/tests/fixtures/dotnet_envelope.json
{
  "specVersion": "1.0",
  "id": "9f86d081884c7d659a2feaa0c55ad015",
  "type": "OrderPlaced",
  "source": "urn:mongobus:unknown",
  "time": "2026-06-24T12:00:00+00:00",
  "correlationId": "11112222333344445555666677778888",
  "data": {"orderId": "123", "total": 50}
}
```

```json
// clients/python/tests/fixtures/dotnet_inbox_fields.json
["EndpointId", "Topic", "TypeId", "PayloadJson", "CreatedUtc", "VisibleUtc",
 "LockedUntilUtc", "LockOwner", "Attempt", "Status", "ProcessedUtc", "LastError",
 "CorrelationId", "CausationId", "CloudEventId"]
```

- [ ] **Step 2: Write the failing test**

```python
# clients/python/tests/test_golden_compat.py
import json
from datetime import datetime, timezone
from pathlib import Path

from mongobus import envelope, documents

FIXTURES = Path(__file__).parent / "fixtures"


def test_python_parses_dotnet_envelope():
    text = (FIXTURES / "dotnet_envelope.json").read_text()
    env = envelope.parse_envelope(text)
    assert env["specVersion"] == "1.0"
    assert env["id"] == "9f86d081884c7d659a2feaa0c55ad015"
    assert env["data"]["orderId"] == "123"


def test_python_envelope_keys_match_dotnet_set():
    golden = json.loads((FIXTURES / "dotnet_envelope.json").read_text())
    env = envelope.build_envelope(
        type_id="OrderPlaced",
        data={"orderId": "123", "total": 50},
        source="urn:mongobus:unknown",
        event_id="9f86d081884c7d659a2feaa0c55ad015",
        time_utc=datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc),
        correlation_id="11112222333344445555666677778888",
    )
    assert set(env.keys()) == set(golden.keys())


def test_inbox_document_field_set_matches_dotnet():
    expected = set(json.loads((FIXTURES / "dotnet_inbox_fields.json").read_text()))
    now = datetime(2026, 6, 24, tzinfo=timezone.utc)
    doc = documents.build_inbox_document(
        endpoint_id="ep", topic="T", type_id="T", payload_json="{}",
        cloud_event_id="ce", created_utc=now, visible_utc=now,
        correlation_id=None, causation_id=None,
    )
    assert set(doc.keys()) == expected
```

- [ ] **Step 3: Run test to verify it fails, then passes**

Run: `cd clients/python && python -m pytest tests/test_golden_compat.py -v`
Expected: PASS (3 passed) — fixtures and implementation already exist from prior tasks; this task locks the contract. If any assertion fails, the producing task (3, 5) has drifted and must be fixed.

- [ ] **Step 4: Commit**

```bash
git add clients/python/tests/fixtures clients/python/tests/test_golden_compat.py
git commit -m "test(python): lock cross-language wire-format with golden fixtures +semver:patch"
```

---

### Task 11: Synchronous bus — publish & bind

**Files:**
- Create: `clients/python/src/mongobus/_sync/__init__.py`
- Create: `clients/python/src/mongobus/_sync/bus.py`
- Modify: `clients/python/src/mongobus/__init__.py`
- Test: `clients/python/tests/test_integration_sync.py`

**Interfaces:**
- Consumes: `mongobus.{constants,envelope,documents,queries,context}`, `pymongo`.
- Produces:
  - `class MongoBus` with:
    - `__init__(self, uri: str, database: str, *, client=None)` — uses provided
      `client` (for tests) or `MongoClient(uri)`.
    - `def bind(self, type_id: str, *, endpoint_id: str) -> None` — ensures the unique
      `(Topic, EndpointId)` index, then upserts the binding.
    - `def publish(self, type_id, data, *, source=None, subject=None, id=None, time_utc=None, deliver_at=None, correlation_id=None, causation_id=None) -> int` —
      fans out one inbox doc per bound endpoint; returns the count. No-op (returns 0)
      when no bindings exist.

**Note on `now`:** use `datetime.now(timezone.utc)` for `created_utc`; `visible_utc = deliver_at or now`.

- [ ] **Step 1: Write the failing test** (integration; requires Docker)

```python
# clients/python/tests/test_integration_sync.py
import json

import pytest
from pymongo import MongoClient
from testcontainers.mongodb import MongoDbContainer

from mongobus import MongoBus


@pytest.fixture(scope="module")
def mongo():
    with MongoDbContainer("mongo:7") as container:
        yield container.get_connection_url()


@pytest.fixture
def db(mongo):
    client = MongoClient(mongo)
    name = "testdb"
    client.drop_database(name)
    yield client, name
    client.drop_database(name)


def test_publish_without_bindings_creates_no_messages(db):
    client, name = db
    bus = MongoBus(uri="", database=name, client=client)
    created = bus.publish("OrderPlaced", {"orderId": "1"})
    assert created == 0
    assert client[name]["bus_inbox"].count_documents({}) == 0


def test_publish_fans_out_one_message_per_endpoint(db):
    client, name = db
    bus = MongoBus(uri="", database=name, client=client)
    bus.bind("OrderPlaced", endpoint_id="ep-a")
    bus.bind("OrderPlaced", endpoint_id="ep-b")

    created = bus.publish("OrderPlaced", {"orderId": "1"}, source="urn:python:test")

    assert created == 2
    docs = list(client[name]["bus_inbox"].find({}))
    assert {d["EndpointId"] for d in docs} == {"ep-a", "ep-b"}
    one = docs[0]
    assert one["Status"] == "Pending"
    assert one["Topic"] == "OrderPlaced"
    assert one["Attempt"] == 0
    env = json.loads(one["PayloadJson"])
    assert env["source"] == "urn:python:test"
    assert env["data"]["orderId"] == "1"
    assert one["CloudEventId"] == env["id"]


def test_bind_is_idempotent(db):
    client, name = db
    bus = MongoBus(uri="", database=name, client=client)
    bus.bind("OrderPlaced", endpoint_id="ep-a")
    bus.bind("OrderPlaced", endpoint_id="ep-a")
    assert client[name]["bus_bindings"].count_documents({}) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && python -m pytest tests/test_integration_sync.py -v`
Expected: FAIL with `ImportError: cannot import name 'MongoBus'`

- [ ] **Step 3: Write minimal implementation**

```python
# clients/python/src/mongobus/_sync/__init__.py
```

```python
# clients/python/src/mongobus/_sync/bus.py
from datetime import datetime, timezone

from pymongo import ASCENDING, MongoClient

from .. import constants, context, documents, envelope, queries


class MongoBus:
    def __init__(self, uri: str, database: str, *, client: MongoClient | None = None):
        self._client = client if client is not None else MongoClient(uri)
        self._db = self._client[database]
        self._inbox = self._db[constants.INBOX_COLLECTION]
        self._bindings = self._db[constants.BINDINGS_COLLECTION]

    def bind(self, type_id: str, *, endpoint_id: str) -> None:
        self._bindings.create_index(
            [("Topic", ASCENDING), ("EndpointId", ASCENDING)], unique=True
        )
        self._bindings.update_one(
            queries.binding_filter(topic=type_id, endpoint_id=endpoint_id),
            queries.binding_set_on_insert(topic=type_id, endpoint_id=endpoint_id),
            upsert=True,
        )

    def publish(
        self,
        type_id: str,
        data,
        *,
        source: str | None = None,
        subject: str | None = None,
        id: str | None = None,
        time_utc: datetime | None = None,
        deliver_at: datetime | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> int:
        routes = list(self._bindings.find(queries.bindings_for_topic_filter(topic=type_id)))
        if not routes:
            return 0

        now = datetime.now(timezone.utc)
        event_id = id if id is not None else envelope.new_event_id()
        final_source = source if source is not None else constants.DEFAULT_SOURCE
        final_correlation = context.resolve_correlation_id(correlation_id)
        final_causation = context.resolve_causation_id(causation_id)

        env = envelope.build_envelope(
            type_id=type_id,
            data=data,
            source=final_source,
            event_id=event_id,
            time_utc=time_utc if time_utc is not None else now,
            subject=subject,
            correlation_id=final_correlation,
            causation_id=final_causation,
        )
        payload_json = envelope.serialize_envelope(env)
        visible = deliver_at if deliver_at is not None else now

        docs = [
            documents.build_inbox_document(
                endpoint_id=route["EndpointId"],
                topic=type_id,
                type_id=type_id,
                payload_json=payload_json,
                cloud_event_id=event_id,
                created_utc=now,
                visible_utc=visible,
                correlation_id=final_correlation,
                causation_id=final_causation,
            )
            for route in routes
        ]
        self._inbox.insert_many(docs)
        return len(docs)
```

```python
# clients/python/src/mongobus/__init__.py
"""MongoBus Python client — wire-compatible with the .NET MongoBus implementation."""

from ._sync.bus import MongoBus

__version__ = "0.1.0"
__all__ = ["MongoBus", "__version__"]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && python -m pytest tests/test_integration_sync.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/_sync clients/python/src/mongobus/__init__.py clients/python/tests/test_integration_sync.py
git commit -m "feat(python): add synchronous publish and bind +semver:minor"
```

---

### Task 12: Synchronous consume pump

**Files:**
- Create: `clients/python/src/mongobus/_sync/pump.py`
- Modify: `clients/python/src/mongobus/_sync/bus.py`
- Modify: `clients/python/tests/test_integration_sync.py`

**Interfaces:**
- Consumes: `mongobus.{constants,queries,dispatch,context,envelope}`, the sync bus.
- Produces:
  - On `MongoBus`:
    - `def consumer(self, *, endpoint_id, type_id, max_attempts=10, idempotent=True)` —
      decorator registering a handler `(ctx: ConsumeContext) -> None`.
    - `def run_once(self, endpoint_id: str) -> bool` — claim+process at most one message
      for an endpoint; returns `True` if a message was handled (success or skip), `False`
      if none was available. Used by tests for determinism.
    - `def run(self, *, stop_event=None) -> None` — loop over all registered endpoints,
      sleeping `DEFAULT_POLL_SECONDS` when idle; stops when `stop_event` is set.
  - Registration record: a private `_Consumer` dataclass `(endpoint_id, type_id, handler,
    max_attempts, idempotent)`.

**Processing algorithm (mirror `MongoMessagePump`):**
1. `find_one_and_update(lock_filter, lock_update, sort=[("VisibleUtc", 1)], return_document=AFTER)`.
2. If `None`, return `False`.
3. `envelope.parse_envelope(doc["PayloadJson"])` → `ctx = ConsumeContext.from_message(env, doc)`.
4. If `idempotent` and a `dedup_filter` match exists → `processed_update(now, "Skipped due to idempotency")`, return `True`.
5. Within `use_context(ctx)`, call the handler.
6. Success → `processed_update(now)`. Exception → `plan_failure(attempt=doc["Attempt"], ...)` using `str(exc)` as the concise error.

- [ ] **Step 1: Write the failing test**

```python
# append to clients/python/tests/test_integration_sync.py
def test_consume_processes_a_published_message(db):
    client, name = db
    bus = MongoBus(uri="", database=name, client=client)
    bus.bind("OrderPlaced", endpoint_id="ep")
    received = []

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced")
    def handle(ctx):
        received.append(ctx.data["orderId"])

    bus.publish("OrderPlaced", {"orderId": "42"})
    handled = bus.run_once("ep")

    assert handled is True
    assert received == ["42"]
    doc = client[name]["bus_inbox"].find_one({})
    assert doc["Status"] == "Processed"
    assert doc["ProcessedUtc"] is not None
    assert doc["LockOwner"] is None


def test_consume_retries_on_handler_failure(db):
    client, name = db
    bus = MongoBus(uri="", database=name, client=client)
    bus.bind("OrderPlaced", endpoint_id="ep")

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced", max_attempts=10)
    def handle(ctx):
        raise RuntimeError("boom")

    bus.publish("OrderPlaced", {"orderId": "1"})
    bus.run_once("ep")

    doc = client[name]["bus_inbox"].find_one({})
    assert doc["Status"] == "Pending"
    assert doc["Attempt"] == 1
    assert doc["LastError"] == "boom"
    assert doc["LockOwner"] is None


def test_consume_dead_letters_after_max_attempts(db):
    client, name = db
    bus = MongoBus(uri="", database=name, client=client)
    bus.bind("OrderPlaced", endpoint_id="ep")

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced", max_attempts=1)
    def handle(ctx):
        raise RuntimeError("boom")

    bus.publish("OrderPlaced", {"orderId": "1"})
    bus.run_once("ep")

    doc = client[name]["bus_inbox"].find_one({})
    assert doc["Status"] == "Dead"
    assert doc["Attempt"] == 1


def test_idempotency_skips_duplicate_cloud_event(db):
    client, name = db
    bus = MongoBus(uri="", database=name, client=client)
    bus.bind("OrderPlaced", endpoint_id="ep")
    calls = []

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced", idempotent=True)
    def handle(ctx):
        calls.append(1)

    # Two inbox docs for the same endpoint + cloud event id.
    bus.publish("OrderPlaced", {"orderId": "1"}, id="dup-ce")
    client[name]["bus_inbox"].update_one(
        {}, {"$setOnInsert": {}}, upsert=False
    )
    first = client[name]["bus_inbox"].find_one({})
    client[name]["bus_inbox"].insert_one({**{k: v for k, v in first.items() if k != "_id"}})

    bus.run_once("ep")  # processes the first
    bus.run_once("ep")  # second must be skipped by idempotency

    assert sum(calls) == 1
    statuses = [d["Status"] for d in client[name]["bus_inbox"].find({})]
    assert statuses.count("Processed") == 2
    skipped = client[name]["bus_inbox"].find_one({"LastError": "Skipped due to idempotency"})
    assert skipped is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && python -m pytest tests/test_integration_sync.py -k "consume or idempotency or retries or dead" -v`
Expected: FAIL with `AttributeError: 'MongoBus' object has no attribute 'consumer'`

- [ ] **Step 3: Write minimal implementation**

```python
# clients/python/src/mongobus/_sync/pump.py
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from pymongo import ASCENDING
from pymongo.collection import ReturnDocument

from .. import constants, context, dispatch, envelope, queries


@dataclass
class Consumer:
    endpoint_id: str
    type_id: str
    handler: Callable
    max_attempts: int
    idempotent: bool


def process_one(inbox, consumer: Consumer) -> bool:
    now = datetime.now(timezone.utc)
    pump_id = dispatch.build_pump_id(consumer.endpoint_id)
    doc = inbox.find_one_and_update(
        queries.lock_filter(endpoint_id=consumer.endpoint_id, now=now, type_ids=[consumer.type_id]),
        queries.lock_update(now=now, lock_seconds=constants.DEFAULT_LOCK_SECONDS, pump_id=pump_id),
        sort=[("VisibleUtc", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )
    if doc is None:
        return False

    env = envelope.parse_envelope(doc["PayloadJson"])
    ctx = context.ConsumeContext.from_message(env, doc)

    if consumer.idempotent and _already_processed(inbox, ctx, doc["_id"]):
        inbox.update_one(
            {"_id": doc["_id"]},
            queries.processed_update(now=datetime.now(timezone.utc),
                                     last_error="Skipped due to idempotency"),
        )
        return True

    try:
        with context.use_context(ctx):
            consumer.handler(ctx)
    except Exception as exc:  # noqa: BLE001 - failure is mapped to retry/dead-letter
        inbox.update_one(
            {"_id": doc["_id"]},
            dispatch.plan_failure(
                attempt=doc["Attempt"],
                max_attempts=consumer.max_attempts,
                now=datetime.now(timezone.utc),
                error=str(exc),
            ),
        )
        return True

    inbox.update_one(
        {"_id": doc["_id"]},
        queries.processed_update(now=datetime.now(timezone.utc)),
    )
    return True


def _already_processed(inbox, ctx, current_id) -> bool:
    return inbox.count_documents(
        queries.dedup_filter(
            endpoint_id=ctx.raw["EndpointId"],
            cloud_event_id=ctx.cloud_event_id,
            exclude_id=current_id,
        ),
        limit=1,
    ) > 0
```

```python
# append to clients/python/src/mongobus/_sync/bus.py
# (imports)
import time

from .pump import Consumer, process_one


# (inside class MongoBus.__init__, add:)
#     self._consumers: list[Consumer] = []

# (add methods to MongoBus)
    def consumer(self, *, endpoint_id, type_id, max_attempts=constants.DEFAULT_MAX_ATTEMPTS, idempotent=True):
        def register(handler):
            self._consumers.append(
                Consumer(endpoint_id, type_id, handler, max_attempts, idempotent)
            )
            return handler
        return register

    def run_once(self, endpoint_id: str) -> bool:
        for consumer in self._consumers:
            if consumer.endpoint_id == endpoint_id and process_one(self._inbox, consumer):
                return True
        return False

    def run(self, *, stop_event=None) -> None:
        while stop_event is None or not stop_event.is_set():
            did_work = any(process_one(self._inbox, c) for c in self._consumers)
            if not did_work:
                time.sleep(constants.DEFAULT_POLL_SECONDS)
```

Update `MongoBus.__init__` to initialize `self._consumers: list[Consumer] = []` (add the
line shown in the comment above).

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && python -m pytest tests/test_integration_sync.py -v`
Expected: PASS (all sync tests pass)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/_sync clients/python/tests/test_integration_sync.py
git commit -m "feat(python): add synchronous consume pump with retry, dead-letter, idempotency +semver:minor"
```

---

### Task 13: Asynchronous bus & pump

**Files:**
- Create: `clients/python/src/mongobus/_async/__init__.py`
- Create: `clients/python/src/mongobus/_async/bus.py`
- Create: `clients/python/src/mongobus/_async/pump.py`
- Modify: `clients/python/src/mongobus/__init__.py`
- Test: `clients/python/tests/test_integration_async.py`

**Interfaces:**
- Consumes: same pure core; `pymongo` `AsyncMongoClient`.
- Produces:
  - `class AsyncMongoBus` mirroring `MongoBus` with `async def bind`, `async def publish`,
    `consumer` (decorator; handler may be a coroutine function), `async def run_once`,
    `async def run`.
  - Async `process_one(inbox, consumer)` coroutine mirroring the sync algorithm using
    `await` on the async driver, and `await handler(ctx)` (awaiting only if the handler
    returns an awaitable).

**Note:** the pure-core builders are reused unchanged; only the I/O calls become `await`.
The handler-invocation must support both sync and async handlers: `result = handler(ctx); if inspect.isawaitable(result): await result`.

- [ ] **Step 1: Write the failing test**

```python
# clients/python/tests/test_integration_async.py
import json

import pytest
from pymongo import AsyncMongoClient
from testcontainers.mongodb import MongoDbContainer

from mongobus import AsyncMongoBus


@pytest.fixture(scope="module")
def mongo():
    with MongoDbContainer("mongo:7") as container:
        yield container.get_connection_url()


@pytest.fixture
async def db(mongo):
    client = AsyncMongoClient(mongo)
    name = "testdb_async"
    await client.drop_database(name)
    yield client, name
    await client.drop_database(name)


async def test_async_publish_fans_out(db):
    client, name = db
    bus = AsyncMongoBus(uri="", database=name, client=client)
    await bus.bind("OrderPlaced", endpoint_id="ep-a")
    await bus.bind("OrderPlaced", endpoint_id="ep-b")
    created = await bus.publish("OrderPlaced", {"orderId": "1"})
    assert created == 2
    assert await client[name]["bus_inbox"].count_documents({}) == 2


async def test_async_consume_processes_message(db):
    client, name = db
    bus = AsyncMongoBus(uri="", database=name, client=client)
    await bus.bind("OrderPlaced", endpoint_id="ep")
    received = []

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced")
    async def handle(ctx):
        received.append(ctx.data["orderId"])

    await bus.publish("OrderPlaced", {"orderId": "42"})
    handled = await bus.run_once("ep")

    assert handled is True
    assert received == ["42"]
    doc = await client[name]["bus_inbox"].find_one({})
    assert doc["Status"] == "Processed"


async def test_async_consume_dead_letters(db):
    client, name = db
    bus = AsyncMongoBus(uri="", database=name, client=client)
    await bus.bind("OrderPlaced", endpoint_id="ep")

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced", max_attempts=1)
    async def handle(ctx):
        raise RuntimeError("boom")

    await bus.publish("OrderPlaced", {"orderId": "1"})
    await bus.run_once("ep")

    doc = await client[name]["bus_inbox"].find_one({})
    assert doc["Status"] == "Dead"
    assert doc["Attempt"] == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && python -m pytest tests/test_integration_async.py -v`
Expected: FAIL with `ImportError: cannot import name 'AsyncMongoBus'`

- [ ] **Step 3: Write minimal implementation**

```python
# clients/python/src/mongobus/_async/__init__.py
```

```python
# clients/python/src/mongobus/_async/pump.py
import inspect
from datetime import datetime, timezone

from pymongo import ASCENDING
from pymongo.collection import ReturnDocument

from .. import constants, context, dispatch, envelope, queries
from .._sync.pump import Consumer


async def process_one(inbox, consumer: Consumer) -> bool:
    now = datetime.now(timezone.utc)
    pump_id = dispatch.build_pump_id(consumer.endpoint_id)
    doc = await inbox.find_one_and_update(
        queries.lock_filter(endpoint_id=consumer.endpoint_id, now=now, type_ids=[consumer.type_id]),
        queries.lock_update(now=now, lock_seconds=constants.DEFAULT_LOCK_SECONDS, pump_id=pump_id),
        sort=[("VisibleUtc", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )
    if doc is None:
        return False

    env = envelope.parse_envelope(doc["PayloadJson"])
    ctx = context.ConsumeContext.from_message(env, doc)

    if consumer.idempotent and await _already_processed(inbox, ctx, doc["_id"]):
        await inbox.update_one(
            {"_id": doc["_id"]},
            queries.processed_update(now=datetime.now(timezone.utc),
                                     last_error="Skipped due to idempotency"),
        )
        return True

    try:
        with context.use_context(ctx):
            result = consumer.handler(ctx)
            if inspect.isawaitable(result):
                await result
    except Exception as exc:  # noqa: BLE001 - failure is mapped to retry/dead-letter
        await inbox.update_one(
            {"_id": doc["_id"]},
            dispatch.plan_failure(
                attempt=doc["Attempt"],
                max_attempts=consumer.max_attempts,
                now=datetime.now(timezone.utc),
                error=str(exc),
            ),
        )
        return True

    await inbox.update_one(
        {"_id": doc["_id"]},
        queries.processed_update(now=datetime.now(timezone.utc)),
    )
    return True


async def _already_processed(inbox, ctx, current_id) -> bool:
    count = await inbox.count_documents(
        queries.dedup_filter(
            endpoint_id=ctx.raw["EndpointId"],
            cloud_event_id=ctx.cloud_event_id,
            exclude_id=current_id,
        ),
        limit=1,
    )
    return count > 0
```

```python
# clients/python/src/mongobus/_async/bus.py
import asyncio
from datetime import datetime, timezone

from pymongo import ASCENDING, AsyncMongoClient

from .. import constants, context, documents, envelope, queries
from .._sync.pump import Consumer
from .pump import process_one


class AsyncMongoBus:
    def __init__(self, uri: str, database: str, *, client: AsyncMongoClient | None = None):
        self._client = client if client is not None else AsyncMongoClient(uri)
        self._db = self._client[database]
        self._inbox = self._db[constants.INBOX_COLLECTION]
        self._bindings = self._db[constants.BINDINGS_COLLECTION]
        self._consumers: list[Consumer] = []

    async def bind(self, type_id: str, *, endpoint_id: str) -> None:
        await self._bindings.create_index(
            [("Topic", ASCENDING), ("EndpointId", ASCENDING)], unique=True
        )
        await self._bindings.update_one(
            queries.binding_filter(topic=type_id, endpoint_id=endpoint_id),
            queries.binding_set_on_insert(topic=type_id, endpoint_id=endpoint_id),
            upsert=True,
        )

    async def publish(self, type_id, data, *, source=None, subject=None, id=None,
                      time_utc=None, deliver_at=None, correlation_id=None, causation_id=None) -> int:
        routes = await self._bindings.find(
            queries.bindings_for_topic_filter(topic=type_id)
        ).to_list(length=None)
        if not routes:
            return 0

        now = datetime.now(timezone.utc)
        event_id = id if id is not None else envelope.new_event_id()
        final_source = source if source is not None else constants.DEFAULT_SOURCE
        final_correlation = context.resolve_correlation_id(correlation_id)
        final_causation = context.resolve_causation_id(causation_id)

        env = envelope.build_envelope(
            type_id=type_id, data=data, source=final_source, event_id=event_id,
            time_utc=time_utc if time_utc is not None else now, subject=subject,
            correlation_id=final_correlation, causation_id=final_causation,
        )
        payload_json = envelope.serialize_envelope(env)
        visible = deliver_at if deliver_at is not None else now

        docs = [
            documents.build_inbox_document(
                endpoint_id=route["EndpointId"], topic=type_id, type_id=type_id,
                payload_json=payload_json, cloud_event_id=event_id, created_utc=now,
                visible_utc=visible, correlation_id=final_correlation, causation_id=final_causation,
            )
            for route in routes
        ]
        await self._inbox.insert_many(docs)
        return len(docs)

    def consumer(self, *, endpoint_id, type_id, max_attempts=constants.DEFAULT_MAX_ATTEMPTS, idempotent=True):
        def register(handler):
            self._consumers.append(Consumer(endpoint_id, type_id, handler, max_attempts, idempotent))
            return handler
        return register

    async def run_once(self, endpoint_id: str) -> bool:
        for consumer in self._consumers:
            if consumer.endpoint_id == endpoint_id and await process_one(self._inbox, consumer):
                return True
        return False

    async def run(self, *, stop_event=None) -> None:
        while stop_event is None or not stop_event.is_set():
            did_work = False
            for consumer in self._consumers:
                if await process_one(self._inbox, consumer):
                    did_work = True
            if not did_work:
                await asyncio.sleep(constants.DEFAULT_POLL_SECONDS)
```

```python
# clients/python/src/mongobus/__init__.py
"""MongoBus Python client — wire-compatible with the .NET MongoBus implementation."""

from ._sync.bus import MongoBus
from ._async.bus import AsyncMongoBus

__version__ = "0.1.0"
__all__ = ["MongoBus", "AsyncMongoBus", "__version__"]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && python -m pytest tests/test_integration_async.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/_async clients/python/src/mongobus/__init__.py clients/python/tests/test_integration_async.py
git commit -m "feat(python): add asynchronous bus and pump +semver:minor"
```

---

### Task 14: Coverage gate & full-suite verification

**Files:**
- Modify: `clients/python/pyproject.toml` (coverage config already present; confirm gate)

**Interfaces:**
- Consumes: the entire test suite.
- Produces: a passing run with **≥95% coverage**.

- [ ] **Step 1: Run the full suite with coverage**

Run: `cd clients/python && python -m pytest --cov=mongobus --cov-report=term-missing --cov-fail-under=95`
Expected: PASS with total coverage ≥95%. If below, add focused unit tests for the
uncovered lines (most likely the `run()` loops — test by setting a `stop_event` after one
iteration, and the `MongoClient`-construction branch in `__init__` by leaving `client=None`
and asserting a `MongoBus` is created against a Testcontainers URI).

- [ ] **Step 2: Add any tests needed to reach the gate, then re-run**

Run: `cd clients/python && python -m pytest --cov=mongobus --cov-fail-under=95`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add clients/python
git commit -m "test(python): enforce 95% coverage gate +semver:patch"
```

---

### Task 15: CI workflow (PyPI publish)

**Files:**
- Create: `.github/workflows/python-client.yml`

**Interfaces:**
- Consumes: the package and tests.
- Produces: a GitHub Actions workflow that tests on PRs touching `clients/python/**` and
  publishes to PyPI on a `py-v*` tag via trusted publishing (OIDC).

- [ ] **Step 1: Create the workflow**

```yaml
# .github/workflows/python-client.yml
name: python-client

on:
  push:
    branches: [main]
    paths: ["clients/python/**"]
    tags: ["py-v*"]
  pull_request:
    paths: ["clients/python/**"]

defaults:
  run:
    working-directory: clients/python

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -e ".[test]"
      - run: python -m pytest --cov=mongobus --cov-report=term-missing --cov-fail-under=95

  publish:
    needs: test
    if: startsWith(github.ref, 'refs/tags/py-v')
    runs-on: ubuntu-latest
    permissions:
      id-token: write
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install build
      - run: python -m build
      - uses: pypa/gh-action-pypi-publish@release/v1
        with:
          packages-dir: clients/python/dist
```

- [ ] **Step 2: Validate the YAML locally**

Run: `python -c "import yaml,sys; yaml.safe_load(open('.github/workflows/python-client.yml'))" && echo OK`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/python-client.yml
git commit -m "ci(python): add test and PyPI publish workflow +semver:patch"
```

---

## Self-Review Notes

- **Spec coverage:** publish+fan-out (T11), atomic lock pump (T12/T13), retry/backoff (T7/T9/T12), dead-letter (T9/T12), idempotency (T6/T12), correlation/causation (T8), bindings+index (T11), sync+async (T11–T13), wire-format fidelity (T3–T6, golden T10), testing+coverage (all, T14), CI/PyPI (T15), claim-check guard (T4). All spec sections map to tasks.
- **Docker dependency:** Tasks 11–14 require Docker for Testcontainers (project convention). The subagent executor must have Docker available; if not, these tasks fail at the "verify it fails/passes" steps and must be run where Docker is present.
- **`now` determinism:** integration tests assert state transitions, not exact timestamps, so wall-clock `datetime.now` is acceptable. Pure-core tests inject `now` explicitly.
- **Type consistency:** `process_one`, `Consumer`, `build_pump_id`, `plan_failure`, `build_envelope`, `parse_envelope`, `build_inbox_document`, and all `queries.*` names are used identically across tasks.
