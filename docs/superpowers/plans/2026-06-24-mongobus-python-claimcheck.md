# Claim-Check (Python Client) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add wire-compatible claim-check (large-payload offloading) to the `mongo-bus` Python client — publish-side offload and consume-side rehydrate — with GridFS and S3 storage backends, for both the sync and async buses.

**Architecture:** A pure, I/O-free `claimcheck/core.py` holds the wire contract (the `ClaimCheckReference` model, reference↔dict serialization with null omission, the offload decision, gzip compress/decompress with a bomb guard, and the provider protocols). Thin provider shells (`gridfs.py`, `s3.py`) do storage I/O in sync and async variants. The sync/async buses gain a `claim_check` config: `publish` offloads the serialized `data` when over threshold or forced; the consume pump rehydrates a claim-checked message before invoking the handler.

**Tech Stack:** Python 3.10+, PyMongo 4.9+ (sync `gridfs.GridFSBucket` + async `gridfs.asynchronous.AsyncGridFSBucket`), `boto3` (optional `[s3]` extra, imported lazily), `moto` (test-only S3 mock), pytest / pytest-asyncio / pytest-cov, testcontainers[mongodb].

## Global Constraints

- Claim-check content type exact: `application/vnd.mongobus.claim-check+json` (already `constants.CLAIM_CHECK_CONTENT_TYPE`).
- `ClaimCheckReference` JSON keys exact (camelCase): `provider`, `container`, `key`, `length`, `contentType`, `metadata`, `createdAt`. The last three are **omitted when None** (mirrors .NET `WhenWritingNull`); the first four are always present.
- Stored-blob (object) content type: `application/json`.
- Metadata keys exact: `x-mongobus-created-at`, `x-mongobus-compression`. Compression value exact: `gzip`.
- Compression is a **plain gzip stream** (Python `gzip` module / wbits=31), byte-compatible with .NET `GZipStream`.
- Blob key format: `uuid4().hex` — 32 lowercase hex chars, no dashes (matches .NET `Guid.ToString("N")`).
- GridFS: provider name `"gridfs"`; default bucket `"claimcheck"` (collections `claimcheck.files` / `claimcheck.chunks`); GridFS **filename = key**; `contentType` + `x-mongobus-*` go in the file `metadata` subdocument.
- S3: default provider name `"s3"`; `container` = bucket; `key` = `key_prefix + uuid4().hex`.
- Defaults: threshold `256 * 1024` bytes; max decompressed `100 * 1024 * 1024` bytes; `enabled=False`; `compress=False`.
- Offload decision: `use_claim_check` forces offload; otherwise offload iff `enabled and size >= threshold_bytes`.
- `boto3` MUST be imported lazily (inside functions/methods in `s3.py`) so the core package has no hard boto3 dependency.
- Test coverage gate: **≥95%** (`--cov-fail-under=95`), enforced in CI.
- Interpreter for ALL test commands: `/home/elusiven/projects/mongo-bus/clients/python-venv/bin/python`. Do NOT pip install ad hoc (the venv has pytest, pytest-asyncio, pytest-cov, pymongo, testcontainers; Task 1 adds boto3 + moto to the venv).
- TDD throughout: red → green → refactor. Frequent commits. Feature branch `feat/python-claimcheck`; never push to `main`.

---

### Task 1: Add boto3/moto deps and the `[s3]` extra

**Files:**
- Modify: `clients/python/pyproject.toml`

**Interfaces:**
- Consumes: nothing.
- Produces: `mongo-bus[s3]` optional extra (`boto3`); `moto` + `boto3` in the `[test]` extra so the venv and CI can run S3 tests.

- [ ] **Step 1: Edit pyproject optional dependencies**

In `clients/python/pyproject.toml`, update the `[project.optional-dependencies]` block to:

```toml
[project.optional-dependencies]
s3 = ["boto3>=1.34"]
test = ["pytest>=8", "pytest-asyncio>=0.24", "pytest-cov>=5", "testcontainers[mongodb]>=4", "boto3>=1.34", "moto>=5"]
```

- [ ] **Step 2: Install the new deps into the venv**

Run: `/home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pip --python /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python install "boto3>=1.34" "moto>=5"`

(If `pip` is unavailable in the venv, install from the system pip: `python3 -m pip --python /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python install "boto3>=1.34" "moto>=5"`.)

Expected: boto3 and moto install successfully.

- [ ] **Step 3: Verify imports**

Run: `/home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -c "import boto3, moto; print('ok')"`
Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add clients/python/pyproject.toml
git commit -m "build(python): add boto3 [s3] extra and moto test dep +semver:patch"
```

---

### Task 2: Claim-check constants & errors

**Files:**
- Create: `clients/python/src/mongobus/claimcheck/__init__.py`
- Create: `clients/python/src/mongobus/claimcheck/core.py` (constants only this task)
- Modify: `clients/python/src/mongobus/errors.py`
- Test: `clients/python/tests/test_claimcheck_core.py`

**Interfaces:**
- Consumes: `mongobus.constants.CLAIM_CHECK_CONTENT_TYPE`.
- Produces, in `mongobus.claimcheck.core`:
  - `OBJECT_CONTENT_TYPE = "application/json"`
  - `CREATED_AT_KEY = "x-mongobus-created-at"`
  - `COMPRESSION_KEY = "x-mongobus-compression"`
  - `COMPRESSION_GZIP = "gzip"`
  - `DEFAULT_THRESHOLD_BYTES = 256 * 1024`
  - `DEFAULT_MAX_DECOMPRESSED_BYTES = 100 * 1024 * 1024`
- In `mongobus.errors`: `class ClaimCheckError(MongoBusError)`.

- [ ] **Step 1: Write the failing test**

```python
# clients/python/tests/test_claimcheck_core.py
from mongobus.claimcheck import core
from mongobus.errors import ClaimCheckError, MongoBusError


def test_constants_match_dotnet():
    assert core.OBJECT_CONTENT_TYPE == "application/json"
    assert core.CREATED_AT_KEY == "x-mongobus-created-at"
    assert core.COMPRESSION_KEY == "x-mongobus-compression"
    assert core.COMPRESSION_GZIP == "gzip"
    assert core.DEFAULT_THRESHOLD_BYTES == 256 * 1024
    assert core.DEFAULT_MAX_DECOMPRESSED_BYTES == 100 * 1024 * 1024


def test_claim_check_error_is_a_mongobus_error():
    assert issubclass(ClaimCheckError, MongoBusError)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_claimcheck_core.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mongobus.claimcheck'`

- [ ] **Step 3: Write minimal implementation**

```python
# clients/python/src/mongobus/claimcheck/__init__.py
```

```python
# clients/python/src/mongobus/claimcheck/core.py
OBJECT_CONTENT_TYPE = "application/json"

CREATED_AT_KEY = "x-mongobus-created-at"
COMPRESSION_KEY = "x-mongobus-compression"
COMPRESSION_GZIP = "gzip"

DEFAULT_THRESHOLD_BYTES = 256 * 1024
DEFAULT_MAX_DECOMPRESSED_BYTES = 100 * 1024 * 1024
```

Add to `clients/python/src/mongobus/errors.py`:

```python
class ClaimCheckError(MongoBusError):
    """Raised on claim-check storage or decoding failures (e.g. a decompression-bomb guard trip)."""
```

(Keep the existing `MongoBusError` and `ClaimCheckNotSupportedError`.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_claimcheck_core.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/claimcheck clients/python/src/mongobus/errors.py clients/python/tests/test_claimcheck_core.py
git commit -m "feat(python): add claim-check constants and ClaimCheckError +semver:minor"
```

---

### Task 3: `ClaimCheckReference` model + reference↔dict serialization

**Files:**
- Modify: `clients/python/src/mongobus/claimcheck/core.py`
- Modify: `clients/python/tests/test_claimcheck_core.py`

**Interfaces:**
- Consumes: task-2 constants.
- Produces:
  - `@dataclass(frozen=True) class ClaimCheckReference` with fields `provider: str`,
    `container: str`, `key: str`, `length: int`, `content_type: str | None = None`,
    `metadata: dict[str, str] | None = None`, `created_at: str | None = None`.
  - `def reference_to_data(ref: ClaimCheckReference) -> dict` — camelCase dict;
    always includes `provider`/`container`/`key`/`length`; includes `contentType`/
    `metadata`/`createdAt` only when not None.
  - `def reference_from_data(data: dict) -> ClaimCheckReference` — inverse.

- [ ] **Step 1: Write the failing test**

```python
# append to clients/python/tests/test_claimcheck_core.py
def test_reference_to_data_omits_none_fields():
    ref = core.ClaimCheckReference(provider="gridfs", container="claimcheck", key="abc", length=10)
    data = core.reference_to_data(ref)
    assert data == {"provider": "gridfs", "container": "claimcheck", "key": "abc", "length": 10}
    for absent in ("contentType", "metadata", "createdAt"):
        assert absent not in data


def test_reference_to_data_includes_optional_fields():
    ref = core.ClaimCheckReference(
        provider="gridfs", container="claimcheck", key="abc", length=10,
        content_type="application/json", metadata={"x-mongobus-compression": "gzip"},
        created_at="2026-06-24T12:00:00Z",
    )
    data = core.reference_to_data(ref)
    assert data["contentType"] == "application/json"
    assert data["metadata"] == {"x-mongobus-compression": "gzip"}
    assert data["createdAt"] == "2026-06-24T12:00:00Z"


def test_reference_from_data_round_trips():
    data = {
        "provider": "gridfs", "container": "claimcheck", "key": "abc", "length": 10,
        "contentType": "application/json",
        "metadata": {"x-mongobus-compression": "gzip"},
        "createdAt": "2026-06-24T12:00:00Z",
    }
    ref = core.reference_from_data(data)
    assert ref.provider == "gridfs"
    assert ref.container == "claimcheck"
    assert ref.key == "abc"
    assert ref.length == 10
    assert ref.content_type == "application/json"
    assert ref.metadata == {"x-mongobus-compression": "gzip"}
    assert ref.created_at == "2026-06-24T12:00:00Z"


def test_reference_from_data_with_only_required_fields():
    ref = core.reference_from_data({"provider": "s3", "container": "b", "key": "k", "length": 5})
    assert ref.content_type is None
    assert ref.metadata is None
    assert ref.created_at is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_claimcheck_core.py -k reference -v`
Expected: FAIL with `AttributeError: module 'mongobus.claimcheck.core' has no attribute 'ClaimCheckReference'`

- [ ] **Step 3: Write minimal implementation**

```python
# add to clients/python/src/mongobus/claimcheck/core.py
from dataclasses import dataclass


@dataclass(frozen=True)
class ClaimCheckReference:
    provider: str
    container: str
    key: str
    length: int
    content_type: str | None = None
    metadata: dict[str, str] | None = None
    created_at: str | None = None


def reference_to_data(ref: ClaimCheckReference) -> dict:
    data: dict = {
        "provider": ref.provider,
        "container": ref.container,
        "key": ref.key,
        "length": ref.length,
    }
    if ref.content_type is not None:
        data["contentType"] = ref.content_type
    if ref.metadata is not None:
        data["metadata"] = ref.metadata
    if ref.created_at is not None:
        data["createdAt"] = ref.created_at
    return data


def reference_from_data(data: dict) -> ClaimCheckReference:
    return ClaimCheckReference(
        provider=data["provider"],
        container=data["container"],
        key=data["key"],
        length=data["length"],
        content_type=data.get("contentType"),
        metadata=data.get("metadata"),
        created_at=data.get("createdAt"),
    )
```

(Place the `from dataclasses import dataclass` import at the top of the module.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_claimcheck_core.py -v`
Expected: PASS (6 passed)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/claimcheck/core.py clients/python/tests/test_claimcheck_core.py
git commit -m "feat(python): add ClaimCheckReference model and serialization +semver:minor"
```

---

### Task 4: Offload decision, gzip helpers, key gen, claim-check detection

**Files:**
- Modify: `clients/python/src/mongobus/claimcheck/core.py`
- Modify: `clients/python/tests/test_claimcheck_core.py`

**Interfaces:**
- Consumes: task-2 constants, `mongobus.constants.CLAIM_CHECK_CONTENT_TYPE`, `mongobus.errors.ClaimCheckError`.
- Produces:
  - `def is_claim_check(envelope: dict) -> bool` — `envelope.get("dataContentType") == CLAIM_CHECK_CONTENT_TYPE`.
  - `def should_offload(*, size: int, threshold_bytes: int, enabled: bool, use_claim_check: bool | None) -> bool` — `True if use_claim_check else (enabled and size >= threshold_bytes)`.
  - `def gzip_compress(data: bytes) -> bytes`.
  - `def gzip_decompress(data: bytes, *, max_bytes: int) -> bytes` — raises `ClaimCheckError` if output would exceed `max_bytes`.
  - `def new_blob_key() -> str` — `uuid4().hex`.

- [ ] **Step 1: Write the failing test**

```python
# append to clients/python/tests/test_claimcheck_core.py
import pytest

from mongobus.constants import CLAIM_CHECK_CONTENT_TYPE


def test_is_claim_check_true_for_claim_check_content_type():
    assert core.is_claim_check({"dataContentType": CLAIM_CHECK_CONTENT_TYPE}) is True


def test_is_claim_check_false_otherwise():
    assert core.is_claim_check({"dataContentType": "application/json"}) is False
    assert core.is_claim_check({}) is False


def test_should_offload_forced_by_use_claim_check():
    assert core.should_offload(size=1, threshold_bytes=999, enabled=False, use_claim_check=True) is True


def test_should_offload_threshold_when_enabled():
    assert core.should_offload(size=100, threshold_bytes=100, enabled=True, use_claim_check=None) is True
    assert core.should_offload(size=99, threshold_bytes=100, enabled=True, use_claim_check=None) is False


def test_should_offload_disabled_and_not_forced():
    assert core.should_offload(size=10_000, threshold_bytes=100, enabled=False, use_claim_check=None) is False


def test_gzip_round_trips():
    original = b'{"orderId": "123"}' * 100
    compressed = core.gzip_compress(original)
    assert core.gzip_decompress(compressed, max_bytes=1_000_000) == original


def test_gzip_decompress_guards_against_bombs():
    compressed = core.gzip_compress(b"a" * 10_000)
    with pytest.raises(core_error_type()):
        core.gzip_decompress(compressed, max_bytes=100)


def core_error_type():
    from mongobus.errors import ClaimCheckError
    return ClaimCheckError


def test_new_blob_key_is_32_char_hex():
    key = core.new_blob_key()
    assert len(key) == 32
    assert "-" not in key
    int(key, 16)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_claimcheck_core.py -k "offload or gzip or blob_key or claim_check" -v`
Expected: FAIL with `AttributeError` for `should_offload`/`gzip_compress`/etc.

- [ ] **Step 3: Write minimal implementation**

```python
# add to clients/python/src/mongobus/claimcheck/core.py
import gzip
import uuid

from ..constants import CLAIM_CHECK_CONTENT_TYPE
from ..errors import ClaimCheckError


def is_claim_check(envelope: dict) -> bool:
    return envelope.get("dataContentType") == CLAIM_CHECK_CONTENT_TYPE


def should_offload(*, size: int, threshold_bytes: int, enabled: bool, use_claim_check: bool | None) -> bool:
    if use_claim_check:
        return True
    return enabled and size >= threshold_bytes


def gzip_compress(data: bytes) -> bytes:
    return gzip.compress(data)


def gzip_decompress(data: bytes, *, max_bytes: int) -> bytes:
    decompressor = gzip.GzipFile(fileobj=__import__("io").BytesIO(data))
    out = decompressor.read(max_bytes + 1)
    if len(out) > max_bytes:
        raise ClaimCheckError(
            f"Decompressed claim-check payload exceeds the {max_bytes}-byte limit."
        )
    return out


def new_blob_key() -> str:
    return uuid.uuid4().hex
```

Note on `gzip_decompress`: reading `max_bytes + 1` bytes and checking the length detects an over-limit payload without materializing the whole stream. Replace the `__import__("io")` shim with a proper `import io` at the top of the module.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_claimcheck_core.py -v`
Expected: PASS (15 passed)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/claimcheck/core.py clients/python/tests/test_claimcheck_core.py
git commit -m "feat(python): add offload decision, gzip helpers, blob key, detection +semver:minor"
```

---

### Task 5: Provider protocols + `ClaimCheckConfig`

**Files:**
- Modify: `clients/python/src/mongobus/claimcheck/core.py`
- Create: `clients/python/src/mongobus/claimcheck/config.py`
- Test: `clients/python/tests/test_claimcheck_config.py`

**Interfaces:**
- Consumes: `ClaimCheckReference`, task-2 constants.
- Produces, in `core`:
  - `class ClaimCheckProvider(Protocol)`: attribute `name: str`; method
    `put(self, data: bytes, *, content_type: str | None, metadata: dict[str, str] | None) -> ClaimCheckReference`;
    method `open_read(self, reference: ClaimCheckReference) -> bytes`.
  - `class AsyncClaimCheckProvider(Protocol)`: `name: str`; async `put(...)`/`open_read(...)`.
- In `mongobus.claimcheck.config`:
  - `@dataclass class ClaimCheckConfig` with `provider`, `enabled: bool = False`,
    `threshold_bytes: int = DEFAULT_THRESHOLD_BYTES`, `compress: bool = False`,
    `max_decompressed_bytes: int = DEFAULT_MAX_DECOMPRESSED_BYTES`.

- [ ] **Step 1: Write the failing test**

```python
# clients/python/tests/test_claimcheck_config.py
from mongobus.claimcheck import core
from mongobus.claimcheck.config import ClaimCheckConfig


class _StubProvider:
    name = "stub"

    def put(self, data, *, content_type, metadata):
        return core.ClaimCheckReference(provider="stub", container="c", key="k", length=len(data))

    def open_read(self, reference):
        return b""


def test_config_defaults_match_spec():
    cfg = ClaimCheckConfig(provider=_StubProvider())
    assert cfg.enabled is False
    assert cfg.threshold_bytes == core.DEFAULT_THRESHOLD_BYTES
    assert cfg.compress is False
    assert cfg.max_decompressed_bytes == core.DEFAULT_MAX_DECOMPRESSED_BYTES


def test_stub_provider_satisfies_protocol():
    provider: core.ClaimCheckProvider = _StubProvider()
    ref = provider.put(b"hello", content_type="application/json", metadata=None)
    assert ref.key == "k"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_claimcheck_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mongobus.claimcheck.config'`

- [ ] **Step 3: Write minimal implementation**

```python
# add to clients/python/src/mongobus/claimcheck/core.py
from typing import Protocol, runtime_checkable


@runtime_checkable
class ClaimCheckProvider(Protocol):
    name: str

    def put(
        self, data: bytes, *, content_type: str | None, metadata: dict[str, str] | None
    ) -> "ClaimCheckReference": ...

    def open_read(self, reference: "ClaimCheckReference") -> bytes: ...


@runtime_checkable
class AsyncClaimCheckProvider(Protocol):
    name: str

    async def put(
        self, data: bytes, *, content_type: str | None, metadata: dict[str, str] | None
    ) -> "ClaimCheckReference": ...

    async def open_read(self, reference: "ClaimCheckReference") -> bytes: ...
```

```python
# clients/python/src/mongobus/claimcheck/config.py
from dataclasses import dataclass

from .core import (
    DEFAULT_MAX_DECOMPRESSED_BYTES,
    DEFAULT_THRESHOLD_BYTES,
    AsyncClaimCheckProvider,
    ClaimCheckProvider,
)


@dataclass
class ClaimCheckConfig:
    provider: ClaimCheckProvider | AsyncClaimCheckProvider
    enabled: bool = False
    threshold_bytes: int = DEFAULT_THRESHOLD_BYTES
    compress: bool = False
    max_decompressed_bytes: int = DEFAULT_MAX_DECOMPRESSED_BYTES
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_claimcheck_config.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/claimcheck/core.py clients/python/src/mongobus/claimcheck/config.py clients/python/tests/test_claimcheck_config.py
git commit -m "feat(python): add claim-check provider protocols and config +semver:minor"
```

---

### Task 6: GridFS provider (sync + async)

**Files:**
- Create: `clients/python/src/mongobus/claimcheck/gridfs.py`
- Test: `clients/python/tests/test_claimcheck_gridfs.py`

**Interfaces:**
- Consumes: `core.ClaimCheckReference`, `core.new_blob_key`; `gridfs.GridFSBucket`, `gridfs.asynchronous.AsyncGridFSBucket`.
- Produces:
  - `class GridFsClaimCheckProvider`: `__init__(self, database, *, bucket_name="claimcheck")`;
    `name = "gridfs"`; `put(...)`; `open_read(...)`.
  - `class AsyncGridFsClaimCheckProvider`: same, async `put`/`open_read`, using `AsyncGridFSBucket`.

**Storage mechanics (both):** `put` generates `key = core.new_blob_key()`, builds a metadata
subdocument `{**(metadata or {}), "contentType": content_type}` (omit `contentType` when None),
uploads the bytes with **filename = key**, returns `ClaimCheckReference(provider="gridfs",
container=bucket_name, key=key, length=len(data), content_type=content_type, metadata=metadata)`.
`open_read` downloads by filename (`reference.key`) and returns the bytes.

- [ ] **Step 1: Write the failing test**

```python
# clients/python/tests/test_claimcheck_gridfs.py
import pytest
from pymongo import AsyncMongoClient, MongoClient
from testcontainers.mongodb import MongoDbContainer

from mongobus.claimcheck.gridfs import AsyncGridFsClaimCheckProvider, GridFsClaimCheckProvider


@pytest.fixture(scope="module")
def mongo():
    with MongoDbContainer("mongo:7") as container:
        yield container.get_connection_url()


def test_gridfs_put_and_open_read_round_trip(mongo):
    client = MongoClient(mongo)
    db = client["cc_sync"]
    client.drop_database("cc_sync")
    provider = GridFsClaimCheckProvider(db)

    ref = provider.put(b"hello-blob", content_type="application/json", metadata={"k": "v"})

    assert ref.provider == "gridfs"
    assert ref.container == "claimcheck"
    assert len(ref.key) == 32
    assert ref.length == len(b"hello-blob")
    assert provider.open_read(ref) == b"hello-blob"
    # stored under the claimcheck bucket with filename == key
    stored = db["claimcheck.files"].find_one({"filename": ref.key})
    assert stored is not None
    assert stored["metadata"]["contentType"] == "application/json"
    assert stored["metadata"]["k"] == "v"
    client.drop_database("cc_sync")
    client.close()


async def test_async_gridfs_put_and_open_read_round_trip(mongo):
    client = AsyncMongoClient(mongo)
    db = client["cc_async"]
    await client.drop_database("cc_async")
    provider = AsyncGridFsClaimCheckProvider(db)

    ref = await provider.put(b"hello-async", content_type="application/json", metadata=None)

    assert ref.provider == "gridfs"
    assert ref.key and len(ref.key) == 32
    assert await provider.open_read(ref) == b"hello-async"
    await client.drop_database("cc_async")
    await client.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_claimcheck_gridfs.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mongobus.claimcheck.gridfs'`

- [ ] **Step 3: Write minimal implementation**

```python
# clients/python/src/mongobus/claimcheck/gridfs.py
from gridfs import GridFSBucket
from gridfs.asynchronous import AsyncGridFSBucket

from . import core


def _metadata_doc(content_type, metadata):
    doc = dict(metadata or {})
    if content_type is not None:
        doc["contentType"] = content_type
    return doc


class GridFsClaimCheckProvider:
    name = "gridfs"

    def __init__(self, database, *, bucket_name: str = "claimcheck"):
        self._bucket_name = bucket_name
        self._bucket = GridFSBucket(database, bucket_name=bucket_name)

    def put(self, data: bytes, *, content_type, metadata) -> core.ClaimCheckReference:
        key = core.new_blob_key()
        self._bucket.upload_from_stream(key, data, metadata=_metadata_doc(content_type, metadata))
        return core.ClaimCheckReference(
            provider="gridfs", container=self._bucket_name, key=key, length=len(data),
            content_type=content_type, metadata=metadata,
        )

    def open_read(self, reference: core.ClaimCheckReference) -> bytes:
        with self._bucket.open_download_stream_by_name(reference.key) as stream:
            return stream.read()


class AsyncGridFsClaimCheckProvider:
    name = "gridfs"

    def __init__(self, database, *, bucket_name: str = "claimcheck"):
        self._bucket_name = bucket_name
        self._bucket = AsyncGridFSBucket(database, bucket_name=bucket_name)

    async def put(self, data: bytes, *, content_type, metadata) -> core.ClaimCheckReference:
        key = core.new_blob_key()
        await self._bucket.upload_from_stream(key, data, metadata=_metadata_doc(content_type, metadata))
        return core.ClaimCheckReference(
            provider="gridfs", container=self._bucket_name, key=key, length=len(data),
            content_type=content_type, metadata=metadata,
        )

    async def open_read(self, reference: core.ClaimCheckReference) -> bytes:
        stream = await self._bucket.open_download_stream_by_name(reference.key)
        try:
            return await stream.read()
        finally:
            await stream.close()
```

(If the async `open_download_stream_by_name`/`read`/`close` surface differs in the installed PyMongo, adjust to the available async GridFS API — verify with `/home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -c "from gridfs.asynchronous import AsyncGridFSBucket; help(AsyncGridFSBucket)"` — but keep the round-trip behavior identical.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_claimcheck_gridfs.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/claimcheck/gridfs.py clients/python/tests/test_claimcheck_gridfs.py
git commit -m "feat(python): add GridFS claim-check provider (sync + async) +semver:minor"
```

---

### Task 7: S3 provider (sync + async, lazy boto3)

**Files:**
- Create: `clients/python/src/mongobus/claimcheck/s3.py`
- Test: `clients/python/tests/test_claimcheck_s3.py`

**Interfaces:**
- Consumes: `core.ClaimCheckReference`, `core.new_blob_key`; `boto3` (lazy), `asyncio`.
- Produces:
  - `class S3ClaimCheckProvider`: `__init__(self, *, bucket, client=None, key_prefix="",
    provider_name="s3", **client_kwargs)`; `name` (== `provider_name`); `put(...)`; `open_read(...)`.
    If `client` is None, lazily build `boto3.client("s3", **client_kwargs)`.
  - `class AsyncS3ClaimCheckProvider`: same constructor; async `put`/`open_read` that run the
    sync S3 calls via `asyncio.to_thread`.

**Storage mechanics:** `put` → `key = key_prefix + core.new_blob_key()`,
`client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type or "application/octet-stream",
Metadata=metadata or {})`, return `ClaimCheckReference(provider=provider_name, container=bucket,
key=key, length=len(data), content_type=content_type, metadata=metadata)`. `open_read` →
`client.get_object(Bucket=bucket, Key=reference.key)["Body"].read()`.

- [ ] **Step 1: Write the failing test**

```python
# clients/python/tests/test_claimcheck_s3.py
import boto3
import pytest
from moto import mock_aws

from mongobus.claimcheck.s3 import AsyncS3ClaimCheckProvider, S3ClaimCheckProvider

REGION = "us-east-1"
BUCKET = "cc-bucket"


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name=REGION)
        client.create_bucket(Bucket=BUCKET)
        yield client


def test_s3_put_and_open_read_round_trip(s3_client):
    provider = S3ClaimCheckProvider(bucket=BUCKET, client=s3_client, key_prefix="cc/")

    ref = provider.put(b"s3-blob", content_type="application/json", metadata={"k": "v"})

    assert ref.provider == "s3"
    assert ref.container == BUCKET
    assert ref.key.startswith("cc/")
    assert ref.length == len(b"s3-blob")
    assert provider.open_read(ref) == b"s3-blob"


async def test_async_s3_put_and_open_read_round_trip(s3_client):
    provider = AsyncS3ClaimCheckProvider(bucket=BUCKET, client=s3_client)

    ref = await provider.put(b"s3-async", content_type="application/json", metadata=None)

    assert ref.provider == "s3"
    assert await provider.open_read(ref) == b"s3-async"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_claimcheck_s3.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mongobus.claimcheck.s3'`

- [ ] **Step 3: Write minimal implementation**

```python
# clients/python/src/mongobus/claimcheck/s3.py
import asyncio

from . import core


def _make_client(client, client_kwargs):
    if client is not None:
        return client
    import boto3  # lazy: boto3 is an optional [s3] dependency

    return boto3.client("s3", **client_kwargs)


class S3ClaimCheckProvider:
    def __init__(self, *, bucket, client=None, key_prefix="", provider_name="s3", **client_kwargs):
        self.name = provider_name
        self._bucket = bucket
        self._key_prefix = key_prefix
        self._client = _make_client(client, client_kwargs)

    def put(self, data: bytes, *, content_type, metadata) -> core.ClaimCheckReference:
        key = self._key_prefix + core.new_blob_key()
        self._client.put_object(
            Bucket=self._bucket, Key=key, Body=data,
            ContentType=content_type or "application/octet-stream",
            Metadata=metadata or {},
        )
        return core.ClaimCheckReference(
            provider=self.name, container=self._bucket, key=key, length=len(data),
            content_type=content_type, metadata=metadata,
        )

    def open_read(self, reference: core.ClaimCheckReference) -> bytes:
        response = self._client.get_object(Bucket=self._bucket, Key=reference.key)
        return response["Body"].read()


class AsyncS3ClaimCheckProvider:
    def __init__(self, *, bucket, client=None, key_prefix="", provider_name="s3", **client_kwargs):
        self._inner = S3ClaimCheckProvider(
            bucket=bucket, client=client, key_prefix=key_prefix,
            provider_name=provider_name, **client_kwargs,
        )
        self.name = self._inner.name

    async def put(self, data: bytes, *, content_type, metadata) -> core.ClaimCheckReference:
        return await asyncio.to_thread(
            self._inner.put, data, content_type=content_type, metadata=metadata
        )

    async def open_read(self, reference: core.ClaimCheckReference) -> bytes:
        return await asyncio.to_thread(self._inner.open_read, reference)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_claimcheck_s3.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/claimcheck/s3.py clients/python/tests/test_claimcheck_s3.py
git commit -m "feat(python): add S3 claim-check provider (sync + async, lazy boto3) +semver:minor"
```

---

### Task 8: Package exports

**Files:**
- Modify: `clients/python/src/mongobus/claimcheck/__init__.py`
- Modify: `clients/python/src/mongobus/__init__.py`
- Test: `clients/python/tests/test_claimcheck_exports.py`

**Interfaces:**
- Produces: `from mongobus import ClaimCheckConfig, GridFsClaimCheckProvider,
  AsyncGridFsClaimCheckProvider`; and `mongobus.claimcheck` re-exports `core`, the config,
  the GridFS providers, and (lazily) the S3 providers. S3 providers are importable from
  `mongobus.claimcheck.s3` directly (not eagerly from the top level, to avoid importing
  boto3 at package import).

- [ ] **Step 1: Write the failing test**

```python
# clients/python/tests/test_claimcheck_exports.py
def test_top_level_exports_config_and_gridfs():
    from mongobus import (
        AsyncGridFsClaimCheckProvider,
        ClaimCheckConfig,
        GridFsClaimCheckProvider,
    )

    assert ClaimCheckConfig is not None
    assert GridFsClaimCheckProvider.name == "gridfs"
    assert AsyncGridFsClaimCheckProvider.name == "gridfs"


def test_importing_mongobus_does_not_require_boto3(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def deny_boto3(name, *args, **kwargs):
        if name == "boto3" or name.startswith("boto3."):
            raise AssertionError("boto3 must not be imported at package import time")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", deny_boto3)
    import importlib

    import mongobus

    importlib.reload(mongobus)  # re-import under the deny hook
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_claimcheck_exports.py -v`
Expected: FAIL with `ImportError: cannot import name 'ClaimCheckConfig' from 'mongobus'`

- [ ] **Step 3: Write minimal implementation**

```python
# clients/python/src/mongobus/claimcheck/__init__.py
from .config import ClaimCheckConfig
from .core import (
    AsyncClaimCheckProvider,
    ClaimCheckProvider,
    ClaimCheckReference,
)
from .gridfs import AsyncGridFsClaimCheckProvider, GridFsClaimCheckProvider

__all__ = [
    "ClaimCheckConfig",
    "ClaimCheckProvider",
    "AsyncClaimCheckProvider",
    "ClaimCheckReference",
    "GridFsClaimCheckProvider",
    "AsyncGridFsClaimCheckProvider",
]
```

Update `clients/python/src/mongobus/__init__.py` to add (keeping existing exports):

```python
from .claimcheck import (
    AsyncGridFsClaimCheckProvider,
    ClaimCheckConfig,
    GridFsClaimCheckProvider,
)
```

and extend `__all__` with `"ClaimCheckConfig"`, `"GridFsClaimCheckProvider"`, `"AsyncGridFsClaimCheckProvider"`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_claimcheck_exports.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/claimcheck/__init__.py clients/python/src/mongobus/__init__.py clients/python/tests/test_claimcheck_exports.py
git commit -m "feat(python): export claim-check config and GridFS providers +semver:minor"
```

---

### Task 9: Sync bus — publish offload + consume rehydrate

**Files:**
- Modify: `clients/python/src/mongobus/_sync/bus.py`
- Modify: `clients/python/src/mongobus/_sync/pump.py`
- Modify: `clients/python/src/mongobus/envelope.py`
- Modify: `clients/python/tests/test_integration_sync.py`

**Interfaces:**
- Consumes: `claimcheck.core`, `claimcheck.config.ClaimCheckConfig`.
- Produces:
  - `MongoBus.__init__(..., claim_check: ClaimCheckConfig | None = None)` stored as `self._claim_check`.
  - `MongoBus.publish(..., use_claim_check: bool | None = None)` — offloads when
    `core.should_offload(...)` is true; otherwise inline.
  - `process_one(inbox, consumer, claim_check=None)` rehydrates a claim-checked message.
  - `envelope.parse_envelope` no longer raises on claim-check (becomes a plain parser).

**Important — `parse_envelope` change:** Remove the `ClaimCheckNotSupportedError` raise from
`envelope.parse_envelope` so it just returns `json.loads(payload_json)`. The claim-check
handling moves to the pump (which has the provider). Update `tests/test_envelope.py`: the test
`test_parse_envelope_rejects_claim_check_payloads` must be replaced by
`test_parse_envelope_returns_claim_check_dict` asserting it returns the dict with the
claim-check `dataContentType` intact (no raise).

**Offload in publish (after computing `data`, `now`, etc., before building the envelope):**

```python
payload_for_envelope = data
data_content_type = None
cc = self._claim_check
if cc is not None:
    payload_bytes = json.dumps(data).encode("utf-8")
    if claimcheck_core.should_offload(
        size=len(payload_bytes), threshold_bytes=cc.threshold_bytes,
        enabled=cc.enabled, use_claim_check=use_claim_check,
    ):
        created_at = now.isoformat().replace("+00:00", "Z")
        metadata = {claimcheck_core.CREATED_AT_KEY: created_at}
        if cc.compress:
            payload_bytes = claimcheck_core.gzip_compress(payload_bytes)
            metadata[claimcheck_core.COMPRESSION_KEY] = claimcheck_core.COMPRESSION_GZIP
        ref = cc.provider.put(
            payload_bytes, content_type=claimcheck_core.OBJECT_CONTENT_TYPE, metadata=metadata,
        )
        ref = replace(ref, created_at=created_at)
        payload_for_envelope = claimcheck_core.reference_to_data(ref)
        data_content_type = CLAIM_CHECK_CONTENT_TYPE
```

Then pass `data=payload_for_envelope, data_content_type=data_content_type` to `build_envelope`.
(`replace` from `dataclasses`; `CLAIM_CHECK_CONTENT_TYPE` from `mongobus.constants`.)

**Rehydrate in `process_one`:** after `env = envelope.parse_envelope(doc["PayloadJson"])` and
before building the context:

```python
if claimcheck_core.is_claim_check(env):
    if claim_check is None:
        raise ClaimCheckNotSupportedError(
            "Received a claim-check payload but no claim_check provider is configured."
        )
    reference = claimcheck_core.reference_from_data(env["data"])
    blob = claim_check.provider.open_read(reference)
    if (reference.metadata or {}).get(claimcheck_core.COMPRESSION_KEY) == claimcheck_core.COMPRESSION_GZIP:
        blob = claimcheck_core.gzip_decompress(blob, max_bytes=claim_check.max_decompressed_bytes)
    env = {**env, "data": json.loads(blob)}
```

`MongoBus.run_once`/`run` pass `self._claim_check` into `process_one`.

- [ ] **Step 1: Write the failing test**

```python
# append to clients/python/tests/test_integration_sync.py
from mongobus.claimcheck.config import ClaimCheckConfig
from mongobus.claimcheck.gridfs import GridFsClaimCheckProvider


def test_large_message_is_offloaded_and_rehydrated(db):
    client, name = db
    provider = GridFsClaimCheckProvider(client[name])
    cc = ClaimCheckConfig(provider=provider, enabled=True, threshold_bytes=128)
    bus = MongoBus(uri="", database=name, client=client, claim_check=cc)
    bus.bind("Big", endpoint_id="ep")
    received = []

    @bus.consumer(endpoint_id="ep", type_id="Big")
    def handle(ctx):
        received.append(ctx.data["payload"])

    big = "x" * 5000
    bus.publish("Big", {"payload": big})

    inbox_doc = client[name]["bus_inbox"].find_one({})
    env = json.loads(inbox_doc["PayloadJson"])
    assert env["dataContentType"] == "application/vnd.mongobus.claim-check+json"
    assert env["data"]["provider"] == "gridfs"
    assert client[name]["claimcheck.files"].count_documents({}) == 1

    assert bus.run_once("ep") is True
    assert received == [big]


def test_small_message_stays_inline(db):
    client, name = db
    provider = GridFsClaimCheckProvider(client[name])
    cc = ClaimCheckConfig(provider=provider, enabled=True, threshold_bytes=10_000)
    bus = MongoBus(uri="", database=name, client=client, claim_check=cc)
    bus.bind("Small", endpoint_id="ep")
    bus.publish("Small", {"payload": "tiny"})

    env = json.loads(client[name]["bus_inbox"].find_one({})["PayloadJson"])
    assert "dataContentType" not in env
    assert env["data"] == {"payload": "tiny"}
    assert client[name]["claimcheck.files"].count_documents({}) == 0


def test_use_claim_check_forces_offload_below_threshold(db):
    client, name = db
    provider = GridFsClaimCheckProvider(client[name])
    cc = ClaimCheckConfig(provider=provider, enabled=False, threshold_bytes=10_000)
    bus = MongoBus(uri="", database=name, client=client, claim_check=cc)
    bus.bind("Forced", endpoint_id="ep")
    bus.publish("Forced", {"payload": "tiny"}, use_claim_check=True)

    env = json.loads(client[name]["bus_inbox"].find_one({})["PayloadJson"])
    assert env["dataContentType"] == "application/vnd.mongobus.claim-check+json"


def test_compressed_claim_check_round_trips(db):
    client, name = db
    provider = GridFsClaimCheckProvider(client[name])
    cc = ClaimCheckConfig(provider=provider, enabled=True, threshold_bytes=128, compress=True)
    bus = MongoBus(uri="", database=name, client=client, claim_check=cc)
    bus.bind("Zip", endpoint_id="ep")
    received = []

    @bus.consumer(endpoint_id="ep", type_id="Zip")
    def handle(ctx):
        received.append(ctx.data["payload"])

    big = "y" * 5000
    bus.publish("Zip", {"payload": big})
    env = json.loads(client[name]["bus_inbox"].find_one({})["PayloadJson"])
    assert env["data"]["metadata"]["x-mongobus-compression"] == "gzip"

    assert bus.run_once("ep") is True
    assert received == [big]


def test_claim_check_without_provider_raises(db):
    client, name = db
    # Publish a claim-checked message using a provider-backed bus...
    provider = GridFsClaimCheckProvider(client[name])
    cc = ClaimCheckConfig(provider=provider, enabled=True, threshold_bytes=128)
    publisher = MongoBus(uri="", database=name, client=client, claim_check=cc)
    publisher.bind("NoProv", endpoint_id="ep")
    publisher.publish("NoProv", {"payload": "z" * 5000})

    # ...then consume with a bus that has NO claim_check configured.
    consumer_bus = MongoBus(uri="", database=name, client=client)

    @consumer_bus.consumer(endpoint_id="ep", type_id="NoProv")
    def handle(ctx):
        pass

    import pytest

    from mongobus.errors import ClaimCheckNotSupportedError

    with pytest.raises(ClaimCheckNotSupportedError):
        consumer_bus.run_once("ep")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_integration_sync.py -k "offload or inline or forces or compressed or without_provider" -v`
Expected: FAIL with `TypeError` (unexpected `claim_check` kwarg) or `AttributeError`.

- [ ] **Step 3: Write minimal implementation**

Edit `clients/python/src/mongobus/envelope.py` — make `parse_envelope` a plain parser:

```python
def parse_envelope(payload_json: str) -> dict:
    return json.loads(payload_json)
```

Remove the now-unused `CLAIM_CHECK_CONTENT_TYPE` / `ClaimCheckNotSupportedError` imports from
`envelope.py` if they are no longer referenced there.

Edit `clients/python/tests/test_envelope.py` — replace `test_parse_envelope_rejects_claim_check_payloads` with:

```python
def test_parse_envelope_returns_claim_check_dict():
    text = '{"id":"abc","dataContentType":"application/vnd.mongobus.claim-check+json","data":{"provider":"gridfs"}}'
    env = envelope.parse_envelope(text)
    assert env["dataContentType"] == "application/vnd.mongobus.claim-check+json"
    assert env["data"]["provider"] == "gridfs"
```

Edit `clients/python/src/mongobus/_sync/bus.py`:
- Add imports: `import json`, `from dataclasses import replace`, `from ..constants import CLAIM_CHECK_CONTENT_TYPE`, `from ..claimcheck import core as claimcheck_core`, `from ..claimcheck.config import ClaimCheckConfig`.
- `__init__`: add parameter `claim_check: ClaimCheckConfig | None = None` and `self._claim_check = claim_check`.
- `publish`: add parameter `use_claim_check: bool | None = None`; insert the offload block shown in the Interfaces section (compute `payload_for_envelope`/`data_content_type`), and pass `data=payload_for_envelope, data_content_type=data_content_type` to `build_envelope`.
- `run_once`/`run`: change the `process_one(self._inbox, consumer)` calls to `process_one(self._inbox, consumer, self._claim_check)`.

Edit `clients/python/src/mongobus/_sync/pump.py`:
- Add imports: `import json`, `from ..claimcheck import core as claimcheck_core`, `from ..errors import ClaimCheckNotSupportedError`.
- `process_one(inbox, consumer)` → `process_one(inbox, consumer, claim_check=None)`.
- After `env = envelope.parse_envelope(doc["PayloadJson"])`, insert the rehydrate block shown
  in the Interfaces section, then build the context from the (possibly rehydrated) `env`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_integration_sync.py tests/test_envelope.py -v`
Expected: PASS (all sync integration + envelope tests pass)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/_sync clients/python/src/mongobus/envelope.py clients/python/tests/test_integration_sync.py clients/python/tests/test_envelope.py
git commit -m "feat(python): sync claim-check publish offload and consume rehydrate +semver:minor"
```

---

### Task 10: Async bus — publish offload + consume rehydrate

**Files:**
- Modify: `clients/python/src/mongobus/_async/bus.py`
- Modify: `clients/python/src/mongobus/_async/pump.py`
- Modify: `clients/python/tests/test_integration_async.py`

**Interfaces:**
- Mirrors Task 9 for the async bus. `AsyncMongoBus.__init__(..., claim_check=None)`;
  `async def publish(..., use_claim_check=None)` awaiting `cc.provider.put(...)`;
  async `process_one(inbox, consumer, claim_check=None)` awaiting `claim_check.provider.open_read(...)`.
  The gzip and reference (de)serialization are the same pure-core calls (sync, no await).

- [ ] **Step 1: Write the failing test**

```python
# append to clients/python/tests/test_integration_async.py
import json as _json

from mongobus.claimcheck.config import ClaimCheckConfig
from mongobus.claimcheck.gridfs import AsyncGridFsClaimCheckProvider


async def test_async_large_message_offloaded_and_rehydrated(db):
    client, name = db
    provider = AsyncGridFsClaimCheckProvider(client[name])
    cc = ClaimCheckConfig(provider=provider, enabled=True, threshold_bytes=128)
    bus = AsyncMongoBus(uri="", database=name, client=client, claim_check=cc)
    await bus.bind("Big", endpoint_id="ep")
    received = []

    @bus.consumer(endpoint_id="ep", type_id="Big")
    async def handle(ctx):
        received.append(ctx.data["payload"])

    big = "x" * 5000
    await bus.publish("Big", {"payload": big})

    doc = await client[name]["bus_inbox"].find_one({})
    env = _json.loads(doc["PayloadJson"])
    assert env["dataContentType"] == "application/vnd.mongobus.claim-check+json"
    assert await client[name]["claimcheck.files"].count_documents({}) == 1

    assert await bus.run_once("ep") is True
    assert received == [big]


async def test_async_compressed_claim_check_round_trips(db):
    client, name = db
    provider = AsyncGridFsClaimCheckProvider(client[name])
    cc = ClaimCheckConfig(provider=provider, enabled=True, threshold_bytes=128, compress=True)
    bus = AsyncMongoBus(uri="", database=name, client=client, claim_check=cc)
    await bus.bind("Zip", endpoint_id="ep")
    received = []

    @bus.consumer(endpoint_id="ep", type_id="Zip")
    async def handle(ctx):
        received.append(ctx.data["payload"])

    big = "y" * 5000
    await bus.publish("Zip", {"payload": big})
    await bus.run_once("ep")
    assert received == [big]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_integration_async.py -k "claim or offload or compressed" -v`
Expected: FAIL with `TypeError` (unexpected `claim_check` kwarg).

- [ ] **Step 3: Write minimal implementation**

Edit `clients/python/src/mongobus/_async/pump.py`:
- Add imports: `import json`, `from ..claimcheck import core as claimcheck_core`, `from ..errors import ClaimCheckNotSupportedError`.
- `async def process_one(inbox, consumer)` → `async def process_one(inbox, consumer, claim_check=None)`.
- After `env = envelope.parse_envelope(doc["PayloadJson"])`, insert (await the download):

```python
if claimcheck_core.is_claim_check(env):
    if claim_check is None:
        raise ClaimCheckNotSupportedError(
            "Received a claim-check payload but no claim_check provider is configured."
        )
    reference = claimcheck_core.reference_from_data(env["data"])
    blob = await claim_check.provider.open_read(reference)
    if (reference.metadata or {}).get(claimcheck_core.COMPRESSION_KEY) == claimcheck_core.COMPRESSION_GZIP:
        blob = claimcheck_core.gzip_decompress(blob, max_bytes=claim_check.max_decompressed_bytes)
    env = {**env, "data": json.loads(blob)}
```

Edit `clients/python/src/mongobus/_async/bus.py`:
- Add imports: `import json`, `from dataclasses import replace`, `from ..constants import CLAIM_CHECK_CONTENT_TYPE`, `from ..claimcheck import core as claimcheck_core`, `from ..claimcheck.config import ClaimCheckConfig`.
- `__init__`: add `claim_check: ClaimCheckConfig | None = None`; `self._claim_check = claim_check`.
- `publish`: add `use_claim_check: bool | None = None`; insert the offload block but **await**
  the provider put:

```python
payload_for_envelope = data
data_content_type = None
cc = self._claim_check
if cc is not None:
    payload_bytes = json.dumps(data).encode("utf-8")
    if claimcheck_core.should_offload(
        size=len(payload_bytes), threshold_bytes=cc.threshold_bytes,
        enabled=cc.enabled, use_claim_check=use_claim_check,
    ):
        created_at = now.isoformat().replace("+00:00", "Z")
        metadata = {claimcheck_core.CREATED_AT_KEY: created_at}
        if cc.compress:
            payload_bytes = claimcheck_core.gzip_compress(payload_bytes)
            metadata[claimcheck_core.COMPRESSION_KEY] = claimcheck_core.COMPRESSION_GZIP
        ref = await cc.provider.put(
            payload_bytes, content_type=claimcheck_core.OBJECT_CONTENT_TYPE, metadata=metadata,
        )
        ref = replace(ref, created_at=created_at)
        payload_for_envelope = claimcheck_core.reference_to_data(ref)
        data_content_type = CLAIM_CHECK_CONTENT_TYPE
```

  then pass `data=payload_for_envelope, data_content_type=data_content_type` to `build_envelope`.
- `run_once`/`run`: change `await process_one(self._inbox, consumer)` to
  `await process_one(self._inbox, consumer, self._claim_check)`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_integration_async.py -v`
Expected: PASS (all async integration tests pass)

- [ ] **Step 5: Commit**

```bash
git add clients/python/src/mongobus/_async clients/python/tests/test_integration_async.py
git commit -m "feat(python): async claim-check publish offload and consume rehydrate +semver:minor"
```

---

### Task 11: S3 end-to-end integration (sync + async) via moto

**Files:**
- Create: `clients/python/tests/test_integration_s3.py`

**Interfaces:**
- Consumes: `MongoBus`/`AsyncMongoBus` with a `ClaimCheckConfig` whose provider is the S3
  provider; real MongoDB (Testcontainers) for the inbox + `moto` for S3.

This task adds an end-to-end test proving the S3 provider works through the full publish→inbox→consume
path (the prior S3 task only unit-tested the provider). The inbox lives in real Mongo; the blob in moto S3.

- [ ] **Step 1: Write the failing test**

```python
# clients/python/tests/test_integration_s3.py
import json

import boto3
import pytest
from moto import mock_aws
from pymongo import MongoClient
from testcontainers.mongodb import MongoDbContainer

from mongobus import MongoBus
from mongobus.claimcheck.config import ClaimCheckConfig
from mongobus.claimcheck.s3 import S3ClaimCheckProvider

BUCKET = "cc-e2e"


@pytest.fixture(scope="module")
def mongo():
    with MongoDbContainer("mongo:7") as container:
        yield container.get_connection_url()


@pytest.fixture
def db(mongo):
    client = MongoClient(mongo)
    name = "s3_e2e"
    client.drop_database(name)
    yield client, name
    client.drop_database(name)
    client.close()


def test_s3_claim_check_end_to_end(db):
    client, name = db
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        provider = S3ClaimCheckProvider(bucket=BUCKET, client=s3)
        cc = ClaimCheckConfig(provider=provider, enabled=True, threshold_bytes=128)
        bus = MongoBus(uri="", database=name, client=client, claim_check=cc)
        bus.bind("Big", endpoint_id="ep")
        received = []

        @bus.consumer(endpoint_id="ep", type_id="Big")
        def handle(ctx):
            received.append(ctx.data["payload"])

        big = "x" * 5000
        bus.publish("Big", {"payload": big})

        env = json.loads(client[name]["bus_inbox"].find_one({})["PayloadJson"])
        assert env["data"]["provider"] == "s3"
        assert env["data"]["container"] == BUCKET

        assert bus.run_once("ep") is True
        assert received == [big]
```

- [ ] **Step 2: Run test to verify it fails, then passes**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest tests/test_integration_s3.py -v`
Expected: PASS — the implementation from Tasks 7 and 9 already supports this; this task locks the end-to-end S3 path. If it fails, fix the integration (do not weaken the test).

- [ ] **Step 3: Commit**

```bash
git add clients/python/tests/test_integration_s3.py
git commit -m "test(python): S3 claim-check end-to-end via moto +semver:patch"
```

---

### Task 12: Docs + coverage gate

**Files:**
- Modify: `clients/python/README.md`

**Interfaces:**
- Produces: a README "Claim check" section; a full-suite run at ≥95% coverage.

- [ ] **Step 1: Add the README section**

Add to `clients/python/README.md` (after the "Indexes" section):

````markdown
## Claim check (large payloads)

Offload large message `data` to external storage and carry only a reference in `bus_inbox`,
wire-compatible with the .NET MongoBus claim-check format.

```python
from datetime import timedelta
from mongobus import MongoBus, ClaimCheckConfig, GridFsClaimCheckProvider

provider = GridFsClaimCheckProvider(db)                 # GridFS bucket "claimcheck"
cc = ClaimCheckConfig(provider=provider, enabled=True,  # offload at/above threshold
                      threshold_bytes=256 * 1024, compress=True)
bus = MongoBus(uri="...", database="appdb", claim_check=cc)

bus.publish("BigEvent", {"blob": "..."})                # offloaded if serialized >= threshold
bus.publish("BigEvent", {"blob": "..."}, use_claim_check=True)  # force offload regardless of size
```

- **Storage backends:** `GridFsClaimCheckProvider` (built into MongoDB, no extra deps) and
  `S3ClaimCheckProvider` (install `mongo-bus[s3]` for `boto3`). Async variants:
  `AsyncGridFsClaimCheckProvider`, `mongobus.claimcheck.s3.AsyncS3ClaimCheckProvider`.
- **Consuming:** a consumer with a `claim_check` provider transparently rehydrates the original
  `data` before your handler runs. A consumer **without** a provider raises
  `ClaimCheckNotSupportedError` on a claim-checked message.
- **Compression:** `compress=True` gzip-compresses the blob (decompression is bounded by
  `max_decompressed_bytes`, default 100 MiB).

> **Cleanup is not automatic.** Offloaded blobs are not deleted when their `bus_inbox`
> document expires. Run the .NET `ClaimCheckCleanupService`, or prune storage out of band.
````

- [ ] **Step 2: Run the full suite with coverage**

Run: `cd clients/python && /home/elusiven/projects/mongo-bus/clients/python-venv/bin/python -m pytest --cov=mongobus --cov-report=term-missing --cov-fail-under=95`
Expected: PASS at ≥95%. If any claim-check line is uncovered, add a focused test (e.g. the
`gzip_decompress` bomb path is covered by Task 4; the no-provider raise by Task 9; assert the
S3 async path via Task 7).

- [ ] **Step 3: Commit**

```bash
git add clients/python/README.md
git commit -m "docs(python): document claim check +semver:patch"
```

---

## Self-Review Notes

- **Spec coverage:** reference model + null omission (T3), constants (T2), offload decision + gzip + key + detection (T4), provider protocols + config (T5), GridFS provider (T6), S3 provider (T7), exports + lazy boto3 (T8), sync publish/consume integration (T9), async publish/consume integration (T10), S3 end-to-end (T11), docs + coverage + cleanup note (T12). All spec sections map to tasks.
- **`parse_envelope` behavior change** is explicit in T9: it stops raising; the existing rejecting test is replaced, and the claim-check raise moves to the pump (with the no-provider test in T9 covering it).
- **Lazy boto3:** enforced by code (import inside `_make_client`, T7) and guarded by a test (T8 `test_importing_mongobus_does_not_require_boto3`).
- **Docker dependency:** Tasks 6, 9, 10, 11 need Docker (Testcontainers). Run where Docker is available.
- **Type/name consistency:** `should_offload`, `reference_to_data`/`reference_from_data`,
  `is_claim_check`, `gzip_compress`/`gzip_decompress`, `new_blob_key`, `ClaimCheckReference`,
  `ClaimCheckConfig`, `process_one(..., claim_check=None)`, and the `CREATED_AT_KEY`/
  `COMPRESSION_KEY`/`COMPRESSION_GZIP`/`OBJECT_CONTENT_TYPE` constants are used identically across tasks.
- **Async GridFS API risk:** T6 notes verifying the exact `AsyncGridFSBucket` download/read/close
  surface against the installed PyMongo before finalizing.
