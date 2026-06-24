# Claim-Check for the Python Client — Design

**Date:** 2026-06-24
**Status:** Approved (design); pending spec review
**Scope:** Add claim-check (large-payload offloading) to the `mongo-bus` Python client, wire-compatible with the .NET MongoBus claim-check format, with GridFS and S3 storage backends, for both publish (offload) and consume (rehydrate).

## 1. Goal

Today the Python consumer raises `ClaimCheckNotSupportedError` on any claim-checked
message. This makes the Python client a **full peer** for large messages: it can publish
messages whose `data` is offloaded to external storage and consume (rehydrate) such
messages — interoperating byte-for-byte with the .NET implementation.

### In scope
- `ClaimCheckReference` wire model and (de)serialization, matching .NET.
- Publish-side offload: threshold-based and per-publish opt-in, with optional gzip.
- Consume-side rehydrate: download, decompress (bounded), deserialize.
- Two storage providers: **GridFS** (no extra deps) and **S3** (optional `boto3`).
- Sync (`MongoBus`) and async (`AsyncMongoBus`) support.

### Out of scope (deferred)
- **Orphan cleanup** (the .NET background `ClaimCheckCleanupService`). A claim-check blob
  is written but not auto-deleted when its inbox document expires. Documented; `delete`/
  `list` are not in the first-cut provider protocol. Can be a later PR.
- **Azure Blob** provider (GridFS + S3 chosen for this cut).
- Stream-typed messages (the .NET `Stream` special-case). Python messages are JSON-serializable
  objects (dicts); the stored blob is always the JSON-serialized `data`.

## 2. Wire contract (mirrors .NET — verified against source)

### CloudEvent envelope for an offloaded message
`data` becomes the reference object; `dataContentType` = the claim-check content type:
```json
{
  "specVersion": "1.0", "id": "...", "type": "...", "source": "...", "time": "...Z",
  "dataContentType": "application/vnd.mongobus.claim-check+json",
  "data": {
    "provider": "gridfs",
    "container": "claimcheck",
    "key": "<32-hex-guid>",
    "length": 1234,
    "contentType": "application/json",
    "metadata": {"x-mongobus-created-at": "...Z", "x-mongobus-compression": "gzip"},
    "createdAt": "...Z"
  }
}
```

### `ClaimCheckReference` fields (camelCase JSON, nulls omitted)
`provider` (str, required), `container` (str, required), `key` (str, required),
`length` (int, required), `contentType` (str?, omit if None), `metadata`
(dict[str,str]?, omit if None), `createdAt` (str ISO-8601 UTC?, omit if None).

Null omission matches .NET's `CloudEventSerializer` (`DefaultIgnoreCondition =
WhenWritingNull`).

### Constants
- Content type: `application/vnd.mongobus.claim-check+json` (already in `constants.py`).
- Stored blob content type (object): `application/json`.
- Metadata keys: `x-mongobus-created-at`, `x-mongobus-compression`.
- Compression algorithm value: `gzip` (plain gzip stream, Python `gzip`/wbits=31 compatible).
- Default threshold: `256 * 1024` bytes (256 KiB).
- Default max decompressed: `100 * 1024 * 1024` bytes (100 MiB).

### Stored blob
The serialized message `data` as JSON (`json.dumps(data)`), optionally gzip-compressed.
**Not** the whole envelope. For GridFS the blob is a GridFS file whose **filename = key**
(32-char lowercase hex, no dashes) under bucket `claimcheck` (collections
`claimcheck.files` / `claimcheck.chunks`), with `contentType` and the `x-mongobus-*`
keys in the file `metadata` subdocument.

## 3. Architecture

Same pure-core + thin-shell pattern as the rest of the package. New subpackage
`src/mongobus/claimcheck/`:

```
src/mongobus/claimcheck/
  __init__.py     # public exports
  core.py         # PURE: reference model, (de)serialization, should_offload, gzip, key gen, protocols
  gridfs.py       # GridFsClaimCheckProvider (sync) + AsyncGridFsClaimCheckProvider
  s3.py           # S3ClaimCheckProvider (sync) + AsyncS3ClaimCheckProvider (boto3 via asyncio.to_thread)
config.py addition: ClaimCheckConfig dataclass (provider + options)
```

### `core.py` (pure, no I/O)
- `@dataclass(frozen=True) ClaimCheckReference`: `provider, container, key, length,
  content_type=None, metadata=None, created_at=None`.
- `reference_to_data(ref) -> dict`: camelCase dict, omit None `contentType`/`metadata`/`createdAt`.
- `reference_from_data(data: dict) -> ClaimCheckReference`.
- `is_claim_check(envelope: dict) -> bool`: `envelope.get("dataContentType") == CONTENT_TYPE`.
- `should_offload(*, size: int, threshold_bytes: int, enabled: bool, use_claim_check: bool | None) -> bool`:
  `True if use_claim_check else (enabled and size >= threshold_bytes)`.
- `gzip_compress(data: bytes) -> bytes` (plain gzip).
- `gzip_decompress(data: bytes, *, max_bytes: int) -> bytes` — raises `ClaimCheckError`
  (decompression-bomb guard) if output would exceed `max_bytes`.
- `new_blob_key() -> str`: `uuid4().hex`.
- Provider protocols (`typing.Protocol`):
  - `ClaimCheckProvider`: `name: str`; `put(self, data: bytes, *, content_type: str | None,
    metadata: dict[str, str] | None) -> ClaimCheckReference`; `open_read(self, reference) -> bytes`.
  - `AsyncClaimCheckProvider`: async `put` / `open_read`.

### Providers
- **GridFS** (`gridfs.py`):
  - Sync `GridFsClaimCheckProvider(database, *, bucket_name="claimcheck")` using
    `gridfs.GridFSBucket`. `name = "gridfs"`. `put`: `key = new_blob_key()`,
    `upload_from_stream(key, data, metadata={**metadata, "contentType": content_type})`;
    returns reference with `provider="gridfs"`, `container=bucket_name`, `key`, `length=len(data)`,
    `content_type`, `metadata`. `open_read`: `open_download_stream_by_name(key).read()`.
  - Async `AsyncGridFsClaimCheckProvider(database, *, bucket_name="claimcheck")` using
    `gridfs.asynchronous.AsyncGridFSBucket`, with awaited calls.
- **S3** (`s3.py`, lazy `import boto3`):
  - Sync `S3ClaimCheckProvider(*, bucket, client=None, key_prefix="", provider_name="s3", **client_kwargs)`.
    `put`: `key = (key_prefix + new_blob_key())`, `client.put_object(Bucket=bucket, Key=key,
    Body=data, ContentType=content_type, Metadata=metadata)`; reference with `provider=provider_name`,
    `container=bucket`, `key`, `length=len(data)`. `open_read`: `client.get_object(...)["Body"].read()`.
    boto3 stores user metadata under the `Metadata` arg; on read it returns under `["Metadata"]`
    with lowercased keys — the reference's own `metadata` (carried in the envelope) is the
    source of truth for decompression, so S3 object metadata round-tripping is not relied upon.
  - Async `AsyncS3ClaimCheckProvider` wraps the sync client calls in `asyncio.to_thread`.

### `ClaimCheckConfig`
```python
@dataclass
class ClaimCheckConfig:
    provider: ClaimCheckProvider | AsyncClaimCheckProvider
    enabled: bool = False
    threshold_bytes: int = 256 * 1024
    compress: bool = False
    max_decompressed_bytes: int = 100 * 1024 * 1024
```

## 4. Bus integration

### Constructor
`MongoBus(uri, database, *, client=None, claim_check: ClaimCheckConfig | None = None)`
(same for `AsyncMongoBus`). Stored as `self._claim_check`.

### Publish (offload)
`publish(..., use_claim_check: bool | None = None)`. After building `data`:
1. `payload_bytes = json.dumps(data).encode()`.
2. If `self._claim_check` and `core.should_offload(size=len(payload_bytes),
   threshold_bytes=cfg.threshold_bytes, enabled=cfg.enabled, use_claim_check=use_claim_check)`:
   - `metadata = {core.CREATED_AT_KEY: now_iso}`.
   - If `cfg.compress`: `payload_bytes = core.gzip_compress(payload_bytes)`;
     `metadata[core.COMPRESSION_KEY] = "gzip"`.
   - `ref = provider.put(payload_bytes, content_type="application/json", metadata=metadata)`
     then set `ref.created_at = now_iso` (mirroring .NET `EnsureCreatedAt`).
   - `envelope_data = core.reference_to_data(ref)`; `data_content_type = CONTENT_TYPE`.
3. Else inline as today (`envelope_data = data`, no `data_content_type`).
4. Build the envelope with `data=envelope_data`, `data_content_type=...`, serialize, fan out.

`build_envelope` already accepts `data_content_type` (currently unused by publish).

### Consume (rehydrate)
- `envelope.parse_envelope` **stops raising** on claim-check (becomes a plain parser).
- The pump (`process_one`, gaining a `claim_check` parameter from the bus):
  1. `env = parse_envelope(doc["PayloadJson"])`.
  2. If `core.is_claim_check(env)`:
     - If no provider configured → raise `ClaimCheckNotSupportedError` (today's safe failure).
     - Else `ref = core.reference_from_data(env["data"])`;
       `blob = provider.open_read(ref)`;
       if `ref.metadata` has `x-mongobus-compression == "gzip"`:
       `blob = core.gzip_decompress(blob, max_bytes=cfg.max_decompressed_bytes)`;
       `resolved = json.loads(blob)`; `env = {**env, "data": resolved}`.
  3. Build `ConsumeContext.from_message(env, doc)` with the resolved data.

The bus passes `self._claim_check` into `process_one`/`run`/`run_once`.

## 5. Dependencies

- GridFS: none (pymongo ships `gridfs`, sync and async).
- S3: optional extra `mongo-bus[s3]` → `boto3`. Imported lazily inside `s3.py` so the
  core package has no hard boto3 dependency.
- Test-only: `moto` (in-process S3 mock) added to the `[test]` extra.

## 6. Testing

- **Pure unit** (`test_claimcheck_core.py`): reference↔dict null-omission and field
  spelling; `should_offload` truth table (forced, enabled+over, enabled+under, disabled);
  gzip round-trip; `gzip_decompress` raises past `max_bytes`; `new_blob_key` 32-hex format.
- **GridFS integration** (real Mongo via Testcontainers): publish a large payload →
  blob in `claimcheck.files`, consume rehydrates the original; compression round-trip;
  sub-threshold message stays inline (no offload); per-publish `use_claim_check=True` forces
  offload below threshold; reference shape (`provider="gridfs"`, `container="claimcheck"`,
  filename == key); claim-check with no provider raises `ClaimCheckNotSupportedError`;
  sync and async.
- **S3 integration** (`moto` mock): same round-trip + opt-in + compression; sync and async.
- Coverage stays ≥95% (gate enforced in CI).

## 7. Open items (flagged, non-blocking)

1. **Async S3 = boto3 in `asyncio.to_thread`** (no second async-AWS SDK). Decided.
2. **S3 tests use `moto`** (no localstack/minio container). Decided.
3. **Orphan cleanup deferred** — document that offloaded blobs are not auto-removed.
4. **`json.dumps` separators** — Python default (`", "`/`": "`) vs .NET compact. The stored
   blob is only ever deserialized (never byte-compared across languages), so separator
   differences are immaterial; the reference object in the envelope follows the existing
   envelope serializer. No action needed; noted for the reviewer.

## 8. Process

- TDD throughout (red-green-refactor).
- Per-task review + a final whole-branch review, as with the prior PRs.
- Lands as its own PR off `main`.
