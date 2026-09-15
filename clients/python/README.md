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

## Indexes

The consume pump polls `bus_inbox` continuously and the idempotency check queries by
`(EndpointId, CloudEventId)`. Both need supporting indexes — without them every poll
and dedup check is a collection scan. Provision them once at startup:

```python
from datetime import timedelta

bus.ensure_indexes()                                       # lock + dedup + a 7-day TTL
bus.ensure_indexes(processed_message_ttl=timedelta(days=30))  # custom retention window
bus.ensure_indexes(processed_message_ttl=None)             # lock + dedup only, no auto-expiry
```

`ensure_indexes()` mirrors the three inbox indexes the .NET `MongoBusIndexesHostedService`
creates: the lock index (`EndpointId, Status, VisibleUtc, LockedUntilUtc`), the dedup index
(`EndpointId, CloudEventId`), and — unless `processed_message_ttl=None` — a TTL/retention
index on `ProcessedUtc` (default 7 days, matching .NET) that **expires processed inbox
documents** after the window. Pending and dead-lettered messages are never expired. It also
ensures the unique `(Topic, EndpointId)` binding index, and drops the `CreatedUtc` TTL index
earlier versions created, which expired messages that had not been processed.

**Auto-provisioning:** `run()` and `run_once()` call `ensure_indexes()` once on first start
if you haven't already, but the implicit path creates only the **lock and dedup** indexes —
never the data-expiring TTL index. Call `ensure_indexes()` explicitly if you want the TTL
retention behavior. If you run the Python client alongside a .NET MongoBus, that service
already provisions all of these.

> Re-calling `ensure_indexes()` with the **same** TTL is a no-op. Calling it with a
> **different** `processed_message_ttl` updates the existing index's window in place
> (`collMod`), as the .NET service does on startup.

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

## Long-running handlers (lock time and renewal)

A consumer locks each message for `lock_seconds` (default `60`, minimum `3`). While the
handler runs, the client renews the lock every `lock_seconds / 3` seconds, so a handler may
run far longer than `lock_seconds` without another consumer picking the message up.

```python
@bus.consumer(endpoint_id="song-worker", type_id="SongRequested", lock_seconds=120, max_attempts=3)
def generate(ctx):
    for step in plan_steps(ctx.data):
        if ctx.lock_lost:
            raise InterruptedError("lock lost; another consumer owns this message now")
        run(step)
```

- **`lock_seconds`** is how long the message stays hidden if the process stops renewing it
  (crash, kill, network partition). After that it is redelivered — but with the **same**
  `Attempt`: expiry only clears the lock (`LockedUntilUtc` and `LockOwner`), it does not
  increment `Attempt`. Only a handler that raises does that (via the retry/dead-letter path).
  So a handler that keeps crashing or being killed is redelivered repeatedly and those
  redeliveries never count toward `max_attempts`. Long-running jobs should enforce their own
  deadline (for example, an expiry timestamp carried in the message) rather than relying on
  `max_attempts` to bound retries after a crash.
  Values that are not an `int >= 3` raise `ValueError` when the consumer is registered.
- **`ctx.lock_lost`** becomes `True` when a renewal finds that this delivery no longer owns
  the message (its lock expired and another consumer took it, or it was completed elsewhere).
  It never resets. Long handlers should check it and stop early. `lock_lost == False` is not
  proof the lock is still held, though: detection lags by up to `lock_seconds / 3` (the
  renewal interval), and a renewal that fails with a driver error (for example, MongoDB
  unreachable) does not set it, since the client cannot tell whether the lock is still ours.
  Also, the owner check on completion only protects the inbox outcome write (processed, retry,
  or dead-letter) — it does not undo side effects the handler already performed (publishes,
  uploads, etc.). Downstream consumers of those side effects must tolerate duplicates.
- **Stale outcomes are discarded.** Processed, retry and dead-letter updates only apply while
  the message's `LockOwner` is still this delivery, so a handler that lost its lock cannot
  overwrite the new owner's result.
- **Async handlers must await.** `AsyncMongoBus` renews on an asyncio task, which only runs
  while the handler awaits. Offload blocking work with `await asyncio.to_thread(...)`, or use
  the synchronous `MongoBus`, which renews on a background thread.
- **.NET parity:** .NET consumers set a fixed `LockTime` and do not renew; keep their
  `LockTime` above the longest handler.

## Idempotency default

Consumers default to `idempotent=True`, which enables per-endpoint, effectively-once delivery by deduplicating on the CloudEvent `id` field. This **intentionally differs** from the .NET MongoBus default (`IdempotencyEnabled=false`, at-least-once). You can override this per consumer:

```python
@bus.consumer(endpoint_id="my-ep", type_id="OrderPlaced", idempotent=False)
def handle(msg):
    ...
```
