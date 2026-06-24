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
index on `CreatedUtc` (default 7 days, matching .NET) that **expires inbox documents** after
the window. It also ensures the unique `(Topic, EndpointId)` binding index.

**Auto-provisioning:** `run()` and `run_once()` call `ensure_indexes()` once on first start
if you haven't already, but the implicit path creates only the **lock and dedup** indexes —
never the data-expiring TTL index. Call `ensure_indexes()` explicitly if you want the TTL
retention behavior. If you run the Python client alongside a .NET MongoBus, that service
already provisions all of these.

> Re-calling `ensure_indexes()` with the **same** TTL is a no-op. Calling it with a
> **different** `processed_message_ttl` after the `CreatedUtc` index already exists raises
> `pymongo.errors.OperationFailure` (`IndexOptionsConflict`) — MongoDB will not change a
> TTL window in place. To change it, drop and recreate the `CreatedUtc` index. The .NET
> service has the same limitation.

## Idempotency default

Consumers default to `idempotent=True`, which enables per-endpoint, effectively-once delivery by deduplicating on the CloudEvent `id` field. This **intentionally differs** from the .NET MongoBus default (`IdempotencyEnabled=false`, at-least-once). You can override this per consumer:

```python
@bus.consumer(endpoint_id="my-ep", type_id="OrderPlaced", idempotent=False)
def handle(msg):
    ...
```
