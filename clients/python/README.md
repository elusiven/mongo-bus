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

## Idempotency default

Consumers default to `idempotent=True`, which enables per-endpoint, effectively-once delivery by deduplicating on the CloudEvent `id` field. This **intentionally differs** from the .NET MongoBus default (`IdempotencyEnabled=false`, at-least-once). You can override this per consumer:

```python
@bus.consumer(endpoint_id="my-ep", type_id="OrderPlaced", idempotent=False)
def handle(msg):
    ...
```
