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
