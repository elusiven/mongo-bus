# MongoBus.Sample

This sample demonstrates MongoBus features end-to-end:

- Inbox pattern + CloudEvents
- Competing consumers
- Batch consumers (flush, grouping, backpressure)
- Claim check (in-memory) + compression
- Delayed delivery
- Idempotency
- Transactional outbox (explicit API + optional global routing hook)
- Interceptors (publish/consume/batch)
- Observers (publish/consume/batch) with default OpenTelemetry metrics
- Dashboard

## Run

1. Start MongoDB locally (`mongodb://localhost:27017`).
2. Run the sample:

```
dotnet run --project samples/MongoBus.Sample
```

3. Open the dashboard at `/mongobus`.

## Notes

- Claim-check is enabled with a low threshold to show the feature.
- Batch processing groups orders by `CustomerId`.
- Idempotency is demonstrated by publishing the same CloudEvent `id` twice.
- Outbox is enabled (`Outbox.Enabled = true`) and `orders.*` publishes are routed through outbox by `UseOutboxForTypeId`.
- The sample tries `PublishWithTransactionAsync`; if MongoDB does not support transactions (no replica set), it falls back to `PublishToOutboxAsync`.
