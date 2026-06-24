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
