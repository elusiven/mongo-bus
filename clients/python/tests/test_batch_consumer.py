import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from mongobus import MongoBus, constants, envelope
from mongobus._sync.pump import BatchConsumer, process_batch


NOW = datetime.now(timezone.utc)


class _UpdateResult:
    def __init__(self, matched_count=1):
        self.matched_count = matched_count


class FakeInbox:
    def __init__(self, docs):
        self.docs = {doc["_id"]: doc for doc in docs}
        self.claimed_ids = []

    def find_one_and_update(self, _filter, update, *, sort, return_document):
        now = datetime.now(timezone.utc)
        eligible = [
            doc
            for doc in self.docs.values()
            if doc["Status"] == constants.STATUS_PENDING
            and doc["VisibleUtc"] < now
            and (doc["LockedUntilUtc"] is None or doc["LockedUntilUtc"] < now)
            and doc["EndpointId"] == "song-worker"
            and doc["TypeId"] == "SongRequested"
        ]
        if not eligible:
            return None
        doc = min(eligible, key=lambda item: item["VisibleUtc"])
        doc["LockedUntilUtc"] = update["$set"]["LockedUntilUtc"]
        doc["LockOwner"] = update["$set"]["LockOwner"]
        self.claimed_ids.append(doc["_id"])
        return doc

    def update_one(self, selector, update):
        doc = self.docs[selector["_id"]]
        if doc.get("LockOwner") != selector.get("LockOwner"):
            return _UpdateResult(0)
        doc.update(update.get("$set", {}))
        return _UpdateResult(1)

    def count_documents(self, selector, *, limit):
        return sum(
            1
            for doc in self.docs.values()
            if doc["EndpointId"] == selector["EndpointId"]
            and doc["CloudEventId"] == selector["CloudEventId"]
            and doc["Status"] == constants.STATUS_PROCESSED
            and doc["_id"] != selector["_id"]["$ne"]
        )


def _doc(message_id, event_id=None, *, attempt=0, visible=None, status=None):
    event_id = event_id or message_id
    visible = visible or (NOW - timedelta(seconds=1))
    return {
        "_id": message_id,
        "EndpointId": "song-worker",
        "TypeId": "SongRequested",
        "Status": status or constants.STATUS_PENDING,
        "VisibleUtc": visible,
        "LockedUntilUtc": None,
        "LockOwner": None,
        "Attempt": attempt,
        "CloudEventId": event_id,
        "PayloadJson": json.dumps(
            envelope.build_envelope(
                type_id="SongRequested",
                data={"message_id": message_id},
                source="test",
                event_id=event_id,
                time_utc=NOW,
            )
        ),
    }


def _consumer(handler, *, batch_size=3, max_attempts=3, idempotent=True):
    return BatchConsumer(
        endpoint_id="song-worker",
        type_id="SongRequested",
        handler=handler,
        max_attempts=max_attempts,
        idempotent=idempotent,
        lock_seconds=3,
        batch_size=batch_size,
    )


def test_process_batch_claims_visible_messages_in_order_and_allows_short_batches():
    inbox = FakeInbox([_doc("second", visible=NOW), _doc("first")])
    received = []

    def handler(contexts):
        received.extend(ctx.cloud_event_id for ctx in contexts)
        return {ctx.cloud_event_id: None for ctx in contexts}

    consumer = _consumer(handler, batch_size=3)

    assert process_batch(inbox, consumer) is True
    assert received == ["first", "second"]
    assert [inbox.docs[item]["Status"] for item in ("first", "second")] == [
        constants.STATUS_PROCESSED,
        constants.STATUS_PROCESSED,
    ]
    assert process_batch(inbox, consumer) is False


def test_process_batch_finalizes_each_message_independently():
    inbox = FakeInbox([_doc("success"), _doc("retry", attempt=0), _doc("dead", attempt=2)])

    def handler(contexts):
        return {
            "success": None,
            "retry": RuntimeError("temporary"),
            "dead": RuntimeError("permanent"),
        }

    assert process_batch(inbox, _consumer(handler, max_attempts=3)) is True
    assert inbox.docs["success"]["Status"] == constants.STATUS_PROCESSED
    assert inbox.docs["retry"]["Status"] == constants.STATUS_PENDING
    assert inbox.docs["retry"]["Attempt"] == 1
    assert inbox.docs["dead"]["Status"] == constants.STATUS_DEAD
    assert inbox.docs["dead"]["Attempt"] == 3
    assert inbox.docs["retry"]["LastError"] == "temporary"
    assert inbox.docs["dead"]["LastError"] == "permanent"


def test_process_batch_skips_idempotent_duplicates_without_calling_handler():
    inbox = FakeInbox(
        [
            _doc("already-processed", event_id="duplicate", status=constants.STATUS_PROCESSED),
            _doc("delivery", event_id="duplicate"),
        ]
    )
    called = []

    def handler(contexts):
        called.extend(contexts)
        return {}

    assert process_batch(inbox, _consumer(handler)) is True
    assert called == []
    assert inbox.docs["delivery"]["Status"] == constants.STATUS_PROCESSED
    assert inbox.docs["delivery"]["LastError"] == "Skipped due to idempotency"


def test_process_batch_starts_one_lock_renewer_per_context():
    inbox = FakeInbox([_doc("one"), _doc("two")])
    entered = []

    class FakeRenewer:
        def __init__(self, _inbox, *, message_id, **_kwargs):
            entered.append(message_id)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    def handler(contexts):
        return {ctx.cloud_event_id: None for ctx in contexts}

    with patch("mongobus._sync.pump.LockRenewer", FakeRenewer):
        process_batch(inbox, _consumer(handler))

    assert entered == ["one", "two"]


def test_batch_consumer_validates_positive_batch_size():
    values = {
        "endpoint_id": "ep",
        "type_id": "type",
        "handler": lambda contexts: {},
        "max_attempts": 3,
        "idempotent": True,
        "lock_seconds": 3,
    }
    for invalid in (0, -1, 1.5, True, "3"):
        try:
            BatchConsumer(**values, batch_size=invalid)
        except ValueError:
            continue
        raise AssertionError(f"batch_size={invalid!r} should be rejected")


def test_mongo_bus_batch_consumer_registers_without_changing_single_consumer_list():
    class FakeClient:
        def __getitem__(self, _database):
            return self

        def __getattr__(self, _name):
            return self

    bus = MongoBus(uri="", database="test", client=FakeClient())

    @bus.batch_consumer(endpoint_id="ep", type_id="SongRequested", batch_size=3)
    def handler(contexts):
        return {ctx.cloud_event_id: None for ctx in contexts}

    assert bus._consumers == []
    assert len(bus._batch_consumers) == 1
    assert bus._batch_consumers[0].handler is handler
    assert bus._batch_consumers[0].batch_size == 3
