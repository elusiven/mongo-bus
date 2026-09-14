import json
import threading
import time
from datetime import timedelta

import pytest
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from testcontainers.mongodb import MongoDbContainer

from mongobus import MongoBus
from mongobus.claimcheck.config import ClaimCheckConfig
from mongobus.claimcheck.gridfs import GridFsClaimCheckProvider
from mongobus.errors import ClaimCheckNotSupportedError
from tests.support import utc_now_naive, wait_until


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

    # Publish once with a fixed cloud event id, then insert a duplicate inbox doc.
    bus.publish("OrderPlaced", {"orderId": "1"}, id="dup-ce")
    first = client[name]["bus_inbox"].find_one({})
    client[name]["bus_inbox"].insert_one({**{k: v for k, v in first.items() if k != "_id"}})

    bus.run_once("ep")  # processes the first
    bus.run_once("ep")  # second must be skipped by idempotency

    assert sum(calls) == 1
    statuses = [d["Status"] for d in client[name]["bus_inbox"].find({})]
    assert statuses.count("Processed") == 2
    skipped = client[name]["bus_inbox"].find_one({"LastError": "Skipped due to idempotency"})
    assert skipped is not None


def test_run_once_returns_false_when_no_messages(db):
    # Covers the process_one return-False path (empty inbox → doc is None).
    client, name = db
    bus = MongoBus(uri="", database=name, client=client)
    bus.bind("OrderPlaced", endpoint_id="ep")

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced")
    def handle(ctx):  # pragma: no cover
        pass

    result = bus.run_once("ep")  # inbox is empty — process_one must return False
    assert result is False


def test_run_once_returns_false_when_endpoint_not_matched(db):
    # Covers the branch in run_once where consumer.endpoint_id != requested endpoint_id.
    client, name = db
    bus = MongoBus(uri="", database=name, client=client)
    bus.bind("OrderPlaced", endpoint_id="ep-a")

    @bus.consumer(endpoint_id="ep-a", type_id="OrderPlaced")
    def handle(ctx):  # pragma: no cover
        pass

    bus.publish("OrderPlaced", {"orderId": "1"})
    # "ep-b" doesn't match "ep-a" consumer — run_once iterates, finds no match, returns False.
    result = bus.run_once("ep-b")
    assert result is False


def test_run_loop_processes_message_then_stops(mongo):
    # Covers the run() loop body (sync bus.run with stop_event).
    client = MongoClient(mongo)
    name = "testdb_run_loop"
    client.drop_database(name)

    bus = MongoBus(uri="", database=name, client=client)
    bus.bind("OrderPlaced", endpoint_id="ep")
    received = []
    stop = threading.Event()

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced")
    def handle(ctx):
        received.append(ctx.data["orderId"])
        stop.set()  # signal the loop to exit after processing this message

    bus.publish("OrderPlaced", {"orderId": "run-loop"})

    thread = threading.Thread(target=bus.run, kwargs={"stop_event": stop}, daemon=True)
    thread.start()
    thread.join(timeout=15)

    assert not thread.is_alive(), "run() did not stop within 15 seconds"
    assert received == ["run-loop"]

    client.drop_database(name)
    client.close()


def test_run_exits_immediately_when_stop_event_already_set(mongo):
    # Covers the while-condition branch: stop_event already set → loop body never executes.
    client = MongoClient(mongo)
    name = "testdb_run_already_stopped"
    client.drop_database(name)

    bus = MongoBus(uri="", database=name, client=client)

    stop = threading.Event()
    stop.set()  # set before run() is called

    thread = threading.Thread(target=bus.run, kwargs={"stop_event": stop}, daemon=True)
    thread.start()
    thread.join(timeout=5)

    assert not thread.is_alive(), "run() should have returned immediately"

    client.drop_database(name)
    client.close()


def test_construct_bus_from_uri_without_client(mongo):
    # Covers the client=None construction branch: MongoBus(uri=...) must build its own MongoClient.
    name = "testdb_uri_ctor"
    bus = MongoBus(uri=mongo, database=name)
    bus.bind("OrderPlaced", endpoint_id="ep")
    created = bus.publish("OrderPlaced", {"orderId": "uri-ctor"})
    assert created == 1
    bus._client.drop_database(name)
    bus._client.close()


def test_run_loop_sleeps_when_no_work_then_stops(mongo):
    # Covers the time.sleep branch in run() when no messages are available (did_work=False).
    client = MongoClient(mongo)
    name = "testdb_run_sleep"
    client.drop_database(name)

    bus = MongoBus(uri="", database=name, client=client)
    stop = threading.Event()

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced")
    def handle(ctx):  # pragma: no cover
        pass

    # Delay stop to let run() enter at least one idle iteration (triggering the sleep path).
    def stop_soon():
        threading.Event().wait(timeout=0.15)
        stop.set()

    stopper = threading.Thread(target=stop_soon, daemon=True)
    stopper.start()

    thread = threading.Thread(target=bus.run, kwargs={"stop_event": stop}, daemon=True)
    thread.start()
    thread.join(timeout=10)

    assert not thread.is_alive(), "run() did not stop within 10 seconds"

    client.drop_database(name)
    client.close()


def _index_key_lists(info):
    return [list(meta["key"]) for meta in info.values()]


def _ttl_seconds(info):
    return [meta["expireAfterSeconds"] for meta in info.values() if "expireAfterSeconds" in meta]


def test_ensure_indexes_creates_inbox_and_binding_indexes(db):
    client, name = db
    bus = MongoBus(uri="", database=name, client=client)

    bus.ensure_indexes()

    inbox = _index_key_lists(client[name]["bus_inbox"].index_information())
    assert [("EndpointId", 1), ("Status", 1), ("VisibleUtc", 1), ("LockedUntilUtc", 1)] in inbox
    assert [("EndpointId", 1), ("CloudEventId", 1)] in inbox
    assert [("CreatedUtc", 1)] in inbox
    assert _ttl_seconds(client[name]["bus_inbox"].index_information()) == [7 * 24 * 60 * 60]

    bindings = client[name]["bus_bindings"].index_information()
    unique = [m for m in bindings.values() if list(m["key"]) == [("Topic", 1), ("EndpointId", 1)]]
    assert unique and unique[0].get("unique") is True


def test_ensure_indexes_without_ttl_omits_retention_index(db):
    client, name = db
    bus = MongoBus(uri="", database=name, client=client)

    bus.ensure_indexes(processed_message_ttl=None)

    info = client[name]["bus_inbox"].index_information()
    assert _ttl_seconds(info) == []
    assert [("EndpointId", 1), ("CloudEventId", 1)] in _index_key_lists(info)


def test_run_once_auto_creates_lock_and_dedup_without_ttl(db):
    client, name = db
    bus = MongoBus(uri="", database=name, client=client)
    bus.bind("OrderPlaced", endpoint_id="ep")

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced")
    def handle(ctx):
        pass

    # No message published; run_once just triggers the auto-provisioning path.
    bus.run_once("ep")

    info = client[name]["bus_inbox"].index_information()
    keys = _index_key_lists(info)
    assert [("EndpointId", 1), ("Status", 1), ("VisibleUtc", 1), ("LockedUntilUtc", 1)] in keys
    assert [("EndpointId", 1), ("CloudEventId", 1)] in keys
    assert _ttl_seconds(info) == []  # auto path must not create the data-expiring TTL index


def test_ensure_indexes_is_idempotent_with_same_ttl(db):
    client, name = db
    bus = MongoBus(uri="", database=name, client=client)

    bus.ensure_indexes()
    bus.ensure_indexes()  # same TTL again must not raise

    assert _ttl_seconds(client[name]["bus_inbox"].index_information()) == [7 * 24 * 60 * 60]


def test_ensure_indexes_with_conflicting_ttl_raises(db):
    client, name = db
    bus = MongoBus(uri="", database=name, client=client)

    bus.ensure_indexes(processed_message_ttl=timedelta(days=7))
    with pytest.raises(OperationFailure):
        bus.ensure_indexes(processed_message_ttl=timedelta(days=30))


def test_explicit_ensure_indexes_after_auto_path_adds_ttl(db):
    client, name = db
    bus = MongoBus(uri="", database=name, client=client)
    bus.bind("OrderPlaced", endpoint_id="ep")

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced")
    def handle(ctx):
        pass

    bus.run_once("ep")  # auto path: lock + dedup, no TTL
    assert _ttl_seconds(client[name]["bus_inbox"].index_information()) == []

    bus.ensure_indexes()  # explicit opt-in must still run and add the TTL
    assert _ttl_seconds(client[name]["bus_inbox"].index_information()) == [7 * 24 * 60 * 60]


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

    with pytest.raises(ClaimCheckNotSupportedError):
        consumer_bus.run_once("ep")


def test_consumer_locks_for_sixty_seconds_by_default(db):
    client, name = db
    bus = MongoBus(uri="", database=name, client=client)
    bus.bind("SongRequested", endpoint_id="ep")
    lock_windows = []

    @bus.consumer(endpoint_id="ep", type_id="SongRequested")
    def handle(ctx):
        lock_windows.append(ctx.raw["LockedUntilUtc"] - utc_now_naive())

    bus.publish("SongRequested", {"songId": "s-1"})
    assert bus.run_once("ep") is True

    assert timedelta(seconds=50) < lock_windows[0] <= timedelta(seconds=60)


def test_consumer_locks_for_its_configured_lock_seconds(db):
    client, name = db
    bus = MongoBus(uri="", database=name, client=client)
    bus.bind("SongRequested", endpoint_id="ep")
    lock_windows = []

    @bus.consumer(endpoint_id="ep", type_id="SongRequested", lock_seconds=120)
    def handle(ctx):
        lock_windows.append(ctx.raw["LockedUntilUtc"] - utc_now_naive())

    bus.publish("SongRequested", {"songId": "s-1"})
    assert bus.run_once("ep") is True

    assert timedelta(seconds=110) < lock_windows[0] <= timedelta(seconds=120)


def test_consumer_registration_rejects_lock_seconds_below_three(db):
    client, name = db
    bus = MongoBus(uri="", database=name, client=client)

    with pytest.raises(ValueError, match="lock_seconds must be an int >= 3"):

        @bus.consumer(endpoint_id="ep", type_id="SongRequested", lock_seconds=2)
        def handle(ctx):  # pragma: no cover - registration fails first
            pass


def test_lock_is_renewed_while_a_long_handler_runs(db):
    client, name = db
    inbox = client[name]["bus_inbox"]
    bus = MongoBus(uri="", database=name, client=client)
    competing_bus = MongoBus(uri="", database=name, client=client)
    bus.bind("SongRequested", endpoint_id="ep")
    observed = {}

    @bus.consumer(endpoint_id="ep", type_id="SongRequested", lock_seconds=3)
    def handle(ctx):
        time.sleep(4.5)  # outlives the original 3 s lock; only renewal keeps the message ours
        observed["competitor_took_message"] = competing_bus.run_once("ep")
        current = inbox.find_one({"_id": ctx.raw["_id"]})
        observed["lock_still_ours"] = current["LockOwner"] == ctx.raw["LockOwner"]
        observed["locked_until_in_future"] = current["LockedUntilUtc"] > utc_now_naive()
        observed["lock_lost"] = ctx.lock_lost

    @competing_bus.consumer(endpoint_id="ep", type_id="SongRequested")
    def compete(ctx):  # pragma: no cover - must never receive the locked message
        pass

    bus.publish("SongRequested", {"songId": "s-1"})
    assert bus.run_once("ep") is True

    assert observed == {
        "competitor_took_message": False,
        "lock_still_ours": True,
        "locked_until_in_future": True,
        "lock_lost": False,
    }
    assert inbox.find_one({})["Status"] == "Processed"


def test_handler_learns_its_lock_was_lost_and_its_success_is_discarded(db):
    client, name = db
    inbox = client[name]["bus_inbox"]
    bus = MongoBus(uri="", database=name, client=client)
    bus.bind("SongRequested", endpoint_id="ep")
    observed = {}

    @bus.consumer(endpoint_id="ep", type_id="SongRequested", lock_seconds=3)
    def handle(ctx):
        inbox.update_one({"_id": ctx.raw["_id"]}, {"$set": {"LockOwner": "another-pump"}})
        observed["lock_lost"] = wait_until(lambda: ctx.lock_lost, timeout_seconds=5)

    bus.publish("SongRequested", {"songId": "s-1"})
    bus.run_once("ep")

    doc = inbox.find_one({})
    assert observed["lock_lost"] is True
    assert doc["Status"] == "Pending"
    assert doc["LockOwner"] == "another-pump"
    assert doc["Attempt"] == 0


def test_failure_of_a_handler_that_lost_its_lock_is_discarded(db):
    client, name = db
    inbox = client[name]["bus_inbox"]
    bus = MongoBus(uri="", database=name, client=client)
    bus.bind("SongRequested", endpoint_id="ep")

    @bus.consumer(endpoint_id="ep", type_id="SongRequested", lock_seconds=3, max_attempts=1)
    def handle(ctx):
        inbox.update_one({"_id": ctx.raw["_id"]}, {"$set": {"LockOwner": "another-pump"}})
        wait_until(lambda: ctx.lock_lost, timeout_seconds=5)
        raise RuntimeError("stale worker failed")

    bus.publish("SongRequested", {"songId": "s-1"})
    bus.run_once("ep")

    doc = inbox.find_one({})
    assert doc["Status"] == "Pending"
    assert doc["LockOwner"] == "another-pump"
    assert doc["Attempt"] == 0
    assert doc["LastError"] != "stale worker failed"
