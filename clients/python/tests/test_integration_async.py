import asyncio

import pytest
from pymongo import AsyncMongoClient
from testcontainers.mongodb import MongoDbContainer

from mongobus import AsyncMongoBus


@pytest.fixture(scope="module")
def mongo():
    with MongoDbContainer("mongo:7") as container:
        yield container.get_connection_url()


@pytest.fixture
async def db(mongo):
    client = AsyncMongoClient(mongo)
    name = "testdb_async"
    await client.drop_database(name)
    yield client, name
    await client.drop_database(name)


async def test_async_publish_fans_out(db):
    client, name = db
    bus = AsyncMongoBus(uri="", database=name, client=client)
    await bus.bind("OrderPlaced", endpoint_id="ep-a")
    await bus.bind("OrderPlaced", endpoint_id="ep-b")
    created = await bus.publish("OrderPlaced", {"orderId": "1"})
    assert created == 2
    assert await client[name]["bus_inbox"].count_documents({}) == 2


async def test_async_consume_processes_message(db):
    client, name = db
    bus = AsyncMongoBus(uri="", database=name, client=client)
    await bus.bind("OrderPlaced", endpoint_id="ep")
    received = []

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced")
    async def handle(ctx):
        received.append(ctx.data["orderId"])

    await bus.publish("OrderPlaced", {"orderId": "42"})
    handled = await bus.run_once("ep")

    assert handled is True
    assert received == ["42"]
    doc = await client[name]["bus_inbox"].find_one({})
    assert doc["Status"] == "Processed"


async def test_async_consume_dead_letters(db):
    client, name = db
    bus = AsyncMongoBus(uri="", database=name, client=client)
    await bus.bind("OrderPlaced", endpoint_id="ep")

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced", max_attempts=1)
    async def handle(ctx):
        raise RuntimeError("boom")

    await bus.publish("OrderPlaced", {"orderId": "1"})
    await bus.run_once("ep")

    doc = await client[name]["bus_inbox"].find_one({})
    assert doc["Status"] == "Dead"
    assert doc["Attempt"] == 1


async def test_async_consume_runs_sync_handler(db):
    client, name = db
    bus = AsyncMongoBus(uri="", database=name, client=client)
    await bus.bind("OrderPlaced", endpoint_id="ep")
    received = []

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced")
    def handle(ctx):
        received.append(ctx.data["orderId"])

    await bus.publish("OrderPlaced", {"orderId": "99"})
    handled = await bus.run_once("ep")

    assert handled is True
    assert received == ["99"]
    doc = await client[name]["bus_inbox"].find_one({})
    assert doc["Status"] == "Processed"


async def test_async_run_once_returns_false_when_no_messages(db):
    # Covers the async process_one return-False path (empty inbox → doc is None).
    client, name = db
    bus = AsyncMongoBus(uri="", database=name, client=client)
    await bus.bind("OrderPlaced", endpoint_id="ep")

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced")
    async def handle(ctx):  # pragma: no cover
        pass

    result = await bus.run_once("ep")  # inbox is empty
    assert result is False


async def test_async_run_once_returns_false_when_endpoint_not_matched(db):
    # Covers the branch in async run_once where consumer.endpoint_id != requested endpoint_id.
    client, name = db
    bus = AsyncMongoBus(uri="", database=name, client=client)
    await bus.bind("OrderPlaced", endpoint_id="ep-a")

    @bus.consumer(endpoint_id="ep-a", type_id="OrderPlaced")
    async def handle(ctx):  # pragma: no cover
        pass

    await bus.publish("OrderPlaced", {"orderId": "1"})
    result = await bus.run_once("ep-b")  # "ep-b" doesn't match "ep-a"
    assert result is False


async def test_async_idempotency_skips_duplicate_cloud_event(db):
    # Covers the idempotency path in the async pump (lines 27-34 of _async/pump.py).
    client, name = db
    bus = AsyncMongoBus(uri="", database=name, client=client)
    await bus.bind("OrderPlaced", endpoint_id="ep")
    calls = []

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced", idempotent=True)
    async def handle(ctx):
        calls.append(1)

    await bus.publish("OrderPlaced", {"orderId": "1"}, id="async-dup-ce")
    first = await client[name]["bus_inbox"].find_one({})
    await client[name]["bus_inbox"].insert_one({**{k: v for k, v in first.items() if k != "_id"}})

    await bus.run_once("ep")  # processes the first
    await bus.run_once("ep")  # second is skipped by idempotency

    assert sum(calls) == 1
    skipped = await client[name]["bus_inbox"].find_one({"LastError": "Skipped due to idempotency"})
    assert skipped is not None


async def test_async_run_loop_processes_message_then_stops(mongo):
    # Covers the async run() loop body.
    client = AsyncMongoClient(mongo)
    name = "testdb_async_run_loop"
    await client.drop_database(name)

    bus = AsyncMongoBus(uri="", database=name, client=client)
    await bus.bind("OrderPlaced", endpoint_id="ep")
    received = []
    stop = asyncio.Event()

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced")
    async def handle(ctx):
        received.append(ctx.data["orderId"])
        stop.set()

    await bus.publish("OrderPlaced", {"orderId": "async-run-loop"})

    run_task = asyncio.create_task(bus.run(stop_event=stop))
    await asyncio.wait_for(run_task, timeout=15)

    assert received == ["async-run-loop"]

    await client.drop_database(name)
    await client.close()


async def test_async_run_exits_immediately_when_stop_event_already_set(mongo):
    # Covers the while-condition branch: stop_event already set → loop body never executes.
    client = AsyncMongoClient(mongo)
    name = "testdb_async_already_stopped"
    await client.drop_database(name)

    bus = AsyncMongoBus(uri="", database=name, client=client)
    stop = asyncio.Event()
    stop.set()

    await asyncio.wait_for(bus.run(stop_event=stop), timeout=5)

    await client.drop_database(name)
    await client.close()


async def test_async_construct_bus_from_uri_without_client(mongo):
    # Covers the client=None construction branch in AsyncMongoBus.__init__.
    name = "testdb_async_uri_ctor"
    bus = AsyncMongoBus(uri=mongo, database=name)
    await bus.bind("OrderPlaced", endpoint_id="ep")
    created = await bus.publish("OrderPlaced", {"orderId": "uri-ctor"})
    assert created == 1
    await bus._client.drop_database(name)
    await bus._client.close()


async def test_async_publish_without_bindings_returns_zero(db):
    # Covers the async publish early-return-0 branch (line 46 in _async/bus.py).
    client, name = db
    bus = AsyncMongoBus(uri="", database=name, client=client)
    created = await bus.publish("Unsubscribed", {"x": 1})
    assert created == 0
    assert await client[name]["bus_inbox"].count_documents({}) == 0


async def test_async_run_loop_sleeps_when_no_work_then_stops(mongo):
    # Covers the asyncio.sleep branch in run() when no messages are available.
    client = AsyncMongoClient(mongo)
    name = "testdb_async_run_sleep"
    await client.drop_database(name)

    bus = AsyncMongoBus(uri="", database=name, client=client)
    stop = asyncio.Event()

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced")
    async def handle(ctx):  # pragma: no cover
        pass

    # Schedule stop shortly after run() starts — ensures at least one idle iteration (sleep path).
    async def stop_soon():
        await asyncio.sleep(0.05)
        stop.set()

    stop_task = asyncio.create_task(stop_soon())
    await asyncio.wait_for(bus.run(stop_event=stop), timeout=10)
    await stop_task

    await client.drop_database(name)
    await client.close()


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


def _index_key_lists(info):
    return [list(meta["key"]) for meta in info.values()]


def _ttl_seconds(info):
    return [meta["expireAfterSeconds"] for meta in info.values() if "expireAfterSeconds" in meta]


async def test_async_ensure_indexes_creates_inbox_and_binding_indexes(db):
    client, name = db
    bus = AsyncMongoBus(uri="", database=name, client=client)

    await bus.ensure_indexes()

    inbox = await client[name]["bus_inbox"].index_information()
    keys = _index_key_lists(inbox)
    assert [("EndpointId", 1), ("Status", 1), ("VisibleUtc", 1), ("LockedUntilUtc", 1)] in keys
    assert [("EndpointId", 1), ("CloudEventId", 1)] in keys
    assert [("CreatedUtc", 1)] in keys
    assert _ttl_seconds(inbox) == [7 * 24 * 60 * 60]

    bindings = await client[name]["bus_bindings"].index_information()
    unique = [m for m in bindings.values() if list(m["key"]) == [("Topic", 1), ("EndpointId", 1)]]
    assert unique and unique[0].get("unique") is True


async def test_async_run_once_auto_creates_lock_and_dedup_without_ttl(db):
    client, name = db
    bus = AsyncMongoBus(uri="", database=name, client=client)
    await bus.bind("OrderPlaced", endpoint_id="ep")

    @bus.consumer(endpoint_id="ep", type_id="OrderPlaced")
    async def handle(ctx):
        pass

    await bus.run_once("ep")  # no message; triggers auto-provisioning

    info = await client[name]["bus_inbox"].index_information()
    keys = _index_key_lists(info)
    assert [("EndpointId", 1), ("Status", 1), ("VisibleUtc", 1), ("LockedUntilUtc", 1)] in keys
    assert [("EndpointId", 1), ("CloudEventId", 1)] in keys
    assert _ttl_seconds(info) == []
