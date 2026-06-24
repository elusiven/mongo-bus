import json

import pytest
from pymongo import MongoClient
from testcontainers.mongodb import MongoDbContainer

from mongobus import MongoBus


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
