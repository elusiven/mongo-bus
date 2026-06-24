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
