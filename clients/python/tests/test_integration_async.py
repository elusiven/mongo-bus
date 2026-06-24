import json

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
