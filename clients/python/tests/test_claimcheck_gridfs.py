import pytest
from pymongo import AsyncMongoClient, MongoClient
from testcontainers.mongodb import MongoDbContainer

from mongobus.claimcheck.gridfs import AsyncGridFsClaimCheckProvider, GridFsClaimCheckProvider


@pytest.fixture(scope="module")
def mongo():
    with MongoDbContainer("mongo:7") as container:
        yield container.get_connection_url()


def test_gridfs_put_and_open_read_round_trip(mongo):
    client = MongoClient(mongo)
    db = client["cc_sync"]
    client.drop_database("cc_sync")
    provider = GridFsClaimCheckProvider(db)

    ref = provider.put(b"hello-blob", content_type="application/json", metadata={"k": "v"})

    assert ref.provider == "gridfs"
    assert ref.container == "claimcheck"
    assert len(ref.key) == 32
    assert ref.length == len(b"hello-blob")
    assert provider.open_read(ref) == b"hello-blob"
    # stored under the claimcheck bucket with filename == key
    stored = db["claimcheck.files"].find_one({"filename": ref.key})
    assert stored is not None
    assert stored["metadata"]["contentType"] == "application/json"
    assert stored["metadata"]["k"] == "v"
    client.drop_database("cc_sync")
    client.close()


async def test_async_gridfs_put_and_open_read_round_trip(mongo):
    client = AsyncMongoClient(mongo)
    db = client["cc_async"]
    await client.drop_database("cc_async")
    provider = AsyncGridFsClaimCheckProvider(db)

    ref = await provider.put(b"hello-async", content_type="application/json", metadata=None)

    assert ref.provider == "gridfs"
    assert ref.key and len(ref.key) == 32
    assert await provider.open_read(ref) == b"hello-async"
    # stored under the claimcheck bucket with filename == key
    stored = await db["claimcheck.files"].find_one({"filename": ref.key})
    assert stored is not None
    assert stored["metadata"]["contentType"] == "application/json"
    await client.drop_database("cc_async")
    await client.close()


async def test_async_gridfs_omits_content_type_when_none(mongo):
    client = AsyncMongoClient(mongo)
    db = client["cc_async_none"]
    await client.drop_database("cc_async_none")
    provider = AsyncGridFsClaimCheckProvider(db)

    ref = await provider.put(b"x", content_type=None, metadata=None)

    assert ref.provider == "gridfs"
    assert ref.key and len(ref.key) == 32
    # stored under the claimcheck bucket; metadata should not contain contentType
    stored = await db["claimcheck.files"].find_one({"filename": ref.key})
    assert stored is not None
    assert "contentType" not in (stored.get("metadata") or {})
    await client.drop_database("cc_async_none")
    await client.close()
