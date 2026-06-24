import json

import boto3
import pytest
from moto import mock_aws
from pymongo import MongoClient
from testcontainers.mongodb import MongoDbContainer

from mongobus import MongoBus
from mongobus.claimcheck.config import ClaimCheckConfig
from mongobus.claimcheck.s3 import S3ClaimCheckProvider

BUCKET = "cc-e2e"


@pytest.fixture(scope="module")
def mongo():
    with MongoDbContainer("mongo:7") as container:
        yield container.get_connection_url()


@pytest.fixture
def db(mongo):
    client = MongoClient(mongo)
    name = "s3_e2e"
    client.drop_database(name)
    yield client, name
    client.drop_database(name)
    client.close()


def test_s3_claim_check_end_to_end(db):
    client, name = db
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        provider = S3ClaimCheckProvider(bucket=BUCKET, client=s3)
        cc = ClaimCheckConfig(provider=provider, enabled=True, threshold_bytes=128)
        bus = MongoBus(uri="", database=name, client=client, claim_check=cc)
        bus.bind("Big", endpoint_id="ep")
        received = []

        @bus.consumer(endpoint_id="ep", type_id="Big")
        def handle(ctx):
            received.append(ctx.data["payload"])

        big = "x" * 5000
        bus.publish("Big", {"payload": big})

        env = json.loads(client[name]["bus_inbox"].find_one({})["PayloadJson"])
        assert env["data"]["provider"] == "s3"
        assert env["data"]["container"] == BUCKET

        assert bus.run_once("ep") is True
        assert received == [big]
