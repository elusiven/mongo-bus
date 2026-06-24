import boto3
import pytest
from moto import mock_aws

from mongobus.claimcheck.s3 import AsyncS3ClaimCheckProvider, S3ClaimCheckProvider

REGION = "us-east-1"
BUCKET = "cc-bucket"


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name=REGION)
        client.create_bucket(Bucket=BUCKET)
        yield client


def test_s3_put_and_open_read_round_trip(s3_client):
    provider = S3ClaimCheckProvider(bucket=BUCKET, client=s3_client, key_prefix="cc/")

    ref = provider.put(b"s3-blob", content_type="application/json", metadata={"k": "v"})

    assert ref.provider == "s3"
    assert ref.container == BUCKET
    assert ref.key.startswith("cc/")
    assert ref.length == len(b"s3-blob")
    assert provider.open_read(ref) == b"s3-blob"


async def test_async_s3_put_and_open_read_round_trip(s3_client):
    provider = AsyncS3ClaimCheckProvider(bucket=BUCKET, client=s3_client)

    ref = await provider.put(b"s3-async", content_type="application/json", metadata=None)

    assert ref.provider == "s3"
    assert await provider.open_read(ref) == b"s3-async"


def test_s3_provider_builds_its_own_client_when_none_given():
    with mock_aws():
        boto3.client("s3", region_name=REGION).create_bucket(Bucket=BUCKET)
        provider = S3ClaimCheckProvider(bucket=BUCKET, region_name=REGION)  # no client= -> lazy boto3 path
        ref = provider.put(b"selfclient", content_type="application/json", metadata=None)
        assert provider.open_read(ref) == b"selfclient"
