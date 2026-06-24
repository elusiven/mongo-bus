import asyncio

from . import core


def _make_client(client, client_kwargs):
    if client is not None:
        return client
    import boto3  # lazy: boto3 is an optional [s3] dependency

    return boto3.client("s3", **client_kwargs)


class S3ClaimCheckProvider:
    def __init__(self, *, bucket, client=None, key_prefix="", provider_name="s3", **client_kwargs):
        self.name = provider_name
        self._bucket = bucket
        self._key_prefix = key_prefix
        self._client = _make_client(client, client_kwargs)

    def put(self, data: bytes, *, content_type, metadata) -> core.ClaimCheckReference:
        key = self._key_prefix + core.new_blob_key()
        self._client.put_object(
            Bucket=self._bucket, Key=key, Body=data,
            ContentType=content_type or "application/octet-stream",
            Metadata=metadata or {},
        )
        return core.ClaimCheckReference(
            provider=self.name, container=self._bucket, key=key, length=len(data),
            content_type=content_type, metadata=metadata,
        )

    def open_read(self, reference: core.ClaimCheckReference) -> bytes:
        response = self._client.get_object(Bucket=self._bucket, Key=reference.key)
        return response["Body"].read()


class AsyncS3ClaimCheckProvider:
    def __init__(self, *, bucket, client=None, key_prefix="", provider_name="s3", **client_kwargs):
        self._inner = S3ClaimCheckProvider(
            bucket=bucket, client=client, key_prefix=key_prefix,
            provider_name=provider_name, **client_kwargs,
        )
        self.name = self._inner.name

    async def put(self, data: bytes, *, content_type, metadata) -> core.ClaimCheckReference:
        return await asyncio.to_thread(
            self._inner.put, data, content_type=content_type, metadata=metadata
        )

    async def open_read(self, reference: core.ClaimCheckReference) -> bytes:
        return await asyncio.to_thread(self._inner.open_read, reference)
