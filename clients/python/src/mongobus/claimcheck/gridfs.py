from gridfs import GridFSBucket
from gridfs.asynchronous import AsyncGridFSBucket

from . import core


def _metadata_doc(content_type, metadata):
    doc = dict(metadata or {})
    if content_type is not None:
        doc["contentType"] = content_type
    return doc


class GridFsClaimCheckProvider:
    name = "gridfs"

    def __init__(self, database, *, bucket_name: str = "claimcheck"):
        self._bucket_name = bucket_name
        self._bucket = GridFSBucket(database, bucket_name=bucket_name)

    def put(self, data: bytes, *, content_type, metadata) -> core.ClaimCheckReference:
        key = core.new_blob_key()
        self._bucket.upload_from_stream(key, data, metadata=_metadata_doc(content_type, metadata))
        return core.ClaimCheckReference(
            provider="gridfs",
            container=self._bucket_name,
            key=key,
            length=len(data),
            content_type=content_type,
            metadata=metadata,
        )

    def open_read(self, reference: core.ClaimCheckReference) -> bytes:
        with self._bucket.open_download_stream_by_name(reference.key) as stream:
            return stream.read()


class AsyncGridFsClaimCheckProvider:
    name = "gridfs"

    def __init__(self, database, *, bucket_name: str = "claimcheck"):
        self._bucket_name = bucket_name
        self._bucket = AsyncGridFSBucket(database, bucket_name=bucket_name)

    async def put(self, data: bytes, *, content_type, metadata) -> core.ClaimCheckReference:
        key = core.new_blob_key()
        await self._bucket.upload_from_stream(key, data, metadata=_metadata_doc(content_type, metadata))
        return core.ClaimCheckReference(
            provider="gridfs",
            container=self._bucket_name,
            key=key,
            length=len(data),
            content_type=content_type,
            metadata=metadata,
        )

    async def open_read(self, reference: core.ClaimCheckReference) -> bytes:
        stream = await self._bucket.open_download_stream_by_name(reference.key)
        try:
            return await stream.read()
        finally:
            await stream.close()
