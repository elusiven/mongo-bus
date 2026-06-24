from dataclasses import dataclass

OBJECT_CONTENT_TYPE = "application/json"

CREATED_AT_KEY = "x-mongobus-created-at"
COMPRESSION_KEY = "x-mongobus-compression"
COMPRESSION_GZIP = "gzip"

DEFAULT_THRESHOLD_BYTES = 256 * 1024
DEFAULT_MAX_DECOMPRESSED_BYTES = 100 * 1024 * 1024


@dataclass(frozen=True)
class ClaimCheckReference:
    provider: str
    container: str
    key: str
    length: int
    content_type: str | None = None
    metadata: dict[str, str] | None = None
    created_at: str | None = None


def reference_to_data(ref: ClaimCheckReference) -> dict:
    data: dict = {
        "provider": ref.provider,
        "container": ref.container,
        "key": ref.key,
        "length": ref.length,
    }
    if ref.content_type is not None:
        data["contentType"] = ref.content_type
    if ref.metadata is not None:
        data["metadata"] = ref.metadata
    if ref.created_at is not None:
        data["createdAt"] = ref.created_at
    return data


def reference_from_data(data: dict) -> ClaimCheckReference:
    return ClaimCheckReference(
        provider=data["provider"],
        container=data["container"],
        key=data["key"],
        length=data["length"],
        content_type=data.get("contentType"),
        metadata=data.get("metadata"),
        created_at=data.get("createdAt"),
    )
