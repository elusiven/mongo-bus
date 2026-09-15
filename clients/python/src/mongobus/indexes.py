"""Index specifications mirroring the .NET MongoBusIndexesHostedService.

Pure: each spec is a ``(keys, options)`` pair that a caller passes straight to
``create_index(keys, **options)``. No MongoDB I/O happens here.
"""

from datetime import timedelta

from pymongo import ASCENDING

DEFAULT_PROCESSED_MESSAGE_TTL = timedelta(days=7)
INDEX_OPTIONS_CONFLICT = 85

IndexSpec = tuple[list[tuple[str, int]], dict]


def inbox_index_specs(*, processed_message_ttl: timedelta | None) -> list[IndexSpec]:
    """Inbox indexes mirroring .NET BuildInboxIndexes.

    The lock and dedup indexes are always returned. The TTL retention index on
    ``ProcessedUtc`` is included only when ``processed_message_ttl`` is set, because
    it deletes processed inbox documents after the window. Pending and dead-lettered
    documents have no ``ProcessedUtc``, so MongoDB never expires them.
    """
    specs: list[IndexSpec] = [
        (
            [
                ("EndpointId", ASCENDING),
                ("Status", ASCENDING),
                ("VisibleUtc", ASCENDING),
                ("LockedUntilUtc", ASCENDING),
            ],
            {},
        ),
        ([("EndpointId", ASCENDING), ("CloudEventId", ASCENDING)], {}),
    ]
    if processed_message_ttl is not None:
        specs.append(
            (
                [("ProcessedUtc", ASCENDING)],
                {"expireAfterSeconds": int(processed_message_ttl.total_seconds())},
            )
        )
    return specs


def is_legacy_retention_index(index_info: dict) -> bool:
    """Whether an index is the ``CreatedUtc`` TTL index earlier versions created.

    It expired inbox documents by age, deleting messages that were never processed.
    """
    return "expireAfterSeconds" in index_info and list(index_info["key"]) == [("CreatedUtc", ASCENDING)]


def is_retention_window_conflict(error_code: int | None, options: dict) -> bool:
    """Whether a failed ``create_index`` only disagrees with the existing TTL window."""
    return error_code == INDEX_OPTIONS_CONFLICT and "expireAfterSeconds" in options


def retention_window_update(spec: IndexSpec) -> dict:
    """The ``collMod`` ``index`` argument that changes an existing TTL window in place."""
    keys, options = spec
    return {"keyPattern": dict(keys), "expireAfterSeconds": options["expireAfterSeconds"]}


def bindings_index_spec() -> IndexSpec:
    """Unique binding index mirroring .NET BuildBindingIndex."""
    return ([("Topic", ASCENDING), ("EndpointId", ASCENDING)], {"unique": True})
