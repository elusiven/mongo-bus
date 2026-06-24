"""Index specifications mirroring the .NET MongoBusIndexesHostedService.

Pure: each spec is a ``(keys, options)`` pair that a caller passes straight to
``create_index(keys, **options)``. No MongoDB I/O happens here.
"""

from datetime import timedelta

from pymongo import ASCENDING

DEFAULT_PROCESSED_MESSAGE_TTL = timedelta(days=7)

IndexSpec = tuple[list[tuple[str, int]], dict]


def inbox_index_specs(*, processed_message_ttl: timedelta | None) -> list[IndexSpec]:
    """Inbox indexes mirroring .NET BuildInboxIndexes.

    The lock and dedup indexes are always returned. The TTL retention index on
    ``CreatedUtc`` is included only when ``processed_message_ttl`` is set, because
    it expires (deletes) inbox documents after the window.
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
                [("CreatedUtc", ASCENDING)],
                {"expireAfterSeconds": int(processed_message_ttl.total_seconds())},
            )
        )
    return specs


def bindings_index_spec() -> IndexSpec:
    """Unique binding index mirroring .NET BuildBindingIndex."""
    return ([("Topic", ASCENDING), ("EndpointId", ASCENDING)], {"unique": True})
