from datetime import datetime

from .constants import STATUS_PENDING


def build_inbox_document(
    *,
    endpoint_id: str,
    topic: str,
    type_id: str,
    payload_json: str,
    cloud_event_id: str,
    created_utc: datetime,
    visible_utc: datetime,
    correlation_id: str | None,
    causation_id: str | None,
) -> dict:
    return {
        "EndpointId": endpoint_id,
        "Topic": topic,
        "TypeId": type_id,
        "PayloadJson": payload_json,
        "CreatedUtc": created_utc,
        "VisibleUtc": visible_utc,
        "LockedUntilUtc": None,
        "LockOwner": None,
        "Attempt": 0,
        "Status": STATUS_PENDING,
        "ProcessedUtc": None,
        "LastError": None,
        "CorrelationId": correlation_id,
        "CausationId": causation_id,
        "CloudEventId": cloud_event_id,
    }
