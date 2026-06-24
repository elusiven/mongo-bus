import json
import uuid
from datetime import datetime

from .constants import SPEC_VERSION


def new_event_id() -> str:
    return uuid.uuid4().hex


def build_envelope(
    *,
    type_id: str,
    data,
    source: str,
    event_id: str,
    time_utc: datetime,
    subject: str | None = None,
    data_content_type: str | None = None,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    trace_parent: str | None = None,
    trace_state: str | None = None,
) -> dict:
    envelope: dict = {
        "specVersion": SPEC_VERSION,
        "id": event_id,
        "type": type_id,
        "source": source,
        "time": time_utc.isoformat(),
    }
    optional = {
        "subject": subject,
        "dataContentType": data_content_type,
        "correlationId": correlation_id,
        "causationId": causation_id,
        "traceParent": trace_parent,
        "traceState": trace_state,
    }
    for key, value in optional.items():
        if value is not None:
            envelope[key] = value
    envelope["data"] = data
    return envelope


def serialize_envelope(envelope: dict) -> str:
    return json.dumps(envelope)
