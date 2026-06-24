import json
import uuid
from datetime import datetime, timezone

from .constants import SPEC_VERSION, CLAIM_CHECK_CONTENT_TYPE
from .errors import ClaimCheckNotSupportedError


def new_event_id() -> str:
    return uuid.uuid4().hex


def _format_time(time_utc: datetime) -> str:
    if time_utc.tzinfo is None:
        raise ValueError("time_utc must be timezone-aware (UTC)")
    return time_utc.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


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
        "time": _format_time(time_utc),
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


def parse_envelope(payload_json: str) -> dict:
    envelope = json.loads(payload_json)
    if envelope.get("dataContentType") == CLAIM_CHECK_CONTENT_TYPE:
        raise ClaimCheckNotSupportedError(
            "Received a claim-check payload; the Python client does not support claim-check yet."
        )
    return envelope
