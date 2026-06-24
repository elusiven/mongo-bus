import json
from datetime import datetime, timezone
from pathlib import Path

from mongobus import envelope, documents

FIXTURES = Path(__file__).parent / "fixtures"


def test_python_parses_dotnet_envelope():
    text = (FIXTURES / "dotnet_envelope.json").read_text()
    env = envelope.parse_envelope(text)
    assert env["specVersion"] == "1.0"
    assert env["id"] == "9f86d081884c7d659a2feaa0c55ad015"
    assert env["data"]["orderId"] == "123"


def test_python_envelope_keys_match_dotnet_set():
    golden = json.loads((FIXTURES / "dotnet_envelope.json").read_text())
    env = envelope.build_envelope(
        type_id="OrderPlaced",
        data={"orderId": "123", "total": 50},
        source="urn:mongobus:unknown",
        event_id="9f86d081884c7d659a2feaa0c55ad015",
        time_utc=datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc),
        correlation_id="11112222333344445555666677778888",
    )
    assert set(env.keys()) == set(golden.keys())
    assert env["time"].endswith("Z"), f"time must end with 'Z', got: {env['time']!r}"
    assert "+00:00" not in env["time"], f"time must not contain '+00:00', got: {env['time']!r}"


def test_inbox_document_field_set_matches_dotnet():
    expected = set(json.loads((FIXTURES / "dotnet_inbox_fields.json").read_text()))
    now = datetime(2026, 6, 24, tzinfo=timezone.utc)
    doc = documents.build_inbox_document(
        endpoint_id="ep", topic="T", type_id="T", payload_json="{}",
        cloud_event_id="ce", created_utc=now, visible_utc=now,
        correlation_id=None, causation_id=None,
    )
    assert set(doc.keys()) == expected
