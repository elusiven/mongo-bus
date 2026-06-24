import json
from datetime import datetime, timezone

from mongobus import envelope


def test_new_event_id_is_32_char_dashless_hex():
    eid = envelope.new_event_id()
    assert len(eid) == 32
    assert "-" not in eid
    int(eid, 16)  # parses as hex


def test_build_envelope_uses_dotnet_key_spellings():
    env = envelope.build_envelope(
        type_id="OrderPlaced",
        data={"orderId": "123"},
        source="urn:python:orders",
        event_id="abc",
        time_utc=datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc),
    )
    assert env["specVersion"] == "1.0"
    assert env["id"] == "abc"
    assert env["type"] == "OrderPlaced"
    assert env["source"] == "urn:python:orders"
    assert env["data"] == {"orderId": "123"}
    assert env["time"] == "2026-06-24T12:00:00+00:00"


def test_build_envelope_omits_null_keys():
    env = envelope.build_envelope(
        type_id="OrderPlaced",
        data={},
        source="s",
        event_id="abc",
        time_utc=datetime(2026, 6, 24, tzinfo=timezone.utc),
    )
    for absent in ("subject", "dataContentType", "correlationId",
                   "causationId", "traceParent", "traceState"):
        assert absent not in env


def test_build_envelope_includes_optional_keys_when_present():
    env = envelope.build_envelope(
        type_id="OrderPlaced",
        data={},
        source="s",
        event_id="abc",
        time_utc=datetime(2026, 6, 24, tzinfo=timezone.utc),
        subject="sub",
        data_content_type="application/json",
        correlation_id="corr",
        causation_id="cause",
        trace_parent="tp",
        trace_state="ts",
    )
    assert env["subject"] == "sub"
    assert env["dataContentType"] == "application/json"
    assert env["correlationId"] == "corr"
    assert env["causationId"] == "cause"
    assert env["traceParent"] == "tp"
    assert env["traceState"] == "ts"


def test_serialize_envelope_round_trips():
    env = envelope.build_envelope(
        type_id="OrderPlaced",
        data={"orderId": "123"},
        source="s",
        event_id="abc",
        time_utc=datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc),
    )
    text = envelope.serialize_envelope(env)
    assert json.loads(text)["data"]["orderId"] == "123"


import pytest

from mongobus.errors import ClaimCheckNotSupportedError


def test_parse_envelope_reads_camelcase_keys():
    text = '{"specVersion":"1.0","id":"abc","type":"OrderPlaced","data":{"orderId":"123"}}'
    env = envelope.parse_envelope(text)
    assert env["id"] == "abc"
    assert env["data"]["orderId"] == "123"


def test_parse_envelope_rejects_claim_check_payloads():
    text = '{"id":"abc","dataContentType":"application/vnd.mongobus.claim-check+json","data":{}}'
    with pytest.raises(ClaimCheckNotSupportedError):
        envelope.parse_envelope(text)
