from datetime import datetime, timezone

from mongobus import documents


def _doc():
    now = datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc)
    return documents.build_inbox_document(
        endpoint_id="order-processor",
        topic="OrderPlaced",
        type_id="OrderPlaced",
        payload_json='{"id":"abc"}',
        cloud_event_id="abc",
        created_utc=now,
        visible_utc=now,
        correlation_id="corr",
        causation_id=None,
    )


def test_inbox_document_uses_pascalcase_keys():
    doc = _doc()
    assert doc["EndpointId"] == "order-processor"
    assert doc["Topic"] == "OrderPlaced"
    assert doc["TypeId"] == "OrderPlaced"
    assert doc["PayloadJson"] == '{"id":"abc"}'
    assert doc["CloudEventId"] == "abc"
    assert doc["CorrelationId"] == "corr"


def test_inbox_document_defaults_match_dotnet():
    doc = _doc()
    assert doc["Attempt"] == 0
    assert doc["Status"] == "Pending"
    assert doc["LockedUntilUtc"] is None
    assert doc["LockOwner"] is None
    assert doc["ProcessedUtc"] is None
    assert doc["LastError"] is None
    assert doc["CausationId"] is None


def test_inbox_document_has_no_id_field():
    assert "_id" not in _doc()
    assert "Id" not in _doc()
