from datetime import datetime, timedelta, timezone

from mongobus import dispatch

NOW = datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc)


def test_pump_id_format_has_three_colon_parts():
    pid = dispatch.build_pump_id("order-processor")
    parts = pid.split(":")
    assert len(parts) == 3
    assert parts[2] == "order-processor"
    assert len(parts[1]) == 32  # uuid hex


def test_plan_failure_retries_below_max():
    update = dispatch.plan_failure(attempt=2, max_attempts=10, now=NOW, error="boom")
    assert update["$set"]["Status"] == "Pending"
    assert update["$set"]["Attempt"] == 3
    assert update["$set"]["VisibleUtc"] == NOW + timedelta(seconds=8)  # 2**3
    assert update["$set"]["LastError"] == "boom"


def test_plan_failure_dead_letters_at_max():
    update = dispatch.plan_failure(attempt=9, max_attempts=10, now=NOW, error="boom")
    assert update["$set"]["Status"] == "Dead"
    assert update["$set"]["Attempt"] == 10
