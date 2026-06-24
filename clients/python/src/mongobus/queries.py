from datetime import datetime, timedelta

from .constants import STATUS_PENDING, STATUS_PROCESSED, STATUS_DEAD


def lock_filter(*, endpoint_id: str, now: datetime, type_ids: list[str] | None) -> dict:
    f: dict = {
        "EndpointId": endpoint_id,
        "Status": STATUS_PENDING,
        "VisibleUtc": {"$lt": now},
        "$or": [{"LockedUntilUtc": None}, {"LockedUntilUtc": {"$lt": now}}],
    }
    if type_ids:
        f["TypeId"] = {"$in": list(type_ids)}
    return f


def lock_update(*, now: datetime, lock_seconds: int, pump_id: str) -> dict:
    return {
        "$set": {
            "LockedUntilUtc": now + timedelta(seconds=lock_seconds),
            "LockOwner": pump_id,
        }
    }


def dedup_filter(*, endpoint_id: str, cloud_event_id: str, exclude_id) -> dict:
    return {
        "EndpointId": endpoint_id,
        "CloudEventId": cloud_event_id,
        "Status": STATUS_PROCESSED,
        "_id": {"$ne": exclude_id},
    }


def processed_update(*, now: datetime, last_error: str | None = None) -> dict:
    fields = {
        "Status": STATUS_PROCESSED,
        "ProcessedUtc": now,
        "LockOwner": None,
        "LockedUntilUtc": None,
    }
    if last_error is not None:
        fields["LastError"] = last_error
    return {"$set": fields}


def retry_update(*, now: datetime, next_attempt: int, visible_utc: datetime, last_error: str) -> dict:
    return {
        "$set": {
            "Status": STATUS_PENDING,
            "Attempt": next_attempt,
            "VisibleUtc": visible_utc,
            "LastError": last_error,
            "LockOwner": None,
            "LockedUntilUtc": None,
        }
    }


def dead_update(*, next_attempt: int, last_error: str) -> dict:
    return {
        "$set": {
            "Status": STATUS_DEAD,
            "Attempt": next_attempt,
            "LastError": last_error,
            "LockOwner": None,
            "LockedUntilUtc": None,
        }
    }


def binding_filter(*, topic: str, endpoint_id: str) -> dict:
    return {"Topic": topic, "EndpointId": endpoint_id}


def binding_set_on_insert(*, topic: str, endpoint_id: str) -> dict:
    return {"$setOnInsert": {"Topic": topic, "EndpointId": endpoint_id}}


def bindings_for_topic_filter(*, topic: str) -> dict:
    return {"Topic": topic}
