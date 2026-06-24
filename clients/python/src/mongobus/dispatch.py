import socket
import uuid
from datetime import datetime, timedelta

from . import queries, retry


def build_pump_id(endpoint_id: str) -> str:
    return f"{socket.gethostname()}:{uuid.uuid4().hex}:{endpoint_id}"


def plan_failure(*, attempt: int, max_attempts: int, now: datetime, error: str) -> dict:
    next_attempt = attempt + 1
    if retry.should_dead_letter(next_attempt=next_attempt, max_attempts=max_attempts):
        return queries.dead_update(next_attempt=next_attempt, last_error=error)
    visible = now + timedelta(seconds=retry.backoff_seconds(next_attempt))
    return queries.retry_update(
        now=now, next_attempt=next_attempt, visible_utc=visible, last_error=error
    )
