import threading
from datetime import datetime, timezone

from pymongo.errors import PyMongoError

from .. import dispatch, queries
from ..context import LockStatus


class LockRenewer:
    """Extends one delivery's inbox lock on a background thread while its handler runs."""

    def __init__(self, inbox, *, message_id, pump_id: str, lock_seconds: int, lock_status: LockStatus):
        self._inbox = inbox
        self._message_id = message_id
        self._pump_id = pump_id
        self._lock_seconds = lock_seconds
        self._lock_status = lock_status
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._renew_until_stopped, name="mongobus-lock-renewer", daemon=True
        )

    def __enter__(self) -> "LockRenewer":
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self._stop.set()
        self._thread.join()

    def renew_once(self) -> bool:
        try:
            result = self._inbox.update_one(
                queries.renew_lock_filter(message_id=self._message_id, pump_id=self._pump_id),
                queries.renew_lock_update(now=datetime.now(timezone.utc), lock_seconds=self._lock_seconds),
            )
            matched = result.matched_count
        except PyMongoError:
            return True  # transient driver error: the lock may still be ours, try again next interval
        if matched == 0:
            self._lock_status.mark_lost()
            return False
        return True

    def _renew_until_stopped(self) -> None:
        interval = dispatch.renewal_interval_seconds(self._lock_seconds)
        while not self._stop.wait(interval):
            if not self.renew_once():
                return
