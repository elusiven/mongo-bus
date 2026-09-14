import asyncio
from datetime import datetime, timezone

from pymongo.errors import PyMongoError

from .. import dispatch, queries
from ..context import LockStatus


class AsyncLockRenewer:
    """Extends one delivery's inbox lock on a background task while its handler awaits."""

    def __init__(self, inbox, *, message_id, pump_id: str, lock_seconds: int, lock_status: LockStatus):
        self._inbox = inbox
        self._message_id = message_id
        self._pump_id = pump_id
        self._lock_seconds = lock_seconds
        self._lock_status = lock_status
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def __aenter__(self) -> "AsyncLockRenewer":
        self._task = asyncio.create_task(self._renew_until_stopped())
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        self._stop.set()
        await self._task

    async def renew_once(self) -> bool:
        try:
            result = await self._inbox.update_one(
                queries.renew_lock_filter(message_id=self._message_id, pump_id=self._pump_id),
                queries.renew_lock_update(now=datetime.now(timezone.utc), lock_seconds=self._lock_seconds),
            )
        except PyMongoError:
            return True  # transient driver error: the lock may still be ours, try again next interval
        if result.matched_count == 0:
            self._lock_status.mark_lost()
            return False
        return True

    async def _renew_until_stopped(self) -> None:
        interval = dispatch.renewal_interval_seconds(self._lock_seconds)
        while not await self._stopped_within(interval):
            if not await self.renew_once():
                return

    async def _stopped_within(self, seconds: float) -> bool:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return False
        return True
