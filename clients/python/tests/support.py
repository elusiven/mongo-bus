import asyncio
import time
from datetime import datetime, timezone
from typing import Callable


def utc_now_naive() -> datetime:
    """Current UTC time without tzinfo, comparable to datetimes read back from MongoDB."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def wait_until(condition: Callable[[], bool], *, timeout_seconds: float) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.05)
    return condition()


async def async_wait_until(condition: Callable[[], bool], *, timeout_seconds: float) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    while loop.time() < deadline:
        if condition():
            return True
        await asyncio.sleep(0.05)
    return condition()
