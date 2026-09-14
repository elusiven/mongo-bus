from datetime import datetime, timezone


def utc_now_naive() -> datetime:
    """Current UTC time without tzinfo, comparable to datetimes read back from MongoDB."""
    return datetime.now(timezone.utc).replace(tzinfo=None)
