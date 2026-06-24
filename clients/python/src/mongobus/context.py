import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass


@dataclass(frozen=True)
class ConsumeContext:
    data: object
    type_id: str
    cloud_event_id: str
    correlation_id: str | None
    causation_id: str | None
    source: str | None
    subject: str | None
    attempt: int
    envelope: dict
    raw: dict

    @classmethod
    def from_message(cls, envelope: dict, raw: dict) -> "ConsumeContext":
        return cls(
            data=envelope.get("data"),
            type_id=envelope.get("type"),
            cloud_event_id=envelope.get("id"),
            correlation_id=envelope.get("correlationId"),
            causation_id=envelope.get("causationId"),
            source=envelope.get("source"),
            subject=envelope.get("subject"),
            attempt=raw.get("Attempt", 0),
            envelope=envelope,
            raw=raw,
        )


_current: ContextVar[ConsumeContext | None] = ContextVar("mongobus_context", default=None)


def current_context() -> ConsumeContext | None:
    return _current.get()


@contextmanager
def use_context(ctx: ConsumeContext):
    token = _current.set(ctx)
    try:
        yield
    finally:
        _current.reset(token)


def resolve_correlation_id(explicit: str | None) -> str:
    if explicit is not None:
        return explicit
    ctx = _current.get()
    if ctx is not None and ctx.correlation_id is not None:
        return ctx.correlation_id
    return uuid.uuid4().hex


def resolve_causation_id(explicit: str | None) -> str | None:
    if explicit is not None:
        return explicit
    ctx = _current.get()
    return ctx.cloud_event_id if ctx is not None else None
