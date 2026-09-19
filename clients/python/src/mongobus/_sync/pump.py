import json
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Mapping

from pymongo import ASCENDING
from pymongo.collection import ReturnDocument

from .. import constants, context, dispatch, envelope, queries
from ..claimcheck import core as claimcheck_core
from ..errors import ClaimCheckNotSupportedError
from .renewal import LockRenewer


@dataclass
class Consumer:
    endpoint_id: str
    type_id: str
    handler: Callable
    max_attempts: int
    idempotent: bool
    lock_seconds: int = constants.DEFAULT_LOCK_SECONDS

    def __post_init__(self) -> None:
        if type(self.lock_seconds) is not int or self.lock_seconds < constants.MIN_LOCK_SECONDS:
            raise ValueError(
                f"lock_seconds must be an int >= {constants.MIN_LOCK_SECONDS}, got {self.lock_seconds!r}"
            )


@dataclass
class BatchConsumer:
    endpoint_id: str
    type_id: str
    handler: Callable
    max_attempts: int
    idempotent: bool
    batch_size: int
    lock_seconds: int = constants.DEFAULT_LOCK_SECONDS

    def __post_init__(self) -> None:
        if type(self.batch_size) is not int or self.batch_size <= 0:
            raise ValueError(f"batch_size must be a positive int, got {self.batch_size!r}")
        if type(self.lock_seconds) is not int or self.lock_seconds < constants.MIN_LOCK_SECONDS:
            raise ValueError(
                f"lock_seconds must be an int >= {constants.MIN_LOCK_SECONDS}, got {self.lock_seconds!r}"
            )


def process_one(inbox, consumer: Consumer, claim_check=None) -> bool:
    now = datetime.now(timezone.utc)
    pump_id = dispatch.build_pump_id(consumer.endpoint_id)
    doc = inbox.find_one_and_update(
        queries.lock_filter(endpoint_id=consumer.endpoint_id, now=now, type_ids=[consumer.type_id]),
        queries.lock_update(now=now, lock_seconds=consumer.lock_seconds, pump_id=pump_id),
        sort=[("VisibleUtc", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )
    if doc is None:
        return False

    env = _decode_envelope(doc, claim_check)
    ctx = context.ConsumeContext.from_message(env, doc)
    still_owned = queries.owned_message_filter(message_id=doc["_id"], pump_id=pump_id)

    if consumer.idempotent and _already_processed(inbox, ctx, doc["_id"]):
        inbox.update_one(
            still_owned,
            queries.processed_update(
                now=datetime.now(timezone.utc),
                last_error="Skipped due to idempotency",
            ),
        )
        return True

    try:
        with context.use_context(ctx), LockRenewer(
            inbox,
            message_id=doc["_id"],
            pump_id=pump_id,
            lock_seconds=consumer.lock_seconds,
            lock_status=ctx.lock_status,
        ):
            consumer.handler(ctx)
    except Exception as exc:  # noqa: BLE001 - failure is mapped to retry/dead-letter
        inbox.update_one(
            still_owned,
            dispatch.plan_failure(
                attempt=doc["Attempt"],
                max_attempts=consumer.max_attempts,
                now=datetime.now(timezone.utc),
                error=str(exc),
            ),
        )
        return True

    inbox.update_one(
        still_owned,
        queries.processed_update(now=datetime.now(timezone.utc)),
    )
    return True


def process_batch(inbox, consumer: BatchConsumer, claim_check=None) -> bool:
    """Claim a batch and finalize each delivery from its event-id outcome.

    The handler receives public ``ConsumeContext`` objects and must return a mapping
    from cloud-event id to ``None`` for success or an exception for retry/dead-letter.
    MongoDB documents and lock ownership remain internal to the bus.
    """
    pump_id = dispatch.build_pump_id(consumer.endpoint_id)
    active = []
    did_work = False

    while len(active) < consumer.batch_size:
        now = datetime.now(timezone.utc)
        doc = inbox.find_one_and_update(
            queries.lock_filter(
                endpoint_id=consumer.endpoint_id,
                now=now,
                type_ids=[consumer.type_id],
            ),
            queries.lock_update(
                now=now,
                lock_seconds=consumer.lock_seconds,
                pump_id=pump_id,
            ),
            sort=[("VisibleUtc", ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )
        if doc is None:
            break
        did_work = True
        still_owned = queries.owned_message_filter(message_id=doc["_id"], pump_id=pump_id)

        try:
            env = _decode_envelope(doc, claim_check)
            ctx = context.ConsumeContext.from_message(env, doc)
        except Exception as exc:  # noqa: BLE001 - isolate malformed deliveries
            _finalize_failure(inbox, doc, still_owned, consumer, exc)
            continue

        if consumer.idempotent and _already_processed(inbox, ctx, doc["_id"]):
            inbox.update_one(
                still_owned,
                queries.processed_update(
                    now=datetime.now(timezone.utc),
                    last_error="Skipped due to idempotency",
                ),
            )
            continue

        active.append((doc, ctx, still_owned))

    if not active:
        return did_work

    outcomes: Mapping[str, BaseException | None]
    with ExitStack() as stack:
        for doc, ctx, _still_owned in active:
            stack.enter_context(
                LockRenewer(
                    inbox,
                    message_id=doc["_id"],
                    pump_id=pump_id,
                    lock_seconds=consumer.lock_seconds,
                    lock_status=ctx.lock_status,
                )
            )
        try:
            result = consumer.handler([ctx for _doc, ctx, _still_owned in active])
        except Exception as exc:  # noqa: BLE001 - map an uncaught batch failure
            outcomes = {ctx.cloud_event_id: exc for _doc, ctx, _still_owned in active}
        else:
            if not isinstance(result, Mapping):
                error = TypeError("batch handler must return a mapping of cloud-event id outcomes")
                outcomes = {ctx.cloud_event_id: error for _doc, ctx, _still_owned in active}
            else:
                outcomes = result

    for doc, ctx, still_owned in active:
        outcome = outcomes.get(ctx.cloud_event_id, RuntimeError(
            f"batch handler returned no outcome for {ctx.cloud_event_id}"
        ))
        if outcome is None:
            inbox.update_one(
                still_owned,
                queries.processed_update(now=datetime.now(timezone.utc)),
            )
        else:
            _finalize_failure(inbox, doc, still_owned, consumer, outcome)
    return did_work


def _decode_envelope(doc, claim_check):
    env = envelope.parse_envelope(doc["PayloadJson"])
    if claimcheck_core.is_claim_check(env):
        if claim_check is None:
            raise ClaimCheckNotSupportedError(
                "Received a claim-check payload but no claim_check provider is configured."
            )
        reference = claimcheck_core.reference_from_data(env["data"])
        blob = claim_check.provider.open_read(reference)
        if (reference.metadata or {}).get(claimcheck_core.COMPRESSION_KEY) == claimcheck_core.COMPRESSION_GZIP:
            blob = claimcheck_core.gzip_decompress(blob, max_bytes=claim_check.max_decompressed_bytes)
        env = {**env, "data": json.loads(blob)}
    return env


def _finalize_failure(inbox, doc, still_owned, consumer, exc) -> None:
    inbox.update_one(
        still_owned,
        dispatch.plan_failure(
            attempt=doc["Attempt"],
            max_attempts=consumer.max_attempts,
            now=datetime.now(timezone.utc),
            error=str(exc),
        ),
    )


def _already_processed(inbox, ctx: context.ConsumeContext, current_id) -> bool:
    return (
        inbox.count_documents(
            queries.dedup_filter(
                endpoint_id=ctx.raw["EndpointId"],
                cloud_event_id=ctx.cloud_event_id,
                exclude_id=current_id,
            ),
            limit=1,
        )
        > 0
    )
