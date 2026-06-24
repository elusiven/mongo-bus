import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from pymongo import ASCENDING
from pymongo.collection import ReturnDocument

from .. import constants, context, dispatch, envelope, queries
from ..claimcheck import core as claimcheck_core
from ..errors import ClaimCheckNotSupportedError


@dataclass
class Consumer:
    endpoint_id: str
    type_id: str
    handler: Callable
    max_attempts: int
    idempotent: bool


def process_one(inbox, consumer: Consumer, claim_check=None) -> bool:
    now = datetime.now(timezone.utc)
    pump_id = dispatch.build_pump_id(consumer.endpoint_id)
    doc = inbox.find_one_and_update(
        queries.lock_filter(endpoint_id=consumer.endpoint_id, now=now, type_ids=[consumer.type_id]),
        queries.lock_update(now=now, lock_seconds=constants.DEFAULT_LOCK_SECONDS, pump_id=pump_id),
        sort=[("VisibleUtc", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )
    if doc is None:
        return False

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
    ctx = context.ConsumeContext.from_message(env, doc)

    if consumer.idempotent and _already_processed(inbox, ctx, doc["_id"]):
        inbox.update_one(
            {"_id": doc["_id"]},
            queries.processed_update(
                now=datetime.now(timezone.utc),
                last_error="Skipped due to idempotency",
            ),
        )
        return True

    try:
        with context.use_context(ctx):
            consumer.handler(ctx)
    except Exception as exc:  # noqa: BLE001 - failure is mapped to retry/dead-letter
        inbox.update_one(
            {"_id": doc["_id"]},
            dispatch.plan_failure(
                attempt=doc["Attempt"],
                max_attempts=consumer.max_attempts,
                now=datetime.now(timezone.utc),
                error=str(exc),
            ),
        )
        return True

    inbox.update_one(
        {"_id": doc["_id"]},
        queries.processed_update(now=datetime.now(timezone.utc)),
    )
    return True


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
