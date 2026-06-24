import inspect
from datetime import datetime, timezone

from pymongo import ASCENDING
from pymongo.collection import ReturnDocument

from .. import constants, context, dispatch, envelope, queries
from .._sync.pump import Consumer


async def process_one(inbox, consumer: Consumer) -> bool:
    now = datetime.now(timezone.utc)
    pump_id = dispatch.build_pump_id(consumer.endpoint_id)
    doc = await inbox.find_one_and_update(
        queries.lock_filter(endpoint_id=consumer.endpoint_id, now=now, type_ids=[consumer.type_id]),
        queries.lock_update(now=now, lock_seconds=constants.DEFAULT_LOCK_SECONDS, pump_id=pump_id),
        sort=[("VisibleUtc", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )
    if doc is None:
        return False

    env = envelope.parse_envelope(doc["PayloadJson"])
    ctx = context.ConsumeContext.from_message(env, doc)

    if consumer.idempotent and await _already_processed(inbox, ctx, doc["_id"]):
        await inbox.update_one(
            {"_id": doc["_id"]},
            queries.processed_update(
                now=datetime.now(timezone.utc),
                last_error="Skipped due to idempotency",
            ),
        )
        return True

    try:
        with context.use_context(ctx):
            result = consumer.handler(ctx)
            if inspect.isawaitable(result):
                await result
    except Exception as exc:  # noqa: BLE001 - failure is mapped to retry/dead-letter
        await inbox.update_one(
            {"_id": doc["_id"]},
            dispatch.plan_failure(
                attempt=doc["Attempt"],
                max_attempts=consumer.max_attempts,
                now=datetime.now(timezone.utc),
                error=str(exc),
            ),
        )
        return True

    await inbox.update_one(
        {"_id": doc["_id"]},
        queries.processed_update(now=datetime.now(timezone.utc)),
    )
    return True


async def _already_processed(inbox, ctx: context.ConsumeContext, current_id) -> bool:
    count = await inbox.count_documents(
        queries.dedup_filter(
            endpoint_id=ctx.raw["EndpointId"],
            cloud_event_id=ctx.cloud_event_id,
            exclude_id=current_id,
        ),
        limit=1,
    )
    return count > 0
