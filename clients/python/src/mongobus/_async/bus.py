import asyncio
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from pymongo import AsyncMongoClient

from .. import constants, context, documents, envelope, indexes, queries
from ..claimcheck import core as claimcheck_core
from ..claimcheck.config import ClaimCheckConfig
from ..constants import CLAIM_CHECK_CONTENT_TYPE
from .._sync.pump import Consumer
from .pump import process_one


class AsyncMongoBus:
    def __init__(
        self,
        uri: str,
        database: str,
        *,
        client: AsyncMongoClient | None = None,
        claim_check: ClaimCheckConfig | None = None,
    ):
        self._client = client if client is not None else AsyncMongoClient(uri)
        self._db = self._client[database]
        self._inbox = self._db[constants.INBOX_COLLECTION]
        self._bindings = self._db[constants.BINDINGS_COLLECTION]
        self._consumers: list[Consumer] = []
        self._indexes_ensured = False
        self._claim_check = claim_check

    async def ensure_indexes(
        self, *, processed_message_ttl: timedelta | None = indexes.DEFAULT_PROCESSED_MESSAGE_TTL
    ) -> None:
        await self._create_indexes(processed_message_ttl=processed_message_ttl)
        self._indexes_ensured = True

    async def _create_indexes(self, *, processed_message_ttl: timedelta | None) -> None:
        for keys, options in indexes.inbox_index_specs(processed_message_ttl=processed_message_ttl):
            await self._inbox.create_index(keys, **options)
        keys, options = indexes.bindings_index_spec()
        await self._bindings.create_index(keys, **options)

    async def _auto_ensure_indexes(self) -> None:
        if not self._indexes_ensured:
            await self._create_indexes(processed_message_ttl=None)
            self._indexes_ensured = True

    async def bind(self, type_id: str, *, endpoint_id: str) -> None:
        keys, options = indexes.bindings_index_spec()
        await self._bindings.create_index(keys, **options)
        await self._bindings.update_one(
            queries.binding_filter(topic=type_id, endpoint_id=endpoint_id),
            queries.binding_set_on_insert(topic=type_id, endpoint_id=endpoint_id),
            upsert=True,
        )

    async def publish(
        self,
        type_id: str,
        data,
        *,
        source: str | None = None,
        subject: str | None = None,
        id: str | None = None,
        time_utc: datetime | None = None,
        deliver_at: datetime | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        use_claim_check: bool | None = None,
    ) -> int:
        routes = await self._bindings.find(
            queries.bindings_for_topic_filter(topic=type_id)
        ).to_list(length=None)
        if not routes:
            return 0

        now = datetime.now(timezone.utc)
        event_id = id if id is not None else envelope.new_event_id()
        final_source = source if source is not None else constants.DEFAULT_SOURCE
        final_correlation = context.resolve_correlation_id(correlation_id)
        final_causation = context.resolve_causation_id(causation_id)

        payload_for_envelope = data
        data_content_type = None
        cc = self._claim_check
        if cc is not None:
            payload_bytes = json.dumps(data).encode("utf-8")
            if claimcheck_core.should_offload(
                size=len(payload_bytes), threshold_bytes=cc.threshold_bytes,
                enabled=cc.enabled, use_claim_check=use_claim_check,
            ):
                # createdAt is informational only and need not be byte-identical to .NET's DateTime format.
                created_at = now.isoformat().replace("+00:00", "Z")
                metadata = {claimcheck_core.CREATED_AT_KEY: created_at}
                if cc.compress:
                    payload_bytes = claimcheck_core.gzip_compress(payload_bytes)
                    metadata[claimcheck_core.COMPRESSION_KEY] = claimcheck_core.COMPRESSION_GZIP
                ref = await cc.provider.put(
                    payload_bytes, content_type=claimcheck_core.OBJECT_CONTENT_TYPE, metadata=metadata,
                )
                ref = replace(ref, created_at=created_at)
                payload_for_envelope = claimcheck_core.reference_to_data(ref)
                data_content_type = CLAIM_CHECK_CONTENT_TYPE

        env = envelope.build_envelope(
            type_id=type_id,
            data=payload_for_envelope,
            source=final_source,
            event_id=event_id,
            time_utc=time_utc if time_utc is not None else now,
            subject=subject,
            data_content_type=data_content_type,
            correlation_id=final_correlation,
            causation_id=final_causation,
        )
        payload_json = envelope.serialize_envelope(env)
        visible = deliver_at if deliver_at is not None else now

        docs = [
            documents.build_inbox_document(
                endpoint_id=route["EndpointId"],
                topic=type_id,
                type_id=type_id,
                payload_json=payload_json,
                cloud_event_id=event_id,
                created_utc=now,
                visible_utc=visible,
                correlation_id=final_correlation,
                causation_id=final_causation,
            )
            for route in routes
        ]
        await self._inbox.insert_many(docs)
        return len(docs)

    def consumer(
        self,
        *,
        endpoint_id: str,
        type_id: str,
        max_attempts: int = constants.DEFAULT_MAX_ATTEMPTS,
        idempotent: bool = True,
    ):
        def register(handler):
            self._consumers.append(
                Consumer(endpoint_id, type_id, handler, max_attempts, idempotent)
            )
            return handler

        return register

    async def run_once(self, endpoint_id: str) -> bool:
        await self._auto_ensure_indexes()
        for consumer in self._consumers:
            if consumer.endpoint_id == endpoint_id and await process_one(self._inbox, consumer, self._claim_check):
                return True
        return False

    async def run(self, *, stop_event=None) -> None:
        await self._auto_ensure_indexes()
        while stop_event is None or not stop_event.is_set():
            did_work = False
            for consumer in self._consumers:
                if await process_one(self._inbox, consumer, self._claim_check):
                    did_work = True
            if not did_work:
                await asyncio.sleep(constants.DEFAULT_POLL_SECONDS)
