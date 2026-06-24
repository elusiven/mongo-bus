import time
from datetime import datetime, timezone

from pymongo import ASCENDING, MongoClient

from .. import constants, context, documents, envelope, queries
from .pump import Consumer, process_one


class MongoBus:
    def __init__(self, uri: str, database: str, *, client: MongoClient | None = None):
        self._client = client if client is not None else MongoClient(uri)
        self._db = self._client[database]
        self._inbox = self._db[constants.INBOX_COLLECTION]
        self._bindings = self._db[constants.BINDINGS_COLLECTION]
        self._consumers: list[Consumer] = []

    def bind(self, type_id: str, *, endpoint_id: str) -> None:
        self._bindings.create_index(
            [("Topic", ASCENDING), ("EndpointId", ASCENDING)], unique=True
        )
        self._bindings.update_one(
            queries.binding_filter(topic=type_id, endpoint_id=endpoint_id),
            queries.binding_set_on_insert(topic=type_id, endpoint_id=endpoint_id),
            upsert=True,
        )

    def publish(
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
    ) -> int:
        routes = list(self._bindings.find(queries.bindings_for_topic_filter(topic=type_id)))
        if not routes:
            return 0

        now = datetime.now(timezone.utc)
        event_id = id if id is not None else envelope.new_event_id()
        final_source = source if source is not None else constants.DEFAULT_SOURCE
        final_correlation = context.resolve_correlation_id(correlation_id)
        final_causation = context.resolve_causation_id(causation_id)

        env = envelope.build_envelope(
            type_id=type_id,
            data=data,
            source=final_source,
            event_id=event_id,
            time_utc=time_utc if time_utc is not None else now,
            subject=subject,
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
        self._inbox.insert_many(docs)
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

    def run_once(self, endpoint_id: str) -> bool:
        for consumer in self._consumers:
            if consumer.endpoint_id == endpoint_id and process_one(self._inbox, consumer):
                return True
        return False

    def run(self, *, stop_event=None) -> None:
        while stop_event is None or not stop_event.is_set():
            did_work = any(process_one(self._inbox, c) for c in self._consumers)
            if not did_work:
                time.sleep(constants.DEFAULT_POLL_SECONDS)
