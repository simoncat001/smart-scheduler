from __future__ import annotations

import base64
from datetime import datetime, timedelta
import hashlib
from uuid import uuid4

from pydantic import ValidationError
from sqlalchemy import or_, select
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.models import utcnow
from app.workflows.definitions import canonical_json
from app.workflows.engine import WorkflowEngine, db_time, validate_topic_prefix
from app.workflows.models import MessageInbox, MessageOutbox, NodeAttempt
from app.workflows.schemas import EventDisposition, MessageEnvelope


class MessagingService:
    """Reserve inbox identity before applying effects; all writes share caller's transaction."""

    def __init__(self, session, topic_prefix="scheduler"):
        self.session = session
        self.topic_prefix = validate_topic_prefix(topic_prefix)

    def _reserve(self, handler, message_id, digest, envelope, now):
        identity = str(uuid4())
        values = {"id": identity, "handler": handler, "message_id": message_id, "digest": digest,
                  "status": "processing", "envelope_json": envelope, "created_at": now}
        dialect = self.session.get_bind().dialect.name
        if dialect == "mysql":
            statement = mysql_insert(MessageInbox).values(**values)
            statement = statement.on_duplicate_key_update(id=MessageInbox.id)
        elif dialect == "sqlite":
            # Only isolated tests use this dialect. DML starts the outer transaction.
            statement = sqlite_insert(MessageInbox).values(**values).on_conflict_do_nothing(
                index_elements=["handler", "message_id"],
            )
        else:
            raise ValueError("messaging persistence requires MySQL")
        self.session.execute(statement)
        row = self.session.scalar(select(MessageInbox).where(
            MessageInbox.handler == handler, MessageInbox.message_id == message_id,
        ).with_for_update().execution_options(populate_existing=True))
        return row, row.id == identity

    def _quarantine(self, row, raw, source, reason, now):
        row.status, row.error = EventDisposition.QUARANTINED.value, reason
        dlq_id = "dlq:" + hashlib.sha256(f"{row.handler}:{row.message_id}:{row.digest}".encode()).hexdigest()
        body = {"schema_version": 1, "message_id": dlq_id, "message_type": "message.quarantined",
                "source": source[:255], "original_message_id": row.message_id, "digest": row.digest,
                "reason": reason, "raw_base64": base64.b64encode(raw[:32768]).decode("ascii"),
                "truncated": len(raw) > 32768}
        self.session.add(MessageOutbox(
            message_id=dlq_id, topic=f"{self.topic_prefix}.dlq", message_key=row.message_id,
            envelope_json=canonical_json(body), next_attempt_at=now,
        ))
        self.session.flush()
        return EventDisposition.QUARANTINED

    def ingest(self, raw: bytes, source: str, now: datetime) -> EventDisposition:
        now = db_time(now)
        raw_digest = hashlib.sha256(raw).hexdigest()
        try:
            if len(raw) > 1_000_000:
                raise ValueError("message too large")
            envelope = MessageEnvelope.model_validate_json(raw)
            canonical = canonical_json(envelope.model_dump(mode="json"))
            digest = hashlib.sha256(canonical.encode()).hexdigest()
        except (ValueError, ValidationError):
            row, fresh = self._reserve("engine", "invalid:" + raw_digest, raw_digest, "{}", now)
            if not fresh:
                return EventDisposition.DUPLICATE
            return self._quarantine(row, raw, source, "invalid or oversized envelope", now)
        row, fresh = self._reserve("engine", envelope.message_id, digest, canonical, now)
        if not fresh:
            if row.digest == digest:
                return EventDisposition.DUPLICATE
            conflict, fresh = self._reserve("engine-conflict", "conflict:" + raw_digest, raw_digest, canonical, now)
            if not fresh:
                return EventDisposition.DUPLICATE
            return self._quarantine(conflict, raw, source, "message ID reused with a different payload", now)
        disposition = WorkflowEngine(self.session, self.topic_prefix).apply_event(envelope, now)
        if disposition == EventDisposition.QUARANTINED:
            return self._quarantine(row, raw, source, "event ownership, state or outputs failed validation", now)
        row.status = disposition.value
        self.session.flush()
        return disposition


class OutboxPublisher:
    def __init__(self, session_factory, transport, lease_seconds=60, clock=utcnow):
        if not 5 <= lease_seconds <= 600:
            raise ValueError("invalid publisher lease")
        self.session_factory, self.transport = session_factory, transport
        self.lease_seconds, self.clock = lease_seconds, clock

    def _claim(self, now):
        owner = str(uuid4())
        with self.session_factory.begin() as session:
            row = session.scalar(select(MessageOutbox).where(
                MessageOutbox.next_attempt_at <= now,
                or_(MessageOutbox.status == "pending",
                    (MessageOutbox.status == "publishing") & (MessageOutbox.leased_until <= now)),
            ).order_by(MessageOutbox.next_attempt_at, MessageOutbox.message_id).limit(1)
                .with_for_update(skip_locked=True).execution_options(populate_existing=True))
            if row is None:
                return None
            row.status, row.lease_owner = "publishing", owner
            row.leased_until = now + timedelta(seconds=self.lease_seconds)
            row.attempts += 1
            return row.message_id, owner, row.topic, row.message_key, row.envelope_json

    def _sendable(self, identity, envelope, now):
        try:
            event = MessageEnvelope.model_validate_json(envelope)
        except ValidationError:
            return True  # DLQ records deliberately use a separate envelope.
        if not event.message_type.startswith("command."):
            return True
        if db_time(event.expires_at) <= now:
            return False
        with self.session_factory() as session:
            attempt = session.scalar(select(NodeAttempt).where(NodeAttempt.command_id == event.command_id))
            return attempt is not None and attempt.status not in {"completed", "failed", "cancelled", "needs_attention"} and not (
                event.message_type == "command.execute" and attempt.status == "cancel_requested")

    def _finish(self, identity, owner, status, now):
        with self.session_factory.begin() as session:
            row = session.scalar(select(MessageOutbox).where(MessageOutbox.message_id == identity)
                                 .with_for_update().execution_options(populate_existing=True))
            if row is None or row.status != "publishing" or row.lease_owner != owner:
                return False
            row.status, row.lease_owner, row.leased_until = status, None, None
            if status == "sent":
                row.sent_at, row.last_error = now, None
            elif status == "pending":
                row.last_error = "Kafka delivery failed; command identity retained"
                row.next_attempt_at = now + timedelta(seconds=min(300, 2 ** min(row.attempts, 8)))
            else:
                row.last_error = "expired or terminal command; not replayed"
            return status == "sent"

    def publish_batch(self, now: datetime, limit: int = 100) -> int:
        if not 1 <= limit <= 1000:
            raise ValueError("invalid publisher batch size")
        now, sent = db_time(now), 0
        for _ in range(limit):
            current = max(now, db_time(self.clock()))
            claim = self._claim(current)
            if claim is None:
                break
            identity, owner, topic, key, envelope = claim
            current = max(current, db_time(self.clock()))
            if not self._sendable(identity, envelope, current):
                self._finish(identity, owner, "expired", current)
                continue
            try:
                self.transport.publish(topic, key, envelope.encode("utf-8"))
            except Exception:
                self._finish(identity, owner, "pending", max(now, db_time(self.clock())))
                continue
            sent += self._finish(identity, owner, "sent", max(now, db_time(self.clock())))
        return sent


class EventConsumer:
    def __init__(self, session_factory, consumer, topic_prefix="scheduler", clock=utcnow):
        self.session_factory, self.consumer = session_factory, consumer
        self.topic_prefix, self.clock = validate_topic_prefix(topic_prefix), clock
        self._pending = None

    def _handle(self, session, raw, source, now):
        return MessagingService(session, self.topic_prefix).ingest(raw, source, now)

    def process_one(self) -> bool:
        if self._pending is None:
            self._pending = self.consumer.poll(1.0)
        message = self._pending
        if message is None:
            return False
        if message.error():
            self._pending = None
            raise RuntimeError("Kafka consumer polling failed")
        with self.session_factory() as session:
            with session.begin():
                self._handle(session, message.value() or b"",
                             f"{message.topic()}:{message.partition()}:{message.offset()}", self.clock())
        # Keep the same message after either DB or offset failure; never poll ahead.
        positions = self.consumer.commit(message=message, asynchronous=False)
        if positions and any(position.error for position in positions):
            raise RuntimeError("Kafka offset commit failed")
        self._pending = None
        return True
