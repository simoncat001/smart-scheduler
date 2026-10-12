from __future__ import annotations

from datetime import UTC, timedelta
import hashlib
import json
from uuid import uuid4
from threading import Event, Lock, Thread
import sys

from pydantic import TypeAdapter, ValidationError
from sqlalchemy import or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.services.exceptions import ConflictError, DomainError
from app.models import utcnow
from app.workflows.definitions import canonical_json
from app.workflows.engine import db_time, validated_ports
from app.workflows.messaging import EventConsumer, MessagingService
from app.workflows.models import ExecutorJob, MessageOutbox
from app.workflows.registry import validate_node_type, validate_schema
from app.workflows.schemas import Identifier, MessageEnvelope, NodeDefinition


def command_spec(command):
    payload = command.payload
    try:
        spec = NodeDefinition.model_validate({
            "id": command.node_id, "label": command.node_id, "type": payload["node_type"],
            "type_version": payload["type_version"], "config": payload["config"],
            "inputs": payload["input_ports"], "outputs": payload["output_ports"],
            "timeouts": payload["timeouts"], "executor": {"kind": payload["executor_kind"],
                "gateway_id": command.gateway_id, "adapter_id": command.adapter_id},
        })
        validate_node_type(spec)
        for port in [*spec.inputs.values(), *spec.outputs.values()]:
            validate_schema(port.json_schema)
        validated_ports(spec.inputs, payload["inputs"])
        if not isinstance(payload["execution_context"], dict):
            raise ValueError("missing execution context")
        return spec
    except (KeyError, ValueError, TypeError) as error:
        raise ConflictError("invalid pinned executor command descriptor/inputs") from error


def _scope(command):
    return (command.run_id, command.workflow_version_id, command.node_id, command.node_run_id,
            command.attempt, command.command_id, command.gateway_id, command.adapter_id)


class ExecutorService:
    """Durable command/job transitions, flush-only and without adapter or broker I/O."""

    def __init__(self, session, *, role, simulator_enabled=False, topic_prefix="scheduler", lease_seconds=120):
        if role not in {"model", "parser", "simulator"} or not 5 <= lease_seconds <= 3600:
            raise ValueError("invalid executor role or lease")
        self.session, self.role = session, role
        self.simulator_enabled, self.lease_seconds = simulator_enabled, lease_seconds
        self.messages = MessagingService(session, topic_prefix)

    def _handler(self, executor_id):
        return "executor:" + hashlib.sha256(executor_id.encode()).hexdigest()[:48]

    def _validate(self, command, executor_id, now):
        TypeAdapter(Identifier).validate_python(executor_id)
        if command.gateway_id != executor_id:
            raise ConflictError("command belongs to another executor/gateway")
        expected = {"model": "mat_model", "parser": "manage_db", "simulator": "simulator"}[self.role]
        if command.adapter_id != expected or command.fencing_generation is not None:
            raise ConflictError("executor role/adapter ownership mismatch")
        if self.role == "simulator" and not self.simulator_enabled:
            raise ConflictError("simulator requires explicit enablement")
        if command.message_type not in {"command.execute", "command.cancel"}:
            raise ConflictError("executor accepts execute/cancel commands only")
        if command.expires_at is None or db_time(command.expires_at) <= now:
            raise ConflictError("expired command is not executable")
        if command.message_type == "command.execute":
            spec = command_spec(command)
            expected_types = {"model": {"model_inference"}, "parser": {"data_parse"},
                              "simulator": {"instrument_task", "data_upload"}}[self.role]
            if spec.type not in expected_types:
                raise ConflictError("executor node-type ownership mismatch")
        elif command.payload.keys() - {"reason", "require_cancellation_tombstone"} or (
                command.payload.get("require_cancellation_tombstone") is not True):
            raise ConflictError("cancel requires a durable cancellation tombstone")

    def _job(self, command, executor_id, now):
        identity = str(uuid4())
        values = {"id": identity, "command_id": command.command_id, "executor_id": executor_id,
                  "run_id": command.run_id, "envelope_json": command.model_dump_json(),
                  "status": "cancelled" if command.message_type == "command.cancel" else "accepted",
                  "outputs_json": "{}", "created_at": now, "updated_at": now}
        dialect = self.session.get_bind().dialect.name
        if dialect == "mysql":
            statement = mysql_insert(ExecutorJob).values(**values).on_duplicate_key_update(id=ExecutorJob.id)
        elif dialect == "sqlite":
            statement = sqlite_insert(ExecutorJob).values(**values).on_conflict_do_nothing(index_elements=["command_id"])
        else:
            raise ValueError("executor persistence requires MySQL")
        self.session.execute(statement)
        job = self.session.scalar(select(ExecutorJob).where(ExecutorJob.command_id == command.command_id)
                                  .with_for_update().execution_options(populate_existing=True))
        previous = MessageEnvelope.model_validate_json(job.envelope_json)
        if job.executor_id != executor_id or _scope(previous) != _scope(command):
            raise ConflictError("command ID reused with conflicting executor ownership")
        if command.message_type == previous.message_type == "command.execute" and (
                canonical_json(command.model_dump(mode="json")) != canonical_json(previous.model_dump(mode="json"))):
            raise ConflictError("command ID reused with different immutable inputs")
        return job, job.id == identity

    def _emit(self, job, kind, now, payload=None, *, suffix=""):
        command = MessageEnvelope.model_validate_json(job.envelope_json)
        message_id = f"executor:{job.command_id}:{kind}:{suffix}"
        existing = self.session.get(MessageOutbox, message_id)
        if existing is not None:
            return
        event = command.model_copy(update={"message_id": message_id, "message_type": kind,
            "occurred_at": now.replace(tzinfo=UTC), "expires_at": None, "payload": payload or {}})
        self.session.add(MessageOutbox(message_id=message_id, topic=f"{self.messages.topic_prefix}.events",
            message_key=command.run_id, envelope_json=event.model_dump_json(), next_attempt_at=now))

    def _apply(self, command, executor_id, now):
        job, fresh = self._job(command, executor_id, now)
        if command.message_type == "command.cancel":
            if job.status not in {"completed", "failed"}:
                job.status, job.lease_owner, job.leased_until = "cancelled", None, None
                self._emit(job, "node.cancelled", now, {"error": "executor durably cancelled command"})
        elif fresh:
            self._emit(job, "node.accepted", now)
        self.session.flush()
        return job

    def accept(self, command: MessageEnvelope, executor_id: str, now) -> ExecutorJob:
        command = MessageEnvelope.model_validate_json(command.model_dump_json())
        now = db_time(now)
        self._validate(command, executor_id, now)
        canonical = canonical_json(command.model_dump(mode="json"))
        digest = hashlib.sha256(canonical.encode()).hexdigest()
        row, fresh = self.messages._reserve(self._handler(executor_id), command.message_id, digest, canonical, now)
        if not fresh and row.digest != digest:
            raise ConflictError("executor message ID reused with a different payload")
        if not fresh:
            return self.session.scalar(select(ExecutorJob).where(ExecutorJob.command_id == command.command_id)
                                       .with_for_update().execution_options(populate_existing=True))
        job = self._apply(command, executor_id, now)
        row.status = "applied"
        self.session.flush()
        return job

    def ingest(self, raw, executor_id, source, now):
        now = db_time(now)
        handler = self._handler(executor_id)
        digest = hashlib.sha256(raw).hexdigest()
        command = None
        try:
            if len(raw) > 1_000_000:
                raise ValueError("oversized command")
            command = MessageEnvelope.model_validate_json(raw)
        except (ValidationError, ValueError):
            row, fresh = self.messages._reserve(handler, "invalid:" + digest, digest, "{}", now)
            if not fresh:
                return "duplicate"
            return self.messages._quarantine(row, raw, source, "invalid, expired or conflicting executor command", now).value
        canonical = canonical_json(command.model_dump(mode="json"))
        canonical_digest = hashlib.sha256(canonical.encode()).hexdigest()
        row, fresh = self.messages._reserve(handler, command.message_id, canonical_digest, canonical, now)
        if not fresh:
            if row.digest == canonical_digest:
                return "duplicate"
            conflict, fresh = self.messages._reserve(handler, "conflict:" + digest, digest, canonical, now)
            if not fresh:
                return "duplicate"
            return self.messages._quarantine(conflict, raw, source, "executor message ID reused", now).value
        try:
            self._validate(command, executor_id, now)
            self._apply(command, executor_id, now)
        except DomainError:
            return self.messages._quarantine(row, raw, source, "executor ownership, expiry or immutable command conflict", now).value
        row.status = "applied"
        self.session.flush()
        return "applied"

    def claim(self, executor_id, now):
        now = db_time(now)
        job = self.session.scalar(select(ExecutorJob).where(
            ExecutorJob.executor_id == executor_id,
            or_(ExecutorJob.status == "accepted", (ExecutorJob.status == "running") & (ExecutorJob.leased_until <= now)),
        ).order_by(ExecutorJob.created_at, ExecutorJob.id).limit(1).with_for_update(skip_locked=True)
            .execution_options(populate_existing=True))
        if job is None:
            return None
        command = MessageEnvelope.model_validate_json(job.envelope_json)
        expected_adapter = {"model": "mat_model", "parser": "manage_db", "simulator": "simulator"}[self.role]
        if command.adapter_id != expected_adapter or self.role == "simulator" and not self.simulator_enabled:
            raise ConflictError("job belongs to another role or disabled simulator")
        command_spec(command)
        if self.role == "simulator" and job.status == "running" or (
                job.status == "accepted" and db_time(command.expires_at) <= now):
            job.status, job.error = "needs_attention", "uncertain or expired start; not automatically replayed"
            job.lease_owner, job.leased_until = None, None
            self._emit(job, "node.needs_attention", now, {"error": job.error})
            self.session.flush()
            return None
        job.status, job.lease_owner = "running", str(uuid4())
        job.leased_until = now + timedelta(seconds=self.lease_seconds)
        self._emit(job, "node.started", now, suffix=job.lease_owner)
        self.session.flush()
        return job

    def renew(self, job_id, lease_owner, now):
        now = db_time(now)
        job = self.session.scalar(select(ExecutorJob).where(ExecutorJob.id == job_id)
                                  .with_for_update().execution_options(populate_existing=True))
        if job is None or job.status != "running" or job.lease_owner != lease_owner or job.leased_until <= now:
            return False
        job.leased_until = now + timedelta(seconds=self.lease_seconds)
        self.session.flush()
        return True

    def finish(self, job_id, outputs, now, *, lease_owner):
        now = db_time(now)
        job = self.session.scalar(select(ExecutorJob).where(ExecutorJob.id == job_id)
                                  .with_for_update().execution_options(populate_existing=True))
        if job is None or job.status != "running" or job.lease_owner != lease_owner or job.leased_until <= now:
            raise ConflictError("executor lease no longer owns this job")
        spec = command_spec(MessageEnvelope.model_validate_json(job.envelope_json))
        try:
            outputs = validated_ports(spec.outputs, outputs)
        except ValueError as error:
            raise ConflictError("executor outputs failed validation") from error
        job.outputs_json, job.status = canonical_json(outputs), "completed"
        job.lease_owner, job.leased_until = None, None
        self._emit(job, "node.completed", now, {"outputs": outputs})
        self.session.flush()

    def fail(self, job_id, now, *, lease_owner):
        job = self.session.scalar(select(ExecutorJob).where(ExecutorJob.id == job_id)
                                  .with_for_update().execution_options(populate_existing=True))
        if job is None or job.status != "running" or job.lease_owner != lease_owner or job.leased_until <= db_time(now):
            raise ConflictError("executor lease no longer owns this job")
        job.status, job.error = "failed", "domain adapter failed; inspect approved configuration and data"
        job.lease_owner, job.leased_until = None, None
        self._emit(job, "node.failed", db_time(now), {"error": job.error})
        self.session.flush()


class CommandConsumer(EventConsumer):
    def __init__(self, session_factory, consumer, *, role, executor_id, simulator_enabled=False,
                 topic_prefix="scheduler", clock=utcnow):
        super().__init__(session_factory, consumer, topic_prefix, clock)
        self.role, self.executor_id, self.simulator_enabled = role, executor_id, simulator_enabled

    def _handle(self, session, raw, source, now):
        return ExecutorService(session, role=self.role, simulator_enabled=self.simulator_enabled,
            topic_prefix=self.topic_prefix).ingest(raw, self.executor_id, source, now)


class ExecutorRunner:
    """A separate computation loop; polling/acceptance never waits for an adapter."""

    def __init__(self, session_factory, adapters, *, role, executor_id, simulator_enabled=False,
                 topic_prefix="scheduler", lease_seconds=120, clock=utcnow):
        self.session_factory, self.adapters = session_factory, adapters
        self.role, self.executor_id, self.simulator_enabled = role, executor_id, simulator_enabled
        self.topic_prefix, self.lease_seconds, self.clock = topic_prefix, lease_seconds, clock
        self._stop, self._lock, self._thread, self._active = Event(), Lock(), None, None

    def _service(self, session):
        return ExecutorService(session, role=self.role, simulator_enabled=self.simulator_enabled,
                               topic_prefix=self.topic_prefix, lease_seconds=self.lease_seconds)

    def renew_active(self):
        with self._lock:
            active = self._active
        if active:
            with self.session_factory() as session, session.begin():
                return self._service(session).renew(*active, self.clock())
        return False

    def execute_one(self):
        from app.workflows.adapters import ExecutionContext

        with self.session_factory() as session, session.begin():
            job = self._service(session).claim(self.executor_id, self.clock())
            if job is None:
                return False
            job_id, owner, envelope = job.id, job.lease_owner, job.envelope_json
        with self._lock:
            self._active = job_id, owner
        renewal_stop = Event()

        def keep_lease():
            while not self._stop.is_set() and not renewal_stop.wait(min(10, self.lease_seconds / 3)):
                try:
                    self.renew_active()
                except Exception:
                    pass  # A lost lease cannot be revived; finish is owner/deadline fenced.

        renewal = Thread(target=keep_lease, daemon=True)
        renewal.start()
        try:
            command = MessageEnvelope.model_validate_json(envelope)
            try:
                outputs = self.adapters.execute(command, ExecutionContext.from_command(command))
                outputs = validated_ports(command_spec(command).outputs, outputs)
                canonical_json(outputs)
            except Exception:
                with self.session_factory() as session, session.begin():
                    try:
                        self._service(session).fail(job_id, self.clock(), lease_owner=owner)
                    except ConflictError:
                        pass  # Cancellation or a newer claimant already owns the outcome.
                return True
            reported = False
            while not self._stop.is_set():
                try:
                    with self.session_factory() as session, session.begin():
                        self._service(session).finish(job_id, outputs, self.clock(), lease_owner=owner)
                    break
                except ConflictError:
                    break  # A committed finish, cancellation or expired token wins.
                except SQLAlchemyError:
                    if not reported:
                        print("Result commit failed; retaining successful output under the original job token.", file=sys.stderr)
                        reported = True
                    self._stop.wait(0.2)
            return True
        finally:
            renewal_stop.set()
            renewal.join(timeout=5)
            with self._lock:
                self._active = None

    def start(self, poll_seconds=1):
        def loop():
            while not self._stop.is_set():
                try:
                    self.execute_one()
                except Exception:
                    print("Executor cycle failed; durable job retained without acknowledgement replay.", file=sys.stderr)
                self._stop.wait(poll_seconds)
        self._thread = Thread(target=loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30)
