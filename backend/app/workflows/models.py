from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from sqlalchemy import (
    Boolean, CheckConstraint, DateTime, ForeignKey, Index, Integer,
    String, Text, UniqueConstraint, event, inspect,
)
from sqlalchemy.dialects.mysql import LONGTEXT, VARBINARY
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import TypeDecorator

from app.database import Base
from app.models import utcnow
from app.services.exceptions import ConflictError


JSON_TEXT = Text().with_variant(LONGTEXT(), "mysql")
MYSQL_OPTIONS = {"mysql_engine": "InnoDB"}


class ExactMessageId(TypeDecorator):
    """UTF-8 bytes avoid MySQL case/accent folding and trailing-space equivalence."""

    impl = String(128)
    cache_ok = True

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(VARBINARY(512) if dialect.name == "mysql" else String(128))

    def process_bind_param(self, value, dialect):
        return value.encode("utf-8") if value is not None and dialect.name == "mysql" else value

    def process_result_value(self, value, dialect):
        return value.decode("utf-8") if value is not None and dialect.name == "mysql" else value


def require_message_id_schema(engine) -> None:
    inspector = inspect(engine)
    if not inspector.has_table("workflow_message_inbox"):
        raise ValueError("initialize workflow tables before starting workers")
    column = next((column for column in inspector.get_columns("workflow_message_inbox")
                   if column["name"] == "message_id"), None)
    if column is None or not isinstance(column["type"], VARBINARY) or column["type"].length < 512:
        raise ValueError("workflow inbox message_id must use VARBINARY(512); explicit migration required")


class Identity:
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))


class Timestamps:
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=utcnow, onupdate=utcnow,
    )


class WorkflowDefinition(Identity, Timestamps, Base):
    __tablename__ = "workflow_definitions"
    __table_args__ = (CheckConstraint("revision >= 1", name="ck_workflow_revision"), MYSQL_OPTIONS)

    name: Mapped[str] = mapped_column(String(120), nullable=False)
    revision: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    draft_graph_json: Mapped[str] = mapped_column(JSON_TEXT, nullable=False)


class WorkflowVersion(Identity, Base):
    __tablename__ = "workflow_versions"
    __table_args__ = (
        UniqueConstraint("definition_id", "version", name="uq_workflow_definition_version"),
        CheckConstraint("version >= 1", name="ck_workflow_version"),
        MYSQL_OPTIONS,
    )

    definition_id: Mapped[str] = mapped_column(ForeignKey("workflow_definitions.id"), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    graph_json: Mapped[str] = mapped_column(JSON_TEXT, nullable=False)
    digest: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)


@event.listens_for(WorkflowVersion, "before_update")
def immutable_version(_mapper, _connection, _target):
    raise ConflictError("published workflow versions are immutable")


class WorkflowScheduleBinding(Timestamps, Base):
    __tablename__ = "workflow_schedule_bindings"
    __table_args__ = MYSQL_OPTIONS

    schedule_id: Mapped[str] = mapped_column(ForeignKey("schedules.id"), primary_key=True)
    workflow_version_id: Mapped[str] = mapped_column(ForeignKey("workflow_versions.id"), nullable=False)


class WorkflowRun(Timestamps, Base):
    __tablename__ = "workflow_runs"
    __table_args__ = (Index("ix_workflow_runs_status", "status"), MYSQL_OPTIONS)

    execution_id: Mapped[str] = mapped_column(ForeignKey("executions.id"), primary_key=True)
    workflow_version_id: Mapped[str] = mapped_column(ForeignKey("workflow_versions.id"), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="running")
    decision_applied: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class NodeRun(Identity, Timestamps, Base):
    __tablename__ = "workflow_node_runs"
    __table_args__ = (
        UniqueConstraint("run_id", "node_id", name="uq_workflow_run_node"),
        CheckConstraint("attempt >= 0", name="ck_node_run_attempt"),
        Index("ix_workflow_node_runs_status", "status"),
        MYSQL_OPTIONS,
    )

    run_id: Mapped[str] = mapped_column(ForeignKey("workflow_runs.execution_id"), nullable=False)
    node_id: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="waiting_dependencies")
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    outputs_json: Mapped[str] = mapped_column(JSON_TEXT, nullable=False, default="{}")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


class NodeAttempt(Identity, Timestamps, Base):
    __tablename__ = "workflow_node_attempts"
    __table_args__ = (
        UniqueConstraint("node_run_id", "attempt", name="uq_workflow_node_attempt"),
        UniqueConstraint("command_id", name="uq_workflow_attempt_command"),
        CheckConstraint("attempt >= 1", name="ck_workflow_attempt_number"),
        Index("ix_workflow_attempt_status_deadline", "status", "execution_deadline_at"),
        MYSQL_OPTIONS,
    )

    node_run_id: Mapped[str] = mapped_column(ForeignKey("workflow_node_runs.id"), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    command_id: Mapped[str] = mapped_column(String(36), nullable=False, default=lambda: str(uuid4()))
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued")
    inputs_json: Mapped[str] = mapped_column(JSON_TEXT, nullable=False, default="{}")
    command_json: Mapped[str] = mapped_column(JSON_TEXT, nullable=False, default="{}")
    outputs_json: Mapped[str] = mapped_column(JSON_TEXT, nullable=False, default="{}")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    delivery_deadline_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    acceptance_deadline_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    execution_deadline_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    validation_deadline_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class NodeDataPackageLink(Identity, Base):
    __tablename__ = "workflow_node_data_packages"
    __table_args__ = (
        UniqueConstraint("node_attempt_id", "data_package_id", name="uq_node_attempt_data_package"),
        MYSQL_OPTIONS,
    )

    node_attempt_id: Mapped[str] = mapped_column(ForeignKey("workflow_node_attempts.id"), nullable=False)
    data_package_id: Mapped[str] = mapped_column(ForeignKey("data_packages.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)


class DeviceResource(Timestamps, Base):
    __tablename__ = "workflow_device_resources"
    __table_args__ = (
        CheckConstraint("capacity >= 1", name="ck_resource_capacity"),
        CheckConstraint("fencing_generation >= 0", name="ck_resource_generation"),
        MYSQL_OPTIONS,
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    gateway_id: Mapped[str] = mapped_column(String(64), nullable=False)
    adapter_id: Mapped[str] = mapped_column(String(64), nullable=False)
    capacity: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    simulated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    fencing_generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    config_json: Mapped[str] = mapped_column(JSON_TEXT, nullable=False, default="{}")


class ResourceReservation(Identity, Timestamps, Base):
    __tablename__ = "workflow_resource_reservations"
    __table_args__ = (
        UniqueConstraint("resource_id", "active_slot", name="uq_resource_active_slot"),
        UniqueConstraint("node_attempt_id", name="uq_resource_attempt"),
        CheckConstraint("slot >= 1", name="ck_resource_slot"),
        CheckConstraint("fencing_generation >= 1", name="ck_reservation_generation"),
        Index("ix_resource_reservation_status", "resource_id", "status"),
        MYSQL_OPTIONS,
    )

    resource_id: Mapped[str] = mapped_column(ForeignKey("workflow_device_resources.id"), nullable=False)
    node_attempt_id: Mapped[str] = mapped_column(ForeignKey("workflow_node_attempts.id"), nullable=False)
    slot: Mapped[int] = mapped_column(Integer, nullable=False)
    # Released reservations retain history; NULL lets the same capacity slot be reused.
    active_slot: Mapped[int | None] = mapped_column(Integer, nullable=True)
    fencing_generation: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    released_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class MessageOutbox(Timestamps, Base):
    __tablename__ = "workflow_message_outbox"
    __table_args__ = (
        Index("ix_workflow_outbox_ready", "status", "next_attempt_at", "leased_until"),
        MYSQL_OPTIONS,
    )

    message_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    message_key: Mapped[str] = mapped_column(String(255), nullable=False)
    envelope_json: Mapped[str] = mapped_column(JSON_TEXT, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lease_owner: Mapped[str | None] = mapped_column(String(36), nullable=True)
    leased_until: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class MessageInbox(Identity, Base):
    __tablename__ = "workflow_message_inbox"
    __table_args__ = (
        UniqueConstraint("handler", "message_id", name="uq_workflow_inbox_message"),
        MYSQL_OPTIONS,
    )

    handler: Mapped[str] = mapped_column(String(64), nullable=False)
    message_id: Mapped[str] = mapped_column(ExactMessageId(), nullable=False)
    digest: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    envelope_json: Mapped[str] = mapped_column(JSON_TEXT, nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)


class ExecutorJob(Identity, Timestamps, Base):
    __tablename__ = "workflow_executor_jobs"
    __table_args__ = (
        UniqueConstraint("command_id", name="uq_executor_job_command"),
        Index("ix_executor_job_ready", "executor_id", "status", "leased_until"),
        MYSQL_OPTIONS,
    )

    command_id: Mapped[str] = mapped_column(String(36), nullable=False)
    executor_id: Mapped[str] = mapped_column(String(64), nullable=False)
    # Gateways can use an independent durable store, so run_id is not a foreign key.
    run_id: Mapped[str] = mapped_column(String(36), nullable=False)
    envelope_json: Mapped[str] = mapped_column(JSON_TEXT, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="accepted")
    outputs_json: Mapped[str] = mapped_column(JSON_TEXT, nullable=False, default="{}")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    lease_owner: Mapped[str | None] = mapped_column(String(36), nullable=True)
    leased_until: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


WORKFLOW_TABLES = tuple(model.__table__ for model in (
    WorkflowDefinition, WorkflowVersion, WorkflowScheduleBinding, WorkflowRun,
    NodeRun, NodeAttempt, NodeDataPackageLink,
    MessageOutbox, MessageInbox, ExecutorJob,
))
