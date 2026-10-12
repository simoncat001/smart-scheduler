from __future__ import annotations

from datetime import datetime, UTC
from uuid import uuid4

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.enums import ExecutionStatus, ScheduleStatus


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class Schedule(Base):
    __tablename__ = "schedules"
    __table_args__ = (
        Index("ix_schedules_status_next_run", "status", "next_run_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    description: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    target: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[str] = mapped_column(Text, nullable=False, default="{}")

    base_interval_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    min_interval_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    max_interval_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    retry_delay_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    feedback_timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    max_consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False)

    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    dynamic_priority: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    health_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.5)
    consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default=ScheduleStatus.ACTIVE.value
    )
    awaiting_feedback: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    next_run_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_dispatched_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    feedback_deadline_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=utcnow, onupdate=utcnow
    )

    executions: Mapped[list["Execution"]] = relationship(
        back_populates="schedule",
        cascade="all, delete-orphan",
        order_by="desc(Execution.dispatched_at)",
    )


class Execution(Base):
    __tablename__ = "executions"
    __table_args__ = (
        Index("ix_executions_schedule_status", "schedule_id", "status"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    schedule_id: Mapped[str] = mapped_column(ForeignKey("schedules.id"), nullable=False)

    scheduled_for: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    dispatched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default=ExecutionStatus.AWAITING_FEEDBACK.value
    )
    dispatch_payload: Mapped[str] = mapped_column(Text, nullable=False, default="{}")

    feedback_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    feedback_observation: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    note: Mapped[str] = mapped_column(Text, nullable=False, default="")

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=utcnow, onupdate=utcnow
    )

    schedule: Mapped[Schedule] = relationship(back_populates="executions")
    feedback_events: Mapped[list["FeedbackEvent"]] = relationship(
        back_populates="execution",
        cascade="all, delete-orphan",
        order_by="desc(FeedbackEvent.created_at)",
    )
    feedback_snapshot: Mapped["ExecutionFeedbackSnapshot | None"] = relationship(
        back_populates="execution",
        cascade="all, delete-orphan",
        uselist=False,
    )
    instrument_states: Mapped[list["ExecutionInstrumentState"]] = relationship(
        back_populates="execution",
        cascade="all, delete-orphan",
        order_by="ExecutionInstrumentState.instrument_code.asc()",
    )


class FeedbackEvent(Base):
    __tablename__ = "feedback_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    execution_id: Mapped[str] = mapped_column(ForeignKey("executions.id"), nullable=False)
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    quality_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    observation: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    note: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)

    execution: Mapped[Execution] = relationship(back_populates="feedback_events")


class Campaign(Base):
    __tablename__ = "campaigns"
    __table_args__ = (
        Index("ix_campaigns_status_updated", "global_status", "updated_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    objective: Mapped[str] = mapped_column(Text, nullable=False, default="")
    stop_condition: Mapped[str] = mapped_column(Text, nullable=False, default="")
    current_round: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_round: Mapped[int | None] = mapped_column(Integer, nullable=True)
    global_status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    metadata_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=utcnow, onupdate=utcnow
    )


class CampaignScheduleLink(Base):
    __tablename__ = "campaign_schedule_links"
    __table_args__ = (
        Index("ix_campaign_schedule_links_campaign", "campaign_id"),
    )

    schedule_id: Mapped[str] = mapped_column(ForeignKey("schedules.id"), primary_key=True)
    campaign_id: Mapped[str] = mapped_column(ForeignKey("campaigns.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)


class CampaignRound(Base):
    __tablename__ = "campaign_rounds"
    __table_args__ = (
        UniqueConstraint("campaign_id", "round_index", name="uq_campaign_rounds_campaign_round"),
        UniqueConstraint("execution_id", name="uq_campaign_rounds_execution"),
        Index("ix_campaign_rounds_schedule_round", "schedule_id", "round_index"),
        Index("ix_campaign_rounds_batch", "experiment_batch_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    campaign_id: Mapped[str] = mapped_column(ForeignKey("campaigns.id"), nullable=False)
    schedule_id: Mapped[str] = mapped_column(ForeignKey("schedules.id"), nullable=False)
    execution_id: Mapped[str] = mapped_column(ForeignKey("executions.id"), nullable=False)
    round_index: Mapped[int] = mapped_column(Integer, nullable=False)
    experiment_batch_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="running")
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=utcnow, onupdate=utcnow
    )


class ExecutionFeedbackSnapshot(Base):
    __tablename__ = "execution_feedback_snapshots"
    __table_args__ = (
        Index("ix_execution_feedback_snapshots_closed_loop", "closed_loop_ready", "updated_at"),
    )

    execution_id: Mapped[str] = mapped_column(
        ForeignKey("executions.id"), primary_key=True
    )
    closed_loop_ready: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    current_step: Mapped[str | None] = mapped_column(String(64), nullable=True)
    current_output: Mapped[str | None] = mapped_column(String(128), nullable=True)
    completed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_steps: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completion_ratio: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    missing_steps_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    workflow_progress_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=utcnow, onupdate=utcnow
    )

    execution: Mapped[Execution] = relationship(back_populates="feedback_snapshot")


class ExecutionInstrumentState(Base):
    __tablename__ = "execution_instrument_states"
    __table_args__ = (
        UniqueConstraint(
            "execution_id",
            "instrument_code",
            name="uq_execution_instrument_states_execution_instrument",
        ),
        Index("ix_execution_instrument_states_execution_status", "execution_id", "status"),
        Index("ix_execution_instrument_states_status_updated", "status", "updated_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    execution_id: Mapped[str] = mapped_column(ForeignKey("executions.id"), nullable=False)
    instrument_code: Mapped[str] = mapped_column(String(64), nullable=False)
    component_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    triggered_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    uploaded_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    parsed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    failed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    collected_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    error_message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    note: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=utcnow, onupdate=utcnow
    )

    execution: Mapped[Execution] = relationship(back_populates="instrument_states")


class DataPackage(Base):
    __tablename__ = "data_packages"
    __table_args__ = (
        Index("ix_data_packages_execution_instrument", "execution_id", "instrument_code"),
        Index("ix_data_packages_schedule_created", "schedule_id", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    campaign_id: Mapped[str | None] = mapped_column(ForeignKey("campaigns.id"), nullable=True)
    schedule_id: Mapped[str] = mapped_column(ForeignKey("schedules.id"), nullable=False)
    execution_id: Mapped[str] = mapped_column(ForeignKey("executions.id"), nullable=False)
    task_id: Mapped[str] = mapped_column(String(36), nullable=False)
    run_id: Mapped[str] = mapped_column(String(36), nullable=False)
    experiment_batch_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    instrument_code: Mapped[str] = mapped_column(String(64), nullable=False)
    instrument_id: Mapped[str] = mapped_column(String(255), nullable=False)
    component_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    sample_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    package_kind: Mapped[str] = mapped_column(String(64), nullable=False, default="standardized_dataset")
    template_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    template_version: Mapped[str | None] = mapped_column(String(128), nullable=True)
    validation_status: Mapped[str] = mapped_column(String(32), nullable=False, default="uploaded")
    manage_db_record_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    raw_files_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    metadata_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    process_data_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    result_data_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    log_data_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    scheduler_observation_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    standard_data_ref_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    note: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=utcnow, onupdate=utcnow
    )


class RecommendationRecord(Base):
    __tablename__ = "recommendation_records"
    __table_args__ = (
        UniqueConstraint("schedule_id", "fingerprint", name="uq_recommendation_schedule_fingerprint"),
        Index("ix_recommendation_records_schedule_created", "schedule_id", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    campaign_id: Mapped[str | None] = mapped_column(ForeignKey("campaigns.id"), nullable=True)
    schedule_id: Mapped[str] = mapped_column(ForeignKey("schedules.id"), nullable=False)
    task_id: Mapped[str] = mapped_column(String(36), nullable=False)
    source_execution_id: Mapped[str | None] = mapped_column(
        ForeignKey("executions.id"), nullable=True
    )
    latest_outcome: Mapped[str | None] = mapped_column(String(32), nullable=True)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    summary: Mapped[str] = mapped_column(Text, nullable=False, default="")
    rationale_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    suggested_payload_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    model_id: Mapped[str] = mapped_column(String(128), nullable=False, default="heuristic")
    model_version: Mapped[str | None] = mapped_column(String(255), nullable=True)
    input_dataset_version: Mapped[str | None] = mapped_column(String(512), nullable=True)
    approval_status: Mapped[str] = mapped_column(String(32), nullable=False, default="generated")
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=utcnow, onupdate=utcnow
    )


class ModelInferenceRun(Base):
    __tablename__ = "model_inference_runs"
    __table_args__ = (
        UniqueConstraint("source_execution_id", name="uq_model_inference_source_execution"),
        Index("ix_model_inference_runs_schedule_created", "schedule_id", "created_at"),
        Index("ix_model_inference_runs_status_updated", "status", "updated_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    schedule_id: Mapped[str] = mapped_column(ForeignKey("schedules.id"), nullable=False)
    source_execution_id: Mapped[str] = mapped_column(ForeignKey("executions.id"), nullable=False)
    recommendation_id: Mapped[str | None] = mapped_column(
        ForeignKey("recommendation_records.id"), nullable=True
    )
    dispatched_execution_id: Mapped[str | None] = mapped_column(
        ForeignKey("executions.id"), nullable=True
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="running")
    scheduled_for: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    dispatched_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    summary: Mapped[str] = mapped_column(Text, nullable=False, default="")
    error: Mapped[str] = mapped_column(Text, nullable=False, default="")
    result_payload_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=utcnow, onupdate=utcnow
    )


class ClosedLoopEvent(Base):
    __tablename__ = "closed_loop_events"
    __table_args__ = (
        Index("ix_closed_loop_events_campaign_created", "campaign_id", "created_at"),
        Index("ix_closed_loop_events_schedule_created", "schedule_id", "created_at"),
        Index("ix_closed_loop_events_run_created", "run_id", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    campaign_id: Mapped[str | None] = mapped_column(ForeignKey("campaigns.id"), nullable=True)
    schedule_id: Mapped[str | None] = mapped_column(ForeignKey("schedules.id"), nullable=True)
    execution_id: Mapped[str | None] = mapped_column(ForeignKey("executions.id"), nullable=True)
    data_package_id: Mapped[str | None] = mapped_column(ForeignKey("data_packages.id"), nullable=True)
    recommendation_id: Mapped[str | None] = mapped_column(
        ForeignKey("recommendation_records.id"), nullable=True
    )
    task_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    run_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=utcnow)
