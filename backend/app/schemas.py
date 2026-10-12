from __future__ import annotations

from datetime import datetime
import json
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.enums import ExecutionStatus, FeedbackOutcome, ScheduleStatus
from app.materials import validate_tizrnbv_payload
from app.models import (
    Campaign,
    ClosedLoopEvent,
    DataPackage,
    Execution,
    ExecutionFeedbackSnapshot,
    ExecutionInstrumentState,
    ModelInferenceRun,
    RecommendationRecord,
    Schedule,
)


def _load_json(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    return json.loads(value)


class ScheduleCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    description: str = Field(default="", max_length=500)
    target: str = Field(min_length=1, max_length=255)
    payload: dict[str, Any] = Field(default_factory=dict)

    base_interval_seconds: int = Field(ge=1, le=604800)
    min_interval_seconds: int | None = Field(default=None, ge=1, le=604800)
    max_interval_seconds: int | None = Field(default=None, ge=1, le=604800)
    retry_delay_seconds: int = Field(default=30, ge=1, le=604800)
    feedback_timeout_seconds: int = Field(default=300, ge=1, le=604800)
    max_consecutive_failures: int = Field(default=30, ge=1, le=100)
    priority: int = Field(default=5, ge=1, le=10)
    start_in_seconds: int = Field(default=0, ge=0, le=604800)

    @model_validator(mode="after")
    def validate_windows(self) -> "ScheduleCreate":
        min_interval = self.min_interval_seconds or max(1, self.base_interval_seconds // 2)
        max_interval = self.max_interval_seconds or max(
            self.base_interval_seconds, self.base_interval_seconds * 4
        )

        if min_interval > self.base_interval_seconds:
            raise ValueError("min_interval_seconds cannot exceed base_interval_seconds")
        if max_interval < self.base_interval_seconds:
            raise ValueError("max_interval_seconds cannot be below base_interval_seconds")
        if self.retry_delay_seconds > max_interval:
            raise ValueError("retry_delay_seconds cannot exceed max_interval_seconds")
        validate_tizrnbv_payload(self.payload)
        return self


class ScheduleUpdate(BaseModel):
    payload: dict[str, Any] | None = None
    description: str | None = Field(default=None, max_length=500)
    priority: int | None = Field(default=None, ge=1, le=10)
    run_now: bool = False

    @model_validator(mode="after")
    def validate_payload(self) -> "ScheduleUpdate":
        if self.payload is not None:
            validate_tizrnbv_payload(self.payload)
        return self


class FeedbackCreate(BaseModel):
    outcome: FeedbackOutcome
    quality_score: float | None = Field(default=None, ge=0.0, le=1.0)
    latency_ms: int | None = Field(default=None, ge=0)
    observation: dict[str, Any] = Field(default_factory=dict)
    note: str = Field(default="", max_length=1000)


class WorkflowUploadCorrelation(BaseModel):
    node_run_id: str | None = Field(default=None, min_length=36, max_length=36, strict=True)
    attempt: int | None = Field(default=None, ge=1, le=10, strict=True)
    request_id: str | None = Field(default=None, min_length=1, max_length=128, strict=True)

    @model_validator(mode="after")
    def bounded_workflow_request(self):
        if any(value is not None for value in (self.request_id, self.node_run_id, self.attempt)):
            def timestamp(value):
                if isinstance(value, datetime):
                    return value.isoformat()
                raise ValueError("workflow uploads accept JSON values and timestamps only")

            # JSON-mode serialization may replace non-finite floats with null.
            encoded = json.dumps(self.model_dump(mode="python"), allow_nan=False, ensure_ascii=False, default=timestamp)
            if len(encoded.encode()) > 1_000_000:
                raise ValueError("workflow upload request exceeds 1 MB")
        return self

    @field_validator("node_run_id")
    @classmethod
    def canonical_node_id(cls, value):
        if value is not None and str(UUID(value)) != value:
            raise ValueError("node_run_id must be a canonical UUID")
        return value


class InstrumentUploadCreate(WorkflowUploadCorrelation):
    run_id: str | None = Field(default=None, min_length=36, max_length=36, strict=True)
    instrument_code: str = Field(min_length=1, max_length=64)
    component_id: str = Field(min_length=1, max_length=255)
    experiment_batch_id: str = Field(min_length=1, max_length=255)
    status: str = Field(default="uploaded", min_length=1, max_length=32)
    collected_at: datetime | None = None
    observation: dict[str, Any] = Field(default_factory=dict)
    note: str = Field(default="", max_length=1000)


class DataPackageCreate(WorkflowUploadCorrelation):
    task_id: str | None = Field(default=None, max_length=36)
    run_id: str | None = Field(default=None, max_length=36)
    campaign_id: str | None = Field(default=None, max_length=36)
    experiment_batch_id: str | None = Field(default=None, max_length=255)
    instrument_code: str = Field(min_length=1, max_length=64)
    instrument_id: str | None = Field(default=None, max_length=255)
    component_id: str | None = Field(default=None, max_length=255)
    sample_id: str | None = Field(default=None, max_length=255)
    package_kind: str = Field(default="standardized_dataset", min_length=1, max_length=64)
    template_id: str | None = Field(default=None, max_length=255)
    template_version: str | None = Field(default=None, max_length=128)
    validation_status: str = Field(default="uploaded", min_length=1, max_length=32)
    manage_db_record_id: str | None = Field(default=None, max_length=128)
    raw_files: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    process_data: dict[str, Any] = Field(default_factory=dict)
    result_data: dict[str, Any] = Field(default_factory=dict)
    log_data: dict[str, Any] = Field(default_factory=dict)
    scheduler_observation: dict[str, Any] = Field(default_factory=dict)
    standard_data_ref: dict[str, Any] = Field(default_factory=dict)
    note: str = Field(default="", max_length=2000)

    @model_validator(mode="after")
    def validate_context(self) -> "DataPackageCreate":
        if not any([self.task_id, self.run_id, self.experiment_batch_id, self.node_run_id]):
            raise ValueError("run_id, task_id, experiment_batch_id, node_run_id 至少需要提供一个")
        return self


class ScheduleRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    task_id: str
    campaign_id: str | None
    name: str
    description: str
    target: str
    payload: dict[str, Any]
    status: ScheduleStatus
    priority: int
    dynamic_priority: int
    health_score: float
    consecutive_failures: int
    base_interval_seconds: int
    min_interval_seconds: int
    max_interval_seconds: int
    retry_delay_seconds: int
    feedback_timeout_seconds: int
    max_consecutive_failures: int
    awaiting_feedback: bool
    next_run_at: datetime
    last_dispatched_at: datetime | None
    feedback_deadline_at: datetime | None
    created_at: datetime
    updated_at: datetime


class ExecutionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    task_id: str
    run_id: str
    campaign_id: str | None
    schedule_id: str
    status: ExecutionStatus
    scheduled_for: datetime
    dispatched_at: datetime
    closed_at: datetime | None
    attempt: int
    dispatch_payload: dict[str, Any]
    feedback_score: float | None
    latency_ms: int | None
    feedback_observation: dict[str, Any]
    feedback_snapshot: ExecutionFeedbackSnapshotRead | None = None
    instrument_states: list[ExecutionInstrumentStateRead] = Field(default_factory=list)
    model_inference: ModelInferenceRead | None = None
    workflow_runtime: dict[str, Any] | None = None
    note: str
    created_at: datetime
    updated_at: datetime


class ExecutionFeedbackSnapshotRead(BaseModel):
    execution_id: str
    closed_loop_ready: bool
    current_step: str | None
    current_output: str | None
    completed_count: int
    total_steps: int
    completion_ratio: float
    missing_steps: list[dict[str, Any]]
    workflow_progress: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class ExecutionInstrumentStateRead(BaseModel):
    id: str
    execution_id: str
    instrument_code: str
    component_id: str | None
    status: str
    triggered_at: datetime | None
    uploaded_at: datetime | None
    parsed_at: datetime | None
    failed_at: datetime | None
    collected_at: datetime | None
    error_message: str
    note: str
    created_at: datetime
    updated_at: datetime


class ModelInferenceRead(BaseModel):
    id: str
    schedule_id: str
    source_execution_id: str
    recommendation_id: str | None
    dispatched_execution_id: str | None
    status: str
    scheduled_for: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    dispatched_at: datetime | None
    summary: str
    error: str
    result_payload: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class TickResult(BaseModel):
    timed_out_count: int
    dispatched_count: int
    executions: list[ExecutionRead]


class InstrumentUploadRead(BaseModel):
    execution_id: str
    schedule_id: str
    experiment_batch_id: str
    instrument_code: str
    component_id: str
    round_completed: bool
    observation: dict[str, Any]
    missing_steps: list[dict[str, Any]]
    execution: ExecutionRead


class RecommendationRead(BaseModel):
    recommendation_id: str | None = None
    campaign_id: str | None = None
    task_id: str | None = None
    schedule_id: str
    schedule_name: str
    schedule_status: ScheduleStatus
    source_execution_id: str | None
    latest_outcome: ExecutionStatus | None
    confidence: float
    summary: str
    rationale: list[str]
    suggested_payload: dict[str, Any]
    model_id: str | None = None
    model_version: str | None = None
    input_dataset_version: str | None = None
    approval_status: str = "generated"


class CampaignRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    objective: str
    stop_condition: str
    current_round: int
    max_round: int | None
    global_status: str
    metadata: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class ClosedLoopEventRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    event_type: str
    campaign_id: str | None
    schedule_id: str | None
    execution_id: str | None
    data_package_id: str | None
    recommendation_id: str | None
    task_id: str | None
    run_id: str | None
    payload: dict[str, Any]
    created_at: datetime


class DataPackageRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    campaign_id: str | None
    schedule_id: str
    execution_id: str
    task_id: str
    run_id: str
    experiment_batch_id: str | None
    instrument_code: str
    instrument_id: str
    component_id: str | None
    sample_id: str | None
    package_kind: str
    template_id: str | None
    template_version: str | None
    validation_status: str
    manage_db_record_id: str | None
    raw_files: dict[str, Any]
    metadata: dict[str, Any]
    process_data: dict[str, Any]
    result_data: dict[str, Any]
    log_data: dict[str, Any]
    scheduler_observation: dict[str, Any]
    standard_data_ref: dict[str, Any]
    note: str
    created_at: datetime
    updated_at: datetime


class DataPackageAckRead(BaseModel):
    id: str
    execution_id: str
    experiment_batch_id: str | None
    instrument_code: str
    component_id: str | None
    sample_id: str | None
    validation_status: str
    manage_db_record_id: str | None
    created_at: datetime
    updated_at: datetime


class ExecutionProgressRead(BaseModel):
    id: str
    schedule_id: str
    status: ExecutionStatus
    experiment_batch_id: str | None
    experiment_round_index: int | None
    current_step: str | None
    closed_loop_ready: bool
    completed_count: int
    total_steps: int
    completion_ratio: float
    missing_steps: list[dict[str, Any]]
    instrument_uploads: dict[str, Any]
    updated_at: datetime


class DataPackageIngestRead(BaseModel):
    data_package: DataPackageAckRead
    round_completed: bool
    missing_steps: list[dict[str, Any]]
    execution: ExecutionProgressRead | None = None


class ClosedLoopRunRead(BaseModel):
    campaign: CampaignRead | None
    execution: ExecutionRead
    data_packages: list[DataPackageRead]
    recommendations: list[RecommendationRead]
    events: list[ClosedLoopEventRead]


class EdgeContextRead(BaseModel):
    context_version: int = 1
    campaign_id: str | None
    task_id: str
    run_id: str
    schedule_id: str
    execution_id: str
    experiment_batch_id: str | None
    experiment_round_index: int | None
    sample_batch: str | None
    sample_id: str | None
    instrument_code: str
    instrument_id: str | None
    component_id: str | None
    workflow_step_code: str | None
    expected_outputs: list[str] = Field(default_factory=list)
    workflow_name: str | None = None
    target_metric: str | None = None
    target_value: float | str | None = None


class SamplePlanItemRead(BaseModel):
    sample_code: str
    sample_id: str
    grid_row: int | None = None
    grid_col: int | None = None
    snake_index: int | None = None
    composition_at_percent: dict[str, float] = Field(default_factory=dict)


class ExecutionSamplePlanRead(BaseModel):
    campaign_id: str | None
    task_id: str
    run_id: str
    schedule_id: str
    execution_id: str
    experiment_batch_id: str | None
    experiment_round_index: int | None
    round_sample_id: str | None
    rows: int | None = None
    cols: int | None = None
    total_samples: int = 0
    samples: list[SamplePlanItemRead] = Field(default_factory=list)


def schedule_to_read(schedule: Schedule) -> ScheduleRead:
    payload = _load_json(schedule.payload, {})
    return ScheduleRead(
        id=schedule.id,
        task_id=str(payload.get("task_id") or schedule.id),
        campaign_id=payload.get("campaign_id"),
        name=schedule.name,
        description=schedule.description,
        target=schedule.target,
        payload=payload,
        status=ScheduleStatus(schedule.status),
        priority=schedule.priority,
        dynamic_priority=schedule.dynamic_priority,
        health_score=round(schedule.health_score, 3),
        consecutive_failures=schedule.consecutive_failures,
        base_interval_seconds=schedule.base_interval_seconds,
        min_interval_seconds=schedule.min_interval_seconds,
        max_interval_seconds=schedule.max_interval_seconds,
        retry_delay_seconds=schedule.retry_delay_seconds,
        feedback_timeout_seconds=schedule.feedback_timeout_seconds,
        max_consecutive_failures=schedule.max_consecutive_failures,
        awaiting_feedback=schedule.awaiting_feedback,
        next_run_at=schedule.next_run_at,
        last_dispatched_at=schedule.last_dispatched_at,
        feedback_deadline_at=schedule.feedback_deadline_at,
        created_at=schedule.created_at,
        updated_at=schedule.updated_at,
    )


def model_inference_to_read(model_inference: ModelInferenceRun) -> ModelInferenceRead:
    return ModelInferenceRead(
        id=model_inference.id,
        schedule_id=model_inference.schedule_id,
        source_execution_id=model_inference.source_execution_id,
        recommendation_id=model_inference.recommendation_id,
        dispatched_execution_id=model_inference.dispatched_execution_id,
        status=model_inference.status,
        scheduled_for=model_inference.scheduled_for,
        started_at=model_inference.started_at,
        completed_at=model_inference.completed_at,
        dispatched_at=model_inference.dispatched_at,
        summary=model_inference.summary,
        error=model_inference.error,
        result_payload=_load_json(model_inference.result_payload_json, {}),
        created_at=model_inference.created_at,
        updated_at=model_inference.updated_at,
    )


def execution_feedback_snapshot_to_read(
    snapshot: ExecutionFeedbackSnapshot,
) -> ExecutionFeedbackSnapshotRead:
    return ExecutionFeedbackSnapshotRead(
        execution_id=snapshot.execution_id,
        closed_loop_ready=bool(snapshot.closed_loop_ready),
        current_step=snapshot.current_step,
        current_output=snapshot.current_output,
        completed_count=snapshot.completed_count,
        total_steps=snapshot.total_steps,
        completion_ratio=float(snapshot.completion_ratio),
        missing_steps=_load_json(snapshot.missing_steps_json, []),
        workflow_progress=_load_json(snapshot.workflow_progress_json, {}),
        created_at=snapshot.created_at,
        updated_at=snapshot.updated_at,
    )


def execution_instrument_state_to_read(
    state: ExecutionInstrumentState,
) -> ExecutionInstrumentStateRead:
    return ExecutionInstrumentStateRead(
        id=state.id,
        execution_id=state.execution_id,
        instrument_code=state.instrument_code,
        component_id=state.component_id,
        status=state.status,
        triggered_at=state.triggered_at,
        uploaded_at=state.uploaded_at,
        parsed_at=state.parsed_at,
        failed_at=state.failed_at,
        collected_at=state.collected_at,
        error_message=state.error_message,
        note=state.note,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def execution_to_read(
    execution: Execution,
    *,
    feedback_snapshot: ExecutionFeedbackSnapshot | None = None,
    instrument_states: list[ExecutionInstrumentState] | None = None,
    model_inference: ModelInferenceRun | None = None,
    workflow_runtime: dict[str, Any] | None = None,
) -> ExecutionRead:
    dispatch_payload = _load_json(execution.dispatch_payload, {})
    return ExecutionRead(
        id=execution.id,
        task_id=str(dispatch_payload.get("task_id") or execution.schedule_id),
        run_id=str(dispatch_payload.get("run_id") or execution.id),
        campaign_id=dispatch_payload.get("campaign_id"),
        schedule_id=execution.schedule_id,
        status=ExecutionStatus(execution.status),
        scheduled_for=execution.scheduled_for,
        dispatched_at=execution.dispatched_at,
        closed_at=execution.closed_at,
        attempt=execution.attempt,
        dispatch_payload=dispatch_payload,
        feedback_score=execution.feedback_score,
        latency_ms=execution.latency_ms,
        feedback_observation=_load_json(execution.feedback_observation, {}),
        feedback_snapshot=None
        if feedback_snapshot is None
        else execution_feedback_snapshot_to_read(feedback_snapshot),
        instrument_states=[
            execution_instrument_state_to_read(item)
            for item in (instrument_states or [])
        ],
        model_inference=None if model_inference is None else model_inference_to_read(model_inference),
        workflow_runtime=workflow_runtime,
        note=execution.note,
        created_at=execution.created_at,
        updated_at=execution.updated_at,
    )


def campaign_to_read(campaign: Campaign) -> CampaignRead:
    return CampaignRead(
        id=campaign.id,
        name=campaign.name,
        objective=campaign.objective,
        stop_condition=campaign.stop_condition,
        current_round=campaign.current_round,
        max_round=campaign.max_round,
        global_status=campaign.global_status,
        metadata=_load_json(campaign.metadata_json, {}),
        created_at=campaign.created_at,
        updated_at=campaign.updated_at,
    )


def closed_loop_event_to_read(event: ClosedLoopEvent) -> ClosedLoopEventRead:
    return ClosedLoopEventRead(
        id=event.id,
        event_type=event.event_type,
        campaign_id=event.campaign_id,
        schedule_id=event.schedule_id,
        execution_id=event.execution_id,
        data_package_id=event.data_package_id,
        recommendation_id=event.recommendation_id,
        task_id=event.task_id,
        run_id=event.run_id,
        payload=_load_json(event.payload_json, {}),
        created_at=event.created_at,
    )


def data_package_to_read(data_package: DataPackage) -> DataPackageRead:
    return DataPackageRead(
        id=data_package.id,
        campaign_id=data_package.campaign_id,
        schedule_id=data_package.schedule_id,
        execution_id=data_package.execution_id,
        task_id=data_package.task_id,
        run_id=data_package.run_id,
        experiment_batch_id=data_package.experiment_batch_id,
        instrument_code=data_package.instrument_code,
        instrument_id=data_package.instrument_id,
        component_id=data_package.component_id,
        sample_id=data_package.sample_id,
        package_kind=data_package.package_kind,
        template_id=data_package.template_id,
        template_version=data_package.template_version,
        validation_status=data_package.validation_status,
        manage_db_record_id=data_package.manage_db_record_id,
        raw_files=_load_json(data_package.raw_files_json, {}),
        metadata=_load_json(data_package.metadata_json, {}),
        process_data=_load_json(data_package.process_data_json, {}),
        result_data=_load_json(data_package.result_data_json, {}),
        log_data=_load_json(data_package.log_data_json, {}),
        scheduler_observation=_load_json(data_package.scheduler_observation_json, {}),
        standard_data_ref=_load_json(data_package.standard_data_ref_json, {}),
        note=data_package.note,
        created_at=data_package.created_at,
        updated_at=data_package.updated_at,
    )


def data_package_ack_to_read(data_package: DataPackage) -> DataPackageAckRead:
    return DataPackageAckRead(
        id=data_package.id,
        execution_id=data_package.execution_id,
        experiment_batch_id=data_package.experiment_batch_id,
        instrument_code=data_package.instrument_code,
        component_id=data_package.component_id,
        sample_id=data_package.sample_id,
        validation_status=data_package.validation_status,
        manage_db_record_id=data_package.manage_db_record_id,
        created_at=data_package.created_at,
        updated_at=data_package.updated_at,
    )


def execution_progress_to_read(execution: Execution) -> ExecutionProgressRead:
    dispatch_payload = _load_json(execution.dispatch_payload, {})
    feedback_observation = _load_json(execution.feedback_observation, {})
    workflow_progress = feedback_observation.get("workflow_progress")
    if not isinstance(workflow_progress, dict):
        workflow_progress = {}
    instrument_uploads = feedback_observation.get("instrument_uploads")
    if not isinstance(instrument_uploads, dict):
        instrument_uploads = {}

    return ExecutionProgressRead(
        id=execution.id,
        schedule_id=execution.schedule_id,
        status=ExecutionStatus(execution.status),
        experiment_batch_id=dispatch_payload.get("experiment_batch_id"),
        experiment_round_index=dispatch_payload.get("experiment_round_index"),
        current_step=workflow_progress.get("current_step"),
        closed_loop_ready=bool(feedback_observation.get("closed_loop_ready")),
        completed_count=int(workflow_progress.get("completed_count") or 0),
        total_steps=int(workflow_progress.get("total_steps") or 0),
        completion_ratio=float(workflow_progress.get("completion_ratio") or 0.0),
        missing_steps=list(workflow_progress.get("missing_steps") or []),
        instrument_uploads=instrument_uploads,
        updated_at=execution.updated_at,
    )


def recommendation_record_to_read(
    recommendation: RecommendationRecord,
    *,
    schedule_name: str,
    schedule_status: str,
) -> RecommendationRead:
    latest_outcome = recommendation.latest_outcome
    return RecommendationRead(
        recommendation_id=recommendation.id,
        campaign_id=recommendation.campaign_id,
        task_id=recommendation.task_id,
        schedule_id=recommendation.schedule_id,
        schedule_name=schedule_name,
        schedule_status=ScheduleStatus(schedule_status),
        source_execution_id=recommendation.source_execution_id,
        latest_outcome=None if latest_outcome is None else ExecutionStatus(latest_outcome),
        confidence=round(float(recommendation.confidence), 2),
        summary=recommendation.summary,
        rationale=_load_json(recommendation.rationale_json, []),
        suggested_payload=_load_json(recommendation.suggested_payload_json, {}),
        model_id=recommendation.model_id,
        model_version=recommendation.model_version,
        input_dataset_version=recommendation.input_dataset_version,
        approval_status=recommendation.approval_status,
    )
