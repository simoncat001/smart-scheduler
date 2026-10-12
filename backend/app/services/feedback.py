from __future__ import annotations

from datetime import datetime, timedelta
import json
import logging

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.config import get_settings
from app.enums import ExecutionStatus, FeedbackOutcome, ScheduleStatus
from app.materials import enrich_feedback_observation, is_tizrnbv_hardness_loop, validate_tizrnbv_feedback
from app.models import Execution, FeedbackEvent, Schedule, utcnow
from app.schemas import FeedbackCreate
from app.services.exceptions import ConflictError, NotFoundError
from app.services.model_inference import ModelInferenceService
from app.services.state_projection import StateProjectionService
from app.workflows.ownership import (is_workflow_execution, bound_version_id,
                                     require_legacy_execution)


logger = logging.getLogger(__name__)


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _merge_observation(base: dict[str, object], patch: dict[str, object]) -> dict[str, object]:
    merged = dict(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_observation(merged[key], value)
        else:
            merged[key] = value
    return merged


class FeedbackService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.settings = get_settings()

    def submit_feedback(
        self,
        execution_id: str,
        feedback: FeedbackCreate,
        *,
        now: datetime | None = None,
        commit: bool = True,
    ) -> Execution:
        execution = self.session.scalar(
            select(Execution)
            .options(selectinload(Execution.schedule))
            .where(Execution.id == execution_id)
        )
        if execution is None:
            raise NotFoundError(f"execution {execution_id} not found")
        require_legacy_execution(self.session, execution.id)

        self._apply_feedback(
            execution=execution,
            outcome=feedback.outcome,
            quality_score=feedback.quality_score,
            latency_ms=feedback.latency_ms,
            observation=feedback.observation,
            note=feedback.note,
            now=now,
        )

        if commit:
            self.session.commit()
            self.session.refresh(execution)
        return execution

    def register_timeout_feedback(
        self,
        schedule: Schedule,
        *,
        now: datetime | None = None,
    ) -> Execution | None:
        if bound_version_id(self.session, schedule.id) is not None:
            return None
        execution = self.session.scalar(
            select(Execution)
            .where(
                Execution.schedule_id == schedule.id,
                Execution.status == ExecutionStatus.AWAITING_FEEDBACK.value,
            )
            .order_by(Execution.dispatched_at.desc())
        )
        if execution is None:
            schedule.awaiting_feedback = False
            schedule.feedback_deadline_at = None
            return None
        if is_workflow_execution(self.session, execution.id):
            return None

        self._apply_feedback(
            execution=execution,
            outcome=FeedbackOutcome.TIMEOUT,
            quality_score=0.0,
            latency_ms=schedule.feedback_timeout_seconds * 1000,
            observation={"timeout": True},
            note="feedback deadline exceeded",
            now=now,
        )
        return execution

    def finalize_workflow_execution(self, execution_id, feedback: FeedbackCreate, now: datetime) -> Execution:
        from app.workflows.models import NodeRun, WorkflowRun
        from app.workflows.projection import workflow_projection

        self.session.flush()
        schedule_id = self.session.scalar(select(Execution.schedule_id).where(Execution.id == execution_id))
        if schedule_id is None:
            raise NotFoundError("execution not found")
        self.session.scalar(select(Schedule).where(Schedule.id == schedule_id).with_for_update()
                            .execution_options(populate_existing=True))
        execution = self.session.scalar(select(Execution).where(Execution.id == execution_id).with_for_update()
                                        .execution_options(populate_existing=True))
        run = self.session.scalar(select(WorkflowRun).where(WorkflowRun.execution_id == execution_id)
                                  .with_for_update().execution_options(populate_existing=True))
        if run is None or run.status not in {"completed", "failed"}:
            raise ConflictError("workflow is not settled")
        expected = FeedbackOutcome.SUCCESS if run.status == "completed" else FeedbackOutcome.FAILURE
        if feedback.outcome != expected:
            raise ConflictError("feedback outcome does not match the workflow result")
        active = self.session.scalar(select(NodeRun.id).where(
            NodeRun.run_id == execution_id, NodeRun.status.in_(["queued", "accepted", "running", "needs_attention", "cancel_requested"]),
        ).limit(1).with_for_update())
        if active:
            raise ConflictError("workflow still has unresolved parallel attempts")
        if execution.status != "awaiting_feedback":
            if execution.status != self._execution_status(expected).value:
                raise ConflictError("execution outcome conflicts with workflow state")
            return execution
        _run, _graph, _nodes, _instruments, progress = workflow_projection(self.session, execution_id)
        observation = {"workflow_version_id": run.workflow_version_id, "workflow_progress": progress,
                       "closed_loop_ready": run.status == "completed"}
        self._apply_feedback(execution=execution, outcome=expected, quality_score=feedback.quality_score,
                             latency_ms=feedback.latency_ms, observation=observation,
                             note=feedback.note, now=now, workflow_finalization=True)
        self.session.flush()
        return execution

    def _apply_feedback(
        self,
        *,
        execution: Execution,
        outcome: FeedbackOutcome,
        quality_score: float | None,
        latency_ms: int | None,
        observation: dict[str, object],
        note: str,
        now: datetime | None,
        workflow_finalization: bool = False,
    ) -> None:
        if execution.status != ExecutionStatus.AWAITING_FEEDBACK.value:
            raise ConflictError(f"execution {execution.id} is already closed")
        if not workflow_finalization:
            require_legacy_execution(self.session, execution.id)

        schedule = execution.schedule
        if schedule is None:
            raise NotFoundError(f"schedule for execution {execution.id} not found")

        now = now or utcnow()
        dispatch_payload = json.loads(execution.dispatch_payload or "{}")
        existing_observation = json.loads(execution.feedback_observation or "{}")
        merged_input = _merge_observation(existing_observation, observation)
        normalized_observation = observation if workflow_finalization else enrich_feedback_observation(dispatch_payload, merged_input)
        if not workflow_finalization:
            try:
                validate_tizrnbv_feedback(dispatch_payload, normalized_observation, outcome.value)
            except ValueError as exc:
                raise ConflictError(str(exc)) from exc

        normalized_quality = self._normalize_quality(outcome, quality_score)
        signal = self._feedback_signal(
            schedule=schedule,
            outcome=outcome,
            quality_score=normalized_quality,
            latency_ms=latency_ms,
        )

        schedule.health_score = round(
            _clamp(schedule.health_score * 0.7 + signal * 0.3, 0.0, 1.0), 3
        )

        if outcome in {FeedbackOutcome.FAILURE, FeedbackOutcome.TIMEOUT}:
            schedule.consecutive_failures += 1
        else:
            schedule.consecutive_failures = 0

        schedule.dynamic_priority = self._dynamic_priority(
            base_priority=schedule.priority,
            health_score=schedule.health_score,
            consecutive_failures=schedule.consecutive_failures,
            outcome=outcome,
        )

        schedule.awaiting_feedback = False
        schedule.feedback_deadline_at = None

        execution.status = self._execution_status(outcome).value
        execution.feedback_score = normalized_quality
        execution.latency_ms = latency_ms
        execution.feedback_observation = json.dumps(
            normalized_observation, separators=(",", ":"), sort_keys=True
        )
        execution.note = note
        execution.closed_at = now

        event = FeedbackEvent(
            execution=execution,
            outcome=outcome.value,
            quality_score=normalized_quality,
            latency_ms=latency_ms,
            observation=json.dumps(
                normalized_observation, separators=(",", ":"), sort_keys=True
            ),
            note=note,
            created_at=now,
        )
        self.session.add(event)

        if schedule.consecutive_failures >= schedule.max_consecutive_failures:
            schedule.status = ScheduleStatus.PAUSED.value
            schedule.next_run_at = now + timedelta(seconds=schedule.base_interval_seconds)
            if not workflow_finalization:
                return
        if workflow_finalization and outcome == FeedbackOutcome.FAILURE:
            schedule.status = ScheduleStatus.PAUSED.value

        schedule.next_run_at = now + timedelta(
            seconds=self._next_interval_seconds(
                schedule=schedule,
                outcome=outcome,
                quality_score=normalized_quality,
            )
        )

        next_dispatch_plan = None if workflow_finalization else self._maybe_auto_dispatch_next_candidate(
            execution=execution,
            schedule=schedule,
            dispatch_payload=dispatch_payload,
            normalized_observation=normalized_observation,
            outcome=outcome,
            now=now,
        )
        next_execution = None
        if next_dispatch_plan is not None:
            normalized_observation = _merge_observation(
                normalized_observation,
                {
                    "auto_dispatch": {
                        "scheduled": True,
                        "triggered": False,
                        "next_execution_id": None,
                        "next_experiment_batch_id": next_dispatch_plan.get("experiment_batch_id"),
                        "next_round_index": next_dispatch_plan.get("experiment_round_index"),
                        "mat_model_candidate_rank": next_dispatch_plan.get(
                            "mat_model_candidate_rank"
                        ),
                        "scheduled_for": next_dispatch_plan.get("scheduled_for"),
                    }
                },
            )
            serialized_observation = json.dumps(
                normalized_observation, separators=(",", ":"), sort_keys=True
            )
            execution.feedback_observation = serialized_observation
            event.observation = serialized_observation

        from app.services.closed_loop import ClosedLoopService

        final_observation = json.loads(execution.feedback_observation or "{}")
        ClosedLoopService(self.session).record_feedback(
            schedule=schedule,
            execution=execution,
            outcome=outcome.value,
            observation=final_observation,
            next_execution=next_execution,
            now=now,
        )
        campaign_id = dispatch_payload.get("campaign_id")
        StateProjectionService(self.session).sync_execution(
            execution,
            campaign_id=None if campaign_id in (None, "") else str(campaign_id),
            now=now,
            workflow_authoritative=workflow_finalization,
        )

    def _maybe_auto_dispatch_next_candidate(
        self,
        *,
        execution: Execution,
        schedule: Schedule,
        dispatch_payload: dict[str, object],
        normalized_observation: dict[str, object],
        outcome: FeedbackOutcome,
        now: datetime,
    ) -> dict[str, object] | None:
        if schedule.status != ScheduleStatus.ACTIVE.value:
            return None
        if outcome not in {FeedbackOutcome.SUCCESS, FeedbackOutcome.PARTIAL}:
            return None
        if not is_tizrnbv_hardness_loop(dispatch_payload):
            return None
        if not bool(normalized_observation.get("closed_loop_ready")):
            return None

        from app.services.recommendations import RecommendationService
        from app.services.scheduler import SchedulerService

        inference_service = ModelInferenceService(self.session)
        inference_service.record_started(
            schedule_id=schedule.id,
            source_execution_id=execution.id,
            started_at=now,
        )
        recommendation = RecommendationService(self.session).recommend_for_schedule(schedule)
        recommendation_id = recommendation.recommendation_id
        if recommendation.suggested_payload.get("next_action") != "run_mat_model_candidate":
            inference_service.record_result(
                schedule_id=schedule.id,
                source_execution_id=execution.id,
                recommendation_id=recommendation_id,
                status="refreshed"
                if recommendation.suggested_payload.get("mat_model_online_refresh_applied")
                else "failed",
                summary=recommendation.summary,
                payload=recommendation.suggested_payload,
                completed_at=now,
                error=""
                if recommendation.suggested_payload.get("mat_model_online_refresh_applied")
                else "next_action is not run_mat_model_candidate",
            )
            return None
        if recommendation.suggested_payload.get("mat_model_candidate_rank") is None:
            inference_service.record_result(
                schedule_id=schedule.id,
                source_execution_id=execution.id,
                recommendation_id=recommendation_id,
                status="refreshed",
                summary=recommendation.summary,
                payload=recommendation.suggested_payload,
                completed_at=now,
                error="candidate rank is missing",
            )
            return None
        scheduled_for = now + timedelta(
            seconds=max(0, int(self.settings.mat_model_auto_dispatch_delay_seconds))
        )
        SchedulerService(self.session).update_payload_and_schedule(
            schedule,
            recommendation.suggested_payload,
            run_at=scheduled_for,
        )
        inference_service.record_result(
            schedule_id=schedule.id,
            source_execution_id=execution.id,
            recommendation_id=recommendation_id,
            status="dispatch_scheduled",
            summary=recommendation.summary,
            payload=recommendation.suggested_payload,
            completed_at=now,
            scheduled_for=scheduled_for,
        )
        return {
            "experiment_batch_id": recommendation.suggested_payload.get("experiment_batch_id"),
            "experiment_round_index": recommendation.suggested_payload.get(
                "experiment_round_index"
            ),
            "mat_model_candidate_rank": recommendation.suggested_payload.get(
                "mat_model_candidate_rank"
            ),
            "scheduled_for": scheduled_for.isoformat(),
        }

    @staticmethod
    def _execution_status(outcome: FeedbackOutcome) -> ExecutionStatus:
        mapping = {
            FeedbackOutcome.SUCCESS: ExecutionStatus.SUCCESS,
            FeedbackOutcome.PARTIAL: ExecutionStatus.PARTIAL,
            FeedbackOutcome.FAILURE: ExecutionStatus.FAILURE,
            FeedbackOutcome.TIMEOUT: ExecutionStatus.TIMEOUT,
        }
        return mapping[outcome]

    @staticmethod
    def _normalize_quality(
        outcome: FeedbackOutcome, quality_score: float | None
    ) -> float:
        if quality_score is not None:
            return round(_clamp(quality_score, 0.0, 1.0), 3)

        defaults = {
            FeedbackOutcome.SUCCESS: 1.0,
            FeedbackOutcome.PARTIAL: 0.55,
            FeedbackOutcome.FAILURE: 0.1,
            FeedbackOutcome.TIMEOUT: 0.0,
        }
        return defaults[outcome]

    @staticmethod
    def _feedback_signal(
        *,
        schedule: Schedule,
        outcome: FeedbackOutcome,
        quality_score: float,
        latency_ms: int | None,
    ) -> float:
        base = {
            FeedbackOutcome.SUCCESS: 1.0,
            FeedbackOutcome.PARTIAL: 0.55,
            FeedbackOutcome.FAILURE: 0.0,
            FeedbackOutcome.TIMEOUT: 0.0,
        }[outcome]

        signal = base * 0.6 + quality_score * 0.4
        if latency_ms is not None:
            latency_budget = schedule.feedback_timeout_seconds * 1000
            if latency_ms > latency_budget:
                signal -= 0.15
            elif latency_ms > latency_budget * 0.8:
                signal -= 0.05

        return round(_clamp(signal, 0.0, 1.0), 3)

    @staticmethod
    def _dynamic_priority(
        *,
        base_priority: int,
        health_score: float,
        consecutive_failures: int,
        outcome: FeedbackOutcome,
    ) -> int:
        urgency_boost = round((0.5 - health_score) * 6) + min(consecutive_failures, 3)
        if outcome in {FeedbackOutcome.FAILURE, FeedbackOutcome.TIMEOUT}:
            urgency_boost += 1
        elif outcome == FeedbackOutcome.SUCCESS and health_score > 0.85:
            urgency_boost -= 1
        return int(_clamp(base_priority + urgency_boost, 1, 10))

    @staticmethod
    def _next_interval_seconds(
        *,
        schedule: Schedule,
        outcome: FeedbackOutcome,
        quality_score: float,
    ) -> int:
        if outcome in {FeedbackOutcome.FAILURE, FeedbackOutcome.TIMEOUT}:
            retry_window = schedule.retry_delay_seconds * (
                2 ** max(schedule.consecutive_failures - 1, 0)
            )
            return int(
                _clamp(
                    retry_window,
                    schedule.min_interval_seconds,
                    min(schedule.base_interval_seconds, schedule.max_interval_seconds),
                )
            )

        if outcome == FeedbackOutcome.PARTIAL:
            adjusted = schedule.base_interval_seconds * 0.75
        elif quality_score >= 0.9 and schedule.health_score >= 0.85:
            adjusted = schedule.base_interval_seconds * 1.15
        elif quality_score >= 0.7:
            adjusted = schedule.base_interval_seconds
        else:
            adjusted = schedule.base_interval_seconds * 0.8

        return int(
            _clamp(adjusted, schedule.min_interval_seconds, schedule.max_interval_seconds)
        )
