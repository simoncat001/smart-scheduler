from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
import json
import logging
import re
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.enums import ExecutionStatus, ScheduleStatus
from app.materials import (
    QUATERNARY_KEYS,
    build_dispatch_payload,
    normalize_tizrnbv_payload,
    summarize_workflow_progress,
)
from app.models import Execution, Schedule, utcnow
from app.schemas import ScheduleCreate, ScheduleUpdate
from app.services.exceptions import ConflictError, NotFoundError
from app.services.feedback import FeedbackService
from app.workflows.ownership import bound_version_id, legacy_schedule_clause


logger = logging.getLogger(__name__)
NON_ALNUM_PATTERN = re.compile(r"[^A-Z0-9]+")


class SchedulerService:
    def __init__(self, session: Session, *, workflow_topic_prefix: str = "scheduler") -> None:
        self.session = session
        self.workflow_topic_prefix = workflow_topic_prefix

    def _lock_schedule(self, schedule_id):
        self.session.flush()
        schedule = self.session.scalar(select(Schedule).where(Schedule.id == schedule_id)
                                       .with_for_update().execution_options(populate_existing=True))
        if schedule is None:
            raise NotFoundError("schedule not found")
        return schedule

    def _pause_other_active_schedules(self, active_schedule_id: str) -> None:
        self._lock_schedule(active_schedule_id)
        if bound_version_id(self.session, active_schedule_id, lock=True) is not None:
            return
        other_schedules = list(
            self.session.scalars(
                select(Schedule).where(
                    Schedule.id != active_schedule_id,
                    Schedule.status == ScheduleStatus.ACTIVE.value,
                    legacy_schedule_clause(),
                )
                .order_by(Schedule.id).with_for_update().execution_options(populate_existing=True)
            )
        )
        for schedule in other_schedules:
            if bound_version_id(self.session, schedule.id, lock=True) is None:
                schedule.status = ScheduleStatus.PAUSED.value

    def create_schedule(
        self, payload: ScheduleCreate, *, now: datetime | None = None
    ) -> Schedule:
        from app.services.closed_loop import ClosedLoopService

        now = now or utcnow()
        min_interval = payload.min_interval_seconds or max(
            1, payload.base_interval_seconds // 2
        )
        max_interval = payload.max_interval_seconds or max(
            payload.base_interval_seconds, payload.base_interval_seconds * 4
        )

        normalized_payload = normalize_tizrnbv_payload(dict(payload.payload))

        schedule = Schedule(
            name=payload.name,
            description=payload.description,
            target=payload.target,
            payload=json.dumps(normalized_payload, separators=(",", ":"), sort_keys=True),
            base_interval_seconds=payload.base_interval_seconds,
            min_interval_seconds=min_interval,
            max_interval_seconds=max_interval,
            retry_delay_seconds=payload.retry_delay_seconds,
            feedback_timeout_seconds=payload.feedback_timeout_seconds,
            max_consecutive_failures=payload.max_consecutive_failures,
            priority=payload.priority,
            dynamic_priority=payload.priority,
            next_run_at=now + timedelta(seconds=payload.start_in_seconds),
        )
        self.session.add(schedule)
        self.session.flush()
        campaign = ClosedLoopService(self.session).ensure_campaign_for_schedule(
            schedule,
            normalized_payload,
            now=now,
        )
        normalized_payload["task_id"] = schedule.id
        normalized_payload["campaign_id"] = campaign.id
        schedule.payload = json.dumps(
            normalized_payload, separators=(",", ":"), sort_keys=True
        )
        self.session.commit()
        self.session.refresh(schedule)
        return schedule

    def list_schedules(self) -> list[Schedule]:
        return list(
            self.session.scalars(
                select(Schedule).order_by(
                    Schedule.dynamic_priority.desc(), Schedule.next_run_at.asc()
                )
            )
        )

    def get_schedule(self, schedule_id: str) -> Schedule:
        schedule = self.session.get(Schedule, schedule_id)
        if schedule is None:
            raise NotFoundError(f"schedule {schedule_id} not found")
        return schedule

    def pause_schedule(self, schedule_id: str) -> Schedule:
        schedule = self._lock_schedule(schedule_id)
        schedule.status = ScheduleStatus.PAUSED.value
        if bound_version_id(self.session, schedule.id, lock=True) is not None:
            from app.workflows.engine import WorkflowEngine
            from app.workflows.models import WorkflowRun

            run_ids = list(self.session.scalars(select(WorkflowRun.execution_id).join(Execution).where(
                Execution.schedule_id == schedule.id, WorkflowRun.status == "running",
            ).with_for_update()))
            for run_id in run_ids:
                WorkflowEngine(self.session, self.workflow_topic_prefix).pause(run_id, utcnow())
        self.session.commit()
        self.session.refresh(schedule)
        return schedule

    def resume_schedule(self, schedule_id: str, *, now: datetime | None = None) -> Schedule:
        schedule = self._lock_schedule(schedule_id)
        now = now or utcnow()
        schedule.status = ScheduleStatus.ACTIVE.value
        self._pause_other_active_schedules(schedule.id)
        if not schedule.awaiting_feedback and schedule.next_run_at > now:
            pass
        elif not schedule.awaiting_feedback:
            schedule.next_run_at = now
        if bound_version_id(self.session, schedule.id, lock=True) is not None:
            from app.workflows.engine import WorkflowEngine
            from app.workflows.models import WorkflowRun

            run_ids = list(self.session.scalars(select(WorkflowRun.execution_id).join(Execution).where(
                Execution.schedule_id == schedule.id, WorkflowRun.status == "paused",
            ).with_for_update()))
            for run_id in run_ids:
                WorkflowEngine(self.session, self.workflow_topic_prefix).resume(run_id, now)
        self.session.commit()
        self.session.refresh(schedule)
        return schedule

    def update_schedule(
        self,
        schedule_id: str,
        payload: ScheduleUpdate,
        *,
        now: datetime | None = None,
    ) -> Schedule:
        schedule = self._lock_schedule(schedule_id)
        now = now or utcnow()

        if payload.payload is not None:
            current_payload = json.loads(schedule.payload or "{}")
            normalized_payload = normalize_tizrnbv_payload(dict(payload.payload))
            normalized_payload.setdefault("task_id", current_payload.get("task_id", schedule.id))
            normalized_payload.setdefault("campaign_id", current_payload.get("campaign_id"))
            schedule.payload = json.dumps(
                normalized_payload, separators=(",", ":"), sort_keys=True
            )
        if payload.description is not None:
            schedule.description = payload.description
        if payload.priority is not None:
            schedule.priority = payload.priority
            schedule.dynamic_priority = payload.priority
        if payload.run_now and not schedule.awaiting_feedback:
            schedule.next_run_at = now

        self.session.commit()
        self.session.refresh(schedule)
        return schedule

    def list_executions(
        self,
        *,
        schedule_id: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[Execution]:
        query = select(Execution).order_by(Execution.dispatched_at.desc()).limit(limit)
        if schedule_id:
            query = query.where(Execution.schedule_id == schedule_id)
        if status:
            query = query.where(Execution.status == status)
        return list(self.session.scalars(query))

    def manual_trigger(self, schedule_id: str, *, now: datetime | None = None) -> Execution:
        schedule = self._lock_schedule(schedule_id)
        now = now or utcnow()
        if bound_version_id(self.session, schedule_id, lock=True) is not None:
            from app.workflows.scheduling import WorkflowSchedulingService

            run = WorkflowSchedulingService(self.session, self.workflow_topic_prefix).trigger(schedule_id, now)
            execution = self.session.get(Execution, run.execution_id)
            self.session.commit()
            return execution
        if schedule.status != ScheduleStatus.ACTIVE.value:
            raise ConflictError(f"schedule {schedule.id} is not active")
        if schedule.awaiting_feedback:
            raise ConflictError(f"schedule {schedule.id} is waiting for feedback")

        self._pause_other_active_schedules(schedule.id)
        execution = self._dispatch(schedule, now=now, scheduled_for=now)
        self.session.commit()
        self.session.refresh(execution)
        return execution

    def update_payload_and_dispatch(
        self,
        schedule: Schedule,
        payload: dict[str, object],
        *,
        now: datetime | None = None,
    ) -> Execution:
        now = now or utcnow()
        schedule = self._lock_schedule(schedule.id)
        if bound_version_id(self.session, schedule.id, lock=True) is not None:
            raise ConflictError("workflow candidates must be applied by the pinned workflow decision node")
        if schedule.status != ScheduleStatus.ACTIVE.value:
            raise ConflictError(f"schedule {schedule.id} is not active")
        if schedule.awaiting_feedback:
            raise ConflictError(f"schedule {schedule.id} is waiting for feedback")

        normalized_payload = normalize_tizrnbv_payload(dict(payload))
        current_payload = json.loads(schedule.payload or "{}")
        normalized_payload.setdefault("task_id", current_payload.get("task_id", schedule.id))
        normalized_payload.setdefault("campaign_id", current_payload.get("campaign_id"))
        schedule.payload = json.dumps(
            normalized_payload, separators=(",", ":"), sort_keys=True
        )
        self._pause_other_active_schedules(schedule.id)
        execution = self._dispatch(schedule, now=now, scheduled_for=now)
        self.session.flush()
        return execution

    def update_payload_and_schedule(
        self,
        schedule: Schedule,
        payload: dict[str, object],
        *,
        run_at: datetime,
    ) -> Schedule:
        schedule = self._lock_schedule(schedule.id)
        if bound_version_id(self.session, schedule.id, lock=True) is not None:
            raise ConflictError("workflow candidates must be applied by the pinned workflow decision node")
        if schedule.status != ScheduleStatus.ACTIVE.value:
            raise ConflictError(f"schedule {schedule.id} is not active")
        if schedule.awaiting_feedback:
            raise ConflictError(f"schedule {schedule.id} is waiting for feedback")

        normalized_payload = normalize_tizrnbv_payload(dict(payload))
        current_payload = json.loads(schedule.payload or "{}")
        normalized_payload.setdefault("task_id", current_payload.get("task_id", schedule.id))
        normalized_payload.setdefault("campaign_id", current_payload.get("campaign_id"))
        schedule.payload = json.dumps(
            normalized_payload, separators=(",", ":"), sort_keys=True
        )
        self._pause_other_active_schedules(schedule.id)
        schedule.next_run_at = run_at
        self.session.flush()
        return schedule

    def dispatch_due_schedules(
        self, *, now: datetime | None = None, limit: int = 100
    ) -> tuple[list[Execution], int]:
        now = now or utcnow()
        timed_out_count = self._resolve_timeouts(now)
        due_schedules = list(
            self.session.scalars(
                select(Schedule)
                .where(
                    Schedule.status == ScheduleStatus.ACTIVE.value,
                    Schedule.awaiting_feedback.is_(False),
                    Schedule.next_run_at <= now,
                    legacy_schedule_clause(),
                )
                .order_by(Schedule.dynamic_priority.desc(), Schedule.next_run_at.asc())
                .limit(limit)
                .with_for_update(skip_locked=True).execution_options(populate_existing=True)
            )
        )

        executions: list[Execution] = []
        if due_schedules:
            schedule = due_schedules[0]
            if bound_version_id(self.session, schedule.id, lock=True) is None:
                self._pause_other_active_schedules(schedule.id)
                executions.append(
                    self._dispatch(
                        schedule,
                        now=now,
                        scheduled_for=schedule.next_run_at,
                    )
                )

        self.session.commit()
        for execution in executions:
            self.session.refresh(execution)
        return executions, timed_out_count

    def _dispatch(
        self, schedule: Schedule, *, now: datetime, scheduled_for: datetime, workflow_version=None
    ) -> Execution:
        from app.services.closed_loop import ClosedLoopService
        from app.services.model_inference import ModelInferenceService
        from app.services.state_projection import StateProjectionService

        schedule = self._lock_schedule(schedule.id)
        binding_id = bound_version_id(self.session, schedule.id, lock=True)
        if (workflow_version is None and binding_id is not None) or (
            workflow_version is not None and binding_id != workflow_version.id
        ):
            raise ConflictError("workflow dispatch requires its current published binding")
        dispatch_payload = build_dispatch_payload(json.loads(schedule.payload or "{}"))
        if workflow_version is None:
            round_index = self._next_round_index(schedule.id)
        else:
            round_index = len(list(self.session.scalars(select(Execution.id).where(
                Execution.schedule_id == schedule.id,
            ).with_for_update()))) + 1
            dispatch_payload["workflow_version_id"] = workflow_version.id
        batch_root = self._experiment_batch_root(schedule, dispatch_payload)
        if workflow_version is not None:
            batch_root = f"{batch_root}-{schedule.id.replace('-', '').upper()}"
        execution_id = str(uuid4())
        dispatch_payload["experiment_round_index"] = round_index
        dispatch_payload["experiment_round_label"] = f"Round {round_index:02d}"
        dispatch_payload["experiment_batch_root"] = batch_root
        dispatch_payload["experiment_batch_id"] = f"{batch_root}-R{round_index:02d}"
        dispatch_payload["sample_id"] = self._round_sample_id(
            str(dispatch_payload["experiment_batch_id"])
        )
        dispatch_payload["sample_plan"] = self._runtime_sample_plan(dispatch_payload)
        dispatch_payload["task_id"] = str(dispatch_payload.get("task_id") or schedule.id)
        dispatch_payload["workflow_run"] = (summarize_workflow_progress(dispatch_payload, {})
                                              if workflow_version is None else
                                              {"steps": [], "completed_count": 0, "is_complete": False})
        campaign = ClosedLoopService(self.session).ensure_campaign_for_schedule(
            schedule,
            dispatch_payload,
            now=now,
        )
        dispatch_payload["campaign_id"] = str(
            dispatch_payload.get("campaign_id") or campaign.id
        )
        dispatch_payload["run_id"] = execution_id
        execution = Execution(
            id=execution_id,
            schedule=schedule,
            schedule_id=schedule.id,
            scheduled_for=scheduled_for,
            dispatched_at=now,
            attempt=schedule.consecutive_failures + 1,
            status=ExecutionStatus.AWAITING_FEEDBACK.value,
            dispatch_payload=json.dumps(dispatch_payload, separators=(",", ":"), sort_keys=True),
        )
        self.session.add(execution)
        # MySQL checks the closed_loop_events.execution_id foreign key immediately,
        # so persist the execution row before recording dispatch lifecycle events.
        self.session.flush()

        schedule.awaiting_feedback = True
        schedule.last_dispatched_at = now
        schedule.feedback_deadline_at = (now + timedelta(seconds=schedule.feedback_timeout_seconds)
                                        if workflow_version is None else None)
        if schedule.feedback_deadline_at is not None:
            schedule.next_run_at = schedule.feedback_deadline_at
        ClosedLoopService(self.session).record_dispatch(
            schedule=schedule,
            execution=execution,
            dispatch_payload=dispatch_payload,
            now=now,
        )
        if workflow_version is None:
            StateProjectionService(self.session).sync_execution(execution, campaign_id=campaign.id, now=now)
        source_execution_id = dispatch_payload.get("recommended_from_execution_id")
        if source_execution_id and workflow_version is None:
            ModelInferenceService(self.session).mark_dispatched(
                source_execution_id=str(source_execution_id),
                dispatched_execution_id=execution.id,
                dispatched_at=now,
            )
        return execution

    def _next_round_index(self, schedule_id: str) -> int:
        total_executions = self.session.scalar(
            select(func.count()).select_from(Execution).where(Execution.schedule_id == schedule_id)
        )
        return int(total_executions or 0) + 1

    @staticmethod
    def _experiment_batch_root(schedule: Schedule, payload: dict[str, object]) -> str:
        raw_value = str(
            payload.get("sample_batch")
            or payload.get("experiment_batch_root")
            or schedule.name
            or schedule.id[:8]
        ).upper()
        normalized = NON_ALNUM_PATTERN.sub("-", raw_value).strip("-")
        return normalized[:48] or f"SCHED-{schedule.id[:8].upper()}"

    @staticmethod
    def _round_sample_id(experiment_batch_id: str) -> str:
        normalized_batch = NON_ALNUM_PATTERN.sub("-", experiment_batch_id.upper()).strip("-")
        return f"{normalized_batch}-S01" if normalized_batch else "ROUND-S01"

    def _runtime_sample_plan(self, dispatch_payload: dict[str, object]) -> dict[str, object]:
        round_sample_id = str(dispatch_payload.get("sample_id") or "").strip()
        batch_plan = dispatch_payload.get("predicted_batch_plan")
        samples: list[dict[str, object]] = []
        rows: int | None = None
        cols: int | None = None
        total_samples = 1

        if isinstance(batch_plan, dict):
            try:
                rows = int(batch_plan.get("rows")) if batch_plan.get("rows") is not None else None
            except (TypeError, ValueError):
                rows = None
            try:
                cols = int(batch_plan.get("cols")) if batch_plan.get("cols") is not None else None
            except (TypeError, ValueError):
                cols = None
            try:
                total_samples = int(batch_plan.get("total_samples") or total_samples)
            except (TypeError, ValueError):
                total_samples = 1

            raw_samples = batch_plan.get("samples")
            if isinstance(raw_samples, list):
                for index, sample in enumerate(raw_samples, start=1):
                    if not isinstance(sample, dict):
                        continue
                    sample_code = str(
                        sample.get("sample_code")
                        or sample.get("sample_id")
                        or f"S{index:03d}"
                    ).strip()
                    entry: dict[str, object] = {
                        "sample_code": sample_code,
                        "sample_id": f"{round_sample_id}-{sample_code}",
                    }
                    for key in ("grid_row", "grid_col", "snake_index"):
                        value = sample.get(key)
                        if value is not None:
                            entry[key] = value
                    composition = {
                        key: round(float(sample[key]), 3)
                        for key in QUATERNARY_KEYS
                        if sample.get(key) is not None
                    }
                    if composition:
                        entry["composition_at_percent"] = composition
                    samples.append(entry)

        if not samples:
            samples.append(
                {
                    "sample_code": "S01",
                    "sample_id": round_sample_id,
                }
            )

        return {
            "round_sample_id": round_sample_id,
            "rows": rows,
            "cols": cols,
            "total_samples": max(total_samples, len(samples)),
            "samples": samples,
        }

    def _resolve_timeouts(self, now: datetime) -> int:
        waiting = list(
            self.session.scalars(
                select(Schedule).where(
                    Schedule.awaiting_feedback.is_(True),
                    Schedule.feedback_deadline_at.is_not(None),
                    Schedule.feedback_deadline_at <= now,
                    legacy_schedule_clause(),
                )
            )
        )
        feedback_service = FeedbackService(self.session)
        timed_out_count = 0
        for schedule in waiting:
            execution = feedback_service.register_timeout_feedback(schedule, now=now)
            if execution is not None:
                timed_out_count += 1
        return timed_out_count


class BackgroundSchedulerLoop:
    def __init__(self, session_factory, poll_interval_seconds: int) -> None:
        self.session_factory = session_factory
        self.poll_interval_seconds = poll_interval_seconds
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                with self.session_factory() as session:
                    SchedulerService(session).dispatch_due_schedules()
            except Exception:
                logger.exception("scheduler loop iteration failed")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self.poll_interval_seconds
                )
            except asyncio.TimeoutError:
                continue
