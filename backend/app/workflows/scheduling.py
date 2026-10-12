from sqlalchemy import select

from app.enums import ScheduleStatus
from app.models import Execution, Schedule
from app.services.exceptions import ConflictError, NotFoundError
from app.workflows.engine import db_time
from app.workflows.models import WorkflowRun, WorkflowVersion
from app.workflows.ownership import bound_version_id, legacy_schedule_clause


class WorkflowSchedulingService:
    """Schedule-row locks serialize every new round; callers own the transaction."""

    def __init__(self, session, topic_prefix="scheduler"):
        self.session, self.topic_prefix = session, topic_prefix

    def _locked_schedule(self, schedule_id, *, skip_locked=False):
        self.session.flush()
        schedule = self.session.scalar(select(Schedule).where(Schedule.id == schedule_id)
                                       .with_for_update(skip_locked=skip_locked)
                                       .execution_options(populate_existing=True))
        return schedule

    def _dispatch_locked(self, schedule, now, *, manual=False, skip_busy=False):
        from app.services.scheduler import SchedulerService
        from app.workflows.engine import WorkflowEngine

        version_id = bound_version_id(self.session, schedule.id, lock=True)
        if version_id is None:
            raise ConflictError("schedule has no published workflow binding")
        if schedule.status != ScheduleStatus.ACTIVE.value:
            raise ConflictError("workflow schedule is not active")
        waiting = self.session.scalar(select(Execution.id).where(
            Execution.schedule_id == schedule.id, Execution.status == "awaiting_feedback",
        ).limit(1).with_for_update())
        unresolved = self.session.scalar(select(WorkflowRun.execution_id).join(Execution).where(
            Execution.schedule_id == schedule.id,
            WorkflowRun.status.not_in(["completed", "failed", "cancelled"]),
        ).limit(1).with_for_update())
        if schedule.awaiting_feedback or waiting or unresolved:
            if skip_busy:
                return None
            raise ConflictError("workflow schedule already has an active round")
        version = self.session.get(WorkflowVersion, version_id)
        if version is None:
            raise NotFoundError("published workflow version not found")
        execution = SchedulerService(self.session)._dispatch(
            schedule, now=now, scheduled_for=now if manual else schedule.next_run_at,
            workflow_version=version,
        )
        return WorkflowEngine(self.session, self.topic_prefix).start(execution, version, now)

    def trigger(self, schedule_id, now):
        schedule = self._locked_schedule(schedule_id)
        if schedule is None:
            raise NotFoundError("schedule not found")
        return self._dispatch_locked(schedule, db_time(now), manual=True)

    def tick(self, now, limit=100):
        if not 1 <= limit <= 1000:
            raise ValueError("invalid workflow schedule batch size")
        now = db_time(now)
        candidates = list(self.session.scalars(select(Schedule.id).where(
            ~legacy_schedule_clause(), Schedule.status == "active",
            Schedule.awaiting_feedback.is_(False), Schedule.next_run_at <= now,
        ).order_by(Schedule.dynamic_priority.desc(), Schedule.next_run_at, Schedule.id).limit(limit)))
        runs = []
        # Canonical parent lock order also covers overlapping batches with different snapshots.
        for schedule_id in sorted(candidates):
            schedule = self._locked_schedule(schedule_id, skip_locked=True)
            if schedule is None or schedule.status != "active" or schedule.awaiting_feedback or schedule.next_run_at > now:
                continue
            if bound_version_id(self.session, schedule_id, lock=True) is None:
                continue
            run = self._dispatch_locked(schedule, now, skip_busy=True)
            if run is not None:
                runs.append(run)
        self.session.flush()
        return runs
