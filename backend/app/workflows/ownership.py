from sqlalchemy import exists, select

from app.models import Execution, Schedule
from app.services.exceptions import ConflictError
from app.workflows.models import WorkflowRun, WorkflowScheduleBinding


def legacy_schedule_clause():
    return ~exists(select(WorkflowScheduleBinding.schedule_id).where(
        WorkflowScheduleBinding.schedule_id == Schedule.id,
    ))


def legacy_execution_clause():
    return ~exists(select(WorkflowRun.execution_id).where(WorkflowRun.execution_id == Execution.id))


def bound_version_id(session, schedule_id, *, lock=False):
    statement = select(WorkflowScheduleBinding.workflow_version_id).where(
        WorkflowScheduleBinding.schedule_id == schedule_id,
    )
    if lock:
        statement = statement.with_for_update()
    return session.scalar(statement)


def is_workflow_execution(session, execution_id):
    return session.scalar(select(WorkflowRun.execution_id).where(WorkflowRun.execution_id == execution_id)) is not None


def require_legacy_execution(session, execution_id):
    if is_workflow_execution(session, execution_id):
        raise ConflictError("workflow-owned execution requires node-aware workflow results, not legacy feedback/upload/inference")
