from datetime import UTC, datetime
import json
from uuid import uuid4

from app.models import Execution, Schedule
from app.workflows.definitions import DefinitionService
from app.workflows.models import MessageOutbox, NodeAttempt, NodeRun
from app.workflows.schemas import MessageEnvelope, WorkflowGraph
from app.workflows.templates import default_materials_graph
from sqlalchemy import select


NOW = datetime(2026, 10, 10, 8, 0)


def material_graph():
    data = default_materials_graph().model_dump(mode="json", by_alias=True)
    data["nodes"][-1]["config"]["model_version"] = "test-version-1"
    return WorkflowGraph.model_validate(data)


def make_execution(session, graph=None):
    schedule = Schedule(
        name="Isolated workflow test", target="workflow/test", status="active",
        base_interval_seconds=60, min_interval_seconds=10, max_interval_seconds=300,
        retry_delay_seconds=30, feedback_timeout_seconds=60,
        max_consecutive_failures=3, next_run_at=NOW,
    )
    session.add(schedule)
    session.flush()
    execution = Execution(schedule_id=schedule.id, scheduled_for=NOW, dispatched_at=NOW)
    session.add(execution)
    service = DefinitionService(session)
    draft = service.create("Test workflow", graph or material_graph())
    version = service.publish(draft.id, draft.revision)
    execution.dispatch_payload = json.dumps({"workflow_version_id": version.id})
    session.flush()
    return execution, version


def node_run(session, run_id, node_id):
    return session.scalar(select(NodeRun).where(NodeRun.run_id == run_id, NodeRun.node_id == node_id))


def command_for(session, run_id, node_id):
    node = node_run(session, run_id, node_id)
    attempt = session.scalar(select(NodeAttempt).where(
        NodeAttempt.node_run_id == node.id, NodeAttempt.attempt == node.attempt,
    ))
    return MessageEnvelope.model_validate_json(attempt.command_json)


def event_for(session, run_id, owner_node_id, outputs=None, kind="node.completed", **updates):
    command = command_for(session, run_id, owner_node_id)
    data = command.model_dump(mode="python")
    data.update(message_id=str(uuid4()), message_type=kind,
                occurred_at=NOW.replace(tzinfo=UTC), expires_at=None,
                payload={"outputs": outputs} if outputs is not None else {})
    data.update(updates)
    return MessageEnvelope.model_validate(data)


def outboxes(session):
    return list(session.scalars(select(MessageOutbox).order_by(MessageOutbox.created_at)))
