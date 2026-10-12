from copy import deepcopy
from datetime import timedelta
import importlib
import json
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.database import Database
from app.services.exceptions import ConflictError
from app.workflows.models import NodeAttempt, NodeRun, ResourceReservation, WorkflowRun
from app.workflows.schemas import EventDisposition, WorkflowGraph
from workflow_fixtures import NOW, command_for, event_for, make_execution, node_run, outboxes


@pytest.fixture()
def session(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'runtime.db'}")
    db.create_schema()
    with db.session_factory() as session:
        yield session
    db.dispose()


def start(session, graph=None):
    module = importlib.import_module("app.workflows.engine")
    execution, version = make_execution(session, graph)
    engine = module.WorkflowEngine(session)
    run = engine.start(execution, version, NOW)
    return engine, run


def complete_preparation(session, engine, run):
    return engine.apply_event(event_for(session, run.execution_id, "magnetron_sputtering",
                                       {"sample": {"sample_id": "sample-a"}}), NOW)


def test_start_queues_preparation_without_simulating_success(session):
    engine, run = start(session)
    assert node_run(session, run.execution_id, "magnetron_sputtering").status == "queued"
    assert node_run(session, run.execution_id, "xrd").status == "waiting_dependencies"
    assert len(outboxes(session)) == 1
    assert list(session.scalars(select(ResourceReservation))) == []
    engine.start(session.get(importlib.import_module("app.models").Execution, run.execution_id),
                 session.get(importlib.import_module("app.workflows.models").WorkflowVersion,
                             run.workflow_version_id), NOW)
    assert len(outboxes(session)) == 1


def test_preparation_result_releases_all_three_parallel_nodes(session):
    engine, run = start(session)
    assert complete_preparation(session, engine, run) == EventDisposition.APPLIED
    for node_id in ("xrd", "eds", "nanoindentation"):
        assert node_run(session, run.execution_id, node_id).status == "queued"
        command = command_for(session, run.execution_id, node_id)
        assert command.payload["inputs"] == {"sample": {"sample_id": "sample-a"}}
        assert command.payload["node_type"] == "data_upload"
    assert len(outboxes(session)) == 4
    assert node_run(session, run.execution_id, "model_inference").attempt == 0


def test_all_three_valid_results_create_exactly_one_pinned_mat_command(session):
    engine, run = start(session)
    complete_preparation(session, engine, run)
    for node_id in ("xrd", "eds", "nanoindentation"):
        event = event_for(session, run.execution_id, node_id, {"result": {"owner": node_id}})
        assert engine.apply_event(event, NOW) == EventDisposition.APPLIED
        assert engine.apply_event(event, NOW) == EventDisposition.IGNORED_STALE
    assert len(outboxes(session)) == 5
    command = command_for(session, run.execution_id, "model_inference")
    assert command.payload["config"]["model_version"] == "test-version-1"
    assert command.payload["inputs"] == {name: {"owner": name} for name in ("xrd", "eds", "nanoindentation")}
    assert node_run(session, run.execution_id, "gate").status == "completed"
    engine.advance(run.execution_id, NOW)
    assert len(outboxes(session)) == 5
    engine.apply_event(event_for(session, run.execution_id, "model_inference",
                                 {"candidate": {"value": 13}}), NOW)
    assert run.status == "completed"


@pytest.mark.parametrize("outputs", [{}, {"sample": {}}, {"sample": "invalid"},
                                    {"sample": {"ok": 1}, "unknown": 1}])
def test_invalid_outputs_do_not_complete_or_open_downstream(session, outputs):
    engine, run = start(session)
    assert engine.apply_event(event_for(session, run.execution_id, "magnetron_sputtering", outputs), NOW) == EventDisposition.QUARANTINED
    assert node_run(session, run.execution_id, "magnetron_sputtering").status == "queued"
    assert len(outboxes(session)) == 1


@pytest.mark.parametrize("updates", [
    {"command_id": str(uuid4())}, {"workflow_version_id": str(uuid4())},
    {"node_run_id": str(uuid4())}, {"gateway_id": "another-gateway"},
    {"adapter_id": "kafka_gateway"}, {"node_id": "eds"}, {"attempt": 2},
])
def test_event_requires_exact_node_attempt_and_executor_ownership(session, updates):
    engine, run = start(session)
    event = event_for(session, run.execution_id, "magnetron_sputtering", {"sample": {"ok": 1}}, **updates)
    assert engine.apply_event(event, NOW) == EventDisposition.QUARANTINED
    assert len(outboxes(session)) == 1


def test_late_started_and_progress_cannot_regress_completed_node(session):
    engine, run = start(session)
    complete_preparation(session, engine, run)
    for kind in ("node.started", "node.progress", "node.failed"):
        event = event_for(session, run.execution_id, "magnetron_sputtering", kind=kind)
        assert engine.apply_event(event, NOW) == EventDisposition.IGNORED_STALE
    assert node_run(session, run.execution_id, "magnetron_sputtering").status == "completed"


def test_acceptance_is_distinct_from_broker_delivery_and_progress(session):
    engine, run = start(session)
    node_id = "magnetron_sputtering"
    for kind, status in (("node.accepted", "accepted"), ("node.started", "running")):
        assert engine.apply_event(event_for(session, run.execution_id, node_id, kind=kind), NOW) == EventDisposition.APPLIED
        assert node_run(session, run.execution_id, node_id).status == status
    engine.apply_event(event_for(session, run.execution_id, node_id, kind="node.accepted"), NOW)
    assert node_run(session, run.execution_id, node_id).status == "running"


def test_pause_records_results_but_prevents_new_commands_until_resume(session):
    engine, run = start(session)
    engine.pause(run.execution_id, NOW)
    complete_preparation(session, engine, run)
    assert run.status == "paused"
    assert len(outboxes(session)) == 1
    engine.resume(run.execution_id, NOW)
    assert len(outboxes(session)) == 4


def test_timeout_never_creates_a_new_attempt_or_command(session):
    engine, run = start(session)
    command = command_for(session, run.execution_id, "magnetron_sputtering")
    assert engine.expire(NOW + timedelta(seconds=61)) == 1
    assert run.status == "needs_attention"
    engine.advance(run.execution_id, NOW + timedelta(hours=2))
    engine.expire(NOW + timedelta(hours=2))
    assert command_for(session, run.execution_id, "magnetron_sputtering").command_id == command.command_id
    assert len(outboxes(session)) == 1
    assert len(list(session.scalars(select(NodeAttempt)))) == 1
    assert list(session.scalars(select(ResourceReservation))) == []


def test_known_late_completion_resolves_uncertainty_without_restarting_device(session):
    engine, run = start(session)
    engine.expire(NOW + timedelta(seconds=61))
    assert complete_preparation(session, engine, run) == EventDisposition.APPLIED
    assert run.status == "running"
    assert len(outboxes(session)) == 4


def test_failed_parallel_node_stops_downstream_but_preserves_sibling_results(session):
    engine, run = start(session)
    complete_preparation(session, engine, run)
    event = event_for(session, run.execution_id, "xrd", kind="node.failed")
    event = event.model_copy(update={"payload": {"error": "measurement failed"}})
    assert engine.apply_event(event, NOW) == EventDisposition.APPLIED
    for node_id in ("eds", "nanoindentation"):
        assert engine.apply_event(event_for(session, run.execution_id, node_id, {"result": {"ok": 1}}), NOW) == EventDisposition.APPLIED
    assert run.status == "failed"
    assert node_run(session, run.execution_id, "eds").status == "completed"
    assert node_run(session, run.execution_id, "model_inference").attempt == 0


def test_same_type_nodes_do_not_share_attempts_or_results(session):
    graph = WorkflowGraph.model_validate({"nodes": [
        {"id": name, "type": "data_upload", "label": name,
         "config": {"instrument_code": "xrd"},
         "executor": {"kind": "device", "gateway_id": "same-gateway", "adapter_id": "kafka_gateway"},
         "outputs": {"value": {"schema": {"type": "number"}}}}
        for name in ("xrd_a", "xrd_b")
    ]})
    engine, run = start(session, graph)
    assert len(outboxes(session)) == 2
    assert command_for(session, run.execution_id, "xrd_a").command_id != command_for(session, run.execution_id, "xrd_b").command_id
    engine.apply_event(event_for(session, run.execution_id, "xrd_a", {"value": 7}), NOW)
    assert node_run(session, run.execution_id, "xrd_b").status == "queued"


def test_condition_skips_unselected_branch_and_reconverges_after_manual_approval(session):
    graph = WorkflowGraph.model_validate({"nodes": [
        {"id": "route", "type": "condition", "label": "Route",
         "inputs": {"value": {"schema": {"type": "number"}}}, "input_values": {"value": 5},
         "config": {"field": "value", "operator": "gte", "value": 3, "match_route": "yes", "default_route": "no"}},
        {"id": "yes", "type": "manual_approval", "label": "Yes"},
        {"id": "no", "type": "manual_approval", "label": "No"},
        {"id": "join", "type": "join", "label": "Join"},
    ], "edges": [
        {"source": "route", "target": "yes", "route": "yes"},
        {"source": "route", "target": "no", "route": "no"},
        {"source": "yes", "target": "join"}, {"source": "no", "target": "join"},
    ]})
    engine, run = start(session, graph)
    assert node_run(session, run.execution_id, "no").status == "skipped"
    assert node_run(session, run.execution_id, "yes").status == "waiting_approval"
    engine.approve(node_run(session, run.execution_id, "yes").id, {}, NOW)
    assert run.status == "completed"
    assert not outboxes(session)


def test_runtime_projection_exposes_current_instance_states_only(session):
    engine, run = start(session)
    projection = importlib.import_module("app.workflows.projection")
    view = projection.workflow_runtime_read(session, run.execution_id)
    assert view["workflow_version_id"] == run.workflow_version_id
    assert len(view["nodes"]) == 6
    assert next(node for node in view["nodes"] if node["node_id"] == "model_inference")["status"] == "waiting_dependencies"


def test_run_creation_and_commands_rollback_together(session):
    engine, run = start(session)
    run_id = run.execution_id
    session.rollback()
    assert session.get(WorkflowRun, run_id) is None
    assert not outboxes(session)


def test_legacy_execution_cannot_be_reinterpreted_as_a_workflow_run(session):
    execution, version = make_execution(session)
    execution.dispatch_payload = "{}"
    engine = importlib.import_module("app.workflows.engine").WorkflowEngine(session)
    with pytest.raises(ConflictError, match="workflow-owned"):
        engine.start(execution, version, NOW)
    assert not outboxes(session)


def test_delivery_ack_starts_acceptance_deadline_not_execution(session):
    engine, run = start(session)
    outbox = outboxes(session)[0]
    outbox.status, outbox.sent_at = "sent", NOW + timedelta(seconds=10)
    assert engine.expire(NOW + timedelta(seconds=61)) == 0
    assert engine.expire(NOW + timedelta(seconds=131)) == 1
    assert len(outboxes(session)) == 1


def test_failed_run_still_expires_active_siblings_without_reopening_downstream(session):
    engine, run = start(session)
    complete_preparation(session, engine, run)
    engine.apply_event(event_for(session, run.execution_id, "eds", kind="node.started"), NOW)
    failed = event_for(session, run.execution_id, "xrd", kind="node.failed")
    engine.apply_event(failed.model_copy(update={"payload": {"error": "xrd failed"}}), NOW)
    assert engine.expire(NOW + timedelta(hours=2)) == 2
    assert node_run(session, run.execution_id, "eds").status == "needs_attention"
    assert run.status == "failed"
    assert node_run(session, run.execution_id, "model_inference").attempt == 0
    assert engine.apply_event(event_for(session, run.execution_id, "eds", {"result": {"ok": 1}}),
                              NOW + timedelta(hours=2)) == EventDisposition.APPLIED
    assert run.status == "failed"
    assert len(outboxes(session)) == 4
