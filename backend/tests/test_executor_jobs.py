from datetime import UTC, timedelta
import importlib
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.config import Settings
from app.database import Database
from app.models import ClosedLoopEvent, Execution, FeedbackEvent, ModelInferenceRun, utcnow
from app.services.exceptions import ConflictError
from app.workflows.engine import WorkflowEngine
from app.workflows.models import ExecutorJob, MessageInbox, MessageOutbox, WorkflowRun
from app.workflows.schemas import MessageEnvelope, WorkflowGraph
from workflow_fixtures import NOW, command_for, event_for, make_execution, material_graph, node_run, outboxes


@pytest.fixture()
def database(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'executors.db'}")
    database.create_schema()
    yield database
    database.dispose()


@pytest.fixture()
def session(database):
    with database.session_factory() as session:
        yield session


def executor(session, role="model", **kwargs):
    return importlib.import_module("app.workflows.executors").ExecutorService(session, role=role, **kwargs)


def model_graph(*, two=False):
    data = material_graph().model_dump(mode="json", by_alias=True)
    model = dict(data["nodes"][-1], inputs={})
    data["nodes"], data["edges"], data["positions"] = [model], [], {}
    if two:
        data["nodes"].append(dict(model, id="second-model"))
    return WorkflowGraph.model_validate(data)


def seed(session, graph=None):
    execution, version = make_execution(session, graph or model_graph())
    execution.dispatch_payload = json.dumps({"workflow_version_id": version.id, "source_value": 13,
                                            "sample_id": "sample-1", "experiment_batch_id": "batch-1"})
    WorkflowEngine(session).start(execution, version, NOW)
    session.commit()
    return execution, command_for(session, execution.id, "model_inference" if graph is None else graph.nodes[0].id)


def cancel(command, *, message_id=None):
    return command.model_copy(update={"message_id": message_id or str(uuid4()), "message_type": "command.cancel",
        "payload": {"reason": "operator stop", "require_cancellation_tombstone": True}})


def emitted(session, kind):
    return [MessageEnvelope.model_validate_json(row.envelope_json) for row in outboxes(session)
            if json.loads(row.envelope_json).get("message_type") == kind]


def test_acceptance_and_ack_are_durable_and_duplicate_execute_creates_no_second_job(session, database):
    _, command = seed(session)
    service = executor(session)
    job = service.accept(command, "model-worker", NOW)
    with database.session_factory() as fresh:
        assert not list(fresh.scalars(select(ExecutorJob)))
    assert job.status == "accepted"
    assert service.accept(command, "model-worker", NOW).id == job.id
    assert len(emitted(session, "node.accepted")) == 1
    session.commit()
    with database.session_factory() as fresh:
        assert fresh.get(ExecutorJob, job.id).status == "accepted"
        assert len(emitted(fresh, "node.accepted")) == 1


def test_conflicting_duplicate_and_wrong_executor_are_rejected(session):
    _, command = seed(session)
    service = executor(session)
    job = service.accept(command, "model-worker", NOW)
    changed = command.model_copy(update={"payload": {**command.payload, "inputs": {"forged": 1}}})
    with pytest.raises(ConflictError):
        service.accept(changed, "model-worker", NOW)
    with pytest.raises(ConflictError):
        service.accept(command, "other-model", NOW)
    assert len(list(session.scalars(select(ExecutorJob)))) == 1
    assert job.status == "accepted"


def test_worker_validates_pinned_command_inputs_and_output_schemas(session):
    _, command = seed(session)
    assert command.payload["output_ports"]["candidate"]["schema"]["type"] == "object"
    service = executor(session)
    job = service.accept(command, "model-worker", NOW)
    claim = service.claim("model-worker", NOW)
    with pytest.raises(ConflictError, match="outputs"):
        service.finish(job.id, {"candidate": {}}, NOW, lease_owner=claim.lease_owner)
    assert not emitted(session, "node.completed")


def test_finish_stores_result_and_outbox_atomically_and_lost_publication_never_reexecutes(session):
    _, command = seed(session)
    service = executor(session)
    job = service.accept(command, "model-worker", NOW)
    claim = service.claim("model-worker", NOW)
    service.finish(job.id, {"candidate": {"value": 13}}, NOW, lease_owner=claim.lease_owner)
    assert job.status == "completed"
    assert json.loads(job.outputs_json)["candidate"]["value"] == 13
    assert len(emitted(session, "node.completed")) == 1
    assert service.claim("model-worker", NOW) is None
    assert service.accept(command, "model-worker", NOW).status == "completed"
    assert len(emitted(session, "node.completed")) == 1


def test_stale_compute_claim_recovers_same_job_but_old_owner_cannot_finish(session):
    _, command = seed(session)
    service = executor(session, lease_seconds=10)
    job = service.accept(command, "model-worker", NOW)
    first = service.claim("model-worker", NOW)
    owner = first.lease_owner
    second = service.claim("model-worker", NOW + timedelta(seconds=11))
    assert second.id == job.id and second.command_id == command.command_id
    assert second.lease_owner != owner
    with pytest.raises(ConflictError, match="lease"):
        service.finish(job.id, {"candidate": {"value": 12}}, NOW + timedelta(seconds=11), lease_owner=owner)
    service.finish(job.id, {"candidate": {"value": 13}}, NOW + timedelta(seconds=11), lease_owner=second.lease_owner)
    assert len(emitted(session, "node.completed")) == 1


def test_live_lease_can_renew_but_expired_or_wrong_owner_cannot(session):
    _, command = seed(session)
    service = executor(session, lease_seconds=10)
    service.accept(command, "model-worker", NOW)
    job = service.claim("model-worker", NOW)
    owner = job.lease_owner
    assert service.renew(job.id, owner, NOW + timedelta(seconds=5))
    assert job.leased_until == NOW + timedelta(seconds=15)
    assert not service.renew(job.id, str(uuid4()), NOW + timedelta(seconds=6))
    assert not service.renew(job.id, owner, NOW + timedelta(seconds=16))


def test_expired_commands_never_create_executable_job(session):
    _, command = seed(session)
    with pytest.raises(ConflictError, match="expired"):
        executor(session).accept(command, "model-worker", NOW + timedelta(days=1))
    assert not list(session.scalars(select(ExecutorJob)))


def test_cancel_before_execute_persists_tombstone_across_restart(session, database):
    _, command = seed(session)
    job = executor(session).accept(cancel(command), "model-worker", NOW)
    assert job.status == "cancelled"
    assert len(emitted(session, "node.cancelled")) == 1
    session.commit()
    with database.session_factory() as restarted:
        service = executor(restarted)
        assert service.accept(command, "model-worker", NOW).status == "cancelled"
        assert service.claim("model-worker", NOW) is None
        assert not emitted(restarted, "node.accepted")


def test_cancel_running_compute_fences_late_finish_and_duplicate_cancel(session):
    _, command = seed(session)
    service = executor(session)
    job = service.accept(command, "model-worker", NOW)
    owner = service.claim("model-worker", NOW).lease_owner
    cancellation = cancel(command)
    assert service.accept(cancellation, "model-worker", NOW).status == "cancelled"
    assert service.accept(cancellation, "model-worker", NOW).status == "cancelled"
    with pytest.raises(ConflictError):
        service.finish(job.id, {"candidate": {"value": 13}}, NOW, lease_owner=owner)
    assert len(emitted(session, "node.cancelled")) == 1
    assert not emitted(session, "node.completed")


def test_conflicting_cancel_owner_cannot_poison_other_command(session):
    _, command = seed(session)
    service = executor(session)
    job = service.accept(command, "model-worker", NOW)
    with pytest.raises(ConflictError):
        service.accept(cancel(command).model_copy(update={"run_id": str(uuid4())}), "model-worker", NOW)
    assert job.status == "accepted"


def test_simulator_rejects_real_adapter_and_requires_explicit_enablement(session):
    graph = material_graph()
    _, command = seed(session, graph)
    with pytest.raises(ConflictError, match="simulator"):
        executor(session, "simulator").accept(command, "device-gateway", NOW)
    unsafe = command.model_copy(update={"adapter_id": "kafka_gateway"})
    with pytest.raises(ConflictError):
        executor(session, "simulator", simulator_enabled=True).accept(unsafe, "device-gateway", NOW)
    assert not list(session.scalars(select(ExecutorJob)))


def test_device_simulator_running_crash_is_attention_not_automatic_replay(session):
    _, command = seed(session, material_graph())
    service = executor(session, "simulator", simulator_enabled=True, lease_seconds=10)
    job = service.accept(command, "device-gateway", NOW)
    service.claim("device-gateway", NOW)
    assert service.claim("device-gateway", NOW + timedelta(seconds=11)) is None
    assert job.status == "needs_attention"
    assert len(emitted(session, "node.needs_attention")) == 1
    engine = WorkflowEngine(session)
    engine.apply_event(emitted(session, "node.needs_attention")[0], NOW)
    assert node_run(session, command.run_id, command.node_id).status == "needs_attention"


def test_executor_job_effects_roll_back_with_ack(session, database):
    _, command = seed(session)
    executor(session).accept(command, "model-worker", NOW)
    session.rollback()
    with database.session_factory() as fresh:
        assert not list(fresh.scalars(select(ExecutorJob)))
        assert not emitted(fresh, "node.accepted")


def proposal(command, value=13):
    return {"candidate": {"status": "candidate", "source_execution_id": command.run_id,
        "model_id": command.payload["config"]["model_id"], "model_version": command.payload["config"]["model_version"],
        "next_payload": {"target_hardness_gpa": value}, "summary": "pinned candidate", "confidence": 0.9}}


def test_two_model_outputs_are_independent_and_only_designated_node_updates_next_plan(session):
    graph = model_graph(two=True)
    execution, first = seed(session, graph)
    second = command_for(session, execution.id, "second-model")
    engine = WorkflowEngine(session)
    engine.apply_event(event_for(session, execution.id, "second-model", proposal(second, 99)), NOW)
    assert json.loads(execution.schedule.payload).get("target_hardness_gpa") != 99
    engine.apply_event(event_for(session, execution.id, "model_inference", proposal(first, 13)), NOW)
    run = session.get(WorkflowRun, execution.id)
    assert run.decision_applied
    assert json.loads(execution.schedule.payload)["target_hardness_gpa"] == 13
    assert json.loads(node_run(session, execution.id, "second-model").outputs_json)["candidate"]["next_payload"]["target_hardness_gpa"] == 99
    assert not list(session.scalars(select(ModelInferenceRun)))
    assert len(list(session.scalars(select(Execution)))) == 1
    original_due = execution.schedule.next_run_at
    engine.advance(execution.id, NOW + timedelta(seconds=5))
    assert execution.schedule.next_run_at == original_due
    events = list(session.scalars(select(ClosedLoopEvent).where(ClosedLoopEvent.event_type == "workflow.decision.applied")))
    assert len(events) == 1


def test_invalid_designated_proposal_is_quarantined_before_round_finalization(session):
    execution, command = seed(session)
    outputs = proposal(command)
    outputs["candidate"]["source_execution_id"] = str(uuid4())
    assert WorkflowEngine(session).apply_event(event_for(session, execution.id, "model_inference", outputs), NOW).value == "quarantined"
    assert execution.status == "awaiting_feedback"
    assert not list(session.scalars(select(FeedbackEvent)))


def test_paused_schedule_keeps_pause_when_designated_candidate_finishes(session):
    execution, command = seed(session)
    execution.schedule.status = "paused"
    engine = WorkflowEngine(session)
    engine.apply_event(event_for(session, execution.id, "model_inference", proposal(command)), NOW)
    assert execution.schedule.status == "paused"
    assert session.get(WorkflowRun, execution.id).decision_applied
    assert len(list(session.scalars(select(Execution)))) == 1


def test_exhausted_designated_model_stops_automatic_new_rounds(session):
    execution, command = seed(session)
    output = proposal(command)["candidate"]
    output.pop("next_payload")
    output["status"] = "exhausted"
    WorkflowEngine(session).apply_event(event_for(session, execution.id, "model_inference", {"candidate": output}), NOW)
    assert execution.schedule.status == "paused"
    assert session.get(WorkflowRun, execution.id).decision_applied


def test_model_context_is_pinned_to_source_not_later_round(session):
    module = importlib.import_module("app.workflows.adapters")
    execution, command = seed(session)
    later = Execution(schedule_id=execution.schedule_id, scheduled_for=NOW, dispatched_at=NOW,
                      dispatch_payload='{"source_value":999}')
    session.add(later)
    session.flush()
    context = module.ExecutionContext.from_command(command)
    assert context.execution_id == execution.id
    assert context.execution_payload["source_value"] == 13
    assert context.node_run_id == command.node_run_id
    assert context.model_version == "test-version-1"


def test_simulator_durable_jobs_and_events_advance_parallel_graph_without_physical_device(session):
    adapters = importlib.import_module("app.workflows.adapters")
    execution, prep = seed(session, material_graph())
    service = executor(session, "simulator", simulator_enabled=True)
    registry = adapters.AdapterRegistry(Settings(), simulator_enabled=True)
    def execute(command):
        job = service.accept(command, "device-gateway", NOW)
        claim = service.claim("device-gateway", NOW)
        context = adapters.ExecutionContext.from_command(command)
        outputs = registry.execute(command, context)
        service.finish(job.id, outputs, NOW, lease_owner=claim.lease_owner)
        completion = next(item for item in emitted(session, "node.completed") if item.command_id == command.command_id)
        WorkflowEngine(session).apply_event(completion, NOW)
    execute(prep)
    for code in ("xrd", "eds", "nanoindentation"):
        execute(command_for(session, execution.id, code))
    assert node_run(session, execution.id, "model_inference").attempt == 1
    assert json.loads(node_run(session, execution.id, "xrd").outputs_json)["result"]["simulated"] is True


def test_command_consumer_commits_job_before_offset_and_retries_same_message(database, session):
    module = importlib.import_module("app.workflows.executors")
    _, command = seed(session)
    observed = []
    message = SimpleNamespace(value=lambda: command.model_dump_json().encode(), error=lambda: None,
        topic=lambda: "scheduler.model.commands", partition=lambda: 0, offset=lambda: 1)
    class Consumer:
        polls = 0
        commits = 0
        def poll(self, _timeout):
            self.polls += 1
            return message
        def commit(self, **kwargs):
            with database.session_factory() as fresh:
                observed.append(len(list(fresh.scalars(select(ExecutorJob)))))
            self.commits += 1
            if self.commits == 1:
                raise RuntimeError("lost offset response")
    consumer = Consumer()
    receiver = module.CommandConsumer(database.session_factory, consumer, role="model", executor_id="model-worker", clock=lambda: NOW)
    with pytest.raises(RuntimeError):
        receiver.process_one()
    assert receiver.process_one()
    assert consumer.polls == 1 and observed == [1, 1]
    assert len(emitted(session, "node.accepted")) == 1


def test_command_consumer_does_not_ack_database_failure(database, session, monkeypatch):
    module = importlib.import_module("app.workflows.executors")
    _, command = seed(session)
    message = SimpleNamespace(value=lambda: command.model_dump_json().encode(), error=lambda: None,
        topic=lambda: "scheduler.model.commands", partition=lambda: 0, offset=lambda: 1)
    consumer = SimpleNamespace(poll=lambda _: message, commit=lambda **_: pytest.fail("acknowledged database failure"))
    monkeypatch.setattr(module.ExecutorService, "ingest", lambda *_: (_ for _ in ()).throw(RuntimeError("database failure")))
    receiver = module.CommandConsumer(database.session_factory, consumer, role="model", executor_id="model-worker", clock=lambda: NOW)
    with pytest.raises(RuntimeError):
        receiver.process_one()


def test_poison_command_is_quarantined_without_processing_inbox_leak(session):
    _, command = seed(session)
    service = executor(session)
    service.accept(command, "model-worker", NOW)
    forged = command.model_copy(update={"message_id": str(uuid4()), "run_id": str(uuid4())})
    assert service.ingest(forged.model_dump_json().encode(), "model-worker", "test", NOW) == "quarantined"
    session.flush()
    assert not list(session.scalars(select(MessageInbox).where(MessageInbox.status == "processing")))
    assert len(list(session.scalars(select(ExecutorJob)))) == 1


def test_runner_executes_outside_db_transaction_and_only_event_handler_changes_nodes(database, session):
    module = importlib.import_module("app.workflows.executors")
    execution, command = seed(session)
    executor(session).accept(command, "model-worker", NOW)
    session.commit()
    class Adapter:
        def execute(self, incoming, context):
            with database.session_factory() as other:
                job = other.scalar(select(ExecutorJob).where(ExecutorJob.command_id == incoming.command_id))
                assert job.status == "running"  # Claim is already committed.
            assert context.execution_id == execution.id
            return {"candidate": {"value": 13}}
    runner = module.ExecutorRunner(database.session_factory, Adapter(), role="model", executor_id="model-worker", clock=lambda: NOW)
    assert runner.execute_one()
    session.expire_all()
    assert node_run(session, execution.id, "model_inference").status == "queued"
    event = emitted(session, "node.completed")[0]
    WorkflowEngine(session).apply_event(event, NOW)
    assert execution.status == "success"


def test_model_adapter_uses_pinned_context_and_never_queries_latest_schedule(session, monkeypatch):
    from app.services.recommendations import RecommendationService
    from app.workflows.adapters import AdapterRegistry, ExecutionContext
    from app.services.mat_model_service import MatModelService

    execution, command = seed(session)
    def recommend(service, **kwargs):
        assert kwargs["model_id"] == "mat-materials"
        assert kwargs["model_version"] == "test-version-1"
        assert kwargs["base_payload"]["source_value"] == 13
        assert kwargs["inputs"] == {}
        return proposal(command)["candidate"]
    monkeypatch.setattr(MatModelService, "recommend_for_workflow", recommend, raising=False)
    monkeypatch.setattr(RecommendationService, "recommend_for_schedule", lambda *_: pytest.fail("queried latest schedule"))
    result = AdapterRegistry(Settings()).execute(command, ExecutionContext.from_command(command))
    assert result["candidate"]["source_execution_id"] == execution.id


def test_parser_adapter_uses_only_approved_command_reference(session, monkeypatch):
    from app.workflows.adapters import AdapterRegistry, ExecutionContext

    graph = WorkflowGraph.model_validate({"nodes": [{"id": "parse", "type": "data_parse", "label": "Parse",
        "config": {"parser_id": "manage_db"}, "executor": {"kind": "compute", "gateway_id": "parser-worker", "adapter_id": "manage_db"},
        "inputs": {"package": {"schema": {"type": "object"}}}, "outputs": {"result": {"schema": {"type": "object"}}},
        "input_values": {"package": {"instrument_code": "xrd", "manage_db_record_id": "approved-1"}}}], "edges": []})
    execution, command = seed(session, graph)
    def prepare(payload, **kwargs):
        assert payload.run_id == execution.id
        assert payload.manage_db_record_id == "approved-1"
        assert payload.sample_id == "sample-1"
        assert kwargs["experiment_batch_id"] == "batch-1"
        return {}, {"xrd_phase": "bcc"}, None
    monkeypatch.setattr("app.services.closed_loop.prepare_data_package", prepare)
    assert AdapterRegistry(Settings()).execute(command, ExecutionContext.from_command(command)) == {"result": {"xrd_phase": "bcc"}}


def test_pinned_model_version_rejects_changed_artifacts_without_training(tmp_path, monkeypatch):
    from app.services.mat_model_service import MatModelService

    root = tmp_path / "artifacts"
    root.mkdir()
    config = tmp_path / "config.json"
    config.write_text('{}')
    for name in ("resolved_config.json", "summary.json", "optimal_intervals.json", "process_window.json"):
        (root / name).write_text('{}')
    for name in ("experimental_batch.csv", "recommended_process_candidates.csv"):
        (root / name).write_text('Ti,Zr,Nb,V\n25,25,25,25\n')
    service = MatModelService(Settings(mat_model_config_path=str(config), mat_model_output_dir=str(root)))
    version = service.workflow_model_version()
    assert version.startswith("mat-sha256:")
    monkeypatch.setattr(service, "refresh_candidates_from_closed_loop", lambda *_: pytest.fail("training/refresh ran"))
    (root / "summary.json").write_text('{"changed":true}')
    with pytest.raises(ConflictError, match="pinned"):
        service.workflow_artifacts("mat-materials", version)


def test_worker_cli_offers_new_roles_but_requires_identity_and_simulator_opt_in(monkeypatch, capsys):
    import app.workflows.worker as worker
    monkeypatch.setattr("app.config.get_settings", lambda: Settings(database_url="sqlite:///:memory:"))
    assert worker.main(["model", "--once"]) == 1
    assert "executor" in capsys.readouterr().err.lower()
    assert worker.main(["simulator", "--executor-id", "device-gateway", "--once"]) == 1
    assert "simulator" in capsys.readouterr().err.lower()


def test_worker_heartbeat_health_reports_freshness_not_broker_success(session, database):
    module = importlib.import_module("app.workflows.heartbeats")
    module.record_heartbeat(session, "model", "model-worker", NOW)
    session.commit()
    health = module.worker_health(session, NOW + timedelta(seconds=10))
    assert health["model"]["status"] == "online"
    assert module.worker_health(session, NOW + timedelta(minutes=5))["model"]["status"] == "offline"
    assert health["publisher"]["status"] == "unknown"


def test_long_computation_does_not_block_cancel_acceptance(database, session):
    from threading import Event
    from app.workflows.executors import CommandConsumer, ExecutorRunner

    execution, command = seed(session)
    executor(session).accept(command, "model-worker", NOW)
    session.commit()
    entered, release = Event(), Event()
    class Adapter:
        def execute(self, *_):
            entered.set()
            assert release.wait(5)
            return {"candidate": {"value": 13}}
    runner = ExecutorRunner(database.session_factory, Adapter(), role="model", executor_id="model-worker", clock=lambda: NOW)
    runner.start(0.01)
    try:
        assert entered.wait(5)
        message = SimpleNamespace(value=lambda: cancel(command).model_dump_json().encode(), error=lambda: None,
            topic=lambda: "scheduler.model.commands", partition=lambda: 0, offset=lambda: 2)
        committed = []
        consumer = SimpleNamespace(poll=lambda _: message, commit=lambda **_: committed.append(True))
        assert CommandConsumer(database.session_factory, consumer, role="model", executor_id="model-worker", clock=lambda: NOW).process_one()
        with database.session_factory() as fresh:
            assert fresh.scalar(select(ExecutorJob)).status == "cancelled"
        assert committed
    finally:
        release.set()
        runner.stop()
    session.expire_all()
    assert not emitted(session, "node.completed")


def test_claim_cannot_cross_domain_roles_with_same_executor_id(session):
    _, command = seed(session, material_graph())
    executor(session, "simulator", simulator_enabled=True).accept(command, "device-gateway", NOW)
    with pytest.raises(ConflictError, match="role"):
        executor(session, "model").claim("device-gateway", NOW)
    assert not emitted(session, "node.started")


def test_model_command_history_contains_only_unique_used_compositions(session):
    execution, version = make_execution(session, model_graph())
    used = {"ti_at": 25, "zr_at": 25, "nb_at": 25, "v_at": 25}
    session.add(Execution(schedule_id=execution.schedule_id, status="success", scheduled_for=NOW,
        dispatched_at=NOW - timedelta(seconds=1), dispatch_payload=json.dumps({
            "mat_model_candidate_composition_at_percent": used, "unrelated": "x" * 500_000})))
    WorkflowEngine(session).start(execution, version, NOW)
    history = command_for(session, execution.id, "model_inference").payload["execution_context"]["used_model_payloads"]
    assert history == [{"mat_model_candidate_composition_at_percent": used}]


def test_stale_source_decision_cannot_replace_later_same_timestamp_round(session):
    execution, command = seed(session)
    context = json.loads(execution.dispatch_payload)
    context["experiment_round_index"] = 1
    execution.dispatch_payload = json.dumps(context)
    session.add(Execution(schedule_id=execution.schedule_id, status="success", scheduled_for=NOW, dispatched_at=NOW,
                          dispatch_payload='{"experiment_round_index":2}'))
    session.flush()
    original = execution.schedule.payload
    WorkflowEngine(session).apply_event(event_for(session, execution.id, "model_inference", proposal(command)), NOW)
    assert execution.schedule.payload == original
    assert not session.get(WorkflowRun, execution.id).decision_applied


def test_pinned_model_real_readonly_artifacts_support_warm_start_without_global_refresh(monkeypatch):
    from app.services.mat_model_service import MatModelService
    from app.config import _default_path

    settings = Settings(mat_model_config_path=_default_path("model_assets", "mat_model", "config", "ti_zr_nb_v_combined_bcc.json"),
        mat_model_output_dir=_default_path("model_assets", "mat_model", "outputs", "ti_zr_nb_v_combined_bcc"))
    service = MatModelService(settings)
    monkeypatch.setattr(service, "artifacts", lambda: pytest.fail("mutable artifacts read"))
    monkeypatch.setattr(service, "refresh_candidates_from_closed_loop", lambda *_: pytest.fail("global refresh"))
    version = service.workflow_model_version()
    result = service.recommend_for_workflow(model_id="mat-materials", model_version=version,
        execution_id=str(uuid4()), base_payload={"target_hardness_gpa": 13}, inputs={}, used_payloads=[])
    assert result["status"] == "candidate" and result["model_version"] == version
    assert result["next_payload"]["mat_model_candidate_composition_at_percent"]


def test_ordinary_model_status_output_does_not_opt_into_candidate_protocol(session):
    execution, _ = seed(session)
    assert WorkflowEngine(session).apply_event(event_for(session, execution.id, "model_inference",
        {"candidate": {"status": "predicted", "score": 0.9}}), NOW).value == "applied"
    assert not session.get(WorkflowRun, execution.id).decision_applied


def test_operator_payload_edit_during_round_is_not_overwritten_by_old_model_plan(session):
    from app.models import RecommendationRecord

    execution, command = seed(session)
    execution.schedule.payload = '{"target_hardness_gpa":14}'
    session.flush()
    WorkflowEngine(session).apply_event(event_for(session, execution.id, "model_inference", proposal(command, 13)), NOW)
    assert json.loads(execution.schedule.payload)["target_hardness_gpa"] == 14
    record = session.scalar(select(RecommendationRecord))
    assert record.approval_status == "superseded"


def test_full_simulated_graph_uses_pinned_mat_and_schedules_exactly_one_delayed_round(session):
    from app.services.mat_model_service import MatModelService
    from app.config import _default_path
    from app.workflows.adapters import AdapterRegistry, ExecutionContext
    from app.workflows.definitions import DefinitionService

    settings = Settings(mat_model_config_path=_default_path("model_assets", "mat_model", "config", "ti_zr_nb_v_combined_bcc.json"),
        mat_model_output_dir=_default_path("model_assets", "mat_model", "outputs", "ti_zr_nb_v_combined_bcc"))
    data = material_graph().model_dump(mode="json", by_alias=True)
    data["nodes"][-1]["config"]["model_version"] = MatModelService(settings).workflow_model_version()
    graph = WorkflowGraph.model_validate(data)
    execution, _ = seed(session, graph)
    engine, adapters = WorkflowEngine(session), AdapterRegistry(settings, simulator_enabled=True)
    for code in ("magnetron_sputtering", "xrd", "eds", "nanoindentation", "model_inference"):
        command = command_for(session, execution.id, code)
        role = "model" if code == "model_inference" else "simulator"
        service = executor(session, role, simulator_enabled=True)
        job = service.accept(command, command.gateway_id, NOW)
        claim = service.claim(command.gateway_id, NOW)
        outputs = adapters.execute(command, ExecutionContext.from_command(command))
        service.finish(job.id, outputs, NOW, lease_owner=claim.lease_owner)
        result = next(item for item in emitted(session, "node.completed") if item.command_id == command.command_id)
        assert engine.apply_event(result, NOW).value == "applied"
    assert execution.status == "success"
    assert session.get(WorkflowRun, execution.id).decision_applied
    assert len(list(session.scalars(select(Execution)))) == 1
    version_id = session.get(WorkflowRun, execution.id).workflow_version_id
    DefinitionService(session).bind(execution.schedule_id, version_id)
    session.commit()
    assert engine.tick_schedules(NOW + timedelta(seconds=3599)) == []
    runs = engine.tick_schedules(NOW + timedelta(seconds=3600))
    assert len(runs) == 1
    assert engine.tick_schedules(NOW + timedelta(seconds=3600)) == []
    assert len(list(session.scalars(select(Execution)))) == 2


@pytest.mark.parametrize("status", [[], {}, True])
def test_non_string_opaque_status_is_handled_and_offset_can_commit(session, database, status):
    from app.workflows.messaging import EventConsumer

    execution, _ = seed(session)
    result = event_for(session, execution.id, "model_inference", {"candidate": {"status": status}})
    message = SimpleNamespace(value=lambda: result.model_dump_json().encode(), error=lambda: None,
        topic=lambda: "scheduler.events", partition=lambda: 0, offset=lambda: 1)
    committed = []
    consumer = SimpleNamespace(poll=lambda _: message, commit=lambda **_: committed.append(True))
    assert EventConsumer(database.session_factory, consumer, clock=lambda: NOW).process_one()
    assert committed == [True]
    session.expire_all()
    assert execution.status == "success"


def test_successful_adapter_output_survives_transient_result_commit_failure(session, database):
    from sqlalchemy import event
    from sqlalchemy.exc import OperationalError
    from app.workflows.executors import ExecutorRunner

    _, command = seed(session)
    executor(session).accept(command, "model-worker", NOW)
    session.commit()
    calls, failed = [], []
    class Adapter:
        def execute(self, *_):
            calls.append(True)
            return {"candidate": {"value": 13}}
    def before_commit(current):
        if current.get_bind() is not database.engine or failed:
            return
        row = current.scalar(select(ExecutorJob).where(ExecutorJob.command_id == command.command_id))
        if row is not None and row.status == "completed":
            failed.append(True)
            raise OperationalError("commit", {}, RuntimeError("temporary connection failure"))
    event.listen(database.session_factory.class_, "before_commit", before_commit)
    try:
        runner = ExecutorRunner(database.session_factory, Adapter(), role="model", executor_id="model-worker", clock=lambda: NOW)
        assert runner.execute_one()
    finally:
        event.remove(database.session_factory.class_, "before_commit", before_commit)
    session.expire_all()
    job = session.scalar(select(ExecutorJob))
    assert job.status == "completed"
    assert failed == [True]
    assert json.loads(job.outputs_json) == {"candidate": {"value": 13}}
    assert len(calls) == 1
    assert len(emitted(session, "node.completed")) == 1
    assert not emitted(session, "node.failed")
