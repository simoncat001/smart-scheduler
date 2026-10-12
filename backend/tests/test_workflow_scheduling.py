from datetime import timedelta
import json

from fastapi.testclient import TestClient
import pytest
from sqlalchemy import select
from sqlalchemy import event

from app.config import KafkaSettings, Settings
from app.database import Database
from app.main import create_app
from app.models import (CampaignRound, Execution, ExecutionFeedbackSnapshot,
                        ExecutionInstrumentState, FeedbackEvent, ModelInferenceRun,
                        Schedule, utcnow)
from app.schemas import DataPackageCreate, FeedbackCreate, InstrumentUploadCreate, ScheduleCreate
from app.services.acquisition import InstrumentUploadService
from app.services.closed_loop import ClosedLoopService
from app.services.exceptions import ConflictError
from app.services.feedback import FeedbackService
from app.services.model_inference import ModelInferenceService
from app.services.recommendations import RecommendationService
from app.services.scheduler import SchedulerService
from app.services.state_projection import StateProjectionService
from app.workflows.definitions import DefinitionService
from app.workflows.engine import WorkflowEngine
from app.workflows.models import MessageOutbox, NodeRun, WorkflowDefinition, WorkflowRun
from app.workflows.schemas import WorkflowGraph
from workflow_fixtures import NOW, command_for, event_for, material_graph, node_run, outboxes


@pytest.fixture()
def database(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'scheduling.db'}")
    database.create_schema()
    yield database
    database.dispose()


@pytest.fixture()
def session(database):
    with database.session_factory() as session:
        yield session


def make_schedule(session, *, name="Workflow", bind=True, graph=None, now=NOW):
    schedule = SchedulerService(session).create_schedule(ScheduleCreate(
        name=name, target="workflow/test", base_interval_seconds=60,
        min_interval_seconds=10, max_interval_seconds=300, feedback_timeout_seconds=30,
        payload={"sample_batch": "TEST"},
    ), now=now)
    if not bind:
        return schedule, None
    definitions = DefinitionService(session)
    draft = definitions.create(name, graph or material_graph())
    version = definitions.publish(draft.id, draft.revision)
    definitions.bind(schedule.id, version.id)
    session.commit()
    return schedule, version


def dispatch(session):
    schedule, version = make_schedule(session)
    runs = WorkflowEngine(session).tick_schedules(NOW)
    assert len(runs) == 1
    return schedule, version, session.get(Execution, runs[0].execution_id)


def complete_round(session, execution):
    engine = WorkflowEngine(session)
    engine.apply_event(event_for(session, execution.id, "magnetron_sputtering", {"sample": {"id": "a"}}), NOW)
    for code in ("xrd", "eds", "nanoindentation"):
        engine.apply_event(event_for(session, execution.id, code, {"result": {"owner": code}}), NOW)
    return engine.apply_event(event_for(session, execution.id, "model_inference", {"candidate": {"value": 13}}), NOW)


def test_tick_creates_owned_execution_with_pinned_round_context_and_pending_preparation(session):
    schedule, version, execution = dispatch(session)
    context = json.loads(execution.dispatch_payload)
    assert context["workflow_version_id"] == version.id
    assert context["experiment_round_index"] == 1
    assert context["experiment_batch_id"].endswith("-R01")
    assert context["sample_id"].endswith("-S01")
    assert context["campaign_id"]
    assert context["sample_plan"]["samples"]
    assert schedule.awaiting_feedback
    assert schedule.feedback_deadline_at is None
    assert node_run(session, execution.id, "magnetron_sputtering").status == "queued"
    assert len(outboxes(session)) == 1


def test_bound_schedules_are_independent_and_single_active_round_is_enforced(session):
    first, _ = make_schedule(session, name="First")
    second, _ = make_schedule(session, name="Second")
    engine = WorkflowEngine(session)
    assert len(engine.tick_schedules(NOW)) == 2
    assert engine.tick_schedules(NOW + timedelta(days=1)) == []
    assert first.status == second.status == "active"
    assert len(list(session.scalars(select(Execution)))) == 2
    assert len(outboxes(session)) == 2


def test_unbound_legacy_tick_never_dispatches_or_pauses_bound_schedules(session):
    first, _ = make_schedule(session, name="First")
    second, _ = make_schedule(session, name="Second")
    legacy, _ = make_schedule(session, name="Legacy", bind=False)
    executions, _ = SchedulerService(session).dispatch_due_schedules(now=NOW)
    assert [execution.schedule_id for execution in executions] == [legacy.id]
    assert first.status == second.status == "active"
    assert not list(session.scalars(select(WorkflowRun)))
    assert not outboxes(session)


def test_resuming_bound_schedule_does_not_pause_any_other_schedule(session):
    first, _ = make_schedule(session, name="First")
    second, _ = make_schedule(session, name="Second")
    legacy, _ = make_schedule(session, name="Legacy", bind=False)
    SchedulerService(session).resume_schedule(first.id, now=NOW)
    assert second.status == legacy.status == "active"


def test_manual_trigger_uses_workflow_engine_without_legacy_dispatch_side_effects(session):
    schedule, version = make_schedule(session)
    execution = SchedulerService(session).manual_trigger(schedule.id, now=NOW)
    assert session.get(WorkflowRun, execution.id).workflow_version_id == version.id
    assert len(outboxes(session)) == 1
    with pytest.raises(ConflictError):
        SchedulerService(session).manual_trigger(schedule.id, now=NOW)


def test_legacy_timeout_cannot_close_owned_execution_even_with_stale_schedule_deadline(session):
    schedule, _, execution = dispatch(session)
    schedule.feedback_deadline_at = NOW
    service = FeedbackService(session)
    assert service.register_timeout_feedback(schedule, now=NOW + timedelta(hours=2)) is None
    SchedulerService(session)._resolve_timeouts(NOW + timedelta(hours=2))
    assert execution.status == "awaiting_feedback"
    assert schedule.awaiting_feedback
    assert not list(session.scalars(select(FeedbackEvent)))


def test_manual_feedback_cannot_bypass_workflow_nodes(session):
    _, _, execution = dispatch(session)
    with pytest.raises(ConflictError, match="workflow"):
        FeedbackService(session).submit_feedback(execution.id, FeedbackCreate(outcome="success"), now=NOW, commit=False)
    assert execution.status == "awaiting_feedback"


def test_completed_run_finalizes_health_campaign_and_feedback_once_without_auto_inference(session, monkeypatch):
    schedule, _, execution = dispatch(session)
    monkeypatch.setattr(FeedbackService, "_maybe_auto_dispatch_next_candidate", lambda **_: pytest.fail("legacy inference ran"))
    complete_round(session, execution)
    assert execution.status == "success"
    assert not schedule.awaiting_feedback
    assert schedule.next_run_at > NOW
    assert schedule.health_score > 0.5
    events = list(session.scalars(select(FeedbackEvent).where(FeedbackEvent.execution_id == execution.id)))
    assert len(events) == 1
    assert not list(session.scalars(select(ModelInferenceRun)))
    assert session.scalar(select(CampaignRound).where(CampaignRound.execution_id == execution.id)).status == "completed"
    WorkflowEngine(session).advance(execution.id, NOW)
    assert len(list(session.scalars(select(FeedbackEvent)))) == 1
    assert len(outboxes(session)) == 5


def test_finalizer_rejects_nonterminal_or_mismatched_outcome(session):
    _, _, execution = dispatch(session)
    service = FeedbackService(session)
    with pytest.raises(ConflictError):
        service.finalize_workflow_execution(execution.id, FeedbackCreate(outcome="success"), NOW)
    complete_round(session, execution)
    with pytest.raises(ConflictError):
        service.finalize_workflow_execution(execution.id, FeedbackCreate(outcome="failure"), NOW)


def test_timeout_keeps_schedule_waiting_without_new_hardware_round(session):
    schedule, _, execution = dispatch(session)
    engine = WorkflowEngine(session)
    assert engine.expire(NOW + timedelta(minutes=2)) == 1
    assert engine.tick_schedules(NOW + timedelta(days=1)) == []
    assert schedule.awaiting_feedback
    assert execution.status == "awaiting_feedback"
    assert len(outboxes(session)) == 1


def test_failed_run_waits_for_uncertain_siblings_then_pauses_automatic_dispatch(session):
    schedule, _, execution = dispatch(session)
    engine = WorkflowEngine(session)
    engine.apply_event(event_for(session, execution.id, "magnetron_sputtering", {"sample": {"id": "a"}}), NOW)
    failed = event_for(session, execution.id, "xrd", kind="node.failed").model_copy(update={"payload": {"error": "xrd failed"}})
    engine.apply_event(failed, NOW)
    assert schedule.awaiting_feedback
    assert execution.status == "awaiting_feedback"
    for code in ("eds", "nanoindentation"):
        engine.apply_event(event_for(session, execution.id, code, {"result": {"ok": 1}}), NOW)
    assert execution.status == "failure"
    assert not schedule.awaiting_feedback
    assert schedule.status == "paused"
    assert engine.tick_schedules(NOW + timedelta(days=1)) == []


def test_schedule_pause_prevents_new_downstream_commands_and_resume_continues(session):
    schedule, _, execution = dispatch(session)
    service = SchedulerService(session)
    service.pause_schedule(schedule.id)
    WorkflowEngine(session).apply_event(event_for(session, execution.id, "magnetron_sputtering", {"sample": {"id": "a"}}), NOW)
    assert len(outboxes(session)) == 1
    assert session.get(WorkflowRun, execution.id).status == "paused"
    service.resume_schedule(schedule.id, now=NOW)
    assert len(outboxes(session)) == 4


def test_projection_uses_nodes_not_legacy_uploaded_signals_or_dispatch_marker(session):
    _, _, execution = dispatch(session)
    execution.feedback_observation = json.dumps({"deposition_completed": True,
        "xrd_phase": "bcc", "hardness_gpa": 15, "measured_composition_at_percent": {"Ti": 25, "Zr": 25, "Nb": 25, "V": 25}})
    StateProjectionService(session).sync_execution(execution, now=NOW)
    snapshot = session.get(ExecutionFeedbackSnapshot, execution.id)
    assert snapshot.completed_count == 0
    assert not snapshot.closed_loop_ready
    states = list(session.scalars(select(ExecutionInstrumentState).where(ExecutionInstrumentState.execution_id == execution.id)))
    assert next(state for state in states if state.instrument_code == "magnetron_sputtering").status != "parsed"
    assert all(state.parsed_at is None for state in states)


def test_two_same_instrument_nodes_are_not_flattened_to_one_legacy_state(session):
    data = material_graph().model_dump(mode="json", by_alias=True)
    second = dict(data["nodes"][1], id="xrd_b")
    data["nodes"].append(second)
    data["edges"].append({"source": "magnetron_sputtering", "target": "xrd_b", "source_port": "sample", "target_port": "sample"})
    schedule, _ = make_schedule(session, graph=WorkflowGraph.model_validate(data))
    execution = SchedulerService(session).manual_trigger(schedule.id, now=NOW)
    assert not session.scalar(select(ExecutionInstrumentState).where(
        ExecutionInstrumentState.execution_id == execution.id, ExecutionInstrumentState.instrument_code == "xrd"))
    assert len(list(session.scalars(select(NodeRun).where(NodeRun.run_id == execution.id)))) == 7


def test_reconciliation_refresh_and_backfill_cannot_replace_runtime_authority(session, monkeypatch):
    _, _, execution = dispatch(session)
    original = execution.feedback_observation
    service = ClosedLoopService(session)
    session.add(FeedbackEvent(execution_id=execution.id, outcome="success", quality_score=1,
                              observation='{"forged":true}', note="old projection", created_at=NOW))
    session.flush()
    execution.status = "success"
    assert service._reconcile_execution_feedback(execution) is False
    assert execution.feedback_observation == original
    execution.status = "awaiting_feedback"
    monkeypatch.setattr(service, "_fetch_live_manage_db_record", lambda **_: pytest.fail("legacy fetch ran"))
    assert service.refresh_execution_from_manage_db(execution, now=NOW) is False
    ModelInferenceService(session).backfill_from_history()
    assert not list(session.scalars(select(ModelInferenceRun)))


def test_legacy_inference_and_recommendation_entry_points_reject_owned_runs(session, monkeypatch):
    schedule, _, execution = dispatch(session)
    with pytest.raises(ConflictError, match="workflow"):
        ModelInferenceService(session).record_started(schedule_id=schedule.id, source_execution_id=execution.id, started_at=NOW)
    monkeypatch.setattr(RecommendationService, "_recommend_for_schedule", lambda *_: pytest.fail("legacy model ran"))
    with pytest.raises(ConflictError, match="workflow"):
        RecommendationService(session).recommend_for_schedule(schedule)


def test_legacy_upload_entry_points_fail_closed_before_parsing_or_mutating_owned_runs(session):
    _, _, execution = dispatch(session)
    payload = json.loads(execution.dispatch_payload)
    with pytest.raises(ConflictError, match="workflow"):
        InstrumentUploadService(session).ingest_upload(InstrumentUploadCreate(
            instrument_code="xrd", component_id="xrd-test", experiment_batch_id=payload["experiment_batch_id"],
            observation={"xrd_phase": "bcc"}), now=NOW, commit=False)
    with pytest.raises(ConflictError, match="workflow"):
        ClosedLoopService(session).ingest_data_package(DataPackageCreate(run_id=execution.id, instrument_code="xrd"), now=NOW)


def test_run_start_and_schedule_flags_roll_back_atomically(session, database):
    schedule, _ = make_schedule(session)
    schedule_id = schedule.id
    WorkflowEngine(session).tick_schedules(NOW)
    session.rollback()
    with database.session_factory() as fresh:
        assert fresh.get(Schedule, schedule_id).awaiting_feedback is False
        assert not list(fresh.scalars(select(WorkflowRun)))
        assert not outboxes(fresh)


def test_external_mode_never_launches_api_scheduler_worker(tmp_path, monkeypatch):
    monkeypatch.setattr("app.main.BackgroundSchedulerLoop", lambda *_: pytest.fail("API started a worker"))
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'external.db'}", scheduler_mode="external")
    for _ in range(2):
        with TestClient(create_app(settings)) as client:
            assert client.get("/health").status_code == 200


def test_external_mode_rejects_api_tick_but_allows_explicit_manual_workflow_trigger(tmp_path):
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'api.db'}", scheduler_enabled=False,
                        scheduler_mode="external", kafka=KafkaSettings(topic_prefix="test-scheduler"))
    with TestClient(create_app(settings)) as client:
        with client.app.state.database.session_factory() as session:
            schedule, version = make_schedule(session, now=utcnow())
        assert client.post("/api/v1/engine/tick").status_code == 409
        triggered = client.post(f"/api/v1/schedules/{schedule.id}/trigger")
        assert triggered.status_code == 200
        body = triggered.json()
        assert body["workflow_runtime"]["workflow_version_id"] == version.id
        assert body["workflow_runtime"]["status"] == "running"
        assert body["feedback_snapshot"]["completed_count"] == 0
        assert body["model_inference"] is None
        duplicate = client.post(f"/api/v1/schedules/{schedule.id}/trigger")
        assert duplicate.status_code == 409
        with client.app.state.database.session_factory() as session:
            assert outboxes(session)[0].topic == "test-scheduler.commands.device-gateway"
        viewed = client.get("/api/v1/executions").json()
        assert viewed[0]["workflow_runtime"]["nodes"]
        rejected = client.post(f"/api/v1/executions/{body['id']}/feedback", json={"outcome": "success"})
        assert rejected.status_code == 409


def test_minimal_binding_and_runtime_api_preserves_existing_trigger_contract(tmp_path):
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'routes.db'}", scheduler_enabled=False)
    with TestClient(create_app(settings)) as client:
        with client.app.state.database.session_factory() as session:
            schedule, _ = make_schedule(session, bind=False, now=utcnow())
            service = DefinitionService(session)
            draft = service.create("API test", material_graph())
            version = service.publish(draft.id, draft.revision)
            session.commit()
        binding = client.put(f"/api/v1/schedules/{schedule.id}/workflow", json={"workflow_version_id": version.id})
        assert binding.status_code == 200
        run = client.post(f"/api/v1/schedules/{schedule.id}/trigger").json()
        runtime = client.get(f"/api/v1/executions/{run['id']}/workflow")
        assert runtime.status_code == 200
        assert runtime.json()["workflow_version_id"] == version.id
        conflict = client.put(f"/api/v1/schedules/{schedule.id}/workflow", json={"workflow_version_id": version.id})
        assert conflict.status_code == 409


def test_api_resume_keeps_configured_kafka_prefix_for_new_parallel_commands(tmp_path):
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'prefix.db'}", scheduler_enabled=False,
                        kafka=KafkaSettings(topic_prefix="custom-scheduler"))
    with TestClient(create_app(settings)) as client:
        with client.app.state.database.session_factory() as session:
            schedule, _ = make_schedule(session, now=utcnow())
        execution = client.post(f"/api/v1/schedules/{schedule.id}/trigger").json()
        assert client.post(f"/api/v1/schedules/{schedule.id}/pause").status_code == 200
        with client.app.state.database.session_factory.begin() as session:
            WorkflowEngine(session, "custom-scheduler").apply_event(
                event_for(session, execution["id"], "magnetron_sputtering", {"sample": {"ok": 1}}), NOW)
        assert client.post(f"/api/v1/schedules/{schedule.id}/resume").status_code == 200
        with client.app.state.database.session_factory() as session:
            assert len(outboxes(session)) == 4
            assert all(row.topic.startswith("custom-scheduler.") for row in outboxes(session))


def test_legacy_projection_reads_and_backfill_do_not_write_owned_snapshots(session):
    _, _, execution = dispatch(session)
    snapshot = session.get(ExecutionFeedbackSnapshot, execution.id)
    before = snapshot.updated_at
    projection = StateProjectionService(session)
    projection.sync_execution(execution, now=NOW + timedelta(seconds=10))
    projection.backfill_from_history()
    assert snapshot.updated_at == before


def test_worker_cycle_ticks_bound_and_unbound_schedules_in_external_mode(session, database):
    from app.workflows.worker import engine_cycle

    owned, _ = make_schedule(session, now=utcnow())
    legacy, _ = make_schedule(session, bind=False, name="Legacy", now=utcnow())
    session.close()
    engine_cycle(database, "scheduler", scheduler_mode="external")
    with database.session_factory() as fresh:
        assert len(list(fresh.scalars(select(Execution)))) == 2
        assert len(list(fresh.scalars(select(WorkflowRun)))) == 1
        assert fresh.get(Schedule, owned.id).status == "active"
        assert fresh.get(Schedule, legacy.id).status == "active"


def test_worker_with_automatic_scheduling_disabled_does_not_create_rounds(session, database):
    from app.workflows.worker import engine_cycle

    make_schedule(session, now=utcnow())
    session.close()
    engine_cycle(database, "scheduler", scheduler_mode="external", scheduler_enabled=False)
    with database.session_factory() as fresh:
        assert not list(fresh.scalars(select(Execution)))


def test_active_execution_check_survives_incorrect_awaiting_flag(session):
    schedule, _, _ = dispatch(session)
    schedule.awaiting_feedback = False
    assert WorkflowEngine(session).tick_schedules(NOW) == []
    assert len(list(session.scalars(select(Execution)))) == 1


def test_later_publication_does_not_change_active_model_and_next_round_pins_new_version(session):
    schedule, old, execution = dispatch(session)
    definitions = DefinitionService(session)
    data = material_graph().model_dump(mode="json", by_alias=True)
    data["nodes"][-1]["config"]["model_version"] = "test-version-2"
    draft = session.get(WorkflowDefinition, old.definition_id)
    definitions.save(draft.id, draft.revision, WorkflowGraph.model_validate(data))
    new = definitions.publish(draft.id, draft.revision)
    with pytest.raises(ConflictError):
        definitions.bind(schedule.id, new.id)
    complete_round(session, execution)
    assert command_for(session, execution.id, "model_inference").payload["config"]["model_version"] == "test-version-1"
    definitions.bind(schedule.id, new.id)
    next_run = WorkflowEngine(session).tick_schedules(NOW + timedelta(days=1))[0]
    next_execution = session.get(Execution, next_run.execution_id)
    assert next_run.workflow_version_id == new.id
    assert json.loads(next_execution.dispatch_payload)["experiment_round_index"] == 2


def test_schedule_dispatch_uses_current_locking_reads_and_skip_locked(session):
    make_schedule(session)
    statements = []

    def observe(_conn, _cursor, _statement, _parameters, context, _many):
        compiled = getattr(context, "compiled", None)
        if compiled is not None:
            statements.append(compiled.statement)

    event.listen(session.bind, "before_cursor_execute", observe)
    try:
        WorkflowEngine(session).tick_schedules(NOW)
    finally:
        event.remove(session.bind, "before_cursor_execute", observe)
    assert any(getattr(statement, "_for_update_arg", None) is not None and
               statement._for_update_arg.skip_locked for statement in statements)
    current_execution_reads = [statement for statement in statements if
                               getattr(statement, "is_select", False) and
                               getattr(statement, "_for_update_arg", None) is not None and
                               any(getattr(table, "name", None) == "executions" for table in statement.get_final_froms())]
    assert current_execution_reads


def test_same_named_bound_and_legacy_schedules_never_share_batch_or_sample_ids(session):
    legacy, _ = make_schedule(session, bind=False, name="Legacy")
    old = SchedulerService(session).manual_trigger(legacy.id, now=NOW)
    bound, _ = make_schedule(session, name="Bound", now=NOW + timedelta(seconds=1))
    owned = SchedulerService(session).manual_trigger(bound.id, now=NOW + timedelta(seconds=1))
    old_context, new_context = json.loads(old.dispatch_payload), json.loads(owned.dispatch_payload)
    assert old_context["experiment_batch_id"] == "TEST-R01"
    assert old_context["experiment_batch_id"] != new_context["experiment_batch_id"]
    assert old_context["sample_id"] != new_context["sample_id"]
    resolved, _ = InstrumentUploadService(session)._find_execution(old_context["experiment_batch_id"])
    assert resolved.id == old.id


def test_final_inflight_result_completes_paused_run_without_resuming_schedule(session):
    schedule, _, execution = dispatch(session)
    engine = WorkflowEngine(session)
    engine.apply_event(event_for(session, execution.id, "magnetron_sputtering", {"sample": {"id": "a"}}), NOW)
    for code in ("xrd", "eds", "nanoindentation"):
        engine.apply_event(event_for(session, execution.id, code, {"result": {"ok": 1}}), NOW)
    SchedulerService(session).pause_schedule(schedule.id)
    engine.apply_event(event_for(session, execution.id, "model_inference", {"candidate": {"value": 13}}), NOW)
    assert session.get(WorkflowRun, execution.id).status == "completed"
    assert execution.status == "success"
    assert not schedule.awaiting_feedback
    assert schedule.status == "paused"
    assert len(list(session.scalars(select(FeedbackEvent)))) == 1
    assert len(outboxes(session)) == 5
    assert engine.tick_schedules(NOW + timedelta(days=1)) == []


def test_completion_feedback_and_final_event_inbox_roll_back_together(session, monkeypatch):
    from app.workflows.messaging import MessagingService
    from app.workflows.models import MessageInbox

    _, _, execution = dispatch(session)
    engine = WorkflowEngine(session)
    engine.apply_event(event_for(session, execution.id, "magnetron_sputtering", {"sample": {"ok": 1}}), NOW)
    for code in ("xrd", "eds", "nanoindentation"):
        engine.apply_event(event_for(session, execution.id, code, {"result": {"ok": 1}}), NOW)
    result = event_for(session, execution.id, "model_inference", {"candidate": {"value": 13}})
    execution_id = execution.id
    session.commit()

    def failed(*_args, **_kwargs):
        raise RuntimeError("campaign finalization failed")

    monkeypatch.setattr(ClosedLoopService, "record_feedback", failed)
    with pytest.raises(RuntimeError):
        MessagingService(session).ingest(result.model_dump_json().encode(), "events", NOW)
    session.rollback()
    assert session.get(Execution, execution_id).status == "awaiting_feedback"
    assert node_run(session, execution_id, "model_inference").status == "queued"
    assert not list(session.scalars(select(FeedbackEvent)))
    assert not list(session.scalars(select(MessageInbox)))
