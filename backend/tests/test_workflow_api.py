from datetime import timedelta
import importlib
import json
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest
from sqlalchemy import select

from app.database import Database
from app.models import DataPackage, Execution, FeedbackEvent
from app.config import KafkaSettings, Settings
from app.main import create_app
from app.schemas import DataPackageCreate, InstrumentUploadCreate
from app.services.acquisition import InstrumentUploadService
from app.services.closed_loop import ClosedLoopService
from app.services.exceptions import ConflictError
from app.workflows.api import build_workflow_router
from app.workflows.engine import WorkflowEngine
from app.workflows.models import MessageInbox, NodeAttempt, NodeDataPackageLink, WorkflowDefinition
from app.workflows.schemas import WorkflowGraph
from workflow_fixtures import NOW, command_for, event_for, make_execution, material_graph, node_run, outboxes


@pytest.fixture()
def database(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'workflow-api.db'}")
    database.create_schema()
    yield database
    database.dispose()


@pytest.fixture()
def session(database):
    with database.session_factory() as session:
        yield session


@pytest.fixture()
def client(database):
    def sessions():
        with database.session_factory() as session:
            yield session
    app = FastAPI()
    app.include_router(build_workflow_router(sessions))
    with TestClient(app) as client:
        yield client


def graph_json():
    return material_graph().model_dump(mode="json", by_alias=True)


def waiting_uploads(session, *, duplicate=False, serial=False):
    data = graph_json()
    if duplicate:
        other = dict(data["nodes"][1], id="xrd-second")
        data["nodes"].insert(2, other)
        data["edges"].append(dict(source="xrd" if serial else "magnetron_sputtering", target="xrd-second",
                                  source_port="result" if serial else "sample", target_port="sample"))
    execution, version = make_execution(session, WorkflowGraph.model_validate(data))
    context = {"workflow_version_id": version.id, "experiment_batch_id": f"TEST-{execution.id}-R01",
               "sample_id": "sample-1", "instrument_components": {
                   code: {"component_id": f"{code}-test"}
                   for code in ("magnetron_sputtering", "xrd", "eds", "nanoindentation")}}
    execution.dispatch_payload = json.dumps(context)
    engine = WorkflowEngine(session)
    engine.start(execution, version, NOW)
    engine.apply_event(event_for(session, execution.id, "magnetron_sputtering", {"sample": {"id": "sample-1"}}), NOW)
    session.commit()
    return execution, context


def result_service(session):
    return importlib.import_module("app.workflows.results").ResultService(session)


def test_types_and_template_do_not_require_resource_registration(client):
    types = client.get("/api/v1/workflow-types")
    assert types.status_code == 200
    entries = types.json()
    assert {entry["type"] for entry in entries} == {
        "instrument_task", "data_upload", "data_parse", "model_inference", "join", "condition", "manual_approval"}
    assert all("config_schema" in entry for entry in entries)
    template = client.get("/api/v1/workflow-templates/materials")
    assert template.status_code == 200
    assert len(template.json()["graph"]["nodes"]) == 6


def test_runtime_read_includes_pinned_graph_not_later_draft(client, session):
    execution, _ = waiting_uploads(session)
    response = client.get(f"/api/v1/executions/{execution.id}/workflow")
    assert response.status_code == 200
    runtime = response.json()
    assert runtime["graph"]["positions"]["xrd"] == {"x": 576.0, "y": 180.0}
    definition = session.get(WorkflowDefinition, runtime["definition_id"])
    draft = json.loads(definition.draft_graph_json)
    draft["nodes"][1]["label"] = "Edited draft"
    definition.draft_graph_json = json.dumps(draft)
    session.commit()
    reread = client.get(f"/api/v1/executions/{execution.id}/workflow").json()
    assert reread["graph"]["nodes"][1]["label"] != "Edited draft"
    assert reread["workflow_version_id"] == runtime["workflow_version_id"]


def test_draft_crud_revision_publish_and_immutable_version(client):
    response = client.post("/api/v1/workflows", json={"name": "Parallel", "graph": graph_json()})
    assert response.status_code == 201
    draft = response.json()
    path = f"/api/v1/workflows/{draft['id']}"
    assert client.get(path).json()["graph"] == graph_json()
    assert any(item["id"] == draft["id"] for item in client.get("/api/v1/workflows").json())
    edited = graph_json()
    edited["nodes"][0]["label"] = "Preparation"
    saved = client.put(path, json={"revision": 1, "graph": edited})
    assert saved.status_code == 200
    assert saved.json()["revision"] == 2
    assert client.put(path, json={"revision": 1, "graph": edited}).status_code == 409
    assert client.post(path + "/publish", json={"revision": 1}).status_code == 409
    published = client.post(path + "/publish", json={"revision": 2})
    assert published.status_code == 201
    version = published.json()
    assert version["version"] == 1
    client.put(path, json={"revision": 2, "graph": graph_json()})
    assert client.get(path + "/versions").json()[0]["graph"] == edited
    assert client.get(path + f"/versions/{version['id']}").json()["graph"] == edited
    assert client.delete(path, params={"revision": 3}).status_code == 409


def test_delete_unpublished_draft_requires_current_revision(client):
    created = client.post("/api/v1/workflows", json={"name": "Draft", "graph": graph_json()})
    assert created.status_code == 201
    path = f"/api/v1/workflows/{created.json()['id']}"
    assert client.delete(path, params={"revision": 2}).status_code == 409
    assert client.delete(path, params={"revision": 1}).status_code == 204
    assert client.get(path).status_code == 404


def test_invalid_publish_leaves_no_version_and_validatable_draft(client):
    data = graph_json()
    data["nodes"][1]["executor"]["adapter_id"] = "unknown"
    created = client.post("/api/v1/workflows", json={"name": "Broken", "graph": data})
    assert created.status_code == 201
    path = f"/api/v1/workflows/{created.json()['id']}"
    assert client.post(path + "/validate", json={"revision": 1}).status_code == 400
    assert client.post(path + "/publish", json={"revision": 1}).status_code == 400
    assert client.get(path + "/versions").json() == []


@pytest.mark.parametrize("path", ["/api/v1/workflows", "/api/v1/workflow-nodes/missing/operations"])
def test_api_rejects_unknown_fields(client, path):
    assert client.post(path, json={"unexpected": True}).status_code == 422


def test_messaging_health_reports_counts_not_simulated_liveness(client, session):
    waiting_uploads(session)
    body = client.get("/api/v1/messaging/health")
    assert body.status_code == 200
    assert body.json()["outbox"]["pending"] == 4
    assert body.json()["broker"]["status"] == "unknown"
    assert all(item["status"] == "unknown" for item in body.json()["workers"].values())
    assert "password" not in body.text.lower()


def test_ambiguous_upload_requires_explicit_instance(session):
    execution, _ = waiting_uploads(session, duplicate=True)
    with pytest.raises(ConflictError, match="ambiguous"):
        result_service(session).resolve(execution.id, "xrd", "xrd-test", None, None)
    node = node_run(session, execution.id, "xrd-second")
    attempt = result_service(session).resolve(execution.id, "xrd", "xrd-test", node.id, 1)
    assert attempt.node_run_id == node.id
    assert result_service(session).ingest(attempt, "http-xrd-2", {"xrd_phase": "bcc"}, None, NOW).value == "applied"
    assert node.status == "completed"
    assert node_run(session, execution.id, "xrd").status == "queued"


def test_http_retry_is_durable_and_conflicting_request_id_fails(session, database):
    execution, _ = waiting_uploads(session)
    node = node_run(session, execution.id, "xrd")
    service = result_service(session)
    attempt = service.resolve(execution.id, "xrd", "xrd-test", node.id, 1)
    assert service.ingest(attempt, "stable-http", {"xrd_phase": "bcc"}, None, NOW).value == "applied"
    session.commit()
    with database.session_factory() as fresh:
        service = result_service(fresh)
        attempt = service.resolve(execution.id, "xrd", "xrd-test", node.id, 1)
        assert service.ingest(attempt, "stable-http", {"xrd_phase": "bcc"}, None, NOW + timedelta(seconds=2)).value == "duplicate"
        with pytest.raises(ConflictError, match="request ID"):
            service.ingest(attempt, "stable-http", {"xrd_phase": "fcc"}, None, NOW)
    assert node.outputs_json == '{"result":{"xrd_phase":"bcc"}}'


def test_result_service_rejects_wrong_run_component_attempt_and_unready_node(session):
    first, _ = waiting_uploads(session)
    second, _ = waiting_uploads(session)
    node = node_run(session, first.id, "xrd")
    for run, code, component, node_id, attempt in (
        (second.id, "xrd", "xrd-test", node.id, 1),
        (first.id, "xrd", "wrong", node.id, 1),
        (first.id, "xrd", "xrd-test", node.id, 2),
        (first.id, "eds", "eds-test", node.id, 1),
        (first.id, "xrd", "xrd-test", node.id, None),
    ):
        with pytest.raises(ConflictError):
            result_service(session).resolve(run, code, component, node_id, attempt)


def test_invalid_output_rolls_back_inbox_and_node_effects(session, database):
    execution, _ = waiting_uploads(session)
    service = result_service(session)
    node = node_run(session, execution.id, "xrd")
    with pytest.raises(ConflictError, match="outputs"):
        service.ingest(service.resolve(execution.id, "xrd", "xrd-test", node.id, 1), "invalid-http", {}, None, NOW)
    session.rollback()
    with database.session_factory() as fresh:
        assert node_run(fresh, execution.id, "xrd").status == "queued"
        assert not list(fresh.scalars(select(MessageInbox)))


def test_node_aware_instrument_upload_reuses_parser_without_legacy_finish(session, monkeypatch):
    execution, context = waiting_uploads(session)
    node = node_run(session, execution.id, "xrd")
    calls = []
    service = InstrumentUploadService(session)
    original = service._normalize_upload
    def normalize(code, observation):
        calls.append(code)
        return original(code, observation)
    monkeypatch.setattr(service, "_normalize_upload", normalize)
    upload = InstrumentUploadCreate(instrument_code="xrd", component_id="xrd-test",
        experiment_batch_id=context["experiment_batch_id"], node_run_id=node.id, attempt=1,
        request_id="upload-1", observation={"xrd_phase": "bcc"})
    result = service.ingest_upload(upload, now=NOW, commit=False)
    assert calls == ["xrd"]
    assert node.status == "completed"
    assert not result.round_completed
    assert execution.status == "awaiting_feedback"
    assert not list(session.scalars(select(FeedbackEvent)))
    assert node_run(session, execution.id, "model_inference").attempt == 0


def test_node_aware_data_package_retry_returns_same_package_and_link(session):
    execution, context = waiting_uploads(session)
    node = node_run(session, execution.id, "xrd")
    payload = DataPackageCreate(run_id=execution.id, experiment_batch_id=context["experiment_batch_id"],
        instrument_code="xrd", component_id="xrd-test", node_run_id=node.id, attempt=1,
        request_id="package-1", validation_status="validated", scheduler_observation={"xrd_phase": "bcc"})
    first = ClosedLoopService(session).ingest_data_package(payload, now=NOW)
    second = ClosedLoopService(session).ingest_data_package(payload, now=NOW + timedelta(seconds=1))
    assert first.data_package.id == second.data_package.id
    assert len(list(session.scalars(select(DataPackage)))) == 1
    links = list(session.scalars(select(NodeDataPackageLink)))
    assert len(links) == 1 and links[0].data_package_id == first.data_package.id
    assert execution.status == "awaiting_feedback"
    changed = payload.model_copy(update={"note": "changed"})
    with pytest.raises(ConflictError, match="request ID"):
        ClosedLoopService(session).ingest_data_package(changed, now=NOW)


def test_old_explicit_run_never_falls_back_to_latest_or_ignores_other_context(session):
    first, context = waiting_uploads(session)
    second, second_context = waiting_uploads(session)
    node = node_run(session, first.id, "xrd")
    for run_id, batch in ((str(uuid4()), second_context["experiment_batch_id"]),
                          (first.id, second_context["experiment_batch_id"])):
        with pytest.raises((ConflictError, importlib.import_module("app.services.exceptions").NotFoundError)):
            ClosedLoopService(session).ingest_data_package(DataPackageCreate(
                run_id=run_id, experiment_batch_id=batch, instrument_code="xrd", component_id="xrd-test",
                node_run_id=node.id, attempt=1, request_id="wrong-round", validation_status="validated",
                scheduler_observation={"xrd_phase": "bcc"}), now=NOW)
    assert node_run(session, second.id, "xrd").status == "queued"


def test_approval_is_audited_and_idempotent(client, session):
    graph = WorkflowGraph.model_validate({"nodes": [{"id": "approve", "type": "manual_approval", "label": "Approve"}], "edges": []})
    execution, version = make_execution(session, graph)
    WorkflowEngine(session).start(execution, version, NOW)
    session.commit()
    node = node_run(session, execution.id, "approve")
    path = f"/api/v1/workflow-nodes/{node.id}/operations"
    payload = {"action": "approve", "request_id": "approval-1", "actor": "operator", "reason": "verified", "outputs": {}}
    assert client.post(path, json=payload).status_code == 200
    assert client.post(path, json=payload).status_code == 200
    session.expire_all()
    assert node.status == "completed"
    rows = list(session.scalars(select(MessageInbox).where(MessageInbox.handler == "operator")))
    assert len(rows) == 1
    assert json.loads(rows[0].envelope_json)["request"]["actor"] == "operator"
    assert client.post(path, json=dict(payload, reason="different")).status_code == 409


def test_cancel_is_request_until_ack_and_no_duplicate_command(client, session):
    execution, _ = waiting_uploads(session)
    node = node_run(session, execution.id, "xrd")
    path = f"/api/v1/workflow-nodes/{node.id}/operations"
    payload = {"action": "cancel", "request_id": "cancel-1", "actor": "operator", "reason": "stop", "attempt": 1}
    assert client.post(path, json=payload).status_code == 200
    assert client.post(path, json=payload).status_code == 200
    session.expire_all()
    assert node.status == "cancel_requested"
    commands = [json.loads(row.envelope_json) for row in outboxes(session)]
    assert len([command for command in commands if command["message_type"] == "command.cancel"]) == 1
    assert execution.status == "awaiting_feedback"
    WorkflowEngine(session).apply_event(event_for(session, execution.id, "xrd", kind="node.cancelled"), NOW)
    assert node.status == "cancelled"


def test_device_retry_is_refused_without_new_hardware_command(client, session):
    execution, _ = waiting_uploads(session)
    node = node_run(session, execution.id, "xrd")
    WorkflowEngine(session).expire(NOW + timedelta(days=1))
    session.commit()
    count = len(outboxes(session))
    response = client.post(f"/api/v1/workflow-nodes/{node.id}/operations", json={
        "action": "retry", "request_id": "unsafe-retry", "actor": "operator", "reason": "timeout", "attempt": 1})
    assert response.status_code == 409
    assert "device" in response.json()["detail"]
    session.expire_all()
    assert node.attempt == 1 and len(outboxes(session)) == count


def test_compute_retry_is_explicit_bounded_and_stale_results_cannot_win(client, session):
    data = graph_json()
    data["nodes"] = [dict(data["nodes"][-1], inputs={}, retry_policy={"max_attempts": 2})]
    data["edges"], data["positions"] = [], {}
    execution, version = make_execution(session, WorkflowGraph.model_validate(data))
    engine = WorkflowEngine(session)
    engine.start(execution, version, NOW)
    stale = event_for(session, execution.id, "model_inference", {"candidate": {"old": True}})
    engine.expire(NOW + timedelta(days=1))
    session.commit()
    node = node_run(session, execution.id, "model_inference")
    response = client.post(f"/api/v1/workflow-nodes/{node.id}/operations", json={
        "action": "retry", "request_id": "compute-retry", "actor": "operator", "reason": "checked", "attempt": 1})
    assert response.status_code == 200
    session.expire_all()
    assert node.attempt == 2
    assert engine.apply_event(stale, NOW).value == "quarantined"
    assert node.status == "queued"


def test_node_component_configuration_is_pinned_in_command(session):
    data = graph_json()
    data["nodes"][1]["config"]["component_id"] = "xrd-dedicated"
    execution, version = make_execution(session, WorkflowGraph.model_validate(data))
    WorkflowEngine(session).start(execution, version, NOW)
    WorkflowEngine(session).apply_event(event_for(session, execution.id, "magnetron_sputtering", {"sample": {"id": "a"}}), NOW)
    node = node_run(session, execution.id, "xrd")
    assert command_for(session, execution.id, "xrd").payload["config"]["component_id"] == "xrd-dedicated"
    assert result_service(session).resolve(execution.id, "xrd", "xrd-dedicated", node.id, 1)


def test_unqualified_http_retry_uses_saved_result_without_reparsing(session, monkeypatch):
    execution, context = waiting_uploads(session)
    payload = InstrumentUploadCreate(instrument_code="xrd", component_id="xrd-test",
        experiment_batch_id=context["experiment_batch_id"], request_id="fallback-retry", observation={"xrd_phase": "bcc"})
    service = InstrumentUploadService(session)
    service.ingest_upload(payload, now=NOW)
    monkeypatch.setattr(service, "_normalize_upload", lambda *_: pytest.fail("duplicate was reparsed"))
    prepared = service.prepare_workflow_upload(payload)
    service.ingest_upload(payload, prepared_observation=prepared, now=NOW)
    assert node_run(session, execution.id, "xrd").status == "completed"


def test_package_api_retry_skips_manage_db_when_reference_is_unavailable(session, monkeypatch):
    execution, context = waiting_uploads(session)
    node = node_run(session, execution.id, "xrd")
    payload = DataPackageCreate(run_id=execution.id, instrument_code="xrd", component_id="xrd-test",
        node_run_id=node.id, attempt=1, request_id="package-retry", manage_db_record_id="remote-1",
        validation_status="validated", scheduler_observation={"xrd_phase": "bcc"})
    service = ClosedLoopService(session)
    prepared = ({"record_id": "remote-1"}, {"xrd_phase": "bcc"}, None)
    first = service.ingest_data_package(payload, prepared=prepared, now=NOW)
    monkeypatch.setattr("app.services.closed_loop.prepare_data_package", lambda *_args, **_kw: pytest.fail("duplicate remote reference reparsed"))
    prepared = service.prepare_workflow_data_package(payload)
    second = service.ingest_data_package(payload, prepared=prepared, now=NOW)
    assert first.data_package.id == second.data_package.id


def test_public_upload_prepares_without_open_db_transaction_and_uses_configured_prefix(tmp_path, monkeypatch):
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'public-api.db'}", scheduler_enabled=False,
                        kafka=KafkaSettings(topic_prefix="lab"))
    with TestClient(create_app(settings)) as client:
        database = client.app.state.database
        with database.session_factory() as session:
            execution, context = waiting_uploads(session)
            execution_id = execution.id
            node_id = node_run(session, execution.id, "xrd").id
        original = InstrumentUploadService._normalize_upload
        def normalize(service, *args):
            assert not service.session.in_transaction()
            return original(service, *args)
        monkeypatch.setattr(InstrumentUploadService, "_normalize_upload", normalize)
        response = client.post("/api/v1/instrument-uploads", json={
            "instrument_code": "xrd", "component_id": "xrd-test", "experiment_batch_id": context["experiment_batch_id"],
            "node_run_id": node_id, "attempt": 1, "request_id": "public-xrd", "observation": {"xrd_phase": "bcc"}})
        assert response.status_code == 200
        response = client.post("/api/v1/workflow-nodes/" + node_id + "/operations", json={
            "action": "cancel", "request_id": "already-done", "actor": "operator", "reason": "test", "attempt": 1})
        assert response.status_code == 409
        with database.session_factory() as session:
            eds_id = node_run(session, execution_id, "eds").id
        response = client.post(f"/api/v1/workflow-nodes/{eds_id}/operations", json={
            "action": "cancel", "request_id": "public-cancel", "actor": "operator", "reason": "test", "attempt": 1})
        assert response.status_code == 200
        with database.session_factory() as session:
            cancels = [row for row in outboxes(session) if json.loads(row.envelope_json)["message_type"] == "command.cancel"]
            assert cancels[0].topic == "lab.commands.device-gateway"


def test_common_result_handler_refuses_data_package_linked_to_other_node(session):
    execution, _ = waiting_uploads(session, duplicate=True)
    first_node = node_run(session, execution.id, "xrd")
    second_node = node_run(session, execution.id, "xrd-second")
    first = ClosedLoopService(session).ingest_data_package(DataPackageCreate(
        run_id=execution.id, instrument_code="xrd", component_id="xrd-test", node_run_id=first_node.id, attempt=1,
        request_id="first-owner", validation_status="validated", scheduler_observation={"xrd_phase": "bcc"}), now=NOW)
    service = result_service(session)
    attempt = service.resolve(execution.id, "xrd", "xrd-test", second_node.id, 1)
    with pytest.raises(ConflictError, match="data package"):
        service.ingest(attempt, "wrong-owner", {"xrd_phase": "bcc"}, first.data_package.id, NOW)


def test_cancellation_survives_late_start_and_timeout_keeps_uncertainty(client, session):
    execution, _ = waiting_uploads(session)
    node = node_run(session, execution.id, "xrd")
    response = client.post(f"/api/v1/workflow-nodes/{node.id}/operations", json={
        "action": "cancel", "request_id": "cancel-late", "actor": "operator", "reason": "stop", "attempt": 1})
    assert response.status_code == 200
    session.expire_all()
    engine = WorkflowEngine(session)
    assert engine.apply_event(event_for(session, execution.id, "xrd", kind="node.started"), NOW).value == "ignored_stale"
    assert node.status == "cancel_requested"
    engine.expire(NOW + timedelta(days=10))
    assert node.status == "needs_attention"
    assert execution.status == "awaiting_feedback"
    assert node.attempt == 1


def test_operator_action_requires_expected_attempt(client, session):
    execution, _ = waiting_uploads(session)
    node = node_run(session, execution.id, "xrd")
    response = client.post(f"/api/v1/workflow-nodes/{node.id}/operations", json={
        "action": "cancel", "request_id": "cancel-unqualified", "actor": "operator", "reason": "stop"})
    assert response.status_code == 409
    assert "attempt" in response.json()["detail"]


def test_data_package_effects_and_result_link_roll_back_together(session, database):
    execution, _ = waiting_uploads(session)
    node = node_run(session, execution.id, "xrd")
    ClosedLoopService(session).ingest_data_package(DataPackageCreate(
        run_id=execution.id, instrument_code="xrd", component_id="xrd-test", node_run_id=node.id, attempt=1,
        request_id="package-rollback", validation_status="validated", scheduler_observation={"xrd_phase": "bcc"}),
        now=NOW, commit=False)
    session.rollback()
    with database.session_factory() as fresh:
        assert not list(fresh.scalars(select(DataPackage)))
        assert not list(fresh.scalars(select(NodeDataPackageLink)))
        assert not list(fresh.scalars(select(MessageInbox)))
        assert node_run(fresh, execution.id, "xrd").status == "queued"


def test_publisher_does_not_send_pending_execute_after_cancel_request(client, session, database):
    from app.workflows.messaging import OutboxPublisher

    execution, _ = waiting_uploads(session)
    node = node_run(session, execution.id, "xrd")
    assert client.post(f"/api/v1/workflow-nodes/{node.id}/operations", json={
        "action": "cancel", "request_id": "cancel-before-send", "actor": "operator", "reason": "stop", "attempt": 1}).status_code == 200
    seen = []
    class Transport:
        def publish(self, topic, key, envelope):
            from app.workflows.schemas import MessageEnvelope

            seen.append(MessageEnvelope.model_validate_json(envelope))
    OutboxPublisher(database.session_factory, Transport(), clock=lambda: NOW).publish_batch(NOW)
    assert not any(event.node_run_id == node.id and event.message_type == "command.execute" for event in seen)


def test_cancel_can_settle_active_sibling_of_failed_run(client, session):
    execution, _ = waiting_uploads(session)
    WorkflowEngine(session).apply_event(event_for(session, execution.id, "eds", kind="node.failed"), NOW)
    session.commit()
    node = node_run(session, execution.id, "xrd")
    response = client.post(f"/api/v1/workflow-nodes/{node.id}/operations", json={
        "action": "cancel", "request_id": "failed-sibling", "actor": "operator", "reason": "stop", "attempt": 1})
    assert response.status_code == 200
    session.expire_all()
    assert node.status == "cancel_requested"
    assert execution.status == "awaiting_feedback"


def test_unvalidated_package_is_linked_but_does_not_complete_node(session):
    execution, _ = waiting_uploads(session)
    node = node_run(session, execution.id, "xrd")
    result = ClosedLoopService(session).ingest_data_package(DataPackageCreate(
        run_id=execution.id, instrument_code="xrd", component_id="xrd-test", node_run_id=node.id, attempt=1,
        request_id="raw-upload", validation_status="uploaded", scheduler_observation={"xrd_phase": "bcc"}), now=NOW)
    assert node.status == "queued"
    assert not result.round_completed
    assert len(list(session.scalars(select(NodeDataPackageLink)))) == 1
    service = result_service(session)
    with pytest.raises(ConflictError, match="data package"):
        service.ingest(service.resolve(execution.id, "xrd", "xrd-test", node.id, 1), "unchecked", {"xrd_phase": "bcc"}, result.data_package.id, NOW)


def test_manage_db_parser_runs_outside_sql_transaction_and_saved_package_is_node_owned(session, monkeypatch):
    from app.services.manage_db_client import ManageDbRecord
    from app.services.manage_db_raw_parser import ParsedManageDbDataset

    execution, context = waiting_uploads(session)
    node = node_run(session, execution.id, "xrd")
    service = ClosedLoopService(session)
    calls = []
    def fetch(*_, **kwargs):
        assert not session.in_transaction()
        calls.append("fetch")
        return ManageDbRecord("remote-1", {"xrd_phase": "bcc"}, "https://example.invalid/record")
    def parse(*_, **kwargs):
        assert not session.in_transaction()
        assert kwargs["experiment_batch_id"] == context["experiment_batch_id"]
        calls.append("parse")
        return ParsedManageDbDataset("xrd", {"xrd_phase": "bcc"}, [], [])
    monkeypatch.setattr("app.services.closed_loop.ManageDbClient.fetch_development_data", fetch)
    monkeypatch.setattr("app.services.closed_loop.ManageDbRawParser.parse_record", parse)
    payload = DataPackageCreate(run_id=execution.id, instrument_code="xrd", component_id="xrd-test",
        node_run_id=node.id, attempt=1, request_id="parse-remote", manage_db_record_id="remote-1", validation_status="validated")
    prepared = service.prepare_workflow_data_package(payload)
    assert calls == ["fetch", "parse"]
    result = service.ingest_data_package(payload, prepared=prepared, now=NOW)
    assert node.status == "completed"
    assert json.loads(result.data_package.scheduler_observation_json)["xrd_phase"] == "bcc"


def test_http_and_kafka_results_share_validation_and_enqueue_model_only_once(session):
    from app.workflows.messaging import MessagingService

    execution, _ = waiting_uploads(session)
    service = result_service(session)
    for code, observation in (("xrd", {"xrd_phase": "bcc"}), ("eds", {"ti_at": 25}), ("nanoindentation", {"hardness_gpa": 13})):
        node = node_run(session, execution.id, code)
        attempt = service.resolve(execution.id, code, f"{code}-test", node.id, 1)
        assert service.ingest(attempt, f"http-{code}", observation, None, NOW).value == "applied"
        late = event_for(session, execution.id, code, {"result": observation})
        assert MessagingService(session).ingest(late.model_dump_json().encode(), "events", NOW).value == "ignored_stale"
    model = node_run(session, execution.id, "model_inference")
    assert model.attempt == 1
    assert len([row for row in outboxes(session) if json.loads(row.envelope_json)["node_id"] == "model_inference"]) == 1
    assert execution.status == "awaiting_feedback"


@pytest.mark.parametrize("patch", [{"attempt": True}, {"node_run_id": "bad"}, {"request_id": 42}])
def test_upload_correlation_is_strict_even_on_legacy_schema(patch):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        InstrumentUploadCreate.model_validate(dict(instrument_code="xrd", component_id="xrd-test", experiment_batch_id="batch", **patch))


def test_operator_request_rejects_oversized_outputs(client):
    response = client.post("/api/v1/workflow-nodes/missing/operations", json={
        "action": "approve", "request_id": "huge", "actor": "operator", "reason": "test",
        "outputs": {"data": "x" * 1_000_001}})
    assert response.status_code == 422


@pytest.mark.parametrize("kind", ["instrument", "package"])
def test_parser_preflight_refuses_dirty_session_without_flushing_or_rolling_back(session, kind):
    execution, context = waiting_uploads(session)
    session.autoflush = True
    execution.feedback_observation = '{"caller_change":true}'
    if kind == "instrument":
        service = InstrumentUploadService(session)
        payload = InstrumentUploadCreate(instrument_code="xrd", component_id="xrd-test",
            experiment_batch_id=context["experiment_batch_id"], request_id="dirty-upload", observation={"xrd_phase": "bcc"})
        prepare = service.prepare_workflow_upload
    else:
        service = ClosedLoopService(session)
        payload = DataPackageCreate(run_id=execution.id, instrument_code="xrd", component_id="xrd-test",
            request_id="dirty-package", validation_status="validated", scheduler_observation={"xrd_phase": "bcc"})
        prepare = service.prepare_workflow_data_package
    with pytest.raises(ConflictError, match="clean"):
        prepare(payload)
    assert execution in session.dirty
    assert execution.feedback_observation == '{"caller_change":true}'


@pytest.mark.parametrize("value", [float("nan"), float("inf"), "x" * 1_000_001], ids=["nan", "infinity", "oversized"])
@pytest.mark.parametrize("schema", [InstrumentUploadCreate, DataPackageCreate])
def test_correlated_upload_request_is_bounded_finite_json(schema, value):
    from pydantic import ValidationError

    common = dict(instrument_code="xrd", component_id="xrd-test", request_id="bounded-upload")
    if schema is InstrumentUploadCreate:
        common.update(experiment_batch_id="batch", observation={"value": value})
    else:
        common.update(run_id=str(uuid4()), scheduler_observation={"value": value})
    with pytest.raises(ValidationError):
        schema.model_validate(common)


@pytest.mark.parametrize("kind", ["instrument", "package"])
def test_preflight_result_cannot_move_to_serial_successor_during_parsing(session, kind):
    execution, context = waiting_uploads(session, duplicate=True, serial=True)
    execution_id = execution.id
    if kind == "instrument":
        service = InstrumentUploadService(session)
        payload = InstrumentUploadCreate(instrument_code="xrd", component_id="xrd-test",
            experiment_batch_id=context["experiment_batch_id"], request_id="serial-upload", observation={"xrd_phase": "bcc"})
        prepared = service.prepare_workflow_upload(payload)
    else:
        service = ClosedLoopService(session)
        payload = DataPackageCreate(run_id=execution_id, instrument_code="xrd", component_id="xrd-test",
            request_id="serial-package", validation_status="validated", scheduler_observation={"xrd_phase": "bcc"})
        prepared = service.prepare_workflow_data_package(payload)
    engine = WorkflowEngine(session)
    engine.apply_event(event_for(session, execution_id, "xrd", {"result": {"xrd_phase": "concurrent-result"}}), NOW)
    session.commit()
    assert node_run(session, execution_id, "xrd-second").status == "queued"
    with pytest.raises(ConflictError):
        if kind == "instrument":
            service.ingest_upload(payload, prepared_observation=prepared, now=NOW, commit=False)
        else:
            service.ingest_data_package(payload, prepared=prepared, now=NOW, commit=False)
    session.rollback()
    assert node_run(session, execution_id, "xrd-second").status == "queued"
    assert not list(session.scalars(select(NodeDataPackageLink)))


def test_cancel_only_run_expires_to_attention(client, session):
    data = graph_json()["nodes"][1]
    data["inputs"] = {}
    graph = WorkflowGraph.model_validate({"nodes": [data], "edges": []})
    execution, version = make_execution(session, graph)
    WorkflowEngine(session).start(execution, version, NOW)
    session.commit()
    node = node_run(session, execution.id, "xrd")
    assert client.post(f"/api/v1/workflow-nodes/{node.id}/operations", json={
        "action": "cancel", "request_id": "cancel-only", "actor": "operator", "reason": "stop", "attempt": 1}).status_code == 200
    session.expire_all()
    assert WorkflowEngine(session).expire(NOW + timedelta(days=10)) == 1
    assert node.status == "needs_attention"
    assert execution.status == "awaiting_feedback"


def test_node_only_data_package_correlation_is_valid_and_instance_owned(session):
    execution, _ = waiting_uploads(session)
    node = node_run(session, execution.id, "xrd")
    payload = DataPackageCreate(node_run_id=node.id, attempt=1, request_id="node-only",
        instrument_code="xrd", component_id="xrd-test", validation_status="validated", scheduler_observation={"xrd_phase": "bcc"})
    result = ClosedLoopService(session).ingest_data_package(payload, now=NOW)
    assert result.data_package.execution_id == execution.id
    assert node.status == "completed"


def test_cancel_before_late_execute_requires_durable_gateway_tombstone(session, database):
    from app.workflows.api import OperationRequest
    from app.workflows.messaging import OutboxPublisher
    from app.workflows.models import MessageOutbox
    from app.workflows.operations import OperationService
    from app.workflows.schemas import MessageEnvelope

    execution, _ = waiting_uploads(session)
    node = node_run(session, execution.id, "xrd")
    command = command_for(session, execution.id, "xrd")
    sender = OutboxPublisher(database.session_factory, None, clock=lambda: NOW)
    # Model a publisher already past its final database check, before broker I/O.
    assert sender._sendable(command.message_id, command.model_dump_json(), NOW)
    OperationService(session).apply(node.id, OperationRequest(action="cancel", attempt=1,
        request_id="cancel-overtake", actor="operator", reason="stop"), NOW)
    session.commit()
    cancel_row = next(row for row in outboxes(session) if json.loads(row.envelope_json)["message_type"] == "command.cancel")
    cancel = MessageEnvelope.model_validate_json(cancel_row.envelope_json)
    assert cancel.command_id == command.command_id
    assert cancel.payload["require_cancellation_tombstone"] is True
    # The scheduler cannot retract buffered Kafka bytes. Compliant gateways must
    # persist this command-ID tombstone before acknowledging cancellation.
    with database.session_factory.begin() as gateway:
        from app.workflows.messaging import MessagingService
        row, fresh = MessagingService(gateway)._reserve("gateway-cancel-test", cancel.command_id,
            "0" * 64, cancel.model_dump_json(), NOW)
        row.status = "cancelled"
    with database.session_factory() as restarted_gateway:
        tombstone = restarted_gateway.scalar(select(MessageInbox).where(
            MessageInbox.handler == "gateway-cancel-test", MessageInbox.message_id == command.command_id))
        assert tombstone.status == "cancelled"
        assert not (tombstone is None or tombstone.status != "cancelled")
