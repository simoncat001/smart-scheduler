from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
import importlib
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import event, select
from sqlalchemy.dialects import mysql
from sqlalchemy.schema import CreateTable
from sqlalchemy.exc import IntegrityError

from app.database import Base, Database
from app.models import Execution, Schedule, utcnow
from app.schemas import ScheduleCreate
from app.services.exceptions import ConflictError
from app.services.scheduler import SchedulerService


def graph_data():
    return {
        "schema_version": 1,
        "nodes": [
            {"id": "prepare", "type": "manual_approval", "label": "Prepare"},
            {
                "id": "xrd_a", "type": "data_upload", "label": "XRD A",
                "config": {"instrument_code": "xrd"},
                "executor": {
                    "kind": "device", "resource_id": "xrd-test-1",
                    "gateway_id": "test-gateway", "adapter_id": "simulator",
                },
                "outputs": {"phase": {"schema": {"type": "string"}}},
            },
        ],
        "edges": [{"source": "prepare", "target": "xrd_a"}],
        "positions": {"prepare": {"x": 100, "y": 300}},
    }


def workflow_modules():
    return (
        importlib.import_module("app.workflows.schemas"),
        importlib.import_module("app.workflows.definitions"),
        importlib.import_module("app.workflows.models"),
    )


def resources(models):
    device = models.DeviceResource(
        id="xrd-test-1", gateway_id="test-gateway", adapter_id="simulator",
        capacity=1, enabled=True, simulated=True,
    )
    return {device.id: device}


@pytest.fixture()
def db(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'workflow-unit.db'}")
    yield database
    database.dispose()


def test_two_same_type_nodes_are_valid_independent_instances():
    schemas, definitions, models = workflow_modules()
    data = graph_data()
    second = deepcopy(data["nodes"][1])
    second["id"] = "xrd_b"
    data["nodes"].append(second)
    data["edges"].append({"source": "prepare", "target": "xrd_b"})
    graph = schemas.WorkflowGraph.model_validate(data)
    definitions.validate_graph(graph, resources(models))
    assert [node.id for node in graph.nodes] == ["prepare", "xrd_a", "xrd_b"]


@pytest.mark.parametrize("mutation, message", [
    ("cycle", "cycle"),
    ("duplicate", "duplicate"),
    ("missing_endpoint", "endpoint"),
    ("missing_port", "port"),
    ("unknown_type", "type"),
    ("unknown_version", "version"),
    ("missing_executor", "executor"),
    ("unknown_adapter", "adapter"),
    ("device_auto_retry", "retry"),
    ("unexpected_config", "config"),
])
def test_publication_rejects_invalid_workflow(mutation, message):
    schemas, definitions, models = workflow_modules()
    data = graph_data()
    available = resources(models)
    if mutation == "cycle":
        data["edges"].append({"source": "xrd_a", "target": "prepare"})
    elif mutation == "duplicate":
        data["nodes"].append(deepcopy(data["nodes"][1]))
    elif mutation == "missing_endpoint":
        data["edges"][0]["target"] = "absent"
    elif mutation == "missing_port":
        data["edges"][0].update(source_port="absent", target_port="sample")
    elif mutation == "unknown_type":
        data["nodes"][0]["type"] = "arbitrary_python"
    elif mutation == "unknown_version":
        data["nodes"][0]["type_version"] = 99
    elif mutation == "missing_executor":
        data["nodes"][1].pop("executor")
    elif mutation == "unknown_adapter":
        data["nodes"][1]["executor"]["adapter_id"] = "unknown"
    elif mutation == "device_auto_retry":
        data["nodes"][1]["retry_policy"] = {"max_attempts": 2}
    elif mutation == "unexpected_config":
        data["nodes"][0]["config"] = {"python": "exec('unsafe')"}
    with pytest.raises(ValueError, match=message):
        definitions.validate_graph(schemas.WorkflowGraph.model_validate(data), available)


def test_required_input_must_be_bound_and_types_must_match():
    schemas, definitions, models = workflow_modules()
    data = graph_data()
    data["nodes"][0]["outputs"] = {"sample": {"schema": {"type": "string"}}}
    data["nodes"][1]["inputs"] = {"sample": {"schema": {"type": "number"}}}
    with pytest.raises(ValueError, match="required input"):
        definitions.validate_graph(schemas.WorkflowGraph.model_validate(data), resources(models))
    data["edges"][0].update(source_port="sample", target_port="sample")
    with pytest.raises(ValueError, match="compatible"):
        definitions.validate_graph(schemas.WorkflowGraph.model_validate(data), resources(models))
    data["nodes"][1]["inputs"]["sample"]["schema"] = {"type": "string"}
    definitions.validate_graph(schemas.WorkflowGraph.model_validate(data), resources(models))


def test_remote_schema_references_are_rejected_without_network():
    schemas, definitions, models = workflow_modules()
    data = graph_data()
    data["nodes"][1]["outputs"]["phase"]["schema"] = {"$ref": "https://unsafe.invalid/schema"}
    with pytest.raises(ValueError, match="reference"):
        definitions.validate_graph(schemas.WorkflowGraph.model_validate(data), resources(models))


def test_explicit_default_is_validated_against_port_schema():
    schemas, definitions, models = workflow_modules()
    data = graph_data()
    data["nodes"][1]["inputs"] = {
        "sample": {"schema": {"type": "string"}, "use_default": True, "default": 7},
    }
    with pytest.raises(ValueError, match="default"):
        definitions.validate_graph(schemas.WorkflowGraph.model_validate(data), resources(models))


def test_condition_branch_required_inputs_need_optional_or_default():
    schemas, definitions, models = workflow_modules()
    data = {
        "nodes": [
            {"id": "route", "type": "condition", "label": "Route", "config": {
                "field": "value", "operator": "eq", "value": 1,
                "match_route": "yes", "default_route": "no",
            }, "outputs": {"value": {"schema": {"type": "number"}}}},
            {"id": "yes", "type": "manual_approval", "label": "Yes",
             "outputs": {"result": {"schema": {"type": "number"}}}},
            {"id": "no", "type": "manual_approval", "label": "No"},
            {"id": "join", "type": "join", "label": "Join",
             "inputs": {"result": {"schema": {"type": "number"}}}},
        ],
        "edges": [
            {"source": "route", "target": "yes", "route": "yes"},
            {"source": "route", "target": "no", "route": "no"},
            {"source": "yes", "target": "join", "source_port": "result", "target_port": "result"},
            {"source": "no", "target": "join"},
        ],
    }
    with pytest.raises(ValueError, match="branch"):
        definitions.validate_graph(schemas.WorkflowGraph.model_validate(data), {})
    data["nodes"][3]["inputs"]["result"]["required"] = False
    definitions.validate_graph(schemas.WorkflowGraph.model_validate(data), {})


def test_publication_is_immutable_and_positions_do_not_change_semantic_digest(db):
    schemas, definitions, models = workflow_modules()
    db.create_schema()
    with db.session_factory() as session:
        session.add_all(resources(models).values())
        session.flush()
        service = definitions.DefinitionService(session)
        draft = service.create("Test workflow", schemas.WorkflowGraph.model_validate(graph_data()))
        first = service.publish(draft.id, draft.revision)
        session.commit()
        original_json = first.graph_json
        moved = graph_data()
        moved["positions"]["prepare"]["x"] = 400
        service.save(draft.id, draft.revision, schemas.WorkflowGraph.model_validate(moved))
        second = service.publish(draft.id, draft.revision)
        session.commit()
        assert (first.version, second.version) == (1, 2)
        assert first.graph_json == original_json
        assert first.digest == second.digest
        first.graph_json = "{}"
        with pytest.raises(ConflictError, match="immutable"):
            session.flush()
        session.rollback()


def test_stale_revision_does_not_overwrite_the_draft(db):
    schemas, definitions, _ = workflow_modules()
    db.create_schema()
    with db.session_factory() as session:
        service = definitions.DefinitionService(session)
        draft = service.create("Test workflow", schemas.WorkflowGraph.model_validate(graph_data()))
        revision = draft.revision
        changed = graph_data()
        changed["nodes"][0]["label"] = "Updated"
        service.save(draft.id, revision, schemas.WorkflowGraph.model_validate(changed))
        with pytest.raises(ConflictError, match="revision"):
            service.save(draft.id, revision, schemas.WorkflowGraph.model_validate(graph_data()))
        assert "Updated" in draft.draft_graph_json


def test_running_schedule_cannot_change_version_and_pinned_run_stays_unchanged(db):
    schemas, definitions, models = workflow_modules()
    db.create_schema()
    with db.session_factory() as session:
        schedule = SchedulerService(session).create_schedule(ScheduleCreate(
            name="workflow schedule", target="test", base_interval_seconds=60,
        ))
        session.add_all(resources(models).values())
        session.flush()
        service = definitions.DefinitionService(session)
        draft = service.create("Test workflow", schemas.WorkflowGraph.model_validate(graph_data()))
        first = service.publish(draft.id, draft.revision)
        binding = service.bind(schedule.id, first.id)
        now = utcnow()
        execution = Execution(schedule_id=schedule.id, scheduled_for=now,
                              dispatched_at=now, status="awaiting_feedback")
        session.add(execution)
        session.flush()
        run = models.WorkflowRun(execution_id=execution.id, workflow_version_id=first.id)
        session.add(run)
        schedule.awaiting_feedback = True
        moved = graph_data()
        moved["nodes"][0]["label"] = "Second definition"
        service.save(draft.id, draft.revision, schemas.WorkflowGraph.model_validate(moved))
        second = service.publish(draft.id, draft.revision)
        with pytest.raises(ConflictError, match="active"):
            service.bind(schedule.id, second.id)
        assert run.workflow_version_id == first.id
        assert binding.workflow_version_id == first.id


def test_additive_tables_compile_for_mysql_without_altering_legacy_columns(db):
    _, _, models = workflow_modules()
    existing = {column.name for column in Schedule.__table__.columns}
    for table in models.WORKFLOW_TABLES:
        ddl = str(CreateTable(table).compile(dialect=mysql.dialect()))
        assert "ENGINE=InnoDB" in ddl
    assert {column.name for column in Schedule.__table__.columns} == existing
    db.create_schema()
    with db.session_factory() as session:
        assert list(session.scalars(select(models.MessageOutbox))) == []


def test_initializer_rejects_sqlite_before_writing_tables(db):
    initialize = importlib.import_module("app.workflows.initialize")
    with pytest.raises(ValueError, match="MySQL"):
        initialize.initialize_workflows(db)
    with db.engine.connect() as connection:
        assert db.engine.dialect.has_table(connection, "workflow_definitions") is False


def test_default_template_preserves_current_node_positions():
    template = importlib.import_module("app.workflows.templates").default_materials_graph()
    assert len(template.nodes) == 6
    assert template.positions["xrd"].x == 576
    assert template.positions["eds"].y == 320
    assert template.positions["nanoindentation"].y == 460
    assert template.decision_node_id == "model_inference"


def test_invalid_message_and_nonfinite_positions_are_rejected():
    schemas, _, _ = workflow_modules()
    data = graph_data()
    data["positions"]["prepare"]["x"] = float("nan")
    with pytest.raises(ValueError):
        schemas.WorkflowGraph.model_validate(data)
    with pytest.raises(ValueError):
        schemas.MessageEnvelope.model_validate({"schema_version": 2, "payload": {}})


def test_boolean_and_numeric_enums_are_not_interchangeable():
    schemas, definitions, models = workflow_modules()
    data = graph_data()
    data["nodes"][0]["outputs"] = {"sample": {"schema": {"enum": [True]}}}
    data["nodes"][1]["inputs"] = {"sample": {"schema": {"enum": [1]}}}
    data["edges"][0].update(source_port="sample", target_port="sample")
    with pytest.raises(ValueError, match="compatible"):
        definitions.validate_graph(schemas.WorkflowGraph.model_validate(data), resources(models))


@pytest.mark.parametrize("schema", [
    {"$ref": "#/$defs/missing"},
    {"$defs": {"loop": {"$ref": "#/$defs/loop"}}, "$ref": "#/$defs/loop"},
    {"$defs": {"a": {"$ref": "#/$defs/b"}, "b": {"$ref": "#/$defs/a"}}, "$ref": "#/$defs/a"},
    {"title": "not a schema", "$ref": "#/title"},
    {"default": {"type": "number", "minimum": "invalid"}, "$ref": "#/default"},
])
def test_schema_references_must_resolve_and_cannot_recurse(schema):
    schemas, definitions, models = workflow_modules()
    data = graph_data()
    data["nodes"][1]["outputs"]["phase"]["schema"] = schema
    with pytest.raises(ValueError, match="reference"):
        definitions.validate_graph(schemas.WorkflowGraph.model_validate(data), resources(models))


def test_local_schema_and_explicit_null_default_are_supported():
    schemas, definitions, models = workflow_modules()
    data = graph_data()
    data["nodes"][1]["inputs"] = {
        "sample": {"schema": {"$defs": {"optional": {"type": ["string", "null"]}},
                              "$ref": "#/$defs/optional"},
                   "use_default": True, "default": None},
    }
    definitions.validate_graph(schemas.WorkflowGraph.model_validate(data), resources(models))


def test_optional_output_cannot_feed_required_input_without_default():
    schemas, definitions, models = workflow_modules()
    data = graph_data()
    data["nodes"][0]["outputs"] = {"sample": {"schema": {"type": "string"}, "required": False}}
    data["nodes"][1]["inputs"] = {"sample": {"schema": {"type": "string"}}}
    data["edges"][0].update(source_port="sample", target_port="sample")
    with pytest.raises(ValueError, match="optional"):
        definitions.validate_graph(schemas.WorkflowGraph.model_validate(data), resources(models))
    data["nodes"][1]["inputs"]["sample"].update(use_default=True, default="sample-default")
    definitions.validate_graph(schemas.WorkflowGraph.model_validate(data), resources(models))


def test_published_template_has_parallel_dependencies_after_explicit_configuration():
    schemas, definitions, models = workflow_modules()
    graph = importlib.import_module("app.workflows.templates").default_materials_graph()
    with pytest.raises(ValueError, match="pinned model"):
        definitions.validate_graph(graph, {})
    data = graph.model_dump(mode="json", by_alias=True)
    data["nodes"][-1]["config"]["model_version"] = "test-model-sha256"
    configured = schemas.WorkflowGraph.model_validate(data)
    definitions.validate_graph(configured)
    for node_id in ("xrd", "eds", "nanoindentation"):
        assert {edge.source for edge in configured.edges if edge.target == node_id} == {"magnetron_sputtering"}
    assert {edge.source for edge in configured.edges if edge.target == "gate"} == {"xrd", "eds", "nanoindentation"}


def test_invalid_publication_does_not_write_a_version(db):
    schemas, definitions, models = workflow_modules()
    db.create_schema()
    with db.session_factory() as session:
        service = definitions.DefinitionService(session)
        data = graph_data()
        data["nodes"][1]["executor"]["adapter_id"] = "unknown"
        draft = service.create("Editable draft", schemas.WorkflowGraph.model_validate(data))
        with pytest.raises(ValueError, match="adapter"):
            service.publish(draft.id, draft.revision)
        assert list(session.scalars(select(models.WorkflowVersion))) == []
        assert session.get(models.WorkflowDefinition, draft.id) is draft


def test_device_publication_needs_no_resource_registration(db):
    schemas, definitions, models = workflow_modules()
    db.create_schema()
    data = graph_data()
    data["nodes"][1]["executor"].pop("resource_id")
    data["nodes"][1]["executor"]["adapter_id"] = "kafka_gateway"
    with db.session_factory.begin() as session:
        service = definitions.DefinitionService(session)
        draft = service.create("External gateway", schemas.WorkflowGraph.model_validate(data))
        statements = []
        event.listen(session.bind, "before_cursor_execute", lambda *args: statements.append(args[2]))
        version = service.publish(draft.id, draft.revision)
        assert version.version == 1
        assert not any("workflow_device_resources" in sql for sql in statements)
        assert list(session.scalars(select(models.ResourceReservation))) == []


def test_legacy_resource_identifier_is_ignored_by_lightweight_validation():
    schemas, definitions, _ = workflow_modules()
    definitions.validate_graph(schemas.WorkflowGraph.model_validate(graph_data()), {})


def test_initializer_no_longer_creates_resource_control_tables():
    _, _, models = workflow_modules()
    assert not {"workflow_device_resources", "workflow_resource_reservations"} & {
        table.name for table in models.WORKFLOW_TABLES
    }


def test_draft_service_does_not_commit_its_callers_transaction(db):
    schemas, definitions, models = workflow_modules()
    db.create_schema()
    with db.session_factory() as session:
        draft = definitions.DefinitionService(session).create(
            "Rollback draft", schemas.WorkflowGraph.model_validate(graph_data()),
        )
        draft_id = draft.id
        session.rollback()
    with db.session_factory() as session:
        assert session.get(models.WorkflowDefinition, draft_id) is None


def test_message_ids_are_deduplicated_per_handler_by_database(db):
    _, _, models = workflow_modules()
    db.create_schema()
    with db.session_factory() as session:
        session.add(models.MessageInbox(handler="engine", message_id="event-1", digest="a" * 64,
                                        status="applied", envelope_json="{}"))
        session.commit()
        session.add(models.MessageInbox(handler="engine", message_id="event-1", digest="a" * 64,
                                        status="applied", envelope_json="{}"))
        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()
        session.add(models.MessageInbox(handler="gateway", message_id="event-1", digest="a" * 64,
                                        status="applied", envelope_json="{}"))
        session.commit()
        assert len(list(session.scalars(select(models.MessageInbox)))) == 2


def test_valid_message_round_trip_and_expiry_and_timezone_validation():
    schemas, _, _ = workflow_modules()
    now = datetime.now(UTC)
    values = {
        "message_id": str(uuid4()), "message_type": "command.execute", "run_id": str(uuid4()),
        "workflow_version_id": str(uuid4()), "node_id": "xrd_a", "node_run_id": str(uuid4()),
        "attempt": 1, "command_id": str(uuid4()), "gateway_id": "gateway-1", "adapter_id": "simulator",
        "occurred_at": now, "expires_at": now + timedelta(seconds=60), "payload": {"sample": {"batch": "test"}},
    }
    message = schemas.MessageEnvelope.model_validate(values)
    assert schemas.MessageEnvelope.model_validate_json(message.model_dump_json()) == message
    for invalid in ({"occurred_at": now.replace(tzinfo=None)}, {"expires_at": now}, {"expires_at": None},
                    {"unexpected": True}, {"attempt": 0}, {"run_id": "latest"}):
        with pytest.raises(ValueError):
            schemas.MessageEnvelope.model_validate({**values, **invalid})


def test_positions_do_not_affect_digest_but_config_and_labels_do():
    schemas, definitions, _ = workflow_modules()
    graph = schemas.WorkflowGraph.model_validate(graph_data())
    moved = graph_data()
    moved["positions"]["prepare"]["x"] += 1
    assert definitions.semantic_digest(schemas.WorkflowGraph.model_validate(moved)) == definitions.semantic_digest(graph)
    moved["nodes"][0]["label"] = "New label"
    assert definitions.semantic_digest(schemas.WorkflowGraph.model_validate(moved)) != definitions.semantic_digest(graph)


def test_graph_rejects_unknown_properties_and_large_json():
    schemas, _, _ = workflow_modules()
    data = graph_data()
    data["nodes"][0]["script"] = "unsafe"
    with pytest.raises(ValueError):
        schemas.WorkflowGraph.model_validate(data)
    data = graph_data()
    data["nodes"][0]["config"] = {"large": "x" * 1_000_000}
    with pytest.raises(ValueError, match="limit"):
        schemas.WorkflowGraph.model_validate(data)


def test_initialization_cli_redacts_engine_creation_errors(monkeypatch, capsys):
    initialize = importlib.import_module("app.workflows.initialize")
    config = importlib.import_module("app.config")
    monkeypatch.setattr(config, "get_settings", lambda: SimpleNamespace(database_url="invalid-test-url"))
    monkeypatch.setattr("sys.argv", ["workflows"])

    def unavailable_engine(_url):
        raise ValueError("connection config contains test-secret-not-for-logs")

    monkeypatch.setattr(initialize, "Database", unavailable_engine)
    assert initialize.main() == 1
    output = capsys.readouterr()
    assert "test-secret-not-for-logs" not in output.err
    assert "configuration" in output.err


def test_schema_reference_fanout_is_rejected_before_value_validation():
    registry = importlib.import_module("app.workflows.registry")
    definitions = {"base": {"type": "number"}}
    previous = "base"
    for index in range(8):
        current = f"level_{index}"
        definitions[current] = {"allOf": [{"$ref": f"#/$defs/{previous}"} for _ in range(4)]}
        previous = current
    with pytest.raises(ValueError, match="complexity"):
        registry.validate_schema({"$defs": definitions, "$ref": f"#/$defs/{previous}"})


@pytest.mark.parametrize("schema", [{"type": "string", "pattern": "^(a+)+$"},
                                   {"type": "array", "uniqueItems": True}])
def test_unbounded_validation_keywords_are_not_in_initial_schema_profile(schema):
    registry = importlib.import_module("app.workflows.registry")
    with pytest.raises(ValueError, match="unsupported"):
        registry.validate_schema(schema)


def test_required_condition_output_is_valid_inside_its_selected_branch():
    schemas, definitions, _ = workflow_modules()
    graph = schemas.WorkflowGraph.model_validate({
        "nodes": [
            {"id": "route", "type": "condition", "label": "Route",
             "config": {"field": "value", "operator": "eq", "value": 1, "match_route": "yes", "default_route": "no"},
             "outputs": {"value": {"schema": {"type": "number"}}}},
            {"id": "yes", "type": "manual_approval", "label": "Yes",
             "inputs": {"value": {"schema": {"type": "number"}}}},
            {"id": "no", "type": "manual_approval", "label": "No"},
        ],
        "edges": [{"source": "route", "target": "yes", "route": "yes", "source_port": "value", "target_port": "value"},
                  {"source": "route", "target": "no", "route": "no"}],
    })
    definitions.validate_graph(graph, {})


def test_rebind_refreshes_cached_binding_before_assigning_version(db):
    schemas, definitions, models = workflow_modules()
    db.create_schema()
    with db.session_factory() as seed:
        schedule = SchedulerService(seed).create_schedule(ScheduleCreate(name="bind race", target="test", base_interval_seconds=60))
        seed.add_all(resources(models).values())
        service = definitions.DefinitionService(seed)
        draft = service.create("Binding versions", schemas.WorkflowGraph.model_validate(graph_data()))
        first = service.publish(draft.id, draft.revision)
        second = service.publish(draft.id, draft.revision)
        service.bind(schedule.id, first.id)
        seed.commit()
        schedule_id, first_id, second_id = schedule.id, first.id, second.id

    with db.session_factory() as stale:
        cached = stale.get(models.WorkflowScheduleBinding, schedule_id)
        assert cached.workflow_version_id == first_id
        with db.session_factory() as newer:
            definitions.DefinitionService(newer).bind(schedule_id, second_id)
            newer.commit()
        definitions.DefinitionService(stale).bind(schedule_id, first_id)
        stale.commit()
    with db.session_factory() as verify:
        assert verify.get(models.WorkflowScheduleBinding, schedule_id).workflow_version_id == first_id


def test_publication_allocates_version_with_mysql_current_read(db):
    schemas, definitions, models = workflow_modules()
    db.create_schema()
    with db.session_factory() as session:
        session.add_all(resources(models).values())
        service = definitions.DefinitionService(session)
        draft = service.create("Current version read", schemas.WorkflowGraph.model_validate(graph_data()))
        statements = []

        @event.listens_for(session, "do_orm_execute")
        def record_statement(execute_state):
            if execute_state.is_select:
                statements.append(str(execute_state.statement.compile(dialect=mysql.dialect())))

        first = service.publish(draft.id, draft.revision)
        second = service.publish(draft.id, draft.revision)
        assert (first.version, second.version) == (1, 2)
        version_reads = [statement for statement in statements if "FROM workflow_versions" in statement]
        assert version_reads and all("FOR UPDATE" in statement for statement in version_reads)


def test_binding_checks_use_mysql_current_reads_even_with_cached_version(db):
    schemas, definitions, models = workflow_modules()
    db.create_schema()
    with db.session_factory() as session:
        schedule = SchedulerService(session).create_schedule(ScheduleCreate(name="current bind", target="test", base_interval_seconds=60))
        session.add_all(resources(models).values())
        service = definitions.DefinitionService(session)
        draft = service.create("Bind current reads", schemas.WorkflowGraph.model_validate(graph_data()))
        version = service.publish(draft.id, draft.revision)
        statements = []

        @event.listens_for(session, "do_orm_execute")
        def record_statement(execute_state):
            if execute_state.is_select:
                statements.append(str(execute_state.statement.compile(dialect=mysql.dialect())))

        service.bind(schedule.id, version.id)
        assert any("FROM workflow_versions" in statement for statement in statements)
        assert all("FOR UPDATE" in statement for statement in statements)


def test_schema_keyword_limits_do_not_reject_literal_metadata_or_property_names():
    registry = importlib.import_module("app.workflows.registry")
    schema = {"type": "object", "properties": {"pattern": {"type": "string"}},
              "default": {"pattern": "literal", "$ref": "not-a-reference"}}
    registry.validate_schema(schema)
    registry.validate_value(schema, {"pattern": "data"}, "test property")


def test_unevaluated_composition_is_rejected_before_exponential_validation():
    registry = importlib.import_module("app.workflows.registry")
    schema = {"type": "object"}
    for _ in range(18):
        schema = {"allOf": [schema], "unevaluatedProperties": False}
    with pytest.raises(ValueError, match="unsupported"):
        registry.validate_schema(schema)


@pytest.mark.parametrize("separate_branches", [False, True])
def test_exhaustive_routes_guarantee_reconvergent_producer(separate_branches):
    schemas, definitions, _ = workflow_modules()
    nodes = [
        {"id": "route", "type": "condition", "label": "Route",
         "config": {"field": "value", "operator": "eq", "value": 1, "match_route": "yes", "default_route": "no"}},
        {"id": "producer", "type": "manual_approval", "label": "Producer",
         "outputs": {"result": {"schema": {"type": "number"}}}},
        {"id": "consumer", "type": "manual_approval", "label": "Consumer",
         "inputs": {"result": {"schema": {"type": "number"}}}},
    ]
    edges = [{"source": "producer", "target": "consumer", "source_port": "result", "target_port": "result"}]
    for route in ("yes", "no"):
        if separate_branches:
            nodes.append({"id": route, "type": "manual_approval", "label": route})
            edges.extend([{"source": "route", "target": route, "route": route},
                          {"source": route, "target": "producer"}])
        else:
            edges.append({"source": "route", "target": "producer", "route": route})
    definitions.validate_graph(schemas.WorkflowGraph.model_validate({"nodes": nodes, "edges": edges}), {})
