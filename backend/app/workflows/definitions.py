from __future__ import annotations

import hashlib
from itertools import product
import json

import networkx as nx
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Execution, Schedule
from app.services.exceptions import ConflictError, DomainError, NotFoundError
from app.workflows.models import (
    WorkflowDefinition, WorkflowRun,
    WorkflowScheduleBinding, WorkflowVersion,
)
from app.workflows.registry import (
    schemas_compatible, validate_node_type, validate_schema, validate_value,
)
from app.workflows.schemas import WorkflowGraph


class WorkflowValidationError(DomainError, ValueError):
    pass


def canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def graph_json(graph: WorkflowGraph) -> str:
    return canonical_json(graph.model_dump(mode="json", by_alias=True))


def semantic_digest(graph: WorkflowGraph) -> str:
    data = graph.model_dump(mode="json", by_alias=True, exclude={"positions"})
    data["nodes"].sort(key=lambda node: node["id"])
    data["edges"].sort(key=canonical_json)
    return hashlib.sha256(canonical_json(data).encode("utf-8")).hexdigest()


def validate_graph(graph: WorkflowGraph, resources: dict | None = None) -> None:
    # Accept the old argument without depending on resource registration.
    try:
        _validate_graph(graph)
    except WorkflowValidationError:
        raise
    except ValueError as exc:
        raise WorkflowValidationError(str(exc)) from exc


def _guaranteed_input_edges(graph: WorkflowGraph, dag: nx.DiGraph, nodes: dict) -> set[int]:
    # Dominators prove common cases; bounded truth tables resolve exhaustive reconvergence.
    activation = nx.DiGraph()
    root = ("root",)
    activation.add_node(root)
    incoming = {node_id: [] for node_id in nodes}
    for node_id in nodes:
        activation.add_node(("node", node_id))
        if dag.in_degree(node_id) == 0:
            activation.add_edge(root, ("node", node_id))
    for edge in graph.edges:
        source, target = ("node", edge.source), ("node", edge.target)
        incoming[edge.target].append(edge)
        if edge.route is not None:
            gate = ("route", edge.source, edge.route)
            activation.add_edge(source, gate)
            activation.add_edge(gate, target)
        else:
            activation.add_edge(source, target)
    dominators = nx.immediate_dominators(activation, root)
    order = list(nx.topological_sort(dag))
    active_cache, target_guards = {}, {}

    def evaluate(guards):
        key = frozenset(guards.items())
        if key in active_cache:
            return active_cache[key]
        active = {}
        for node_id in order:
            active[node_id] = not incoming[node_id] or any(
                active[edge.source] and (edge.route is None or guards.get(edge.source) == edge.route)
                for edge in incoming[node_id]
            )
        active_cache[key] = active
        return active

    for target in nodes:
        guards = {}
        ancestor = ("node", target)
        while ancestor != root:
            if ancestor[0] == "route":
                guards[ancestor[1]] = ancestor[2]
            ancestor = dominators[ancestor]
        target_guards[target] = guards
        evaluate(guards)

    safe = set()
    for index, edge in enumerate(graph.edges):
        port = nodes[edge.target].inputs.get(edge.target_port)
        if port is None or not port.required or port.use_default:
            continue
        guards = target_guards[edge.target]

        def edge_active(active, choices):
            return active[edge.source] and (edge.route is None or choices.get(edge.source) == edge.route)

        if edge_active(evaluate(guards), guards):
            safe.add(index)
            continue
        unknown = sorted(node_id for node_id in nx.ancestors(dag, edge.target)
                         if nodes[node_id].type == "condition" and node_id not in guards)
        if len(unknown) > 8:
            continue
        routes = [(nodes[node_id].config["match_route"], nodes[node_id].config["default_route"])
                  for node_id in unknown]
        proven = True
        for combination in product(*routes):
            choices = {**guards, **dict(zip(unknown, combination))}
            if frozenset(choices.items()) not in active_cache and len(active_cache) >= 512:
                proven = False
                break
            active = evaluate(choices)
            if active[edge.target] and not edge_active(active, choices):
                proven = False
                break
        if proven:
            safe.add(index)
    return safe


def _validate_graph(graph: WorkflowGraph) -> None:
    nodes = {node.id: node for node in graph.nodes}
    if len(nodes) != len(graph.nodes):
        raise ValueError("duplicate node ID")
    if graph.positions.keys() - nodes.keys():
        raise ValueError("position references an unknown node endpoint")
    if graph.decision_node_id is not None:
        decision = nodes.get(graph.decision_node_id)
        if decision is None or decision.type != "model_inference":
            raise ValueError("decision_node_id must reference a model_inference node")

    dag = nx.DiGraph()
    dag.add_nodes_from(nodes)
    edges = set()
    writers = {}
    for edge in graph.edges:
        if edge.source not in nodes or edge.target not in nodes:
            raise ValueError("edge references a missing endpoint")
        identity = (edge.source, edge.target, edge.source_port, edge.target_port, edge.route)
        if identity in edges:
            raise ValueError("duplicate edge")
        edges.add(identity)
        dag.add_edge(edge.source, edge.target)
    if not nx.is_directed_acyclic_graph(dag):
        raise ValueError("workflow contains a cycle")

    for node in graph.nodes:
        entry = validate_node_type(node)
        for name, port in (*node.inputs.items(), *node.outputs.items()):
            validate_schema(port.json_schema)
            if port.use_default:
                validate_value(port.json_schema, port.default, f"default for {node.id}.{name}")
        for name, value in node.input_values.items():
            if name not in node.inputs:
                raise ValueError(f"input constant references missing port {node.id}.{name}")
            validate_value(node.inputs[name].json_schema, value, f"constant for {node.id}.{name}")
        if entry.executor_kind == "device":
            if node.retry_policy.max_attempts > 1:
                raise ValueError(f"device node {node.id} cannot automatically retry")

    guaranteed_inputs = _guaranteed_input_edges(graph, dag, nodes)
    for index, edge in enumerate(graph.edges):
        source, target = nodes[edge.source], nodes[edge.target]
        if source.type == "condition":
            routes = {source.config["match_route"], source.config["default_route"]}
            if edge.route not in routes:
                raise ValueError(f"condition edge from {source.id} needs a configured route")
        elif edge.route is not None:
            raise ValueError("only condition edges can name a route")
        if edge.source_port is None:
            continue
        if edge.source_port not in source.outputs or edge.target_port not in target.inputs:
            raise ValueError("edge references a missing port")
        key = (target.id, edge.target_port)
        if key in writers or edge.target_port in target.input_values:
            raise ValueError(f"duplicate writer for input port {target.id}.{edge.target_port}")
        writers[key] = source.id
        output, incoming = source.outputs[edge.source_port], target.inputs[edge.target_port]
        if not schemas_compatible(output.json_schema, incoming.json_schema):
            raise ValueError(f"port schemas are not compatible: {source.id} -> {target.id}")
        if incoming.required and not incoming.use_default and not output.required:
            raise ValueError(f"optional output cannot feed required input {target.id}.{edge.target_port}")
        if index not in guaranteed_inputs and incoming.required and not incoming.use_default:
            raise ValueError(f"branch input {target.id}.{edge.target_port} needs optional/default handling")

    for node in graph.nodes:
        for name, port in node.inputs.items():
            if port.required and not port.use_default and name not in node.input_values:
                if (node.id, name) not in writers:
                    raise ValueError(f"unbound required input {node.id}.{name}")
        if node.type == "condition":
            outgoing = {edge.route for edge in graph.edges if edge.source == node.id}
            if outgoing != {node.config["match_route"], node.config["default_route"]}:
                raise ValueError(f"condition {node.id} must have both routes connected")


class DefinitionService:
    """Flush-only services; the caller owns commit/rollback of the whole operation."""

    def __init__(self, session: Session):
        self.session = session

    def create(self, name: str, graph: WorkflowGraph) -> WorkflowDefinition:
        if not isinstance(name, str) or not name.strip() or len(name) > 120:
            raise WorkflowValidationError("workflow name must contain 1-120 characters")
        definition = WorkflowDefinition(name=name.strip(), draft_graph_json=graph_json(graph))
        self.session.add(definition)
        self.session.flush()
        return definition

    def _lock_definition(self, definition_id: str, revision: int) -> WorkflowDefinition:
        self.session.flush()
        definition = self.session.scalar(
            select(WorkflowDefinition).where(WorkflowDefinition.id == definition_id)
            .with_for_update().execution_options(populate_existing=True)
        )
        if definition is None:
            raise NotFoundError("workflow definition not found")
        if definition.revision != revision:
            raise ConflictError("workflow draft revision has changed; reload before saving")
        return definition

    def save(self, definition_id: str, revision: int, graph: WorkflowGraph) -> WorkflowDefinition:
        definition = self._lock_definition(definition_id, revision)
        definition.draft_graph_json = graph_json(graph)
        definition.revision += 1
        self.session.flush()
        return definition

    def publish(self, definition_id: str, revision: int) -> WorkflowVersion:
        definition = self._lock_definition(definition_id, revision)
        graph = WorkflowGraph.model_validate_json(definition.draft_graph_json)
        validate_graph(graph)
        previous = self.session.scalar(
            select(WorkflowVersion.version)
            .where(WorkflowVersion.definition_id == definition.id)
            .order_by(WorkflowVersion.version.desc()).limit(1).with_for_update()
        ) or 0
        version = WorkflowVersion(
            definition_id=definition.id, version=previous + 1,
            graph_json=graph_json(graph), digest=semantic_digest(graph),
        )
        self.session.add(version)
        self.session.flush()
        return version

    def bind(self, schedule_id: str, version_id: str) -> WorkflowScheduleBinding:
        self.session.flush()
        schedule = self.session.scalar(
            select(Schedule).where(Schedule.id == schedule_id).with_for_update()
            .execution_options(populate_existing=True)
        )
        if schedule is None:
            raise NotFoundError("schedule not found")
        active_execution = self.session.scalar(
            select(Execution.id).where(
                Execution.schedule_id == schedule_id, Execution.status == "awaiting_feedback",
            ).limit(1).with_for_update()
        )
        active_run = self.session.scalar(
            select(WorkflowRun.execution_id).join(Execution)
            .where(Execution.schedule_id == schedule_id,
                   WorkflowRun.status.not_in(["completed", "failed", "cancelled"]))
            .limit(1).with_for_update()
        )
        if schedule.awaiting_feedback or active_execution or active_run:
            raise ConflictError("cannot change workflow binding while a schedule has an active run")
        version = self.session.scalar(
            select(WorkflowVersion.id).where(WorkflowVersion.id == version_id).with_for_update()
        )
        if version is None:
            raise NotFoundError("published workflow version not found")
        binding = self.session.scalar(
            select(WorkflowScheduleBinding).where(WorkflowScheduleBinding.schedule_id == schedule_id)
            .with_for_update().execution_options(populate_existing=True)
        )
        if binding is None:
            binding = WorkflowScheduleBinding(schedule_id=schedule_id, workflow_version_id=version_id)
            self.session.add(binding)
        else:
            binding.workflow_version_id = version_id
        self.session.flush()
        return binding
