from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import hashlib
import operator
import re
from uuid import uuid4

import networkx as nx
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Execution, Schedule
from app.materials import QUATERNARY_KEYS, extract_predicted_composition
from app.services.exceptions import ConflictError, NotFoundError
from app.workflows.definitions import canonical_json, validate_graph
from app.workflows.models import MessageOutbox, NodeAttempt, NodeRun, WorkflowRun, WorkflowVersion
from app.workflows.registry import validate_value
from app.workflows.schemas import EventDisposition, MessageEnvelope, NodeDefinition, WorkflowGraph


TERMINAL_NODES = {"completed", "failed", "cancelled", "skipped"}
TERMINAL_RUNS = {"completed", "failed", "cancelled"}


def db_time(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(tzinfo=None) if value.utcoffset() is not None else value


def validate_topic_prefix(value: str) -> str:
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,100}", value):
        raise ValueError("invalid Kafka topic prefix")
    return value


def validated_ports(ports, values: dict) -> dict:
    if not isinstance(values, dict) or values.keys() - ports.keys():
        raise ValueError("unknown or invalid output ports")
    result = dict(values)
    for name, port in ports.items():
        if name not in result and port.use_default:
            result[name] = port.default
        if name not in result:
            if port.required:
                raise ValueError(f"missing required port {name}")
            continue
        validate_value(port.json_schema, result[name], f"port {name}")
    return result


class WorkflowEngine:
    """Flush-only coordinator. Callers own transactions; no network or resource locks."""

    def __init__(self, session: Session, topic_prefix: str = "scheduler"):
        self.session = session
        self.topic_prefix = validate_topic_prefix(topic_prefix)

    def _lock_execution(self, execution_id: str) -> Execution:
        self.session.flush()
        schedule_id = self.session.scalar(select(Execution.schedule_id).where(Execution.id == execution_id))
        if schedule_id is None:
            raise NotFoundError("execution not found")
        self.session.scalar(select(Schedule).where(Schedule.id == schedule_id).with_for_update()
                            .execution_options(populate_existing=True))
        return self.session.scalar(select(Execution).where(Execution.id == execution_id).with_for_update()
                                   .execution_options(populate_existing=True))

    def _locked(self, run_id: str):
        self._lock_execution(run_id)
        run = self.session.scalar(select(WorkflowRun).where(WorkflowRun.execution_id == run_id)
                                  .with_for_update().execution_options(populate_existing=True))
        if run is None:
            raise NotFoundError("workflow run not found")
        version = self.session.get(WorkflowVersion, run.workflow_version_id)
        graph = WorkflowGraph.model_validate_json(version.graph_json)
        nodes = {node.node_id: node for node in self.session.scalars(
            select(NodeRun).where(NodeRun.run_id == run_id).order_by(NodeRun.node_id)
            .with_for_update().execution_options(populate_existing=True)
        )}
        return run, graph, nodes

    def start(self, execution: Execution, version: WorkflowVersion, now: datetime) -> WorkflowRun:
        execution = self._lock_execution(execution.id)
        current = self.session.scalar(select(WorkflowRun).where(WorkflowRun.execution_id == execution.id)
                                      .with_for_update().execution_options(populate_existing=True))
        if current is not None:
            if current.workflow_version_id != version.id:
                raise ConflictError("run already pins another workflow version")
            return current
        if execution.status != "awaiting_feedback":
            raise ConflictError("cannot start a closed execution")
        context = json.loads(execution.dispatch_payload)
        if not isinstance(context, dict) or context.get("workflow_version_id") != version.id:
            raise ConflictError("start requires a newly created workflow-owned execution")
        published = self.session.get(WorkflowVersion, version.id)
        if published is None:
            raise NotFoundError("published workflow version not found")
        graph = WorkflowGraph.model_validate_json(published.graph_json)
        validate_graph(graph)
        other = self.session.scalar(select(Execution.id).where(
            Execution.schedule_id == execution.schedule_id, Execution.id != execution.id,
            Execution.status == "awaiting_feedback",
        ).limit(1).with_for_update())
        if other:
            raise ConflictError("schedule already has another active execution")
        schedule = self.session.get(Schedule, execution.schedule_id)
        if schedule.status != "active":
            raise ConflictError("workflow schedule is not active")
        context["schedule_payload_digest"] = hashlib.sha256(canonical_json(json.loads(schedule.payload)).encode()).hexdigest()
        execution.dispatch_payload = canonical_json(context)
        schedule.awaiting_feedback, schedule.feedback_deadline_at = True, None
        run = WorkflowRun(execution_id=execution.id, workflow_version_id=published.id, status="running")
        self.session.add(run)
        self.session.flush()
        self.session.add_all(NodeRun(run_id=execution.id, node_id=node.id) for node in graph.nodes)
        self.session.flush()
        self.advance(run.execution_id, now)
        return run

    def tick_schedules(self, now: datetime, limit: int = 100) -> list[WorkflowRun]:
        from app.workflows.scheduling import WorkflowSchedulingService

        return WorkflowSchedulingService(self.session, self.topic_prefix).tick(now, limit)

    def _sync_runtime(self, run, now):
        from app.services.feedback import FeedbackService
        from app.services.state_projection import StateProjectionService
        from app.schemas import FeedbackCreate

        self.session.flush()
        execution = self.session.get(Execution, run.execution_id)
        StateProjectionService(self.session).sync_execution(execution, now=now, workflow_authoritative=True)
        if run.status in {"completed", "failed"} and execution.status == "awaiting_feedback":
            active = self.session.scalar(select(NodeRun.id).where(
                NodeRun.run_id == run.execution_id,
                NodeRun.status.in_(["queued", "accepted", "running", "needs_attention", "cancel_requested"]),
            ).limit(1).with_for_update())
            if active is None:
                FeedbackService(self.session).finalize_workflow_execution(
                    run.execution_id, FeedbackCreate(outcome="success" if run.status == "completed" else "failure",
                                                     note="finalized by the pinned workflow engine"), now,
                )
        if run.status == "completed":
            from app.workflows.decisions import apply_designated_decision

            version = self.session.get(WorkflowVersion, run.workflow_version_id)
            graph = WorkflowGraph.model_validate_json(version.graph_json)
            nodes = {item.node_id: item for item in self.session.scalars(select(NodeRun).where(NodeRun.run_id == run.execution_id))}
            apply_designated_decision(self.session, run, graph, nodes, now)

    def _inputs(self, spec, incoming, nodes):
        values = dict(spec.input_values)
        for edge in incoming:
            if edge.target_port is not None:
                output = json.loads(nodes[edge.source].outputs_json)
                if edge.source_port in output:
                    values[edge.target_port] = output[edge.source_port]
        return validated_ports(spec.inputs, values)

    def _enqueue(self, run, node, spec: NodeDefinition, inputs, now):
        node.attempt += 1
        command_id = str(uuid4())
        binding = spec.executor
        execution = self.session.get(Execution, run.execution_id)
        context = json.loads(execution.dispatch_payload)
        if spec.type == "model_inference":
            used = {}
            for item in self.session.scalars(
                select(Execution.dispatch_payload).where(Execution.schedule_id == execution.schedule_id,
                    Execution.dispatched_at <= execution.dispatched_at).order_by(Execution.dispatched_at, Execution.id)
            ):
                previous = json.loads(item)
                composition = previous.get("mat_model_candidate_composition_at_percent")
                if not isinstance(composition, dict) or any(composition.get(key) is None for key in QUATERNARY_KEYS):
                    composition = extract_predicted_composition(previous)
                if composition is not None:
                    snapshot = {"mat_model_candidate_composition_at_percent": {key: composition[key] for key in QUATERNARY_KEYS}}
                    used[canonical_json(snapshot)] = snapshot
            context["used_model_payloads"] = list(used.values())
        command = MessageEnvelope(
            message_id=command_id, message_type="command.execute", run_id=run.execution_id,
            workflow_version_id=run.workflow_version_id, node_id=node.node_id, node_run_id=node.id,
            attempt=node.attempt, command_id=command_id, gateway_id=binding.gateway_id,
            adapter_id=binding.adapter_id, occurred_at=now.replace(tzinfo=UTC),
            expires_at=(now + timedelta(seconds=spec.timeouts.delivery_seconds +
                                         spec.timeouts.acceptance_seconds)).replace(tzinfo=UTC),
            payload={"node_type": spec.type, "type_version": spec.type_version,
                     "executor_kind": binding.kind,
                     "input_ports": {name: port.model_dump(mode="json", by_alias=True) for name, port in spec.inputs.items()},
                     "output_ports": {name: port.model_dump(mode="json", by_alias=True) for name, port in spec.outputs.items()},
                     "timeouts": spec.timeouts.model_dump(mode="json"),
                     "config": spec.config, "inputs": inputs,
                     "execution_context": context},
        )
        self.session.add(NodeAttempt(
            node_run_id=node.id, attempt=node.attempt, command_id=command_id,
            inputs_json=canonical_json(inputs), command_json=command.model_dump_json(), status="queued",
            delivery_deadline_at=now + timedelta(seconds=spec.timeouts.delivery_seconds),
        ))
        topic = f"{self.topic_prefix}.commands.{binding.gateway_id}"
        if binding.kind == "compute":
            topic = f"{self.topic_prefix}.{'model' if spec.type == 'model_inference' else 'parse'}.commands"
        self.session.add(MessageOutbox(
            message_id=command_id, topic=topic, message_key=node.id,
            envelope_json=command.model_dump_json(), status="pending", next_attempt_at=now,
        ))
        node.status = "queued"

    def _advance_locked(self, run, graph, nodes, now):
        schedule = self.session.get(Execution, run.execution_id).schedule
        if run.status == "running" and schedule.status != "active":
            run.status = "paused"
        if run.status in {"running", "paused"} and all(
            node.status in {"completed", "skipped"} for node in nodes.values()
        ):
            run.status = "completed"
        if run.status != "running":
            self._sync_runtime(run, now)
            return
        specs = {node.id: node for node in graph.nodes}
        dag = nx.DiGraph()
        dag.add_nodes_from(specs)
        dag.add_edges_from((edge.source, edge.target) for edge in graph.edges)
        incoming = {node_id: [] for node_id in specs}
        for edge in graph.edges:
            incoming[edge.target].append(edge)
        for node_id in nx.topological_sort(dag):
            node, spec = nodes[node_id], specs[node_id]
            if node.status != "waiting_dependencies":
                continue
            edges = incoming[node_id]
            if any(nodes[edge.source].status not in {"completed", "skipped"} for edge in edges):
                continue
            active = [edge for edge in edges if nodes[edge.source].status == "completed" and
                      (edge.route is None or json.loads(nodes[edge.source].outputs_json).get("__route__") == edge.route)]
            if edges and not active:
                node.status = "skipped"
                continue
            try:
                inputs = self._inputs(spec, active, nodes)
                if spec.executor is not None:
                    self._enqueue(run, node, spec, inputs, now)
                elif spec.type == "manual_approval":
                    node.status = "waiting_approval"
                else:
                    outputs = validated_ports(spec.outputs, {name: inputs[name] for name in spec.outputs if name in inputs})
                    if spec.type == "condition":
                        value = inputs[spec.config["field"]]
                        operations = {"eq": operator.eq, "ne": operator.ne, "gt": operator.gt,
                                      "gte": operator.ge, "lt": operator.lt, "lte": operator.le,
                                      "in": lambda left, right: left in right}
                        matched = operations[spec.config["operator"]](value, spec.config["value"])
                        outputs["__route__"] = spec.config["match_route" if matched else "default_route"]
                    node.outputs_json, node.status = canonical_json(outputs), "completed"
            except (ValueError, KeyError, TypeError):
                node.status, node.error = "needs_attention", "node inputs or internal outputs failed validation"
                run.status = "needs_attention"
                break
        if all(node.status in {"completed", "skipped"} for node in nodes.values()):
            run.status = "completed"
        self.session.flush()
        self._sync_runtime(run, now)

    def advance(self, run_id: str, now: datetime) -> None:
        run, graph, nodes = self._locked(run_id)
        self._advance_locked(run, graph, nodes, db_time(now))

    def apply_event(self, event: MessageEnvelope, now: datetime) -> EventDisposition:
        if not event.message_type.startswith("node."):
            return EventDisposition.QUARANTINED
        try:
            run, graph, nodes = self._locked(event.run_id)
        except NotFoundError:
            return EventDisposition.QUARANTINED
        node = nodes.get(event.node_id)
        spec = next((item for item in graph.nodes if item.id == event.node_id), None)
        if node is None or spec.executor is None:
            return EventDisposition.QUARANTINED
        binding = spec.executor
        if (event.workflow_version_id != run.workflow_version_id or event.node_run_id != node.id or
                event.attempt != node.attempt or event.gateway_id != binding.gateway_id or
                event.adapter_id != binding.adapter_id or event.fencing_generation is not None):
            return EventDisposition.QUARANTINED
        attempt = self.session.scalar(select(NodeAttempt).where(
            NodeAttempt.node_run_id == node.id, NodeAttempt.attempt == node.attempt,
        ).with_for_update().execution_options(populate_existing=True))
        if attempt is None or attempt.command_id != event.command_id:
            return EventDisposition.QUARANTINED
        if node.status in TERMINAL_NODES or run.status in {"completed", "cancelled"}:
            return EventDisposition.IGNORED_STALE
        now = db_time(now)
        if event.message_type == "node.completed":
            try:
                if event.payload.keys() != {"outputs"}:
                    raise ValueError("invalid result payload")
                outputs = validated_ports(spec.outputs, event.payload["outputs"])
                if spec.type == "model_inference":
                    from app.workflows.decisions import proposal_from_outputs

                    proposal_from_outputs(outputs, spec, run.execution_id)
            except ValueError:
                return EventDisposition.QUARANTINED
            node.outputs_json = attempt.outputs_json = canonical_json(outputs)
            node.status = attempt.status = "completed"
            node.error = attempt.error = None
            attempt.completed_at = now
            if spec.type == "model_inference":
                from app.services.model_inference import ModelInferenceService

                ModelInferenceService(self.session).record_workflow_result(
                    self.session.get(Execution, run.execution_id), node, spec, outputs, now)
            if run.status == "needs_attention" and not any(item.status == "needs_attention" for item in nodes.values()):
                run.status = "running"
        elif event.message_type in {"node.failed", "node.cancelled"}:
            error = event.payload.get("error", "executor reported cancellation")
            if not isinstance(error, str) or len(error) > 2000 or event.payload.keys() - {"error"}:
                return EventDisposition.QUARANTINED
            node.status = attempt.status = "failed" if event.message_type == "node.failed" else "cancelled"
            node.error = attempt.error = error
            attempt.completed_at, run.status = now, "failed"
            for sibling in nodes.values():
                if sibling.status in {"waiting_dependencies", "waiting_approval"}:
                    sibling.status = "skipped"
        elif event.message_type == "node.needs_attention":
            if event.payload.keys() != {"error"} or not isinstance(event.payload["error"], str) or len(event.payload["error"]) > 2000:
                return EventDisposition.QUARANTINED
            node.status = attempt.status = "needs_attention"
            node.error = attempt.error = event.payload["error"]
            if run.status == "running":
                run.status = "needs_attention"
        elif event.message_type in {"node.accepted", "node.started", "node.progress"}:
            if event.payload.keys() - {"progress", "note"}:
                return EventDisposition.QUARANTINED
            if node.status in {"needs_attention", "cancel_requested"}:
                return EventDisposition.IGNORED_STALE
            if event.message_type == "node.accepted" and node.status == "running":
                return EventDisposition.IGNORED_STALE
            if event.message_type == "node.progress":
                if node.status != "running":
                    return EventDisposition.IGNORED_STALE
            else:
                if attempt.accepted_at is None:
                    attempt.accepted_at = now
                    attempt.execution_deadline_at = now + timedelta(seconds=spec.timeouts.execution_seconds)
                if event.message_type == "node.started":
                    attempt.started_at = attempt.started_at or now
                node.status = attempt.status = "running" if event.message_type == "node.started" else "accepted"
        self.session.flush()
        self._advance_locked(run, graph, nodes, now)
        return EventDisposition.APPLIED

    def expire(self, now: datetime) -> int:
        now, count = db_time(now), 0
        run_ids = list(self.session.scalars(select(WorkflowRun.execution_id).where(
            WorkflowRun.status.in_(["running", "paused", "needs_attention", "failed"]),
            NodeRun.status.in_(["queued", "accepted", "running", "cancel_requested"]),
        ).join(NodeRun, NodeRun.run_id == WorkflowRun.execution_id).distinct().order_by(WorkflowRun.execution_id)))
        for run_id in run_ids:
            run, graph, nodes = self._locked(run_id)
            if run.status in {"completed", "cancelled"}:
                continue
            specs = {node.id: node for node in graph.nodes}
            for node in nodes.values():
                if node.status not in {"queued", "accepted", "running", "cancel_requested"}:
                    continue
                attempt = self.session.scalar(select(NodeAttempt).where(
                    NodeAttempt.node_run_id == node.id, NodeAttempt.attempt == node.attempt,
                ).with_for_update().execution_options(populate_existing=True))
                deadline = attempt.execution_deadline_at
                if node.status == "queued":
                    outbox = self.session.scalar(select(MessageOutbox).where(
                        MessageOutbox.message_id == attempt.command_id,
                    ).with_for_update().execution_options(populate_existing=True))
                    deadline = attempt.delivery_deadline_at
                    if outbox and outbox.sent_at:
                        deadline = outbox.sent_at + timedelta(seconds=specs[node.node_id].timeouts.acceptance_seconds)
                        attempt.acceptance_deadline_at = deadline
                if deadline is not None and deadline <= now:
                    node.status = attempt.status = "needs_attention"
                    node.error = attempt.error = "deadline exceeded; execution not automatically retried"
                    if run.status == "running":
                        run.status = "needs_attention"
                    count += 1
            self._sync_runtime(run, now)
        self.session.flush()
        return count

    def pause(self, run_id: str, now: datetime) -> None:
        run, _, _ = self._locked(run_id)
        if run.status not in {"running", "paused"}:
            raise ConflictError("only a running workflow can be paused")
        run.status = "paused"
        self.session.flush()

    def resume(self, run_id: str, now: datetime) -> None:
        run, graph, nodes = self._locked(run_id)
        if run.status != "paused":
            raise ConflictError("only a paused workflow can be resumed")
        run.status = "needs_attention" if any(node.status == "needs_attention" for node in nodes.values()) else "running"
        self._advance_locked(run, graph, nodes, db_time(now))
        self.session.flush()

    def approve(self, node_run_id: str, outputs: dict, now: datetime) -> None:
        run_id = self.session.scalar(select(NodeRun.run_id).where(NodeRun.id == node_run_id))
        if run_id is None:
            raise NotFoundError("node run not found")
        run, graph, nodes = self._locked(run_id)
        node = next(item for item in nodes.values() if item.id == node_run_id)
        spec = next(item for item in graph.nodes if item.id == node.node_id)
        if run.status in TERMINAL_RUNS or node.status != "waiting_approval":
            raise ConflictError("node is not awaiting approval")
        node.outputs_json = canonical_json(validated_ports(spec.outputs, outputs))
        node.status = "completed"
        self._advance_locked(run, graph, nodes, db_time(now))
        self.session.flush()
