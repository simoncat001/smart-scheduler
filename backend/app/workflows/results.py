from __future__ import annotations

from datetime import datetime
from dataclasses import dataclass, replace
import hashlib
import json

from sqlalchemy import select

from app.models import DataPackage, Execution
from app.services.exceptions import ConflictError, NotFoundError
from app.workflows.definitions import canonical_json
from app.workflows.engine import WorkflowEngine, db_time, validated_ports
from app.workflows.messaging import MessagingService
from app.workflows.models import MessageInbox, NodeAttempt, NodeDataPackageLink, NodeRun, WorkflowRun, WorkflowVersion
from app.workflows.schemas import EventDisposition, MessageEnvelope, WorkflowGraph


WAITING_RESULTS = {"queued", "accepted", "running", "needs_attention", "cancel_requested"}


@dataclass(frozen=True)
class PreparedResult:
    attempt_id: str
    node_run_id: str
    attempt: int
    execution_id: str
    command_id: str
    request_digest: str
    data: object = None

    def with_data(self, data):
        return replace(self, data=data)


def require_request_id(request_id):
    if not isinstance(request_id, str) or not request_id.strip() or len(request_id) > 128:
        raise ConflictError("workflow upload requires a stable request ID (1-128 characters)")


def upload_request(kind, payload):
    return {"kind": kind, "payload": payload.model_dump(mode="json")}


def resolve_execution(session, *, run_id=None, node_run_id=None, batch_id=None, task_id=None):
    """Explicit identities never fall back to a newer execution; all supplied context must agree."""
    if node_run_id:
        node = session.get(NodeRun, node_run_id)
        if node is None:
            raise NotFoundError("workflow node run not found")
        if run_id and run_id != node.run_id:
            raise ConflictError("node and execution context disagree")
        run_id = node.run_id
    if run_id:
        execution = session.get(Execution, run_id)
        if execution is None:
            raise NotFoundError("execution not found")
    elif batch_id:
        matches = [row for row in session.scalars(select(Execution))
                   if json.loads(row.dispatch_payload or "{}").get("experiment_batch_id") == batch_id]
        if not matches:
            raise NotFoundError("execution for experiment batch not found")
        if len(matches) != 1:
            raise ConflictError("ambiguous experiment batch; supply an explicit execution/node")
        execution = matches[0]
    else:
        raise ConflictError("workflow uploads require an explicit execution, node or batch; not task-latest")
    context = json.loads(execution.dispatch_payload or "{}")
    if batch_id and batch_id != context.get("experiment_batch_id"):
        raise ConflictError("experiment batch and execution context disagree")
    if task_id and task_id != execution.schedule_id:
        raise ConflictError("task and execution context disagree")
    return execution


class ResultService:
    """Correlated HTTP results share Kafka validation; writes are flush-only."""

    def __init__(self, session, topic_prefix="scheduler"):
        self.session = session
        self.messages = MessagingService(session, topic_prefix)
        self.engine = WorkflowEngine(session, topic_prefix)

    def resolve(self, execution_id: str, instrument_code: str, component_id: str,
                node_run_id: str | None, attempt: int | None) -> NodeAttempt:
        if (node_run_id is None) != (attempt is None):
            raise ConflictError("workflow node_run_id and attempt must be supplied together")
        run = self.session.get(WorkflowRun, execution_id)
        if run is None:
            raise NotFoundError("workflow run not found")
        matches = []
        rows = self.session.execute(select(NodeRun, NodeAttempt).join(
            NodeAttempt, (NodeAttempt.node_run_id == NodeRun.id) & (NodeAttempt.attempt == NodeRun.attempt),
        ).where(NodeRun.run_id == execution_id))
        for node, candidate in rows:
            if node_run_id and (node.id != node_run_id or candidate.attempt != attempt):
                continue
            if not node_run_id and node.status not in WAITING_RESULTS:
                continue
            command = MessageEnvelope.model_validate_json(candidate.command_json)
            config = command.payload["config"]
            if command.payload["node_type"] not in {"instrument_task", "data_upload"}:
                continue
            context = command.payload["execution_context"]
            components = context.get("instrument_components", {})
            component = components.get(instrument_code, {}) if isinstance(components, dict) else {}
            expected = config.get("component_id") or component.get("component_id")
            if config.get("instrument_code") == instrument_code and expected == component_id:
                matches.append(candidate)
        if len(matches) > 1:
            raise ConflictError("ambiguous workflow upload; supply node_run_id and attempt")
        if not matches:
            raise ConflictError("workflow upload has no compatible current node attempt/component")
        return matches[0]

    def reserve(self, request_id, request, now):
        require_request_id(request_id)
        try:
            body = canonical_json(request)
        except (ValueError, TypeError, RecursionError) as error:
            raise ConflictError("workflow request must contain finite JSON") from error
        if len(body.encode()) > 1_000_000:
            raise ConflictError("workflow request exceeds 1 MB")
        row, fresh = self.messages._reserve("http-result", request_id,
            hashlib.sha256(body.encode()).hexdigest(), canonical_json({"request": request}), db_time(now))
        if not fresh and row.digest != hashlib.sha256(body.encode()).hexdigest():
            raise ConflictError("request ID reused with a different payload")
        return row, fresh

    def lookup(self, request_id, request):
        require_request_id(request_id)
        row = self.session.scalar(select(MessageInbox).where(
            MessageInbox.handler == "http-result", MessageInbox.message_id == request_id))
        if row is not None:
            if row.digest != hashlib.sha256(canonical_json(request).encode()).hexdigest():
                raise ConflictError("request ID reused with a different payload")
            return json.loads(row.envelope_json).get("result")
        return None

    def pin(self, attempt, request):
        command = MessageEnvelope.model_validate_json(attempt.command_json)
        return PreparedResult(attempt.id, command.node_run_id, command.attempt, command.run_id,
                              command.command_id, hashlib.sha256(canonical_json(request).encode()).hexdigest())

    def _prepared_attempt(self, prepared, request, execution_id, instrument_code, component_id):
        if not isinstance(prepared, PreparedResult) or prepared.execution_id != execution_id or (
                prepared.request_digest != hashlib.sha256(canonical_json(request).encode()).hexdigest()):
            raise ConflictError("prepared result and workflow request context disagree")
        attempt = self.resolve(execution_id, instrument_code, component_id, prepared.node_run_id, prepared.attempt)
        if attempt.id != prepared.attempt_id or attempt.command_id != prepared.command_id:
            raise ConflictError("prepared result refers to a stale workflow attempt")
        return attempt

    def _current(self, attempt):
        node = self.session.get(NodeRun, attempt.node_run_id)
        if node is None:
            raise NotFoundError("workflow node run not found")
        run, graph, nodes = self.engine._locked(node.run_id)
        node = nodes[node.node_id]
        current = self.session.scalar(select(NodeAttempt).where(NodeAttempt.id == attempt.id)
            .with_for_update().execution_options(populate_existing=True))
        if current is None or current.attempt != node.attempt:
            raise ConflictError("workflow upload refers to a stale attempt")
        if node.status not in WAITING_RESULTS or run.status in {"completed", "cancelled"}:
            raise ConflictError("workflow node is no longer awaiting a result; reuse the original request ID for retries")
        spec = next(item for item in graph.nodes if item.id == node.node_id)
        return current, spec

    def _apply(self, attempt, request_id, observation, now, *, kind="node.completed", note=""):
        command = MessageEnvelope.model_validate_json(attempt.command_json)
        version = self.session.get(WorkflowVersion, command.workflow_version_id)
        graph = WorkflowGraph.model_validate_json(version.graph_json)
        spec = next(item for item in graph.nodes if item.id == command.node_id)
        if kind == "node.completed":
            values = {next(iter(spec.outputs)): observation} if len(spec.outputs) == 1 else observation
            try:
                values = validated_ports(spec.outputs, values)
            except ValueError as error:
                raise ConflictError("workflow outputs failed port validation") from error
            payload = {"outputs": values}
        else:
            payload = {"error": note or "HTTP upload reported failure"} if kind == "node.failed" else {"note": note}
        event = command.model_copy(update={
            "message_id": "http:" + hashlib.sha256(request_id.encode()).hexdigest(),
            "message_type": kind, "expires_at": None, "payload": payload,
        })
        disposition = self.messages.ingest(event.model_dump_json().encode(), "http-result", now)
        if disposition not in {EventDisposition.APPLIED, EventDisposition.DUPLICATE}:
            raise ConflictError("workflow result ownership, state or outputs failed validation")
        return disposition

    def ingest(self, attempt: NodeAttempt, request_id: str, observation: dict,
               data_package_id: str | None, now: datetime) -> EventDisposition:
        request = {"attempt_id": attempt.id, "observation": observation, "data_package_id": data_package_id}
        row, fresh = self.reserve(request_id, request, now)
        if not fresh:
            return EventDisposition.DUPLICATE
        attempt, _ = self._current(attempt)
        if data_package_id:
            from app.services.closed_loop import VALIDATED_STATUSES

            package = self.session.get(DataPackage, data_package_id)
            node = self.session.get(NodeRun, attempt.node_run_id)
            if package is None or package.execution_id != node.run_id:
                raise ConflictError("data package does not belong to this workflow execution")
            existing = self.session.scalar(select(NodeDataPackageLink).where(
                NodeDataPackageLink.data_package_id == data_package_id).with_for_update())
            command = MessageEnvelope.model_validate_json(attempt.command_json)
            config, context = command.payload["config"], command.payload["execution_context"]
            components = context.get("instrument_components", {})
            component = components.get(config.get("instrument_code"), {}) if isinstance(components, dict) else {}
            expected = config.get("component_id") or component.get("component_id")
            if (existing and existing.node_attempt_id != attempt.id or package.instrument_code != config.get("instrument_code") or
                    package.component_id != expected or package.validation_status not in VALIDATED_STATUSES or
                    json.loads(package.scheduler_observation_json) != observation):
                raise ConflictError("data package is not a validated result owned by this node attempt")
            if existing is None:
                self.session.add(NodeDataPackageLink(node_attempt_id=attempt.id, data_package_id=package.id, created_at=now))
        disposition = self._apply(attempt, request_id, observation, now)
        row.status = disposition.value
        self.session.flush()
        return disposition

    def ingest_upload(self, upload, prepared, now):
        row, fresh = self.reserve(upload.request_id, upload_request("instrument-upload", upload), now)
        if not fresh:
            return self.session.get(Execution, json.loads(row.envelope_json)["result"]["execution_id"])
        execution = resolve_execution(self.session, run_id=upload.run_id, node_run_id=upload.node_run_id,
                                      batch_id=upload.experiment_batch_id)
        attempt = self._prepared_attempt(prepared, upload_request("instrument-upload", upload), execution.id,
                                         upload.instrument_code, upload.component_id)
        observation = prepared.data
        attempt, _ = self._current(attempt)
        kinds = {"uploaded": "node.completed", "success": "node.completed", "triggered": "node.started", "failed": "node.failed"}
        kind = kinds.get(upload.status)
        if kind is None:
            raise ConflictError("unsupported workflow upload status")
        disposition = self._apply(attempt, upload.request_id, observation, now, kind=kind, note=upload.note)
        row.status = disposition.value
        row.envelope_json = canonical_json({"request": upload_request("instrument-upload", upload),
                                           "result": {"execution_id": execution.id, "attempt_id": attempt.id,
                                                      "observation": observation}})
        self.session.flush()
        return execution

    def ingest_package(self, payload, prepared, now):
        from app.services.closed_loop import VALIDATED_STATUSES, _apply_data_package_snapshot

        row, fresh = self.reserve(payload.request_id, upload_request("data-package", payload), now)
        if not fresh:
            result = json.loads(row.envelope_json)["result"]
            package = self.session.get(DataPackage, result["data_package_id"])
            return package, self.session.get(Execution, package.execution_id)
        execution = resolve_execution(self.session, run_id=payload.run_id, node_run_id=payload.node_run_id,
                                      batch_id=payload.experiment_batch_id, task_id=payload.task_id)
        if not payload.component_id:
            raise ConflictError("workflow data package requires component_id")
        attempt = self._prepared_attempt(prepared, upload_request("data-package", payload), execution.id,
                                         payload.instrument_code, payload.component_id)
        attempt, _ = self._current(attempt)
        context = MessageEnvelope.model_validate_json(attempt.command_json).payload["execution_context"]
        if payload.campaign_id and payload.campaign_id != context.get("campaign_id"):
            raise ConflictError("campaign and workflow execution context disagree")
        if payload.sample_id and context.get("sample_id") and payload.sample_id != context["sample_id"]:
            raise ConflictError("sample and workflow execution context disagree")
        standard_ref, observation, live_record = prepared.data
        package = DataPackage(
            campaign_id=context.get("campaign_id"), schedule_id=execution.schedule_id,
            execution_id=execution.id, task_id=execution.schedule_id, run_id=execution.id,
            experiment_batch_id=context.get("experiment_batch_id"), instrument_code=payload.instrument_code,
            instrument_id=payload.instrument_id or payload.component_id, component_id=payload.component_id,
            sample_id=payload.sample_id or context.get("sample_id"), package_kind=payload.package_kind,
            template_id=payload.template_id, template_version=payload.template_version,
            validation_status=payload.validation_status, manage_db_record_id=payload.manage_db_record_id,
            raw_files_json=canonical_json(payload.raw_files), metadata_json=canonical_json(payload.metadata),
            process_data_json=canonical_json(payload.process_data), result_data_json=canonical_json(payload.result_data),
            log_data_json=canonical_json(payload.log_data), scheduler_observation_json=canonical_json(observation),
            standard_data_ref_json=canonical_json(standard_ref), note=payload.note, created_at=now, updated_at=now,
        )
        _apply_data_package_snapshot(package, standard_ref=standard_ref, scheduler_observation=observation,
                                     manage_db_record=live_record, fetched_at=now)
        self.session.add(package)
        self.session.flush()
        self.session.add(NodeDataPackageLink(node_attempt_id=attempt.id, data_package_id=package.id, created_at=now))
        if payload.validation_status in VALIDATED_STATUSES:
            self._apply(attempt, payload.request_id, observation, now)
        row.status = EventDisposition.APPLIED.value
        row.envelope_json = canonical_json({"request": json.loads(row.envelope_json)["request"],
                                           "result": {"data_package_id": package.id}})
        self.session.flush()
        return package, execution
