from datetime import UTC, timedelta
import hashlib
import json

from sqlalchemy import select

from app.models import Execution
from app.services.exceptions import ConflictError, NotFoundError
from app.workflows.definitions import canonical_json
from app.workflows.engine import WorkflowEngine, db_time
from app.workflows.messaging import MessagingService
from app.workflows.models import MessageOutbox, NodeAttempt, NodeRun
from app.workflows.results import require_request_id
from app.workflows.schemas import MessageEnvelope


class OperationService:
    """Audited, idempotent operator requests, in the caller's transaction."""

    def __init__(self, session, topic_prefix="scheduler"):
        self.session = session
        self.engine = WorkflowEngine(session, topic_prefix)
        self.messages = MessagingService(session, topic_prefix)

    def apply(self, node_run_id, request, now):
        require_request_id(request.request_id)
        body = {"node_run_id": node_run_id, "request": request.model_dump(mode="json")}
        encoded = canonical_json(body)
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        row, fresh = self.messages._reserve("operator", request.request_id, digest, encoded, db_time(now))
        if not fresh:
            if row.digest != digest:
                raise ConflictError("request ID reused with a different operator action")
            return json.loads(row.envelope_json)["result"]
        node = self.session.get(NodeRun, node_run_id)
        if node is None:
            raise NotFoundError("node run not found")
        run, graph, nodes = self.engine._locked(node.run_id)
        node = nodes[node.node_id]
        spec = next(item for item in graph.nodes if item.id == node.node_id)
        now = db_time(now)
        if request.action == "approve":
            if request.attempt is not None:
                raise ConflictError("manual approval has no executor attempt")
            try:
                self.engine.approve(node.id, request.outputs, now)
            except ValueError as error:
                raise ConflictError("approval outputs failed validation") from error
        else:
            if request.outputs:
                raise ConflictError("outputs are only valid for approval")
            if request.attempt is None:
                raise ConflictError("retry/cancel requires the expected attempt")
            allowed_runs = {"running", "paused", "needs_attention"}
            if request.action == "cancel":
                allowed_runs.add("failed")
            if run.status not in allowed_runs or spec.executor is None:
                raise ConflictError("node has no active executor operation")
            if request.attempt is not None and request.attempt != node.attempt:
                raise ConflictError("operator action refers to a stale attempt")
            attempt = self.session.scalar(select(NodeAttempt).where(
                NodeAttempt.node_run_id == node.id, NodeAttempt.attempt == node.attempt,
            ).with_for_update().execution_options(populate_existing=True))
            if attempt is None:
                raise ConflictError("node has no dispatched attempt")
            if request.action == "retry":
                if spec.executor.kind == "device":
                    raise ConflictError("device retry requires external reconciliation; not supported here")
                if (node.status != "needs_attention" or node.attempt >= spec.retry_policy.max_attempts or
                        self.session.get(Execution, run.execution_id).schedule.status != "active"):
                    raise ConflictError("compute retry requires attention, an active schedule and remaining pinned attempts")
                self.engine._enqueue(run, node, spec, json.loads(attempt.inputs_json), now)
                node.error = None
                run.status = "needs_attention" if any(item.status == "needs_attention" for item in nodes.values()) else "running"
                self.engine._sync_runtime(run, now)
            else:
                if node.status not in {"queued", "accepted", "running", "needs_attention"}:
                    raise ConflictError("node cannot request cancellation in its current state")
                command = MessageEnvelope.model_validate_json(attempt.command_json)
                cancel = command.model_copy(update={
                    "message_id": "operator:" + hashlib.sha256(request.request_id.encode()).hexdigest(),
                    "message_type": "command.cancel", "occurred_at": now.replace(tzinfo=UTC),
                    "expires_at": (now + timedelta(seconds=spec.timeouts.acceptance_seconds)).replace(tzinfo=UTC),
                    "payload": {"reason": request.reason, "require_cancellation_tombstone": True},
                })
                topic = f"{self.engine.topic_prefix}.commands.{spec.executor.gateway_id}"
                if spec.executor.kind == "compute":
                    topic = f"{self.engine.topic_prefix}.{'model' if spec.type == 'model_inference' else 'parse'}.commands"
                self.session.add(MessageOutbox(message_id=cancel.message_id, topic=topic, message_key=node.id,
                    envelope_json=cancel.model_dump_json(), next_attempt_at=now))
                node.status = attempt.status = "cancel_requested"
                attempt.execution_deadline_at = db_time(cancel.expires_at)
                self.engine._sync_runtime(run, now)
        result = {"node_run_id": node.id, "status": node.status, "attempt": node.attempt, "action": request.action}
        row.status = "applied"
        row.envelope_json = canonical_json(dict(body, result=result))
        self.session.flush()
        return result
