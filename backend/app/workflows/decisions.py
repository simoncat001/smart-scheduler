from datetime import timedelta
import hashlib
import json

from pydantic import Field, JsonValue, model_validator
from sqlalchemy import select
from typing import Literal

from app.materials import normalize_tizrnbv_payload, strip_runtime_payload_fields, validate_tizrnbv_payload
from app.models import Execution, RecommendationRecord
from app.workflows.definitions import canonical_json
from app.workflows.schemas import StrictModel


class ModelProposal(StrictModel):
    status: Literal["candidate", "exhausted"]
    source_execution_id: str
    model_id: str = Field(min_length=1, max_length=120)
    model_version: str = Field(min_length=1, max_length=120)
    next_payload: dict[str, JsonValue] | None = None
    summary: str = Field(default="", max_length=1000)
    confidence: float = Field(default=0.0, ge=0, le=1)

    @model_validator(mode="after")
    def payload_required(self):
        if (self.status == "candidate") != (self.next_payload is not None):
            raise ValueError("candidate/exhausted payload mismatch")
        if self.next_payload is not None:
            validate_tizrnbv_payload(self.next_payload)
        return self


def proposal_from_outputs(outputs, spec, execution_id):
    value = outputs.get("candidate")
    if not isinstance(value, dict) or not ("next_payload" in value or (
            isinstance(value.get("status"), str) and value["status"] in {"candidate", "exhausted"})):
        return None  # An ordinary model output need not opt into schedule decisions.
    proposal = ModelProposal.model_validate(value)
    if (proposal.source_execution_id != execution_id or proposal.model_id != spec.config["model_id"] or
            proposal.model_version != spec.config["model_version"]):
        raise ValueError("model proposal does not match its pinned source/version")
    return proposal


def recommendation_fingerprint(node, outputs):
    return hashlib.sha256(canonical_json({"node_run_id": node.id, "attempt": node.attempt, "outputs": outputs}).encode()).hexdigest()


def record_model_result(session, execution, node, spec, outputs, now):
    proposal = proposal_from_outputs(outputs, spec, execution.id)
    if proposal is None:
        return None
    fingerprint = recommendation_fingerprint(node, outputs)
    existing = session.scalar(select(RecommendationRecord).where(
        RecommendationRecord.schedule_id == execution.schedule_id, RecommendationRecord.fingerprint == fingerprint,
    ).with_for_update())
    if existing is not None:
        return existing
    context = json.loads(execution.dispatch_payload)
    record = RecommendationRecord(schedule_id=execution.schedule_id, source_execution_id=execution.id,
        campaign_id=context.get("campaign_id"), task_id=execution.schedule_id, confidence=proposal.confidence,
        summary=proposal.summary, rationale_json=canonical_json([f"workflow node {node.node_id}, attempt {node.attempt}"]),
        suggested_payload_json=canonical_json(proposal.next_payload or {}), model_id=proposal.model_id,
        model_version=proposal.model_version, fingerprint=fingerprint, approval_status="generated",
        created_at=now, updated_at=now)
    session.add(record)
    session.flush()
    return record


def apply_designated_decision(session, run, graph, nodes, now):
    if run.status != "completed" or run.decision_applied or graph.decision_node_id is None:
        return
    node = nodes[graph.decision_node_id]
    if node.status != "completed":
        return
    spec = next(item for item in graph.nodes if item.id == node.node_id)
    if spec.type != "model_inference":
        return
    outputs = json.loads(node.outputs_json)
    proposal = proposal_from_outputs(outputs, spec, run.execution_id)
    if proposal is None:
        return
    execution = session.get(Execution, run.execution_id)
    schedule = execution.schedule
    # These current reads share the engine's parent lock; historical results cannot
    # replace a plan already used by another active or later round.
    waiting = session.scalar(select(Execution.id).where(Execution.schedule_id == schedule.id,
        Execution.id != execution.id, Execution.status == "awaiting_feedback").limit(1).with_for_update())
    source_index = json.loads(execution.dispatch_payload).get("experiment_round_index")
    later = False
    for other in session.scalars(select(Execution).where(Execution.schedule_id == schedule.id,
        Execution.id != execution.id, Execution.dispatched_at >= execution.dispatched_at).with_for_update()):
        other_index = json.loads(other.dispatch_payload).get("experiment_round_index")
        if other.dispatched_at > execution.dispatched_at or not (
            isinstance(source_index, int) and isinstance(other_index, int) and other_index < source_index
        ):
            later = True
            break
    if waiting or later or execution.status != "success":
        return
    record = record_model_result(session, execution, node, spec, outputs, now)
    context = json.loads(execution.dispatch_payload)
    digest = hashlib.sha256(canonical_json(json.loads(schedule.payload)).encode()).hexdigest()
    if record.approval_status == "superseded" or context.get("schedule_payload_digest") != digest:
        record.approval_status = "superseded"
        session.flush()
        return
    event_type = "workflow.decision.exhausted"
    if proposal.status == "candidate":
        payload = strip_runtime_payload_fields(normalize_tizrnbv_payload(dict(proposal.next_payload)))
        for key in ("run_id", "workflow_version_id", "used_model_payloads", "schedule_payload_digest"):
            payload.pop(key, None)
        context = json.loads(execution.dispatch_payload)
        payload["task_id"], payload["campaign_id"] = schedule.id, context.get("campaign_id")
        payload["recommended_from_execution_id"] = execution.id
        schedule.payload = canonical_json(payload)
        schedule.next_run_at = now + timedelta(seconds=spec.config.get("next_round_delay_seconds", 3600))
        event_type = "workflow.decision.applied"
    else:
        schedule.status = "paused"
    run.decision_applied, record.approval_status = True, "applied"
    from app.services.closed_loop import ClosedLoopService

    ClosedLoopService(session).record_event(event_type, schedule_id=schedule.id, execution_id=execution.id,
        task_id=schedule.id, run_id=execution.id,
        campaign_id=json.loads(execution.dispatch_payload).get("campaign_id"), recommendation_id=record.id,
        payload={"workflow_version_id": run.workflow_version_id, "node_run_id": node.id,
                 "attempt": node.attempt, "next_run_at": schedule.next_run_at.isoformat()}, created_at=now)
    session.flush()
