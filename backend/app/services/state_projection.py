from __future__ import annotations

from datetime import UTC, datetime
import json
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.materials import measured_composition_from_observation, summarize_workflow_progress
from app.models import (
    CampaignRound,
    CampaignScheduleLink,
    Execution,
    ExecutionFeedbackSnapshot,
    ExecutionInstrumentState,
    utcnow,
)
from app.workflows.ownership import is_workflow_execution


def _load_json(value: str | None) -> dict:
    if not value:
        return {}
    try:
        loaded = json.loads(value)
    except Exception:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _dump_json(value: object, fallback: str) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except Exception:
        return fallback


def _coerce_datetime(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(UTC).replace(tzinfo=None)
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        normalized = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is not None:
            return parsed.astimezone(UTC).replace(tzinfo=None)
        return parsed
    return None


def _instrument_has_parsed_result(instrument_code: str, observation: dict) -> bool:
    if instrument_code == "xrd":
        return any(
            isinstance(observation.get(key), str) and observation.get(key, "").strip()
            for key in ("xrd_phase", "xrd_note")
        )
    if instrument_code == "eds":
        return measured_composition_from_observation(observation) is not None
    if instrument_code == "nanoindentation":
        value = observation.get("hardness_gpa")
        return isinstance(value, (int, float)) or (
            isinstance(value, str) and value.strip() != ""
        )
    if instrument_code == "magnetron_sputtering":
        return bool(
            observation.get("deposition_completed")
            or observation.get("deposition_sample_id")
            or observation.get("sputter_run_id")
        )
    return False


def _round_status(execution_status: str) -> str:
    if execution_status == "awaiting_feedback":
        return "running"
    if execution_status in {"success", "partial"}:
        return "completed"
    if execution_status == "timeout":
        return "timeout"
    if execution_status == "failure":
        return "failed"
    return execution_status


class StateProjectionService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def latest_feedback_snapshots_for_execution_ids(
        self, execution_ids: list[str]
    ) -> dict[str, ExecutionFeedbackSnapshot]:
        if not execution_ids:
            return {}
        rows = list(
            self.session.scalars(
                select(ExecutionFeedbackSnapshot).where(
                    ExecutionFeedbackSnapshot.execution_id.in_(execution_ids)
                )
            )
        )
        return {row.execution_id: row for row in rows}

    def instrument_states_for_execution_ids(
        self, execution_ids: list[str]
    ) -> dict[str, list[ExecutionInstrumentState]]:
        grouped: dict[str, list[ExecutionInstrumentState]] = {item: [] for item in execution_ids}
        if not execution_ids:
            return grouped
        rows = list(
            self.session.scalars(
                select(ExecutionInstrumentState)
                .where(ExecutionInstrumentState.execution_id.in_(execution_ids))
                .order_by(
                    ExecutionInstrumentState.execution_id.asc(),
                    ExecutionInstrumentState.instrument_code.asc(),
                )
            )
        )
        for row in rows:
            if is_workflow_execution(self.session, row.execution_id):
                from app.workflows.projection import workflow_projection

                if row.instrument_code not in workflow_projection(self.session, row.execution_id)[3]:
                    continue
            grouped.setdefault(row.execution_id, []).append(row)
        return grouped

    def sync_execution(
        self,
        execution: Execution,
        *,
        campaign_id: str | None = None,
        now: datetime | None = None,
        workflow_authoritative: bool = False,
    ) -> None:
        now = now or utcnow()
        dispatch_payload = _load_json(execution.dispatch_payload)
        if is_workflow_execution(self.session, execution.id):
            if workflow_authoritative:
                self._sync_workflow_execution(execution, dispatch_payload, campaign_id, now)
            return
        observation = _load_json(execution.feedback_observation)

        workflow_progress = observation.get("workflow_progress")
        if not isinstance(workflow_progress, dict):
            workflow_progress = summarize_workflow_progress(dispatch_payload, observation)

        self._upsert_feedback_snapshot(
            execution=execution,
            workflow_progress=workflow_progress,
            closed_loop_ready=bool(observation.get("closed_loop_ready")),
            now=now,
        )
        self._upsert_instrument_states(
            execution=execution,
            dispatch_payload=dispatch_payload,
            observation=observation,
            now=now,
        )
        self._upsert_campaign_round(
            execution=execution,
            dispatch_payload=dispatch_payload,
            campaign_id=campaign_id,
            now=now,
        )
        self.session.flush()

    def _sync_workflow_execution(self, execution, dispatch_payload, campaign_id, now):
        from app.workflows.models import NodeAttempt
        from app.workflows.projection import workflow_projection

        run, _graph, _nodes, instruments, progress = workflow_projection(self.session, execution.id)
        observation = _load_json(execution.feedback_observation)
        observation["workflow_progress"] = progress
        observation["closed_loop_ready"] = run.status == "completed"
        execution.feedback_observation = _dump_json(observation, "{}")
        dispatch_payload["workflow_run"] = progress
        execution.dispatch_payload = _dump_json(dispatch_payload, "{}")
        self._upsert_feedback_snapshot(execution=execution, workflow_progress=progress,
                                      closed_loop_ready=run.status == "completed", now=now)
        components = dispatch_payload.get("instrument_components", {})
        existing = {row.instrument_code: row for row in self.session.scalars(select(ExecutionInstrumentState).where(
            ExecutionInstrumentState.execution_id == execution.id,
        ))}
        for code, (spec, node) in instruments.items():
            row = existing.get(code)
            if row is None:
                row = ExecutionInstrumentState(execution_id=execution.id, instrument_code=code, created_at=now)
                self.session.add(row)
            attempt = self.session.scalar(select(NodeAttempt).where(
                NodeAttempt.node_run_id == node.id, NodeAttempt.attempt == node.attempt,
            )) if node.attempt else None
            component = components.get(code, {}) if isinstance(components, dict) else {}
            row.component_id = component.get("component_id")
            row.status = ("parsed" if node.status == "completed" else
                          "failed" if node.status in {"failed", "cancelled", "needs_attention"} else
                          "triggered" if spec.type == "instrument_task" and node.status in {"accepted", "running"} else "pending")
            row.triggered_at = (attempt.started_at or attempt.accepted_at) if attempt else None
            row.parsed_at = (attempt.completed_at or now) if attempt and node.status == "completed" else None
            row.uploaded_at = row.parsed_at
            row.failed_at = (attempt.completed_at or now) if attempt and node.status in {"failed", "cancelled"} else None
            row.error_message, row.note, row.updated_at = node.error or "", f"workflow node {node.node_id}", now
        self._upsert_campaign_round(execution=execution, dispatch_payload=dispatch_payload, campaign_id=campaign_id, now=now)
        self.session.flush()

    def backfill_from_history(self) -> int:
        executions = list(
            self.session.scalars(
                select(Execution).order_by(Execution.dispatched_at.asc())
            )
        )
        campaign_links = {
            row.schedule_id: row.campaign_id
            for row in self.session.scalars(select(CampaignScheduleLink))
        }
        count = 0
        for execution in executions:
            if is_workflow_execution(self.session, execution.id):
                continue
            self.sync_execution(
                execution,
                campaign_id=campaign_links.get(execution.schedule_id),
                now=execution.updated_at or execution.dispatched_at or utcnow(),
            )
            count += 1
        self.session.flush()
        return count

    def _upsert_feedback_snapshot(
        self,
        *,
        execution: Execution,
        workflow_progress: dict,
        closed_loop_ready: bool,
        now: datetime,
    ) -> None:
        row = self.session.get(ExecutionFeedbackSnapshot, execution.id)
        if row is None:
            row = ExecutionFeedbackSnapshot(execution_id=execution.id, created_at=now)
            self.session.add(row)

        row.closed_loop_ready = closed_loop_ready
        row.current_step = (
            None
            if workflow_progress.get("current_step") in (None, "")
            else str(workflow_progress.get("current_step"))
        )
        row.current_output = (
            None
            if workflow_progress.get("current_output") in (None, "")
            else str(workflow_progress.get("current_output"))
        )
        row.completed_count = int(workflow_progress.get("completed_count") or 0)
        row.total_steps = int(workflow_progress.get("total_steps") or 0)
        row.completion_ratio = float(workflow_progress.get("completion_ratio") or 0.0)
        row.missing_steps_json = _dump_json(
            workflow_progress.get("missing_steps") or [],
            "[]",
        )
        row.workflow_progress_json = _dump_json(workflow_progress, "{}")
        row.updated_at = now

    def _upsert_instrument_states(
        self,
        *,
        execution: Execution,
        dispatch_payload: dict,
        observation: dict,
        now: datetime,
    ) -> None:
        components = dispatch_payload.get("instrument_components")
        expected_components = components if isinstance(components, dict) else {}
        uploads = observation.get("instrument_uploads")
        upload_states = uploads if isinstance(uploads, dict) else {}

        codes = sorted(set(expected_components.keys()) | set(upload_states.keys()))
        existing = {
            row.instrument_code: row
            for row in self.session.scalars(
                select(ExecutionInstrumentState).where(
                    ExecutionInstrumentState.execution_id == execution.id
                )
            )
        }

        for instrument_code in codes:
            upload_state = (
                upload_states.get(instrument_code)
                if isinstance(upload_states.get(instrument_code), dict)
                else {}
            )
            component = (
                expected_components.get(instrument_code)
                if isinstance(expected_components.get(instrument_code), dict)
                else {}
            )

            row = existing.get(instrument_code)
            if row is None:
                row = ExecutionInstrumentState(
                    id=str(uuid4()),
                    execution_id=execution.id,
                    instrument_code=instrument_code,
                    created_at=now,
                )
                self.session.add(row)

            row.component_id = (
                str(upload_state.get("component_id") or component.get("component_id") or "").strip()
                or None
            )

            triggered_at = (
                _coerce_datetime(upload_state.get("triggered_at"))
                or row.triggered_at
            )
            uploaded_at = (
                _coerce_datetime(upload_state.get("uploaded_at"))
                or row.uploaded_at
            )
            failed_at = (
                _coerce_datetime(upload_state.get("failed_at"))
                or row.failed_at
            )
            collected_at = (
                _coerce_datetime(upload_state.get("collected_at"))
                or row.collected_at
            )
            parsed_at = (
                _coerce_datetime(upload_state.get("parsed_at"))
                or row.parsed_at
            )

            if _instrument_has_parsed_result(instrument_code, observation):
                parsed_at = parsed_at or uploaded_at or collected_at or execution.closed_at or now
                uploaded_at = uploaded_at or parsed_at

            if failed_at is not None:
                triggered_at = triggered_at or failed_at
            if uploaded_at is not None:
                triggered_at = triggered_at or uploaded_at
            if parsed_at is not None:
                triggered_at = triggered_at or parsed_at

            upload_status = str(upload_state.get("status") or "").strip().lower()
            if failed_at is not None or upload_status == "failed":
                status = "failed"
            elif parsed_at is not None:
                status = "parsed"
            elif uploaded_at is not None or upload_status == "uploaded":
                status = "uploaded"
            elif triggered_at is not None or upload_status == "triggered":
                status = "triggered"
            else:
                status = "pending"

            row.status = status
            row.triggered_at = triggered_at
            row.uploaded_at = uploaded_at
            row.parsed_at = parsed_at
            row.failed_at = failed_at
            row.collected_at = collected_at
            row.error_message = str(
                upload_state.get("error_message")
                or upload_state.get("error")
                or row.error_message
                or ""
            )
            row.note = str(upload_state.get("note") or row.note or "")
            row.updated_at = now

    def _upsert_campaign_round(
        self,
        *,
        execution: Execution,
        dispatch_payload: dict,
        campaign_id: str | None,
        now: datetime,
    ) -> None:
        resolved_campaign_id = campaign_id or self._campaign_id_for_schedule(execution.schedule_id)
        if not resolved_campaign_id:
            return

        row = self.session.scalar(
            select(CampaignRound).where(CampaignRound.execution_id == execution.id)
        )
        if row is None:
            row = CampaignRound(
                id=str(uuid4()),
                campaign_id=resolved_campaign_id,
                schedule_id=execution.schedule_id,
                execution_id=execution.id,
                created_at=now,
            )
            self.session.add(row)

        raw_round_index = dispatch_payload.get("experiment_round_index")
        try:
            round_index = int(raw_round_index)
        except (TypeError, ValueError):
            round_index = 0
        if round_index <= 0:
            if (row.round_index or 0) > 0:
                round_index = row.round_index
            else:
                current_max = self.session.scalar(
                    select(func.max(CampaignRound.round_index)).where(
                        CampaignRound.campaign_id == resolved_campaign_id,
                        CampaignRound.execution_id != execution.id,
                    )
                )
                round_index = int(current_max or 0) + 1

        row.campaign_id = resolved_campaign_id
        row.schedule_id = execution.schedule_id
        row.round_index = round_index
        row.experiment_batch_id = (
            None
            if dispatch_payload.get("experiment_batch_id") in (None, "")
            else str(dispatch_payload.get("experiment_batch_id"))
        )
        row.status = _round_status(execution.status)
        row.started_at = execution.dispatched_at or now
        row.completed_at = None if row.status == "running" else execution.closed_at
        row.updated_at = now

    def _campaign_id_for_schedule(self, schedule_id: str) -> str | None:
        link = self.session.get(CampaignScheduleLink, schedule_id)
        return None if link is None else link.campaign_id
