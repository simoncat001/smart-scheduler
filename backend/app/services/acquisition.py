from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.enums import ExecutionStatus
from app.materials import enrich_feedback_observation, measured_composition_from_observation
from app.models import Execution, utcnow
from app.schemas import FeedbackCreate, InstrumentUploadCreate
from app.services.exceptions import ConflictError, NotFoundError
from app.services.feedback import FeedbackService
from app.services.state_projection import StateProjectionService
from app.xrd_parser import XrdParsingError, analyze_xrd_pair, resolve_xrd_pair

if TYPE_CHECKING:
    from app.workflows.results import PreparedResult


def _parse_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    return json.loads(value)


def _merge_observation(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_observation(merged[key], value)
        else:
            merged[key] = value
    return merged


def _to_iso(value: datetime | None) -> str:
    if value is None:
        value = utcnow()
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    return value.isoformat()


def _normalize_upload_status(value: str | None) -> str:
    status = str(value or "uploaded").strip().lower()
    if status == "success":
        return "uploaded"
    if status in {"triggered", "uploaded", "failed"}:
        return status
    return "uploaded"


@dataclass(slots=True)
class InstrumentUploadResult:
    execution: Execution
    experiment_batch_id: str
    instrument_code: str
    component_id: str
    round_completed: bool
    observation: dict[str, Any]
    missing_steps: list[dict[str, Any]]


class InstrumentUploadService:
    def __init__(self, session: Session, *, xrd_data_dir: str | Path | None = None,
                 workflow_topic_prefix: str = "scheduler") -> None:
        self.session = session
        self.xrd_data_dir = Path(xrd_data_dir).expanduser().resolve() if xrd_data_dir else None
        self.workflow_topic_prefix = workflow_topic_prefix

    def prepare_workflow_upload(self, upload: InstrumentUploadCreate):
        """API preflight ends its read transaction before raw-file parsing."""
        if self.session.new or self.session.dirty or self.session.deleted:
            raise ConflictError("prepare workflow uploads in a clean API session before state writes")
        from app.workflows.ownership import is_workflow_execution
        from app.workflows.results import ResultService, require_request_id, resolve_execution, upload_request

        execution = (resolve_execution(self.session, run_id=upload.run_id, node_run_id=upload.node_run_id,
                                       batch_id=upload.experiment_batch_id)
                     if upload.run_id or upload.node_run_id else self._find_execution(upload.experiment_batch_id)[0])
        if not is_workflow_execution(self.session, execution.id):
            return None
        require_request_id(upload.request_id)
        service = ResultService(self.session, self.workflow_topic_prefix)
        saved = service.lookup(upload.request_id, upload_request("instrument-upload", upload))
        if saved is not None:
            return saved["observation"]
        attempt = service.resolve(
            execution.id, upload.instrument_code, upload.component_id, upload.node_run_id, upload.attempt)
        prepared = service.pin(attempt, upload_request("instrument-upload", upload))
        if self.session.new or self.session.dirty or self.session.deleted:
            raise ConflictError("prepare workflow uploads in a clean API session before state writes")
        self.session.rollback()
        observation = self._normalize_upload(upload.instrument_code, upload.observation) if upload.status in {"uploaded", "success"} else {}
        return prepared.with_data(observation)

    def ingest_upload(
        self,
        upload: InstrumentUploadCreate,
        *,
        now: datetime | None = None,
        commit: bool = True,
        prepared_observation: PreparedResult | dict | None = None,
    ) -> InstrumentUploadResult:
        now = now or utcnow()
        from app.workflows.ownership import is_workflow_execution
        from app.workflows.results import ResultService, require_request_id, resolve_execution

        if upload.run_id or upload.node_run_id:
            execution = resolve_execution(self.session, run_id=upload.run_id, node_run_id=upload.node_run_id,
                                          batch_id=upload.experiment_batch_id)
            dispatch_payload = _parse_json(execution.dispatch_payload)
        else:
            execution, dispatch_payload = self._find_execution(upload.experiment_batch_id)
        if is_workflow_execution(self.session, execution.id):
            from app.workflows.results import PreparedResult, upload_request

            require_request_id(upload.request_id)
            service = ResultService(self.session, self.workflow_topic_prefix)
            prepared = prepared_observation
            saved = service.lookup(upload.request_id, upload_request("instrument-upload", upload))
            if saved is None and not isinstance(prepared, PreparedResult):
                if prepared is not None and upload.node_run_id is None:
                    raise ConflictError("prepared workflow results must preserve node attempt identity")
                attempt = service.resolve(execution.id, upload.instrument_code, upload.component_id, upload.node_run_id, upload.attempt)
                pinned = service.pin(attempt, upload_request("instrument-upload", upload))
                observation = prepared if prepared is not None else (
                    self._normalize_upload(upload.instrument_code, upload.observation) if upload.status in {"uploaded", "success"} else {})
                prepared = pinned.with_data(observation)
            execution = service.ingest_upload(upload, prepared, now)
            if commit:
                self.session.commit()
                self.session.refresh(execution)
            observation = _parse_json(execution.feedback_observation)
            progress = observation.get("workflow_progress", {})
            return InstrumentUploadResult(execution=execution, experiment_batch_id=upload.experiment_batch_id,
                instrument_code=upload.instrument_code, component_id=upload.component_id,
                round_completed=bool(progress.get("is_complete")), observation=observation,
                missing_steps=progress.get("missing_steps", []))
        instrument_components = dispatch_payload.get("instrument_components")
        if not isinstance(instrument_components, dict):
            raise ConflictError(f"execution {execution.id} has no instrument component configuration")

        component = instrument_components.get(upload.instrument_code)
        if not isinstance(component, dict):
            raise NotFoundError(
                f"instrument component {upload.instrument_code} is not configured for execution {execution.id}"
            )

        expected_component_id = str(component.get("component_id") or "").strip()
        if expected_component_id != upload.component_id:
            raise ConflictError(
                f"component mismatch for {upload.instrument_code}: expected {expected_component_id}, got {upload.component_id}"
            )

        upload_status = _normalize_upload_status(upload.status)
        existing_observation = _parse_json(execution.feedback_observation)
        existing_state = {}
        instrument_uploads = existing_observation.get("instrument_uploads")
        if isinstance(instrument_uploads, dict):
            current_state = instrument_uploads.get(upload.instrument_code)
            if isinstance(current_state, dict):
                existing_state = current_state

        if upload_status in {"triggered", "failed"} and existing_state.get("uploaded_at"):
            workflow_progress = existing_observation.get("workflow_progress", {})
            campaign_id = dispatch_payload.get("campaign_id")
            StateProjectionService(self.session).sync_execution(
                execution,
                campaign_id=None if campaign_id in (None, "") else str(campaign_id),
                now=now,
            )
            return InstrumentUploadResult(
                execution=execution,
                experiment_batch_id=upload.experiment_batch_id,
                instrument_code=upload.instrument_code,
                component_id=upload.component_id,
                round_completed=bool(workflow_progress.get("is_complete")),
                observation=existing_observation,
                missing_steps=workflow_progress.get("missing_steps", []),
            )

        normalized_patch: dict[str, Any] = {}
        if upload_status == "uploaded":
            normalized_patch = self._normalize_upload(upload.instrument_code, upload.observation)

        merged = _merge_observation(existing_observation, normalized_patch)
        collected_at = _to_iso(upload.collected_at or now)
        upload_state = {
            "component_id": upload.component_id,
            "collected_at": collected_at,
            "status": upload_status,
            "note": upload.note,
            "triggered_at": existing_state.get("triggered_at") or _to_iso(now),
        }
        if upload_status == "uploaded":
            upload_state["uploaded_at"] = _to_iso(now)
        elif upload_status == "failed":
            upload_state["failed_at"] = _to_iso(now)
        merged = _merge_observation(
            merged,
            {
                "instrument_uploads": {
                    upload.instrument_code: upload_state
                },
            },
        )
        if normalized_patch:
            merged = _merge_observation(
                merged,
                {
                    "instrument_data": {
                        upload.instrument_code: {
                            "component_id": upload.component_id,
                            "collected_at": collected_at,
                            "observation": normalized_patch,
                        }
                    }
                },
            )
        merged = enrich_feedback_observation(dispatch_payload, merged)
        workflow_progress = merged.get("workflow_progress", {})
        round_completed = bool(workflow_progress.get("is_complete"))

        if upload_status == "uploaded" and round_completed:
            latency_ms = max(
                0, int((now - execution.dispatched_at).total_seconds() * 1000)
            )
            feedback = FeedbackCreate(
                outcome="success",
                quality_score=1.0,
                latency_ms=latency_ms,
                observation=merged,
                note=upload.note or "all required instrument data uploaded",
            )
            execution = FeedbackService(self.session).submit_feedback(
                execution.id, feedback, now=now, commit=False
            )
            final_observation = _parse_json(execution.feedback_observation)
        else:
            execution.feedback_observation = json.dumps(
                merged, separators=(",", ":"), sort_keys=True
            )
            final_observation = merged

        campaign_id = dispatch_payload.get("campaign_id")
        StateProjectionService(self.session).sync_execution(
            execution,
            campaign_id=None if campaign_id in (None, "") else str(campaign_id),
            now=now,
        )

        if commit:
            self.session.commit()
            self.session.refresh(execution)
            final_observation = _parse_json(execution.feedback_observation)

        workflow_progress = final_observation.get("workflow_progress", {})
        return InstrumentUploadResult(
            execution=execution,
            experiment_batch_id=upload.experiment_batch_id,
            instrument_code=upload.instrument_code,
            component_id=upload.component_id,
            round_completed=bool(workflow_progress.get("is_complete")),
            observation=final_observation,
            missing_steps=workflow_progress.get("missing_steps", []),
        )

    def _find_execution(
        self, experiment_batch_id: str
    ) -> tuple[Execution, dict[str, Any]]:
        waiting = list(
            self.session.scalars(
                select(Execution)
                .order_by(Execution.dispatched_at.desc())
            )
        )
        from app.workflows.ownership import is_workflow_execution

        matches = []
        for execution in waiting:
            dispatch_payload = _parse_json(execution.dispatch_payload)
            if dispatch_payload.get("experiment_batch_id") == experiment_batch_id and (
                execution.status == ExecutionStatus.AWAITING_FEEDBACK.value or is_workflow_execution(self.session, execution.id)
            ):
                matches.append((execution, dispatch_payload))
        if len(matches) > 1:
            raise ConflictError("ambiguous experiment batch; supply an explicit execution/node")
        if matches:
            return matches[0]
        raise NotFoundError(
            f"awaiting execution for experiment batch {experiment_batch_id} not found"
        )

    def _normalize_upload(
        self,
        instrument_code: str,
        observation: dict[str, Any],
    ) -> dict[str, Any]:
        if instrument_code == "magnetron_sputtering":
            result = {
                "deposition_sample_id": observation.get("deposition_sample_id")
                or observation.get("sample_id"),
                "sputter_run_id": observation.get("sputter_run_id")
                or observation.get("run_id"),
                "deposition_completed": observation.get("deposition_completed", True),
                "deposition_note": observation.get("deposition_note")
                or observation.get("note"),
            }
            return {key: value for key, value in result.items() if value is not None}

        if instrument_code == "xrd":
            result = {
                "xrd_phase": observation.get("xrd_phase"),
                "xrd_note": observation.get("xrd_note") or observation.get("note"),
                "xrd_pattern_file": observation.get("xrd_pattern_file"),
                "xrd_low_angle_file": observation.get("xrd_low_angle_file")
                or observation.get("low_angle_file"),
                "xrd_high_angle_file": observation.get("xrd_high_angle_file")
                or observation.get("high_angle_file"),
                "xrd_sample_id": observation.get("xrd_sample_id")
                or observation.get("sample_id"),
                "xrd_data_dir": observation.get("xrd_data_dir")
                or observation.get("xrd_directory"),
            }
            low_angle_file = result.get("xrd_low_angle_file")
            high_angle_file = result.get("xrd_high_angle_file")
            sample_id = result.get("xrd_sample_id")
            if not (low_angle_file and high_angle_file) and sample_id:
                data_dir = result.get("xrd_data_dir") or self.xrd_data_dir
                if data_dir is None:
                    raise ConflictError(
                        "xrd_sample_id was provided but no XRD data directory is configured"
                    )
                try:
                    pair = resolve_xrd_pair(data_dir, sample_id)
                except XrdParsingError as exc:
                    raise ConflictError(f"failed to locate raw XRD files: {exc}") from exc
                result["xrd_data_dir"] = str(Path(data_dir).expanduser().resolve())
                result["xrd_low_angle_file"] = str(pair.low_angle_file)
                result["xrd_high_angle_file"] = str(pair.high_angle_file)
                low_angle_file = result["xrd_low_angle_file"]
                high_angle_file = result["xrd_high_angle_file"]
            if low_angle_file and high_angle_file:
                try:
                    analysis = analyze_xrd_pair(low_angle_file, high_angle_file)
                except XrdParsingError as exc:
                    raise ConflictError(f"failed to parse raw XRD files: {exc}") from exc
                if not result.get("xrd_phase"):
                    result["xrd_phase"] = analysis.phase
                if not result.get("xrd_note"):
                    result["xrd_note"] = analysis.note
                result["xrd_fit_summary"] = analysis.to_dict()
                result["xrd_peak_positions"] = analysis.peak_positions
            return {key: value for key, value in result.items() if value is not None}

        if instrument_code == "eds":
            normalized = dict(observation)
            measured = measured_composition_from_observation(normalized)
            result: dict[str, Any] = {
                "eds_note": observation.get("eds_note") or observation.get("note")
            }
            if measured is not None:
                result["measured_composition_at_percent"] = {
                    key: round(value, 3) for key, value in measured.items()
                }
                result["eds_ti_at"] = measured["ti_at"]
                result["eds_zr_at"] = measured["zr_at"]
                result["eds_nb_at"] = measured["nb_at"]
                result["eds_v_at"] = measured["v_at"]
            return {key: value for key, value in result.items() if value is not None}

        if instrument_code == "nanoindentation":
            result = {
                "hardness_gpa": observation.get("hardness_gpa"),
                "hardness_std_gpa": observation.get("hardness_std_gpa"),
                "indentation_note": observation.get("indentation_note")
                or observation.get("note"),
            }
            return {key: value for key, value in result.items() if value is not None}

        raise NotFoundError(f"unsupported instrument code: {instrument_code}")
