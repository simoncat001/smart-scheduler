from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import logging
from typing import Any, TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import get_settings
from app.enums import ExecutionStatus
from app.materials import measured_composition_from_observation
from app.models import (
    Campaign,
    CampaignScheduleLink,
    ClosedLoopEvent,
    DataPackage,
    Execution,
    FeedbackEvent,
    RecommendationRecord,
    Schedule,
    utcnow,
)
from app.schemas import (
    DataPackageCreate,
    EdgeContextRead,
    ExecutionSamplePlanRead,
    SamplePlanItemRead,
)
from app.services.exceptions import ConflictError, NotFoundError
from app.services.manage_db_client import ManageDbClient, ManageDbClientError, ManageDbRecord
from app.services.manage_db_raw_parser import ManageDbRawParser
from app.workflows.ownership import is_workflow_execution, require_legacy_execution

if TYPE_CHECKING:
    from app.workflows.results import PreparedResult


VALIDATED_STATUSES = {"validated", "accepted", "ready", "synced"}
PARSE_REQUIRED_FIELDS = {
    "xrd": "xrd_phase",
    "eds": "measured_composition_at_percent",
    "nanoindentation": "hardness_gpa",
}
logger = logging.getLogger(__name__)


def _dump_json(value: Any) -> str:
    return json.dumps(value or {}, separators=(",", ":"), sort_keys=True)


def _load_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    return json.loads(value)


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _reference_projection(payload: DataPackageCreate) -> dict[str, Any]:
    source_ref = payload.standard_data_ref if isinstance(payload.standard_data_ref, dict) else {}
    projected: dict[str, Any] = {}
    system = source_ref.get("system")
    if isinstance(system, str) and system:
        projected["system"] = system
    elif payload.manage_db_record_id:
        projected["system"] = "manage_db"

    for key in ("base_url", "resource_path", "lookup_url"):
        value = source_ref.get(key)
        if isinstance(value, str) and value:
            projected[key] = value

    record_id = source_ref.get("record_id")
    if isinstance(record_id, str) and record_id:
        projected["record_id"] = record_id
    if payload.manage_db_record_id:
        projected["record_id"] = payload.manage_db_record_id
        projected["manage_db_record_id"] = payload.manage_db_record_id
    if payload.template_id:
        projected["template_id"] = payload.template_id
    if payload.template_version:
        projected["template_version"] = payload.template_version
    if payload.validation_status:
        projected["validation_status"] = payload.validation_status
    return projected


def _status_projection(
    *,
    package_kind: str,
    instrument_code: str,
    note: str,
    template_id: str | None,
    template_version: str | None,
    manage_db_record_id: str | None,
    validation_status: str,
    result_data: dict[str, Any] | None,
    scheduler_observation: dict[str, Any] | None,
    manage_db_record: ManageDbRecord | None = None,
    fetched_at: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    scheduler_observation = dict(scheduler_observation or {})
    metadata_summary = {
        "mode": "reference_only",
        "template_id": template_id,
        "template_version": template_version,
        "manage_db_record_id": manage_db_record_id,
        "validation_status": validation_status,
        "has_scheduler_observation": bool(scheduler_observation),
    }
    result_projection = dict(scheduler_observation or result_data or {})
    log_projection = {
        "mode": "reference_only",
        "package_kind": package_kind,
        "instrument_code": instrument_code,
        "note": note,
    }
    if manage_db_record is not None:
        metadata_summary["mode"] = "manage_db_live"
        metadata_summary["live_fetch"] = {
            "record_id": manage_db_record.record_id,
            "fetched_at": fetched_at,
            "source_url": manage_db_record.source_url,
        }
        if manage_db_record.payload.get("template_name"):
            metadata_summary["template_name"] = manage_db_record.payload.get("template_name")
        if manage_db_record.payload.get("review_status"):
            metadata_summary["review_status"] = manage_db_record.payload.get("review_status")
        result_projection = dict(manage_db_record.payload)
        log_projection = {
            "mode": "manage_db_live",
            "package_kind": package_kind,
            "instrument_code": instrument_code,
            "note": note,
            "source_url": manage_db_record.source_url,
            "fetched_at": fetched_at,
        }
    return {}, metadata_summary, {}, result_projection, log_projection


def _non_empty_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _compact_mapping(data: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in data.items()
        if value not in (None, "", {})
    }


def _deep_merge_dicts(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in patch.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = _deep_merge_dicts(existing, value)
        else:
            merged[key] = value
    return merged


def _merge_observation_layers(*layers: dict[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for layer in layers:
        if isinstance(layer, dict) and layer:
            merged = _deep_merge_dicts(merged, layer)
    return merged


def _derive_scheduler_observation_from_manage_db(
    instrument_code: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    direct = payload.get("scheduler_observation")
    if isinstance(direct, dict) and direct:
        return dict(direct)

    if instrument_code == "magnetron_sputtering":
        return _compact_mapping(
            {
                "deposition_sample_id": payload.get("deposition_sample_id")
                or payload.get("sample_id"),
                "sputter_run_id": payload.get("sputter_run_id") or payload.get("run_id"),
                "deposition_completed": payload.get("deposition_completed"),
                "deposition_note": payload.get("deposition_note") or payload.get("note"),
            }
        )

    if instrument_code == "xrd":
        return _compact_mapping(
            {
                "xrd_phase": payload.get("xrd_phase"),
                "xrd_note": payload.get("xrd_note") or payload.get("note"),
                "xrd_pattern_file": payload.get("xrd_pattern_file"),
                "xrd_low_angle_file": payload.get("xrd_low_angle_file"),
                "xrd_high_angle_file": payload.get("xrd_high_angle_file"),
                "xrd_sample_id": payload.get("xrd_sample_id") or payload.get("sample_id"),
                "xrd_data_dir": payload.get("xrd_data_dir"),
            }
        )

    if instrument_code == "eds":
        measured = payload.get("measured_composition_at_percent")
        observation = {
            "eds_note": payload.get("eds_note") or payload.get("note"),
        }
        if isinstance(measured, dict) and measured:
            observation["measured_composition_at_percent"] = measured
        else:
            for key in ("eds_ti_at", "eds_zr_at", "eds_nb_at", "eds_v_at"):
                observation[key] = payload.get(key)
        return _compact_mapping(observation)

    if instrument_code == "nanoindentation":
        return _compact_mapping(
            {
                "hardness_gpa": payload.get("hardness_gpa"),
                "hardness_std_gpa": payload.get("hardness_std_gpa"),
                "indentation_note": payload.get("indentation_note") or payload.get("note"),
            }
        )

    return {}


def _merge_standard_ref_with_live_record(
    standard_ref: dict[str, Any],
    manage_db_record: ManageDbRecord | None,
) -> dict[str, Any]:
    merged = dict(standard_ref)
    if manage_db_record is None:
        return merged
    merged.setdefault("system", "manage_db")
    merged["record_id"] = manage_db_record.record_id
    merged["manage_db_record_id"] = manage_db_record.record_id
    if manage_db_record.base_url:
        merged.setdefault("base_url", manage_db_record.base_url)
    merged["lookup_url"] = manage_db_record.source_url
    return merged


def _has_required_scheduler_observation(
    instrument_code: str,
    observation: dict[str, Any],
) -> bool:
    if not isinstance(observation, dict) or not observation:
        return False
    if instrument_code == "eds":
        return measured_composition_from_observation(observation) is not None

    required_field = PARSE_REQUIRED_FIELDS.get(instrument_code)
    if not required_field:
        return True
    value = observation.get(required_field)
    return value not in (None, "", {})


def _apply_data_package_snapshot(
    data_package: DataPackage,
    *,
    standard_ref: dict[str, Any],
    scheduler_observation: dict[str, Any],
    manage_db_record: ManageDbRecord | None,
    fetched_at: datetime | None,
) -> None:
    (
        raw_file_refs,
        metadata_summary,
        process_projection,
        result_projection,
        log_projection,
    ) = _status_projection(
        package_kind=data_package.package_kind,
        instrument_code=data_package.instrument_code,
        note=data_package.note,
        template_id=data_package.template_id,
        template_version=data_package.template_version,
        manage_db_record_id=data_package.manage_db_record_id,
        validation_status=data_package.validation_status,
        result_data=_load_json(data_package.result_data_json),
        scheduler_observation=scheduler_observation,
        manage_db_record=manage_db_record,
        fetched_at=None if fetched_at is None else fetched_at.isoformat(),
    )
    data_package.raw_files_json = _dump_json(raw_file_refs)
    data_package.metadata_json = _dump_json(metadata_summary)
    data_package.process_data_json = _dump_json(process_projection)
    data_package.result_data_json = _dump_json(result_projection)
    data_package.log_data_json = _dump_json(log_projection)
    data_package.scheduler_observation_json = _dump_json(scheduler_observation)
    data_package.standard_data_ref_json = _dump_json(
        _merge_standard_ref_with_live_record(standard_ref, manage_db_record)
    )
    if fetched_at is not None:
        data_package.updated_at = fetched_at


@dataclass(slots=True)
class DataPackageIngestResult:
    data_package: DataPackage
    execution: Execution | None
    round_completed: bool
    missing_steps: list[dict[str, Any]]


def prepare_data_package(payload: DataPackageCreate, *, experiment_batch_id=None, settings=None):
    """Existing Manage DB parsing, independent of any SQLAlchemy session or locks."""
    standard_ref = _reference_projection(payload)
    settings = settings or get_settings()
    strict_parsing = payload.validation_status in VALIDATED_STATUSES and payload.instrument_code in PARSE_REQUIRED_FIELDS
    live_record = None
    live_fetch_error = None
    derived_observation = {}
    parsed_observation = {}
    if payload.manage_db_record_id or standard_ref.get("record_id") or standard_ref.get("lookup_url"):
        try:
            live_record = ManageDbClient(settings).fetch_development_data(
                record_id=None if payload.manage_db_record_id in (None, "") else str(payload.manage_db_record_id),
                standard_ref=standard_ref,
            )
        except ManageDbClientError as error:
            live_fetch_error = error
            if not strict_parsing:
                logger.warning("manage_db live fetch skipped: %s", error)
    if live_record is not None:
        derived_observation = _derive_scheduler_observation_from_manage_db(payload.instrument_code, live_record.payload)
        parsed_dataset = ManageDbRawParser(settings).parse_record(
            live_record, instrument_code=payload.instrument_code, sample_id=payload.sample_id,
            experiment_batch_id=payload.experiment_batch_id or experiment_batch_id,
        )
        if parsed_dataset is not None:
            parsed_observation = parsed_dataset.summary_observation
    observation = _merge_observation_layers(derived_observation, parsed_observation, _non_empty_dict(payload.scheduler_observation))
    if strict_parsing and not _has_required_scheduler_observation(payload.instrument_code, observation):
        required = PARSE_REQUIRED_FIELDS[payload.instrument_code]
        if live_fetch_error is not None:
            raise ConflictError("validated " + f"{payload.instrument_code} data package could not fetch manage_db record "
                                f"{payload.manage_db_record_id or standard_ref.get('record_id')}: {live_fetch_error}")
        if live_record is None:
            raise ConflictError(f"validated {payload.instrument_code} data package is missing a usable manage_db record reference; "
                                f"required parsed field {required} was not produced")
        raise ConflictError(f"validated {payload.instrument_code} data package did not produce required parsed field "
                            f"{required} from manage_db record {live_record.record_id}")
    return standard_ref, observation, live_record


class ClosedLoopService:
    def __init__(self, session: Session, *, workflow_topic_prefix: str = "scheduler") -> None:
        self.session = session
        self.workflow_topic_prefix = workflow_topic_prefix

    def prepare_workflow_data_package(self, payload: DataPackageCreate):
        if self.session.new or self.session.dirty or self.session.deleted:
            raise ConflictError("prepare workflow data packages in a clean API session before state writes")
        from app.workflows.results import ResultService, require_request_id, upload_request

        execution = self._resolve_execution(payload)
        if not is_workflow_execution(self.session, execution.id):
            return None
        require_request_id(payload.request_id)
        if not payload.component_id:
            raise ConflictError("workflow data package requires component_id")
        service = ResultService(self.session, self.workflow_topic_prefix)
        if service.lookup(payload.request_id, upload_request("data-package", payload)) is not None:
            return {}, {}, None
        attempt = service.resolve(
            execution.id, payload.instrument_code, payload.component_id, payload.node_run_id, payload.attempt)
        prepared = service.pin(attempt, upload_request("data-package", payload))
        batch_id = _load_json(execution.dispatch_payload).get("experiment_batch_id")
        if self.session.new or self.session.dirty or self.session.deleted:
            raise ConflictError("prepare workflow data packages in a clean API session before state writes")
        self.session.rollback()
        return prepared.with_data(prepare_data_package(payload, experiment_batch_id=batch_id))

    def reconcile_execution_feedbacks(
        self,
        executions: list[Execution],
        *,
        commit: bool = True,
    ) -> bool:
        repaired = False
        for execution in executions:
            repaired = self._reconcile_execution_feedback(execution) or repaired

        if repaired and commit:
            self.session.commit()
            for execution in executions:
                self.session.refresh(execution)
        return repaired

    def _reconcile_execution_feedback(self, execution: Execution) -> bool:
        if is_workflow_execution(self.session, execution.id):
            return False
        if execution.status == ExecutionStatus.AWAITING_FEEDBACK.value:
            return False

        latest_event = self.session.scalar(
            select(FeedbackEvent)
            .where(FeedbackEvent.execution_id == execution.id)
            .order_by(FeedbackEvent.created_at.desc(), FeedbackEvent.id.desc())
            .limit(1)
        )
        if latest_event is None or not latest_event.observation:
            return False
        if latest_event.observation == (execution.feedback_observation or "{}"):
            return False

        execution.feedback_observation = latest_event.observation
        self.session.add(execution)
        return True

    def ensure_campaign_for_schedule(
        self,
        schedule: Schedule,
        payload: dict[str, Any] | None = None,
        *,
        now: datetime | None = None,
    ) -> Campaign:
        link = self.session.get(CampaignScheduleLink, schedule.id)
        if link is not None:
            campaign = self.session.get(Campaign, link.campaign_id)
            if campaign is None:
                raise NotFoundError(f"campaign {link.campaign_id} not found")
            return campaign

        now = now or utcnow()
        payload = payload or _load_json(schedule.payload)
        objective = str(payload.get("objective") or "").strip()
        stop_condition = str(payload.get("stop_condition") or "").strip()
        target_hardness = payload.get("target_hardness_gpa")
        if not objective and target_hardness not in (None, ""):
            objective = f"maximize hardness to >= {target_hardness} GPa"
        if not stop_condition:
            stop_condition = self._derive_stop_condition(payload)

        campaign = Campaign(
            name=str(payload.get("sample_batch") or schedule.name or schedule.id),
            objective=objective,
            stop_condition=stop_condition,
            current_round=0,
            max_round=_coerce_int(payload.get("max_round")),
            global_status="active",
            metadata_json=_dump_json(
                {
                    "schedule_name": schedule.name,
                    "target": schedule.target,
                    "sample_batch": payload.get("sample_batch"),
                    "workflow_kind": payload.get("workflow_kind"),
                }
            ),
            created_at=now,
            updated_at=now,
        )

        created = False
        try:
            with self.session.begin_nested():
                self.session.add(campaign)
                self.session.flush()
                self.session.add(
                    CampaignScheduleLink(
                        schedule_id=schedule.id,
                        campaign_id=campaign.id,
                        created_at=now,
                    )
                )
                self.session.flush()
                created = True
        except IntegrityError:
            link = self.session.get(CampaignScheduleLink, schedule.id)
            if link is None:
                raise
            campaign = self.session.get(Campaign, link.campaign_id)
            if campaign is None:
                raise NotFoundError(f"campaign {link.campaign_id} not found")
            return campaign

        if not created:
            return campaign

        self.record_event(
            "task.created",
            campaign_id=campaign.id,
            schedule_id=schedule.id,
            task_id=schedule.id,
            run_id=None,
            payload={
                "schedule_name": schedule.name,
                "target": schedule.target,
                "sample_batch": payload.get("sample_batch"),
                "objective": campaign.objective,
                "stop_condition": campaign.stop_condition,
            },
            created_at=now,
        )
        return campaign

    def campaign_for_schedule(self, schedule_id: str) -> Campaign | None:
        link = self.session.get(CampaignScheduleLink, schedule_id)
        if link is None:
            return None
        return self.session.get(Campaign, link.campaign_id)

    def list_campaigns(self, *, limit: int = 100) -> list[Campaign]:
        return list(
            self.session.scalars(
                select(Campaign).order_by(Campaign.updated_at.desc()).limit(limit)
            )
        )

    def get_campaign(self, campaign_id: str) -> Campaign:
        campaign = self.session.get(Campaign, campaign_id)
        if campaign is None:
            raise NotFoundError(f"campaign {campaign_id} not found")
        return campaign

    def record_dispatch(
        self,
        *,
        schedule: Schedule,
        execution: Execution,
        dispatch_payload: dict[str, Any],
        now: datetime | None = None,
    ) -> Campaign:
        now = now or utcnow()
        campaign = self.ensure_campaign_for_schedule(schedule, dispatch_payload, now=now)
        # MySQL enforces the closed_loop_events execution_id foreign key immediately,
        # so the execution row must exist before we append dispatch events.
        self.session.flush()
        round_index = _coerce_int(dispatch_payload.get("experiment_round_index")) or 0
        if round_index > campaign.current_round:
            campaign.current_round = round_index
            campaign.updated_at = now
        if campaign.global_status != "active":
            campaign.global_status = "active"

        self.record_event(
            "task.dispatched",
            campaign_id=campaign.id,
            schedule_id=schedule.id,
            execution_id=execution.id,
            task_id=schedule.id,
            run_id=execution.id,
            payload={
                "experiment_batch_id": dispatch_payload.get("experiment_batch_id"),
                "experiment_round_index": round_index,
                "instrument_components": dispatch_payload.get("instrument_components"),
            },
            created_at=now,
        )
        self.record_event(
            "run.started",
            campaign_id=campaign.id,
            schedule_id=schedule.id,
            execution_id=execution.id,
            task_id=schedule.id,
            run_id=execution.id,
            payload={
                "status": execution.status,
                "experiment_batch_id": dispatch_payload.get("experiment_batch_id"),
            },
            created_at=now,
        )
        return campaign

    def record_feedback(
        self,
        *,
        schedule: Schedule,
        execution: Execution,
        outcome: str,
        observation: dict[str, Any],
        next_execution: Execution | None = None,
        now: datetime | None = None,
    ) -> None:
        now = now or utcnow()
        campaign = self.ensure_campaign_for_schedule(schedule, _load_json(schedule.payload), now=now)
        if schedule.status == "paused":
            campaign.global_status = "paused"
        else:
            campaign.global_status = "active"
        campaign.updated_at = now

        event_type = {
            "success": "run.completed",
            "partial": "run.completed",
            "failure": "run.failed",
            "timeout": "run.timeout",
        }.get(outcome, "run.updated")
        self.record_event(
            event_type,
            campaign_id=campaign.id,
            schedule_id=schedule.id,
            execution_id=execution.id,
            task_id=schedule.id,
            run_id=execution.id,
            payload={
                "outcome": outcome,
                "closed_loop_ready": bool(observation.get("closed_loop_ready")),
                "workflow_progress": observation.get("workflow_progress"),
                "auto_dispatch": observation.get("auto_dispatch"),
            },
            created_at=now,
        )

        if next_execution is not None:
            next_payload = _load_json(next_execution.dispatch_payload)
            self.record_event(
                "task.regenerated",
                campaign_id=campaign.id,
                schedule_id=schedule.id,
                execution_id=next_execution.id,
                task_id=schedule.id,
                run_id=next_execution.id,
                payload={
                    "previous_run_id": execution.id,
                    "next_run_id": next_execution.id,
                    "next_experiment_batch_id": next_payload.get("experiment_batch_id"),
                    "mat_model_candidate_rank": next_payload.get("mat_model_candidate_rank"),
                },
                created_at=now,
            )

    def record_recommendation(
        self,
        recommendation: RecommendationRecord,
        *,
        created_at: datetime | None = None,
    ) -> None:
        created_at = created_at or utcnow()
        self.record_event(
            "recommendation.generated",
            campaign_id=recommendation.campaign_id,
            schedule_id=recommendation.schedule_id,
            execution_id=recommendation.source_execution_id,
            recommendation_id=recommendation.id,
            task_id=recommendation.task_id,
            run_id=recommendation.source_execution_id,
            payload={
                "model_id": recommendation.model_id,
                "model_version": recommendation.model_version,
                "input_dataset_version": recommendation.input_dataset_version,
                "approval_status": recommendation.approval_status,
            },
            created_at=created_at,
        )

    def ingest_data_package(
        self,
        payload: DataPackageCreate,
        *,
        now: datetime | None = None,
        prepared: PreparedResult | tuple | None = None,
        commit: bool = True,
    ) -> DataPackageIngestResult:
        from app.schemas import InstrumentUploadCreate
        from app.services.acquisition import InstrumentUploadService

        now = now or utcnow()
        execution = self._resolve_execution(payload)
        if is_workflow_execution(self.session, execution.id):
            from app.workflows.results import PreparedResult, ResultService, require_request_id, upload_request

            require_request_id(payload.request_id)
            service = ResultService(self.session, self.workflow_topic_prefix)
            if service.lookup(payload.request_id, upload_request("data-package", payload)) is None and not isinstance(prepared, PreparedResult):
                if prepared is not None and payload.node_run_id is None:
                    raise ConflictError("prepared workflow results must preserve node attempt identity")
                attempt = service.resolve(execution.id, payload.instrument_code, payload.component_id, payload.node_run_id, payload.attempt)
                pinned = service.pin(attempt, upload_request("data-package", payload))
                if prepared is None:
                    ref = _reference_projection(payload)
                    if payload.manage_db_record_id or ref.get("record_id") or ref.get("lookup_url"):
                        raise ConflictError("prepare workflow data package parsing outside the state transaction first")
                    prepared = prepare_data_package(payload)
                prepared = pinned.with_data(prepared)
            package, execution = service.ingest_package(payload, prepared, now)
            if commit:
                self.session.commit()
                self.session.refresh(package)
                self.session.refresh(execution)
            progress = _load_json(execution.feedback_observation).get("workflow_progress", {})
            return DataPackageIngestResult(data_package=package, execution=execution,
                round_completed=bool(progress.get("is_complete")), missing_steps=progress.get("missing_steps", []))
        require_legacy_execution(self.session, execution.id)
        schedule = self.session.get(Schedule, execution.schedule_id)
        if schedule is None:
            raise NotFoundError(f"schedule {execution.schedule_id} not found")

        campaign = self.ensure_campaign_for_schedule(schedule, _load_json(schedule.payload), now=now)
        dispatch_payload = _load_json(execution.dispatch_payload)
        instrument_components = dispatch_payload.get("instrument_components", {})
        component = (
            instrument_components.get(payload.instrument_code)
            if isinstance(instrument_components, dict)
            else None
        )
        component_id = payload.component_id or (
            str(component.get("component_id")) if isinstance(component, dict) else None
        )
        instrument_id = payload.instrument_id or component_id or payload.instrument_code
        experiment_batch_id = payload.experiment_batch_id or dispatch_payload.get("experiment_batch_id")
        run_id = execution.id
        task_id = execution.schedule_id
        standard_ref, scheduler_observation, live_record = prepare_data_package(payload, experiment_batch_id=experiment_batch_id)

        data_package = DataPackage(
            campaign_id=campaign.id,
            schedule_id=schedule.id,
            execution_id=execution.id,
            task_id=task_id,
            run_id=run_id,
            experiment_batch_id=experiment_batch_id,
            instrument_code=payload.instrument_code,
            instrument_id=str(instrument_id),
            component_id=component_id,
            sample_id=payload.sample_id
            or str(dispatch_payload.get("sample_id") or experiment_batch_id or ""),
            package_kind=payload.package_kind,
            template_id=payload.template_id,
            template_version=payload.template_version,
            validation_status=payload.validation_status,
            manage_db_record_id=payload.manage_db_record_id,
            raw_files_json="{}",
            metadata_json="{}",
            process_data_json="{}",
            result_data_json="{}",
            log_data_json="{}",
            scheduler_observation_json=_dump_json(scheduler_observation),
            standard_data_ref_json=_dump_json(standard_ref),
            note=payload.note,
            created_at=now,
            updated_at=now,
        )
        _apply_data_package_snapshot(
            data_package,
            standard_ref=standard_ref,
            scheduler_observation=scheduler_observation,
            manage_db_record=live_record,
            fetched_at=now,
        )
        self.session.add(data_package)
        self.session.flush()

        self.record_event(
            "data.uploaded",
            campaign_id=campaign.id,
            schedule_id=schedule.id,
            execution_id=execution.id,
            data_package_id=data_package.id,
            task_id=task_id,
            run_id=run_id,
            payload={
                "instrument_code": payload.instrument_code,
                "instrument_id": instrument_id,
                "component_id": component_id,
                "validation_status": payload.validation_status,
                "template_id": payload.template_id,
            },
            created_at=now,
        )
        if payload.validation_status in VALIDATED_STATUSES:
            self.record_event(
                "data.validated",
                campaign_id=campaign.id,
                schedule_id=schedule.id,
                execution_id=execution.id,
                data_package_id=data_package.id,
                task_id=task_id,
                run_id=run_id,
                payload={
                    "instrument_code": payload.instrument_code,
                    "validation_status": payload.validation_status,
                },
                created_at=now,
            )

        round_completed = False
        missing_steps: list[dict[str, Any]] = []
        updated_execution: Execution | None = None
        if (
            component_id
            and experiment_batch_id
            and payload.validation_status in VALIDATED_STATUSES
        ):
            upload_result = InstrumentUploadService(self.session).ingest_upload(
                InstrumentUploadCreate(
                    instrument_code=payload.instrument_code,
                    component_id=component_id,
                    experiment_batch_id=str(experiment_batch_id),
                    observation=scheduler_observation,
                    note=payload.note,
                ),
                now=now,
                commit=False,
            )
            round_completed = upload_result.round_completed
            missing_steps = upload_result.missing_steps
            updated_execution = upload_result.execution
            self.record_event(
                "instrument.uploaded",
                campaign_id=campaign.id,
                schedule_id=schedule.id,
                execution_id=updated_execution.id,
                data_package_id=data_package.id,
                task_id=task_id,
                run_id=run_id,
                payload={
                    "instrument_code": payload.instrument_code,
                    "round_completed": round_completed,
                    "missing_steps": missing_steps,
                },
                created_at=now,
            )

        if commit:
            self.session.commit()
            self.session.refresh(data_package)
            if updated_execution is not None:
                self.session.refresh(updated_execution)

        return DataPackageIngestResult(
            data_package=data_package,
            execution=updated_execution,
            round_completed=round_completed,
            missing_steps=missing_steps,
        )

    def build_run_view(
        self,
        run_id: str,
    ) -> tuple[Campaign | None, Execution, list[DataPackage], list[RecommendationRecord], list[ClosedLoopEvent]]:
        execution = self.session.get(Execution, run_id)
        if execution is None:
            raise NotFoundError(f"run {run_id} not found")
        execution_repaired = self._reconcile_execution_feedback(execution)

        campaign = self.campaign_for_schedule(execution.schedule_id)
        data_packages = list(
            self.session.scalars(
                select(DataPackage)
                .where(DataPackage.execution_id == run_id)
                .order_by(DataPackage.created_at.desc())
            )
        )
        package_refreshed = False
        for data_package in data_packages:
            package_refreshed = self._refresh_data_package_from_manage_db(
                data_package,
                now=utcnow(),
            ) or package_refreshed
        if execution_repaired or package_refreshed:
            self.session.commit()
            self.session.refresh(execution)
            for data_package in data_packages:
                self.session.refresh(data_package)
        recommendations = list(
            self.session.scalars(
                select(RecommendationRecord)
                .where(RecommendationRecord.schedule_id == execution.schedule_id)
                .order_by(RecommendationRecord.created_at.desc())
                .limit(10)
            )
        )
        events = list(
            self.session.scalars(
                select(ClosedLoopEvent)
                .where(ClosedLoopEvent.run_id == run_id)
                .order_by(ClosedLoopEvent.created_at.asc())
                .limit(200)
            )
        )
        return campaign, execution, data_packages, recommendations, events

    def refresh_executions_from_manage_db(
        self,
        executions: list[Execution],
        *,
        now: datetime | None = None,
    ) -> bool:
        candidates = [
            execution
            for execution in executions
            if execution.status == ExecutionStatus.AWAITING_FEEDBACK.value
        ]
        if not candidates:
            return False

        refreshed = False
        refresh_now = now or utcnow()
        for execution in candidates:
            refreshed = (
                self.refresh_execution_from_manage_db(
                    execution,
                    now=refresh_now,
                    commit=False,
                )
                or refreshed
            )

        if refreshed:
            self.session.commit()
            for execution in candidates:
                self.session.refresh(execution)
        return refreshed

    def refresh_execution_from_manage_db(
        self,
        execution: Execution | str,
        *,
        now: datetime | None = None,
        commit: bool = True,
    ) -> bool:
        if isinstance(execution, str):
            resolved = self.session.get(Execution, execution)
            if resolved is None:
                raise NotFoundError(f"execution {execution} not found")
            execution = resolved

        if is_workflow_execution(self.session, execution.id):
            return False

        data_packages = list(
            self.session.scalars(
                select(DataPackage)
                .where(DataPackage.execution_id == execution.id)
                .order_by(DataPackage.created_at.desc())
            )
        )
        if not data_packages:
            return False

        refreshed = False
        refresh_now = now or utcnow()
        for data_package in data_packages:
            refreshed = (
                self._refresh_data_package_from_manage_db(
                    data_package,
                    now=refresh_now,
                )
                or refreshed
            )

        if refreshed and commit:
            self.session.commit()
            self.session.refresh(execution)
        return refreshed

    def build_edge_context(
        self,
        execution_id: str,
        *,
        instrument_code: str | None = None,
    ) -> list[EdgeContextRead]:
        execution = self.session.get(Execution, execution_id)
        if execution is None:
            raise NotFoundError(f"execution {execution_id} not found")

        schedule = self.session.get(Schedule, execution.schedule_id)
        if schedule is None:
            raise NotFoundError(f"schedule {execution.schedule_id} not found")

        campaign = self.campaign_for_schedule(schedule.id)
        dispatch_payload = _load_json(execution.dispatch_payload)
        instrument_components = dispatch_payload.get("instrument_components")
        if not isinstance(instrument_components, dict):
            return []

        round_sample_id = self._round_sample_id(dispatch_payload)
        contexts: list[EdgeContextRead] = []
        for code, component in instrument_components.items():
            if instrument_code and code != instrument_code:
                continue
            component_payload = component if isinstance(component, dict) else {}
            contexts.append(
                EdgeContextRead(
                    campaign_id=None if campaign is None else campaign.id,
                    task_id=str(dispatch_payload.get("task_id") or schedule.id),
                    run_id=str(dispatch_payload.get("run_id") or execution.id),
                    schedule_id=schedule.id,
                    execution_id=execution.id,
                    experiment_batch_id=dispatch_payload.get("experiment_batch_id"),
                    experiment_round_index=_coerce_int(dispatch_payload.get("experiment_round_index")),
                    sample_batch=dispatch_payload.get("sample_batch"),
                    sample_id=round_sample_id,
                    instrument_code=code,
                    instrument_id=component_payload.get("component_id") or component_payload.get("instrument_id"),
                    component_id=component_payload.get("component_id"),
                    workflow_step_code=component_payload.get("step_code"),
                    expected_outputs=list(component_payload.get("outputs") or []),
                    workflow_name=dispatch_payload.get("workflow_name"),
                    target_metric=dispatch_payload.get("measurement_metric"),
                    target_value=dispatch_payload.get("target_hardness_gpa")
                    or dispatch_payload.get("target_metric_value"),
                )
            )
        return contexts

    def build_sample_plan(self, execution_id: str) -> ExecutionSamplePlanRead:
        execution = self.session.get(Execution, execution_id)
        if execution is None:
            raise NotFoundError(f"execution {execution_id} not found")

        schedule = self.session.get(Schedule, execution.schedule_id)
        if schedule is None:
            raise NotFoundError(f"schedule {execution.schedule_id} not found")

        campaign = self.campaign_for_schedule(schedule.id)
        dispatch_payload = _load_json(execution.dispatch_payload)
        sample_plan = dispatch_payload.get("sample_plan")
        if not isinstance(sample_plan, dict):
            sample_plan = {}

        items: list[SamplePlanItemRead] = []
        raw_samples = sample_plan.get("samples")
        if isinstance(raw_samples, list):
            for sample in raw_samples:
                if not isinstance(sample, dict):
                    continue
                composition = sample.get("composition_at_percent")
                items.append(
                    SamplePlanItemRead(
                        sample_code=str(sample.get("sample_code") or sample.get("sample_id") or ""),
                        sample_id=str(
                            sample.get("sample_id")
                            or sample.get("allocated_sample_id")
                            or self._round_sample_id(dispatch_payload)
                        ),
                        grid_row=_coerce_int(sample.get("grid_row")),
                        grid_col=_coerce_int(sample.get("grid_col")),
                        snake_index=_coerce_int(sample.get("snake_index")),
                        composition_at_percent=composition if isinstance(composition, dict) else {},
                    )
                )

        if not items:
            round_sample_id = self._round_sample_id(dispatch_payload)
            items = [
                SamplePlanItemRead(
                    sample_code="S01",
                    sample_id=round_sample_id,
                )
            ]

        return ExecutionSamplePlanRead(
            campaign_id=None if campaign is None else campaign.id,
            task_id=str(dispatch_payload.get("task_id") or schedule.id),
            run_id=str(dispatch_payload.get("run_id") or execution.id),
            schedule_id=schedule.id,
            execution_id=execution.id,
            experiment_batch_id=dispatch_payload.get("experiment_batch_id"),
            experiment_round_index=_coerce_int(dispatch_payload.get("experiment_round_index")),
            round_sample_id=self._round_sample_id(dispatch_payload),
            rows=_coerce_int(sample_plan.get("rows")),
            cols=_coerce_int(sample_plan.get("cols")),
            total_samples=_coerce_int(sample_plan.get("total_samples")) or len(items),
            samples=items,
        )

    def record_event(
        self,
        event_type: str,
        *,
        campaign_id: str | None,
        schedule_id: str | None,
        execution_id: str | None = None,
        data_package_id: str | None = None,
        recommendation_id: str | None = None,
        task_id: str | None,
        run_id: str | None,
        payload: dict[str, Any],
        created_at: datetime | None = None,
    ) -> ClosedLoopEvent:
        event = ClosedLoopEvent(
            event_type=event_type,
            campaign_id=campaign_id,
            schedule_id=schedule_id,
            execution_id=execution_id,
            data_package_id=data_package_id,
            recommendation_id=recommendation_id,
            task_id=task_id,
            run_id=run_id,
            payload_json=_dump_json(payload),
            created_at=created_at or utcnow(),
        )
        self.session.add(event)
        return event

    def _resolve_execution(self, payload: DataPackageCreate) -> Execution:
        from app.workflows.results import resolve_execution
        from app.workflows.ownership import bound_version_id

        if payload.run_id or payload.node_run_id or payload.experiment_batch_id:
            return resolve_execution(self.session, run_id=payload.run_id, node_run_id=payload.node_run_id,
                batch_id=payload.experiment_batch_id, task_id=payload.task_id)

        if payload.task_id:
            if bound_version_id(self.session, payload.task_id):
                raise ConflictError("workflow data package requires explicit execution/node/batch; not task-latest")
            execution = self.session.scalar(
                select(Execution)
                .where(Execution.schedule_id == payload.task_id)
                .order_by(Execution.dispatched_at.desc())
            )
            if execution is not None:
                if is_workflow_execution(self.session, execution.id):
                    raise ConflictError("workflow data package requires explicit execution/node/batch; not task-latest")
                return execution

        if payload.run_id:
            raise NotFoundError(f"run {payload.run_id} not found")
        if payload.experiment_batch_id:
            raise NotFoundError(
                f"execution for experiment batch {payload.experiment_batch_id} not found"
            )
        raise NotFoundError("unable to resolve execution from data package payload")

    def _fetch_live_manage_db_record(
        self,
        *,
        record_id: str | None,
        standard_ref: dict[str, Any],
    ) -> ManageDbRecord | None:
        if record_id in (None, "") and not standard_ref.get("record_id") and not standard_ref.get("lookup_url"):
            return None
        try:
            return ManageDbClient(get_settings()).fetch_development_data(
                record_id=None if record_id in (None, "") else str(record_id),
                standard_ref=standard_ref,
            )
        except ManageDbClientError as exc:
            logger.warning("manage_db live fetch skipped: %s", exc)
            return None

    def _refresh_data_package_from_manage_db(
        self,
        data_package: DataPackage,
        *,
        now: datetime | None = None,
    ) -> bool:
        if is_workflow_execution(self.session, data_package.execution_id):
            return False
        standard_ref = _load_json(data_package.standard_data_ref_json)
        live_record = self._fetch_live_manage_db_record(
            record_id=data_package.manage_db_record_id,
            standard_ref=standard_ref,
        )
        if live_record is None:
            return False

        current_result = _load_json(data_package.result_data_json)
        current_observation = _load_json(data_package.scheduler_observation_json)
        derived_observation = _derive_scheduler_observation_from_manage_db(
            data_package.instrument_code,
            live_record.payload,
        )
        parsed_observation: dict[str, Any] = {}
        parsed_dataset = ManageDbRawParser(get_settings()).parse_record(
            live_record,
            instrument_code=data_package.instrument_code,
            sample_id=data_package.sample_id,
            experiment_batch_id=data_package.experiment_batch_id,
        )
        if parsed_dataset is not None:
            parsed_observation = parsed_dataset.summary_observation
        refreshed_observation = _merge_observation_layers(
            current_observation,
            derived_observation,
            parsed_observation,
        )
        if (
            current_result == live_record.payload
            and standard_ref == _merge_standard_ref_with_live_record(standard_ref, live_record)
            and current_observation == refreshed_observation
        ):
            return False

        _apply_data_package_snapshot(
            data_package,
            standard_ref=standard_ref,
            scheduler_observation=refreshed_observation,
            manage_db_record=live_record,
            fetched_at=now or utcnow(),
        )
        self.session.add(data_package)

        execution = self.session.get(Execution, data_package.execution_id)
        if (
            execution is not None
            and execution.status == ExecutionStatus.AWAITING_FEEDBACK.value
            and data_package.validation_status in VALIDATED_STATUSES
            and data_package.component_id
            and data_package.experiment_batch_id
        ):
            from app.schemas import InstrumentUploadCreate
            from app.services.acquisition import InstrumentUploadService

            InstrumentUploadService(self.session).ingest_upload(
                InstrumentUploadCreate(
                    instrument_code=data_package.instrument_code,
                    component_id=data_package.component_id,
                    experiment_batch_id=data_package.experiment_batch_id,
                    observation=refreshed_observation,
                    note=data_package.note,
                ),
                now=now or utcnow(),
                commit=False,
            )
        return True

    @staticmethod
    def _round_sample_id(dispatch_payload: dict[str, Any]) -> str | None:
        sample_id = dispatch_payload.get("sample_id")
        if isinstance(sample_id, str) and sample_id.strip():
            return sample_id
        batch_id = dispatch_payload.get("experiment_batch_id")
        if isinstance(batch_id, str) and batch_id.strip():
            return f"{batch_id.strip()}-S01"
        return None

    @staticmethod
    def _derive_stop_condition(payload: dict[str, Any]) -> str:
        target_hardness = payload.get("target_hardness_gpa")
        max_round = payload.get("max_round")
        if target_hardness not in (None, "") and max_round not in (None, ""):
            return f"target_hardness_gpa >= {target_hardness} or current_round >= {max_round}"
        if target_hardness not in (None, ""):
            return f"target_hardness_gpa >= {target_hardness}"
        if max_round not in (None, ""):
            return f"current_round >= {max_round}"
        return "manual_stop"
