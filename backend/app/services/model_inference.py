from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Execution, ModelInferenceRun, RecommendationRecord
from app.workflows.ownership import legacy_execution_clause, require_legacy_execution, is_workflow_execution


class ModelInferenceService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def record_workflow_result(self, execution, node, spec, outputs, now):
        """Node locks are held by the caller; never flatten owned nodes into legacy rows."""
        from app.workflows.decisions import record_model_result

        return record_model_result(self.session, execution, node, spec, outputs, now)

    def latest_for_source_execution_ids(
        self, execution_ids: list[str]
    ) -> dict[str, ModelInferenceRun]:
        if not execution_ids:
            return {}

        rows = list(
            self.session.scalars(
                select(ModelInferenceRun).where(
                    ModelInferenceRun.source_execution_id.in_(execution_ids)
                )
            )
        )
        return {row.source_execution_id: row for row in rows if not is_workflow_execution(self.session, row.source_execution_id)}

    def record_started(
        self,
        *,
        schedule_id: str,
        source_execution_id: str,
        started_at: datetime,
    ) -> ModelInferenceRun:
        require_legacy_execution(self.session, source_execution_id)
        row = self._get_by_source_execution_id(source_execution_id)
        if row is None:
            row = ModelInferenceRun(
                schedule_id=schedule_id,
                source_execution_id=source_execution_id,
                status="running",
                started_at=started_at,
            )
            self.session.add(row)
        else:
            row.schedule_id = schedule_id
            row.status = "running"
            row.started_at = started_at
            row.completed_at = None
            row.dispatched_at = None
            row.dispatched_execution_id = None
            row.error = ""
        self.session.flush()
        return row

    def backfill_from_history(self) -> int:
        recommendations = list(
            self.session.scalars(
                select(RecommendationRecord).order_by(RecommendationRecord.updated_at.desc())
            )
        )
        executions = list(
            self.session.scalars(
                select(Execution).where(legacy_execution_clause()).order_by(Execution.dispatched_at.asc())
            )
        )
        by_source: dict[str, RecommendationRecord] = {}
        for record in recommendations:
            if not record.source_execution_id:
                continue
            by_source.setdefault(record.source_execution_id, record)

        executions_by_schedule: dict[str, list[Execution]] = {}
        for execution in executions:
            executions_by_schedule.setdefault(execution.schedule_id, []).append(execution)

        created_or_updated = 0
        for source_execution_id, record in by_source.items():
            payload = self._load_json(record.suggested_payload_json)
            summary = record.summary or ""
            status = "failed"
            scheduled_for = None
            dispatched_execution_id = None
            dispatched_at = None

            source_execution = next((item for item in executions if item.id == source_execution_id), None)
            if source_execution is None:
                continue

            later_same_schedule = [
                item
                for item in executions_by_schedule.get(source_execution.schedule_id, [])
                if item.dispatched_at > source_execution.dispatched_at
            ]
            dispatched_execution = next(
                (
                    item
                    for item in later_same_schedule
                    if self._load_json(item.dispatch_payload).get("recommended_from_execution_id")
                    == source_execution_id
                ),
                later_same_schedule[0] if later_same_schedule else None,
            )
            if dispatched_execution is not None:
                dispatched_execution_id = dispatched_execution.id
                dispatched_at = dispatched_execution.dispatched_at

            if payload.get("next_action") == "run_mat_model_candidate":
                if dispatched_execution_id:
                    status = "dispatched"
                elif payload.get("mat_model_online_refresh_applied"):
                    status = "dispatch_scheduled"
            elif payload.get("mat_model_online_refresh_applied"):
                status = "refreshed"

            row = self._get_by_source_execution_id(source_execution_id)
            if row is None:
                row = ModelInferenceRun(
                    schedule_id=record.schedule_id,
                    source_execution_id=source_execution_id,
                )
                self.session.add(row)

            row.schedule_id = record.schedule_id
            row.recommendation_id = record.id
            row.status = status
            row.summary = summary
            row.result_payload_json = record.suggested_payload_json or "{}"
            row.started_at = row.started_at or record.created_at
            row.completed_at = record.updated_at or record.created_at
            row.scheduled_for = scheduled_for
            row.dispatched_execution_id = dispatched_execution_id
            row.dispatched_at = dispatched_at
            row.error = ""
            created_or_updated += 1

        self.session.flush()
        return created_or_updated

    def record_result(
        self,
        *,
        schedule_id: str,
        source_execution_id: str,
        recommendation_id: str | None,
        status: str,
        summary: str,
        payload: dict[str, Any] | None,
        completed_at: datetime,
        scheduled_for: datetime | None = None,
        error: str = "",
    ) -> ModelInferenceRun:
        require_legacy_execution(self.session, source_execution_id)
        row = self._get_by_source_execution_id(source_execution_id)
        if row is None:
            row = ModelInferenceRun(
                schedule_id=schedule_id,
                source_execution_id=source_execution_id,
            )
            self.session.add(row)

        row.schedule_id = schedule_id
        row.recommendation_id = recommendation_id
        row.status = status
        row.summary = summary
        row.error = error
        row.scheduled_for = scheduled_for
        row.completed_at = completed_at
        row.result_payload_json = json.dumps(
            payload or {}, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )
        self.session.flush()
        return row

    def mark_dispatched(
        self,
        *,
        source_execution_id: str,
        dispatched_execution_id: str,
        dispatched_at: datetime,
    ) -> ModelInferenceRun | None:
        require_legacy_execution(self.session, source_execution_id)
        require_legacy_execution(self.session, dispatched_execution_id)
        row = self._get_by_source_execution_id(source_execution_id)
        if row is None:
            return None
        row.status = "dispatched"
        row.dispatched_execution_id = dispatched_execution_id
        row.dispatched_at = dispatched_at
        self.session.flush()
        return row

    def _get_by_source_execution_id(self, source_execution_id: str) -> ModelInferenceRun | None:
        return self.session.scalar(
            select(ModelInferenceRun).where(
                ModelInferenceRun.source_execution_id == source_execution_id
            )
        )

    @staticmethod
    def _load_json(value: str | None) -> dict[str, Any]:
        if not value:
            return {}
        try:
            return json.loads(value)
        except Exception:
            return {}
