from __future__ import annotations

from datetime import timedelta
import json
from pathlib import Path

from fastapi.testclient import TestClient
import numpy as np
import pytest

from app.config import Settings
from app.database import Database
from app.enums import ScheduleStatus
from app.main import create_app
from app.materials import extract_predicted_composition
from app.models import CampaignScheduleLink, Schedule, utcnow
from app.schemas import DataPackageCreate, FeedbackCreate, InstrumentUploadCreate, ScheduleCreate
from app.services.acquisition import InstrumentUploadService
from app.services.closed_loop import ClosedLoopService
from app.services.feedback import FeedbackService
from app.services.manage_db_client import ManageDbClient, ManageDbRecord, ManageDbSearchResult
from app.services.manage_db_raw_parser import ManageDbRawParser
from app.services.mat_model_service import MatModelService
from app.services.recommendations import RecommendationService
from app.services.scheduler import SchedulerService


@pytest.fixture()
def database(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'test.db'}")
    database.create_schema()
    yield database
    database.dispose()


@pytest.fixture()
def session(database):
    with database.session_factory() as session:
        yield session


def test_success_feedback_updates_health_and_releases_closed_loop(session):
    service = SchedulerService(session)
    now = utcnow()
    schedule = service.create_schedule(
        ScheduleCreate(
            name="demo",
            target="etl/demo",
            base_interval_seconds=60,
            min_interval_seconds=10,
            max_interval_seconds=300,
            retry_delay_seconds=10,
            feedback_timeout_seconds=30,
            priority=5,
        ),
        now=now,
    )

    executions, timed_out = service.dispatch_due_schedules(now=now)
    assert timed_out == 0
    assert len(executions) == 1
    execution = executions[0]
    assert execution.status == "awaiting_feedback"

    FeedbackService(session).submit_feedback(
        execution.id,
        FeedbackCreate(
            outcome="success",
            quality_score=0.95,
            latency_ms=1500,
            observation={"metric_name": "c/a ratio", "metric_value": "4.99"},
            note="healthy run",
        ),
        now=now + timedelta(seconds=5),
    )

    session.refresh(schedule)
    session.refresh(execution)

    assert execution.status == "success"
    assert schedule.awaiting_feedback is False
    assert schedule.consecutive_failures == 0
    assert schedule.health_score > 0.5
    assert schedule.next_run_at > now + timedelta(seconds=60)
    assert execution.feedback_observation == '{"metric_name":"c/a ratio","metric_value":"4.99"}'


def test_failure_feedback_shortens_retry_window_and_raises_priority(session):
    service = SchedulerService(session)
    now = utcnow()
    schedule = service.create_schedule(
        ScheduleCreate(
            name="retryable",
            target="worker/retryable",
            base_interval_seconds=120,
            min_interval_seconds=5,
            max_interval_seconds=300,
            retry_delay_seconds=10,
            feedback_timeout_seconds=20,
            priority=4,
        ),
        now=now,
    )

    execution = service.dispatch_due_schedules(now=now)[0][0]
    FeedbackService(session).submit_feedback(
        execution.id,
        FeedbackCreate(outcome="failure", quality_score=0.1, latency_ms=2500),
        now=now + timedelta(seconds=2),
    )

    session.refresh(schedule)
    assert schedule.consecutive_failures == 1
    assert schedule.dynamic_priority > schedule.priority

    retry_window = schedule.next_run_at - (now + timedelta(seconds=2))
    assert retry_window.total_seconds() == 10


def test_tick_auto_closes_timeout_and_pauses_after_threshold(session):
    service = SchedulerService(session)
    now = utcnow()
    schedule = service.create_schedule(
        ScheduleCreate(
            name="timeout-sensitive",
            target="alerts/check",
            base_interval_seconds=30,
            min_interval_seconds=1,
            max_interval_seconds=120,
            retry_delay_seconds=1,
            feedback_timeout_seconds=5,
            max_consecutive_failures=1,
            priority=6,
        ),
        now=now,
    )

    service.dispatch_due_schedules(now=now)
    executions, timed_out = service.dispatch_due_schedules(now=now + timedelta(seconds=6))

    session.refresh(schedule)
    assert executions == []
    assert timed_out == 1
    assert schedule.status == "paused"
    assert schedule.awaiting_feedback is False


def test_api_smoke_flow(database):
    app = create_app(
        Settings(
            database_url=f"sqlite:///{(database.engine.url.database)}",
            scheduler_enabled=False,
            auto_create_schema=False,
        )
    )
    with TestClient(app) as client:
        created = client.post(
            "/api/v1/schedules",
            json={
                "name": "api-flow",
                "target": "api/task",
                "base_interval_seconds": 30,
                "min_interval_seconds": 5,
                "max_interval_seconds": 120,
            },
        )
        assert created.status_code == 200
        schedule = created.json()

        tick = client.post("/api/v1/engine/tick")
        assert tick.status_code == 200
        body = tick.json()
        assert body["dispatched_count"] == 1

        execution_id = body["executions"][0]["id"]
        feedback = client.post(
            f"/api/v1/executions/{execution_id}/feedback",
            json={
                "outcome": "success",
                "quality_score": 0.9,
                "latency_ms": 900,
                "observation": {
                    "metric_name": "capacity_retention",
                    "metric_value": "93.2",
                    "metric_unit": "%",
                },
            },
        )
        assert feedback.status_code == 200
        assert feedback.json()["feedback_observation"]["metric_name"] == "capacity_retention"

        fetched = client.get(f"/api/v1/schedules/{schedule['id']}")
        assert fetched.status_code == 200
        assert fetched.json()["awaiting_feedback"] is False


def test_root_serves_ui_entry_or_build_hint(database):
    app = create_app(
        Settings(
            database_url=f"sqlite:///{(database.engine.url.database)}",
            scheduler_enabled=False,
            auto_create_schema=False,
        )
    )
    with TestClient(app) as client:
        response = client.get("/")
        assert response.status_code == 200
        assert "Smart Scheduler UI" in response.text
        assert "/assets/" in response.text or "前端构建产物尚未生成" in response.text


def test_mat_model_space_endpoint_returns_13x13_batch_plan(database):
    app = create_app(
        Settings(
            database_url=f"sqlite:///{(database.engine.url.database)}",
            scheduler_enabled=False,
            auto_create_schema=False,
        )
    )
    with TestClient(app) as client:
        response = client.get("/api/v1/mat-model/space")
        assert response.status_code == 200
        payload = response.json()
        assert payload["predicted_composition_space"]["grid_shape"]["rows"] == 13
        assert payload["predicted_composition_space"]["grid_shape"]["cols"] == 13
        assert payload["predicted_composition_space"]["grid_shape"]["total_samples"] == 169
        assert payload["predicted_batch_plan"]["rows"] == 13
        assert payload["predicted_batch_plan"]["cols"] == 13
        assert payload["predicted_batch_plan"]["total_samples"] == 169
        assert len(payload["predicted_batch_plan"]["samples"]) == 169


def test_tizrnbv_schedule_normalizes_workflow_payload(session):
    service = SchedulerService(session)
    schedule = service.create_schedule(
        ScheduleCreate(
            name="tizrnbv-loop",
            target="materials/sputter-cluster-01",
            base_interval_seconds=1800,
            min_interval_seconds=300,
            max_interval_seconds=7200,
            payload={
                "material_system": "Ti-Zr-Nb-V quaternary thin film",
                "sample_batch": "TZNV-0320-A",
                "ti_at": 25,
                "zr_at": 25,
                "nb_at": 30,
                "v_at": 20,
                "target_hardness_gpa": 13.5,
            },
        )
    )

    payload = schedule_to_dict(schedule.payload)
    assert payload["workflow_kind"] == "tizrnbv_hardness_loop"
    assert payload["workflow_steps"][0]["code"] == "magnetron_sputtering"
    assert payload["workflow_steps"][-1]["code"] == "nanoindentation"
    assert extract_predicted_composition(payload) == {
        "ti_at": 25.0,
        "zr_at": 25.0,
        "nb_at": 30.0,
        "v_at": 20.0,
    }

    execution = service.dispatch_due_schedules(now=utcnow())[0][0]
    dispatch_payload = execution.dispatch_payload
    dispatch_data = schedule_to_dict(dispatch_payload)
    assert dispatch_data["experiment_round_index"] == 1
    assert dispatch_data["experiment_batch_id"] == "TZNV-0320-A-R01"
    assert dispatch_data["workflow_run"]["current_step"] == "xrd"
    assert dispatch_data["workflow_run"]["steps"][0]["status"] == "completed"
    assert dispatch_data["workflow_run"]["steps"][1]["status"] == "current"


def test_closed_loop_campaign_is_created_with_schedule(session):
    service = SchedulerService(session)
    schedule = service.create_schedule(
        ScheduleCreate(
            name="closed-loop-context",
            target="materials/sputter-cluster-ctx",
            base_interval_seconds=1800,
            min_interval_seconds=300,
            max_interval_seconds=7200,
            payload={
                "sample_batch": "TZNV-CTX-01",
                "objective": "maximize hardness while keeping bcc phase",
                "ti_at": 25,
                "zr_at": 25,
                "nb_at": 30,
                "v_at": 20,
            },
        )
    )

    payload = schedule_to_dict(schedule.payload)
    campaigns = ClosedLoopService(session).list_campaigns(limit=10)

    assert payload["task_id"] == schedule.id
    assert payload["campaign_id"]
    assert len(campaigns) == 1
    assert campaigns[0].id == payload["campaign_id"]
    assert campaigns[0].name == "TZNV-CTX-01"


def test_dispatch_legacy_schedule_without_campaign_link_creates_single_link(session):
    now = utcnow()
    legacy_payload = {
        "sample_batch": "TZNV-LEGACY-01",
        "workflow_kind": "tizrnbv_hardness_loop",
        "target_hardness_gpa": 13.5,
    }
    schedule = Schedule(
        name="legacy-without-link",
        description="",
        target="materials/sputter-cluster-legacy",
        payload=json.dumps(legacy_payload, separators=(",", ":"), sort_keys=True),
        base_interval_seconds=1800,
        min_interval_seconds=300,
        max_interval_seconds=7200,
        retry_delay_seconds=120,
        feedback_timeout_seconds=900,
        max_consecutive_failures=3,
        priority=5,
        dynamic_priority=5,
        health_score=0.5,
        consecutive_failures=0,
        status=ScheduleStatus.ACTIVE.value,
        awaiting_feedback=False,
        next_run_at=now,
        created_at=now,
        updated_at=now,
    )
    session.add(schedule)
    session.commit()

    executions, timed_out = SchedulerService(session).dispatch_due_schedules(now=now)

    assert timed_out == 0
    assert len(executions) == 1
    assert executions[0].schedule_id == schedule.id

    links = list(
        session.query(CampaignScheduleLink).filter(CampaignScheduleLink.schedule_id == schedule.id)
    )
    assert len(links) == 1

    campaign = ClosedLoopService(session).campaign_for_schedule(schedule.id)
    assert campaign is not None
    assert campaign.id == links[0].campaign_id


def test_api_rejects_invalid_tizrnbv_sum(database):
    app = create_app(
        Settings(
            database_url=f"sqlite:///{(database.engine.url.database)}",
            scheduler_enabled=False,
            auto_create_schema=False,
        )
    )
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/schedules",
            json={
                "name": "bad-sum",
                "target": "materials/sputter-cluster-01",
                "base_interval_seconds": 1800,
                "min_interval_seconds": 300,
                "max_interval_seconds": 7200,
                "payload": {
                    "ti_at": 20,
                    "zr_at": 20,
                    "nb_at": 20,
                    "v_at": 20,
                },
            },
        )
        assert response.status_code == 422


def test_recommendation_engine_suggests_higher_temperature_when_metric_below_target(session):
    service = SchedulerService(session)
    now = utcnow()
    schedule = service.create_schedule(
        ScheduleCreate(
            name="anneal-opt",
            target="materials/furnace-02",
            base_interval_seconds=1800,
            min_interval_seconds=300,
            max_interval_seconds=7200,
            retry_delay_seconds=120,
            feedback_timeout_seconds=900,
            priority=7,
            payload={
                "material_system": "Li-rich layered oxide",
                "experiment_stage": "annealing",
                "temperature_c": 850,
                "duration_h": 8,
                "measurement_metric": "I(003)/I(104)",
                "target_metric_value": 1.5,
                "optimize_goal": "maximize",
            },
        ),
        now=now,
    )

    execution = service.dispatch_due_schedules(now=now)[0][0]
    FeedbackService(session).submit_feedback(
        execution.id,
        FeedbackCreate(
            outcome="partial",
            quality_score=0.72,
            latency_ms=2400,
            observation={
                "metric_name": "I(003)/I(104)",
                "metric_value": "1.31",
                "metric_unit": "ratio",
            },
            note="peak split improved but still below target",
        ),
        now=now + timedelta(minutes=5),
    )

    recommendation = RecommendationService(session).list_recommendations(limit=10)[0]
    assert recommendation.schedule_id == schedule.id
    assert recommendation.suggested_payload["temperature_c"] == 860.0
    assert recommendation.suggested_payload["duration_h"] == 8.5


def test_recommendation_api_and_patch_update_schedule(database):
    app = create_app(
        Settings(
            database_url=f"sqlite:///{(database.engine.url.database)}",
            scheduler_enabled=False,
            auto_create_schema=False,
        )
    )
    with TestClient(app) as client:
        created = client.post(
            "/api/v1/schedules",
            json={
                "name": "patchable-loop",
                "target": "materials/furnace-04",
                "base_interval_seconds": 1800,
                "min_interval_seconds": 300,
                "max_interval_seconds": 7200,
                "payload": {
                    "experiment_stage": "annealing",
                    "temperature_c": 780,
                    "duration_h": 10,
                    "target_metric_value": 1.6,
                    "measurement_metric": "I(003)/I(104)",
                    "optimize_goal": "maximize",
                },
            },
        )
        schedule_id = created.json()["id"]

        tick = client.post("/api/v1/engine/tick")
        execution_id = tick.json()["executions"][0]["id"]
        client.post(
            f"/api/v1/executions/{execution_id}/feedback",
            json={
                "outcome": "partial",
                "quality_score": 0.8,
                "latency_ms": 1800,
                "observation": {
                    "metric_name": "I(003)/I(104)",
                    "metric_value": "1.44",
                },
            },
        )

        recommendations = client.get("/api/v1/recommendations?limit=5")
        assert recommendations.status_code == 200
        suggested_payload = recommendations.json()[0]["suggested_payload"]

        updated = client.patch(
            f"/api/v1/schedules/{schedule_id}",
            json={"payload": suggested_payload, "description": "apply recommendation"},
        )
        assert updated.status_code == 200
        assert updated.json()["payload"]["temperature_c"] == 790.0
        assert updated.json()["description"] == "apply recommendation"


def test_tizrnbv_recommendation_prefers_sputter_calibration_when_eds_drifts(session):
    service = SchedulerService(session)
    now = utcnow()
    schedule = service.create_schedule(
        ScheduleCreate(
            name="tizrnbv-calibration",
            target="materials/sputter-cluster-03",
            base_interval_seconds=1800,
            min_interval_seconds=300,
            max_interval_seconds=7200,
            payload={
                "material_system": "Ti-Zr-Nb-V quaternary thin film",
                "ti_at": 25,
                "zr_at": 25,
                "nb_at": 30,
                "v_at": 20,
                "target_hardness_gpa": 14.0,
                "composition_tolerance_at": 1.5,
            },
        ),
        now=now,
    )

    execution = service.dispatch_due_schedules(now=now)[0][0]
    FeedbackService(session).submit_feedback(
        execution.id,
        FeedbackCreate(
            outcome="partial",
            quality_score=0.7,
            latency_ms=1800,
            observation={
                "deposition_sample_id": "TZNV-CAL-01-R01",
                "sputter_run_id": "SPT-20260320-03",
                "xrd_phase": "bcc",
                "hardness_gpa": 12.6,
                "measured_composition_at_percent": {
                    "ti_at": 20.0,
                    "zr_at": 29.0,
                    "nb_at": 31.0,
                    "v_at": 20.0,
                },
            },
            note="composition drift observed after sputtering",
        ),
        now=now + timedelta(minutes=5),
    )

    recommendation = RecommendationService(session).list_recommendations(limit=10)[0]
    assert recommendation.schedule_id == schedule.id
    assert recommendation.suggested_payload["next_action"] == "recalibrate_sputter_power"
    assert "偏离模型设定" in recommendation.summary


def test_tizrnbv_recommendation_uses_mat_model_first_batch_candidate(session):
    service = SchedulerService(session)
    now = utcnow()
    schedule = service.create_schedule(
        ScheduleCreate(
            name="tizrnbv-mat-model-bootstrap",
            target="materials/sputter-cluster-11",
            base_interval_seconds=1800,
            min_interval_seconds=300,
            max_interval_seconds=7200,
            payload={
                "material_system": "Ti-Zr-Nb-V quaternary thin film",
                "sample_batch": "TZNV-MM-BOOT",
                "ti_at": 25,
                "zr_at": 25,
                "nb_at": 30,
                "v_at": 20,
                "target_hardness_gpa": 14.0,
            },
        ),
        now=now,
    )

    candidate = MatModelService().pick_next_candidate(used_payloads=[], preferred_rank=1)
    assert candidate is not None

    recommendation = RecommendationService(session).list_recommendations(limit=10)[0]
    suggested = recommendation.suggested_payload["mat_model_candidate_composition_at_percent"]

    assert recommendation.schedule_id == schedule.id
    assert recommendation.suggested_payload["mat_model_candidate_source"] == "experimental_batch"
    assert recommendation.suggested_payload["mat_model_candidate_rank"] == candidate.rank
    assert recommendation.suggested_payload["model_version"].startswith("mat_model/")
    assert recommendation.suggested_payload["predicted_composition_space"]["grid_shape"]["total_samples"] == 169
    assert recommendation.suggested_payload["predicted_batch_plan"]["total_samples"] == 169
    assert suggested["ti_at"] == pytest.approx(candidate.composition["ti_at"], abs=1e-3)
    assert suggested["zr_at"] == pytest.approx(candidate.composition["zr_at"], abs=1e-3)
    assert suggested["nb_at"] == pytest.approx(candidate.composition["nb_at"], abs=1e-3)
    assert suggested["v_at"] == pytest.approx(candidate.composition["v_at"], abs=1e-3)
    assert "mat_model" in recommendation.summary


def test_tizrnbv_recommendation_advances_to_next_mat_model_candidate(session):
    mat_model_service = MatModelService()
    first_candidate = mat_model_service.pick_next_candidate(used_payloads=[], preferred_rank=1)
    assert first_candidate is not None

    payload = mat_model_service.build_schedule_payload(
        base_payload={
            "material_system": "Ti-Zr-Nb-V quaternary thin film",
            "sample_batch": "TZNV-MM-NEXT",
            "target_hardness_gpa": 14.0,
        },
        candidate=first_candidate,
    )

    service = SchedulerService(session)
    now = utcnow()
    schedule = service.create_schedule(
        ScheduleCreate(
            name="tizrnbv-mat-model-next",
            target="materials/sputter-cluster-12",
            base_interval_seconds=1800,
            min_interval_seconds=300,
            max_interval_seconds=7200,
            payload=payload,
        ),
        now=now,
    )

    execution = service.dispatch_due_schedules(now=now)[0][0]
    FeedbackService(session).submit_feedback(
        execution.id,
        FeedbackCreate(
            outcome="success",
            quality_score=0.93,
            latency_ms=2100,
            observation={
                "deposition_sample_id": "TZNV-MM-NEXT-R01",
                "sputter_run_id": "SPT-20260325-12",
                "xrd_phase": "bcc",
                "hardness_gpa": 11.0,
                "measured_composition_at_percent": {
                    "ti_at": first_candidate.composition["ti_at"] + 0.2,
                    "zr_at": first_candidate.composition["zr_at"] - 0.1,
                    "nb_at": first_candidate.composition["nb_at"] - 0.1,
                    "v_at": first_candidate.composition["v_at"],
                },
            },
        ),
        now=now + timedelta(minutes=5),
    )

    next_candidate = mat_model_service.pick_next_candidate(
        used_payloads=[payload, schedule_to_dict(execution.dispatch_payload)],
        preferred_rank=2,
    )
    assert next_candidate is not None

    recommendation = RecommendationService(session).list_recommendations(limit=10)[0]
    suggested = recommendation.suggested_payload["mat_model_candidate_composition_at_percent"]

    assert recommendation.schedule_id == schedule.id
    assert recommendation.suggested_payload["next_action"] == "run_mat_model_candidate"
    assert recommendation.suggested_payload["mat_model_candidate_rank"] == next_candidate.rank
    assert recommendation.suggested_payload["predicted_batch_plan"]["total_samples"] == 169
    assert suggested["ti_at"] == pytest.approx(next_candidate.composition["ti_at"], abs=1e-3)
    assert suggested["zr_at"] == pytest.approx(next_candidate.composition["zr_at"], abs=1e-3)
    assert suggested["nb_at"] == pytest.approx(next_candidate.composition["nb_at"], abs=1e-3)
    assert suggested["v_at"] == pytest.approx(next_candidate.composition["v_at"], abs=1e-3)
    assert "mat_model 候选" in recommendation.summary


def test_tizrnbv_feedback_enriches_workflow_progress(session):
    service = SchedulerService(session)
    now = utcnow()
    schedule = service.create_schedule(
        ScheduleCreate(
            name="tizrnbv-complete-loop",
            target="materials/sputter-cluster-08",
            base_interval_seconds=1800,
            min_interval_seconds=300,
            max_interval_seconds=7200,
            payload={
                "material_system": "Ti-Zr-Nb-V quaternary thin film",
                "ti_at": 24,
                "zr_at": 26,
                "nb_at": 30,
                "v_at": 20,
                "target_hardness_gpa": 13.8,
            },
        ),
        now=now,
    )

    execution = service.dispatch_due_schedules(now=now)[0][0]
    FeedbackService(session).submit_feedback(
        execution.id,
        FeedbackCreate(
            outcome="success",
            quality_score=0.94,
            latency_ms=2200,
            observation={
                "deposition_sample_id": "TZNV-0320-C08-01",
                "sputter_run_id": "SPT-20260320-08",
                "xrd_phase": "bcc",
                "hardness_gpa": 14.1,
                "measured_composition_at_percent": {
                    "ti_at": 24.3,
                    "zr_at": 25.8,
                    "nb_at": 29.9,
                    "v_at": 20.0,
                },
            },
        ),
        now=now + timedelta(minutes=5),
    )

    session.refresh(schedule)
    session.refresh(execution)
    observation = schedule_to_dict(execution.feedback_observation)
    executions = SchedulerService(session).list_executions(schedule_id=schedule.id, limit=5)
    next_execution = executions[0]
    next_payload = schedule_to_dict(next_execution.dispatch_payload)

    assert schedule.awaiting_feedback is True
    assert observation["closed_loop_ready"] is True
    assert observation["workflow_progress"]["is_complete"] is True
    assert observation["workflow_progress"]["completed_count"] == 4
    assert observation["auto_dispatch"]["triggered"] is True
    assert observation["auto_dispatch"]["next_execution_id"] == next_execution.id
    assert next_execution.status == "awaiting_feedback"
    assert next_payload["experiment_round_index"] == 2
    assert next_payload["mat_model_candidate_rank"] >= 1


def test_tizrnbv_success_feedback_requires_all_steps(session):
    service = SchedulerService(session)
    now = utcnow()
    schedule = service.create_schedule(
        ScheduleCreate(
            name="tizrnbv-incomplete-success",
            target="materials/sputter-cluster-09",
            base_interval_seconds=1800,
            min_interval_seconds=300,
            max_interval_seconds=7200,
            payload={
                "material_system": "Ti-Zr-Nb-V quaternary thin film",
                "ti_at": 25,
                "zr_at": 25,
                "nb_at": 30,
                "v_at": 20,
            },
        ),
        now=now,
    )

    execution = service.dispatch_due_schedules(now=now)[0][0]
    with pytest.raises(Exception, match="success 反馈必须补齐全部实验步骤"):
        FeedbackService(session).submit_feedback(
            execution.id,
            FeedbackCreate(
                outcome="success",
                quality_score=0.9,
                latency_ms=1500,
                observation={
                    "deposition_sample_id": "TZNV-0320-C09-01",
                    "xrd_phase": "bcc",
                },
            ),
            now=now + timedelta(minutes=5),
        )


def test_experiment_batch_increments_for_each_round(session):
    service = SchedulerService(session)
    now = utcnow()
    service.create_schedule(
        ScheduleCreate(
            name="tizrnbv-batches",
            target="materials/sputter-cluster-10",
            base_interval_seconds=1800,
            min_interval_seconds=300,
            max_interval_seconds=7200,
            payload={
                "material_system": "Ti-Zr-Nb-V quaternary thin film",
                "sample_batch": "TZNV-0320-D",
                "ti_at": 25,
                "zr_at": 25,
                "nb_at": 30,
                "v_at": 20,
            },
        ),
        now=now,
    )

    first_execution = service.dispatch_due_schedules(now=now)[0][0]
    first_payload = schedule_to_dict(first_execution.dispatch_payload)
    assert first_payload["experiment_batch_id"] == "TZNV-0320-D-R01"

    FeedbackService(session).submit_feedback(
        first_execution.id,
        FeedbackCreate(
            outcome="partial",
            quality_score=0.6,
            latency_ms=1000,
            observation={"deposition_sample_id": "TZNV-0320-D-R01"},
        ),
        now=now + timedelta(minutes=5),
    )

    second_execution = service.manual_trigger(
        first_execution.schedule_id, now=now + timedelta(minutes=6)
    )
    second_payload = schedule_to_dict(second_execution.dispatch_payload)
    assert second_payload["experiment_round_index"] == 2
    assert second_payload["experiment_batch_id"] == "TZNV-0320-D-R02"


def test_manual_trigger_pauses_other_active_schedules(session):
    service = SchedulerService(session)
    now = utcnow()
    first = service.create_schedule(
        ScheduleCreate(
            name="exclusive-first",
            target="materials/first",
            base_interval_seconds=60,
            min_interval_seconds=10,
            max_interval_seconds=300,
        ),
        now=now,
    )
    second = service.create_schedule(
        ScheduleCreate(
            name="exclusive-second",
            target="materials/second",
            base_interval_seconds=60,
            min_interval_seconds=10,
            max_interval_seconds=300,
        ),
        now=now,
    )

    execution = service.manual_trigger(second.id, now=now + timedelta(seconds=1))

    session.refresh(first)
    session.refresh(second)

    assert execution.schedule_id == second.id
    assert second.status == ScheduleStatus.ACTIVE.value
    assert first.status == ScheduleStatus.PAUSED.value


def test_dispatch_due_schedules_runs_only_one_active_schedule(session):
    service = SchedulerService(session)
    now = utcnow()
    first = service.create_schedule(
        ScheduleCreate(
            name="due-first",
            target="materials/due-first",
            base_interval_seconds=60,
            min_interval_seconds=10,
            max_interval_seconds=300,
            priority=5,
        ),
        now=now,
    )
    second = service.create_schedule(
        ScheduleCreate(
            name="due-second",
            target="materials/due-second",
            base_interval_seconds=60,
            min_interval_seconds=10,
            max_interval_seconds=300,
            priority=9,
        ),
        now=now,
    )

    executions, timed_out = service.dispatch_due_schedules(now=now)

    session.refresh(first)
    session.refresh(second)

    assert timed_out == 0
    assert len(executions) == 1
    assert executions[0].schedule_id == second.id
    assert second.status == ScheduleStatus.ACTIVE.value
    assert first.status == ScheduleStatus.PAUSED.value


def test_instrument_upload_service_auto_completes_round_when_all_data_ready(session):
    service = SchedulerService(session)
    now = utcnow()
    schedule = service.create_schedule(
        ScheduleCreate(
            name="tizrnbv-upload",
            target="materials/sputter-cluster-02",
            base_interval_seconds=1800,
            min_interval_seconds=300,
            max_interval_seconds=7200,
            payload={
                "material_system": "Ti-Zr-Nb-V quaternary thin film",
                "sample_batch": "TZNV-ACQ-01",
                "deposition_chamber": "Sputter-Cluster-02",
                "xrd_component_id": "xrd-lab-02",
                "eds_component_id": "eds-lab-02",
                "nanoindentation_component_id": "nanoindentation-lab-02",
                "ti_at": 22,
                "zr_at": 28,
                "nb_at": 30,
                "v_at": 20,
                "target_hardness_gpa": 14.0,
            },
        ),
        now=now,
    )

    execution = service.dispatch_due_schedules(now=now)[0][0]
    dispatch_payload = schedule_to_dict(execution.dispatch_payload)
    batch_id = dispatch_payload["experiment_batch_id"]
    upload_service = InstrumentUploadService(session)

    first = upload_service.ingest_upload(
        InstrumentUploadCreate(
            instrument_code="xrd",
            component_id="xrd-lab-02",
            experiment_batch_id=batch_id,
            observation={"xrd_phase": "bcc", "xrd_note": "single-phase"},
        ),
        now=now + timedelta(minutes=1),
    )
    second = upload_service.ingest_upload(
        InstrumentUploadCreate(
            instrument_code="eds",
            component_id="eds-lab-02",
            experiment_batch_id=batch_id,
            observation={
                "eds_ti_at": 22.1,
                "eds_zr_at": 27.8,
                "eds_nb_at": 30.0,
                "eds_v_at": 20.1,
            },
        ),
        now=now + timedelta(minutes=2),
    )
    third = upload_service.ingest_upload(
        InstrumentUploadCreate(
            instrument_code="nanoindentation",
            component_id="nanoindentation-lab-02",
            experiment_batch_id=batch_id,
            observation={"hardness_gpa": 14.3},
        ),
        now=now + timedelta(minutes=3),
    )

    assert first.round_completed is False
    assert second.round_completed is False
    assert third.round_completed is True

    session.refresh(schedule)
    session.refresh(execution)
    observation = schedule_to_dict(execution.feedback_observation)
    executions = SchedulerService(session).list_executions(schedule_id=execution.schedule_id, limit=5)
    next_execution = executions[0]
    next_payload = schedule_to_dict(next_execution.dispatch_payload)

    assert execution.status == "success"
    assert schedule.awaiting_feedback is True
    assert observation["hardness_gpa"] == 14.3
    assert observation["xrd_phase"] == "bcc"
    assert observation["hardness_gpa"] == 14.3
    assert observation["workflow_progress"]["is_complete"] is True
    assert observation["auto_dispatch"]["triggered"] is True
    assert next_execution.status == "awaiting_feedback"
    assert next_payload["experiment_round_index"] == 2
    assert next_payload["experiment_batch_id"].endswith("-R02")
    assert next_payload["mat_model_candidate_rank"] >= 1


def test_xrd_upload_can_parse_raw_xy_files(session, tmp_path):
    def synthetic_trace(
        x_values: np.ndarray,
        peaks: list[tuple[float, float, float]],
        *,
        baseline: float,
    ) -> np.ndarray:
        y_values = baseline + 0.002 * (x_values - float(x_values.min()))
        for amplitude, center, sigma in peaks:
            y_values = y_values + amplitude * np.exp(-((x_values - center) ** 2) / (2.0 * sigma**2))
        return y_values

    def write_xy_file(path, x_values: np.ndarray, y_values: np.ndarray) -> None:
        lines = ['Id: "" Comment: "synthetic test"']
        lines.extend(
            f"{x_value:.6f} {y_value:.9f}" for x_value, y_value in zip(x_values, y_values)
        )
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    service = SchedulerService(session)
    now = utcnow()
    service.create_schedule(
        ScheduleCreate(
            name="tizrnbv-xrd-raw-upload",
            target="materials/sputter-cluster-06",
            base_interval_seconds=1800,
            min_interval_seconds=300,
            max_interval_seconds=7200,
            payload={
                "material_system": "Ti-Zr-Nb-V quaternary thin film",
                "sample_batch": "TZNV-XRD-01",
                "deposition_chamber": "Sputter-Cluster-06",
                "xrd_component_id": "xrd-lab-06",
                "ti_at": 25,
                "zr_at": 25,
                "nb_at": 30,
                "v_at": 20,
            },
        ),
        now=now,
    )

    execution = service.dispatch_due_schedules(now=now)[0][0]
    batch_id = schedule_to_dict(execution.dispatch_payload)["experiment_batch_id"]

    low_x = np.linspace(25.0, 43.0, 1200)
    high_x = np.linspace(45.0, 75.0, 1600)
    low_file = tmp_path / "40-006-0000_exported.xy"
    high_file = tmp_path / "60-006-0000_exported.xy"
    write_xy_file(low_file, low_x, synthetic_trace(low_x, [(8.0, 39.0, 0.18)], baseline=0.12))
    write_xy_file(
        high_file,
        high_x,
        synthetic_trace(high_x, [(6.0, 57.0, 0.2), (5.0, 70.0, 0.22)], baseline=0.1),
    )

    result = InstrumentUploadService(session).ingest_upload(
        InstrumentUploadCreate(
            instrument_code="xrd",
            component_id="xrd-lab-06",
            experiment_batch_id=batch_id,
            observation={
                "xrd_low_angle_file": str(low_file),
                "xrd_high_angle_file": str(high_file),
            },
        ),
        now=now + timedelta(minutes=1),
    )

    assert result.round_completed is False
    assert result.observation["xrd_phase"] == "bcc"
    assert "xrd_fit_summary" in result.observation
    assert result.observation["xrd_peak_positions"]["bcc_110"] > 38.5


def test_xrd_upload_can_read_raw_files_from_directory(session, tmp_path):
    def synthetic_trace(
        x_values: np.ndarray,
        peaks: list[tuple[float, float, float]],
        *,
        baseline: float,
    ) -> np.ndarray:
        y_values = baseline + 0.002 * (x_values - float(x_values.min()))
        for amplitude, center, sigma in peaks:
            y_values = y_values + amplitude * np.exp(-((x_values - center) ** 2) / (2.0 * sigma**2))
        return y_values

    def write_xy_file(path, x_values: np.ndarray, y_values: np.ndarray) -> None:
        lines = ['Id: "" Comment: "synthetic test"']
        lines.extend(
            f"{x_value:.6f} {y_value:.9f}" for x_value, y_value in zip(x_values, y_values)
        )
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    service = SchedulerService(session)
    now = utcnow()
    service.create_schedule(
        ScheduleCreate(
            name="tizrnbv-xrd-dir-upload",
            target="materials/sputter-cluster-07",
            base_interval_seconds=1800,
            min_interval_seconds=300,
            max_interval_seconds=7200,
            payload={
                "material_system": "Ti-Zr-Nb-V quaternary thin film",
                "sample_batch": "TZNV-XRD-02",
                "deposition_chamber": "Sputter-Cluster-07",
                "xrd_component_id": "xrd-lab-07",
                "ti_at": 25,
                "zr_at": 25,
                "nb_at": 30,
                "v_at": 20,
            },
        ),
        now=now,
    )

    execution = service.dispatch_due_schedules(now=now)[0][0]
    batch_id = schedule_to_dict(execution.dispatch_payload)["experiment_batch_id"]

    low_x = np.linspace(25.0, 43.0, 1200)
    high_x = np.linspace(45.0, 75.0, 1600)
    write_xy_file(
        tmp_path / "40-007-0000_exported.xy",
        low_x,
        synthetic_trace(low_x, [(8.0, 39.0, 0.18)], baseline=0.12),
    )
    write_xy_file(
        tmp_path / "60-007-0000_exported.xy",
        high_x,
        synthetic_trace(high_x, [(6.0, 57.0, 0.2), (5.0, 70.0, 0.22)], baseline=0.1),
    )

    result = InstrumentUploadService(session, xrd_data_dir=tmp_path).ingest_upload(
        InstrumentUploadCreate(
            instrument_code="xrd",
            component_id="xrd-lab-07",
            experiment_batch_id=batch_id,
            observation={
                "xrd_sample_id": "7",
            },
        ),
        now=now + timedelta(minutes=1),
    )

    assert result.round_completed is False
    assert result.observation["xrd_phase"] == "bcc"
    assert result.observation["xrd_sample_id"] == "7"
    assert result.observation["xrd_data_dir"] == str(tmp_path.resolve())
    assert result.observation["xrd_low_angle_file"].endswith("40-007-0000_exported.xy")
    assert result.observation["xrd_high_angle_file"].endswith("60-007-0000_exported.xy")


def test_instrument_upload_api_auto_completes_round(database):
    app = create_app(
        Settings(
            database_url=f"sqlite:///{(database.engine.url.database)}",
            scheduler_enabled=False,
            auto_create_schema=False,
        )
    )
    with TestClient(app) as client:
        created = client.post(
            "/api/v1/schedules",
            json={
                "name": "tick-upload",
                "target": "materials/sputter-cluster-05",
                "base_interval_seconds": 1800,
                "min_interval_seconds": 300,
                "max_interval_seconds": 7200,
                "payload": {
                    "material_system": "Ti-Zr-Nb-V quaternary thin film",
                    "sample_batch": "TZNV-TICK-01",
                    "deposition_chamber": "Sputter-Cluster-05",
                    "xrd_component_id": "xrd-lab-05",
                    "eds_component_id": "eds-lab-05",
                    "nanoindentation_component_id": "nanoindentation-lab-05",
                    "ti_at": 25,
                    "zr_at": 25,
                    "nb_at": 30,
                    "v_at": 20,
                },
            },
        )
        assert created.status_code == 200

        first_tick = client.post("/api/v1/engine/tick")
        execution = first_tick.json()["executions"][0]
        batch_id = execution["dispatch_payload"]["experiment_batch_id"]
        uploads = [
            {
                "instrument_code": "xrd",
                "component_id": "xrd-lab-05",
                "experiment_batch_id": batch_id,
                "observation": {"xrd_phase": "bcc"},
            },
            {
                "instrument_code": "eds",
                "component_id": "eds-lab-05",
                "experiment_batch_id": batch_id,
                "observation": {
                    "eds_ti_at": 25,
                    "eds_zr_at": 25,
                    "eds_nb_at": 30,
                    "eds_v_at": 20,
                },
            },
            {
                "instrument_code": "nanoindentation",
                "component_id": "nanoindentation-lab-05",
                "experiment_batch_id": batch_id,
                "observation": {"hardness_gpa": 13.7},
            },
        ]
        final_response = None
        for index, payload in enumerate(uploads):
            response = client.post("/api/v1/instrument-uploads", json=payload)
            assert response.status_code == 200
            if index < 2:
                assert response.json()["round_completed"] is False
            final_response = response

        assert final_response is not None
        assert final_response.json()["round_completed"] is True

        executions = client.get("/api/v1/executions?limit=5")
        payload = executions.json()
        latest = next(item for item in payload if item["status"] == "awaiting_feedback")
        closed = next(item for item in payload if item["status"] == "success")
        observation = closed["feedback_observation"]
        assert observation["xrd_phase"] == "bcc"
        assert observation["hardness_gpa"] == 13.7
        assert observation["auto_dispatch"]["triggered"] is True
        assert latest["status"] == "awaiting_feedback"
        assert latest["dispatch_payload"]["experiment_round_index"] == 2
        assert closed["status"] == "success"


def test_edge_context_and_data_package_api(database):
    app = create_app(
        Settings(
            database_url=f"sqlite:///{(database.engine.url.database)}",
            scheduler_enabled=False,
            auto_create_schema=False,
        )
    )
    with TestClient(app) as client:
        created = client.post(
            "/api/v1/schedules",
            json={
                "name": "data-package-loop",
                "target": "materials/sputter-cluster-13",
                "base_interval_seconds": 1800,
                "min_interval_seconds": 300,
                "max_interval_seconds": 7200,
                "payload": {
                    "sample_batch": "TZNV-DPK-01",
                    "deposition_chamber": "Sputter-Cluster-13",
                    "xrd_component_id": "xrd-lab-13",
                    "ti_at": 25,
                    "zr_at": 25,
                    "nb_at": 30,
                    "v_at": 20,
                },
            },
        )
        assert created.status_code == 200

        tick = client.post("/api/v1/engine/tick")
        execution = tick.json()["executions"][0]
        run_id = execution["id"]

        context_response = client.get(
            f"/api/v1/executions/{run_id}/edge-context?instrument_code=xrd"
        )
        assert context_response.status_code == 200
        context = context_response.json()[0]
        sample_plan_response = client.get(f"/api/v1/executions/{run_id}/sample-plan")
        assert sample_plan_response.status_code == 200
        sample_plan = sample_plan_response.json()
        assert context["run_id"] == run_id
        assert context["task_id"] == execution["schedule_id"]
        assert context["component_id"] == "xrd-lab-13"
        assert context["sample_id"] == sample_plan["round_sample_id"]

        package_response = client.post(
            "/api/v1/closed-loop/data-packages",
            json={
                "run_id": run_id,
                "task_id": execution["schedule_id"],
                "campaign_id": context["campaign_id"],
                "experiment_batch_id": context["experiment_batch_id"],
                "instrument_code": "xrd",
                "instrument_id": context["instrument_id"],
                "component_id": context["component_id"],
                "sample_id": context["sample_id"],
                "template_id": "xrd-template-001",
                "validation_status": "validated",
                "raw_files": {"pattern_file": "/tmp/fake.xy"},
                "metadata": {"experiment_name": "demo-xrd"},
                "result_data": {"xrd_phase": "bcc"},
                "scheduler_observation": {"xrd_phase": "bcc"},
                "standard_data_ref": {
                    "system": "manage_db",
                    "base_url": "http://127.0.0.1:8000",
                    "record_id": "devdata-001",
                    "resource_path": "/api/development-data/devdata-001",
                    "record": {"raw": "should-not-be-stored"},
                },
            },
        )
        assert package_response.status_code == 200
        body = package_response.json()
        assert body["data_package"]["instrument_code"] == "xrd"
        assert body["data_package"]["validation_status"] == "validated"
        assert body["data_package"]["raw_files"] == {}
        assert body["data_package"]["metadata"]["mode"] == "reference_only"
        assert "experiment_name" not in body["data_package"]["metadata"]
        assert body["data_package"]["result_data"]["xrd_phase"] == "bcc"
        assert body["data_package"]["standard_data_ref"]["record_id"] == "devdata-001"
        assert "record" not in body["data_package"]["standard_data_ref"]
        assert body["round_completed"] is False
        assert body["execution"]["feedback_observation"]["xrd_phase"] == "bcc"

        run_response = client.get(f"/api/v1/runs/{run_id}")
        assert run_response.status_code == 200
        run_body = run_response.json()
        assert run_body["campaign"]["id"] == context["campaign_id"]
        assert len(run_body["data_packages"]) == 1
        event_types = [item["event_type"] for item in run_body["events"]]
        assert "data.uploaded" in event_types
        assert "data.validated" in event_types
        assert "instrument.uploaded" in event_types


def test_data_package_ingest_reads_live_manage_db_payload(database, monkeypatch):
    app = create_app(
        Settings(
            database_url=f"sqlite:///{(database.engine.url.database)}",
            scheduler_enabled=False,
            auto_create_schema=False,
        )
    )

    def fake_fetch(self, *, record_id, standard_ref):
        assert record_id == "devdata-live-001"
        assert standard_ref["record_id"] == "devdata-live-001"
        return ManageDbRecord(
            record_id="devdata-live-001",
            payload={
                "xrd_phase": "bcc",
                "template_name": "live-xrd-template",
                "review_status": "approved",
            },
            source_url="http://manage-db.local/api/development-data/devdata-live-001",
            base_url="http://manage-db.local",
        )

    monkeypatch.setattr(ClosedLoopService, "_fetch_live_manage_db_record", fake_fetch)

    with TestClient(app) as client:
        created = client.post(
            "/api/v1/schedules",
            json={
                "name": "data-package-live-loop",
                "target": "materials/sputter-cluster-14",
                "base_interval_seconds": 1800,
                "min_interval_seconds": 300,
                "max_interval_seconds": 7200,
                "payload": {
                    "sample_batch": "TZNV-DPK-LIVE-01",
                    "deposition_chamber": "Sputter-Cluster-14",
                    "xrd_component_id": "xrd-lab-14",
                    "ti_at": 25,
                    "zr_at": 25,
                    "nb_at": 30,
                    "v_at": 20,
                },
            },
        )
        assert created.status_code == 200

        tick = client.post("/api/v1/engine/tick")
        execution = tick.json()["executions"][0]
        run_id = execution["id"]

        context_response = client.get(
            f"/api/v1/executions/{run_id}/edge-context?instrument_code=xrd"
        )
        context = context_response.json()[0]

        package_response = client.post(
            "/api/v1/closed-loop/data-packages",
            json={
                "run_id": run_id,
                "task_id": execution["schedule_id"],
                "campaign_id": context["campaign_id"],
                "experiment_batch_id": context["experiment_batch_id"],
                "instrument_code": "xrd",
                "instrument_id": context["instrument_id"],
                "component_id": context["component_id"],
                "sample_id": context["sample_id"],
                "template_id": "xrd-template-live-001",
                "validation_status": "validated",
                "manage_db_record_id": "devdata-live-001",
                "standard_data_ref": {
                    "system": "manage_db",
                    "base_url": "http://manage-db.local",
                    "record_id": "devdata-live-001",
                },
            },
        )
        assert package_response.status_code == 200
        body = package_response.json()
        assert body["data_package"]["metadata"]["mode"] == "manage_db_live"
        assert body["data_package"]["metadata"]["template_name"] == "live-xrd-template"
        assert body["data_package"]["result_data"]["xrd_phase"] == "bcc"
        assert body["data_package"]["standard_data_ref"]["lookup_url"].endswith(
            "/api/development-data/devdata-live-001"
        )
        assert body["data_package"]["scheduler_observation"]["xrd_phase"] == "bcc"
        assert body["execution"]["feedback_observation"]["xrd_phase"] == "bcc"


def test_build_run_view_refreshes_manage_db_snapshot(session, monkeypatch):
    service = SchedulerService(session)
    now = utcnow()
    schedule = service.create_schedule(
        ScheduleCreate(
            name="refresh-live-run",
            target="materials/sputter-cluster-15",
            base_interval_seconds=1800,
            min_interval_seconds=300,
            max_interval_seconds=7200,
            payload={
                "sample_batch": "TZNV-DPK-LIVE-02",
                "deposition_chamber": "Sputter-Cluster-15",
                "xrd_component_id": "xrd-lab-15",
                "ti_at": 25,
                "zr_at": 25,
                "nb_at": 30,
                "v_at": 20,
            },
        ),
        now=now,
    )
    execution = service.dispatch_due_schedules(now=now)[0][0]
    batch_id = schedule_to_dict(execution.dispatch_payload)["experiment_batch_id"]

    ingest_service = ClosedLoopService(session)

    def stale_fetch(self, *, record_id, standard_ref):
        return ManageDbRecord(
            record_id=str(record_id),
            payload={"xrd_phase": "bcc"},
            source_url="http://manage-db.local/api/development-data/devdata-live-002",
            base_url="http://manage-db.local",
        )

    monkeypatch.setattr(ClosedLoopService, "_fetch_live_manage_db_record", stale_fetch)
    ingest_service.ingest_data_package(
        DataPackageCreate(
            run_id=execution.id,
            task_id=execution.schedule_id,
            experiment_batch_id=batch_id,
            instrument_code="xrd",
            instrument_id="xrd-lab-15",
            component_id="xrd-lab-15",
            template_id="xrd-template-live-002",
            validation_status="validated",
            manage_db_record_id="devdata-live-002",
            standard_data_ref={
                "system": "manage_db",
                "base_url": "http://manage-db.local",
                "record_id": "devdata-live-002",
            },
        ),
        now=now + timedelta(minutes=1),
    )

    def refreshed_fetch(self, *, record_id, standard_ref):
        return ManageDbRecord(
            record_id=str(record_id),
            payload={"xrd_phase": "hcp", "review_status": "approved"},
            source_url="http://manage-db.local/api/development-data/devdata-live-002",
            base_url="http://manage-db.local",
        )

    monkeypatch.setattr(ClosedLoopService, "_fetch_live_manage_db_record", refreshed_fetch)
    _, _, data_packages, _, _ = ClosedLoopService(session).build_run_view(execution.id)
    assert data_packages[0].result_data_json == '{"review_status":"approved","xrd_phase":"hcp"}'
    assert data_packages[0].metadata_json.find('"mode":"manage_db_live"') != -1


def test_online_refresh_rebuilds_mat_model_candidates_after_closed_loop_success(session, monkeypatch):
    settings = Settings(
        database_url=str(session.get_bind().url),
        scheduler_enabled=False,
        auto_create_schema=False,
        mat_model_online_refresh_enabled=True,
    )
    monkeypatch.setattr("app.services.mat_model_service.get_settings", lambda: settings)

    mat_model = MatModelService(settings)
    base_artifacts = mat_model.base_artifacts()

    service = SchedulerService(session)
    now = utcnow()
    schedule = service.create_schedule(
        ScheduleCreate(
            name="tizrnbv-online-refresh",
            target="materials/sputter-cluster-16",
            base_interval_seconds=1800,
            min_interval_seconds=300,
            max_interval_seconds=7200,
            payload={
                "material_system": "Ti-Zr-Nb-V quaternary thin film",
                "sample_batch": "TZNV-ONLINE-01",
                "ti_at": 25,
                "zr_at": 25,
                "nb_at": 30,
                "v_at": 20,
                "target_hardness_gpa": 14.0,
            },
        ),
        now=now,
    )

    execution = service.dispatch_due_schedules(now=now)[0][0]
    FeedbackService(session).submit_feedback(
        execution.id,
        FeedbackCreate(
            outcome="success",
            quality_score=0.95,
            latency_ms=2000,
            observation={
                "deposition_sample_id": "TZNV-ONLINE-01-R01",
                "sputter_run_id": "SPT-20260327-01",
                "xrd_phase": "bcc",
                "hardness_gpa": 14.2,
                "measured_composition_at_percent": {
                    "ti_at": 24.8,
                    "zr_at": 25.1,
                    "nb_at": 30.2,
                    "v_at": 19.9,
                },
            },
        ),
        now=now + timedelta(minutes=5),
    )

    refresh_result = mat_model.refresh_candidates_from_closed_loop(session)
    refreshed_artifacts = mat_model.artifacts()
    candidate = mat_model.pick_next_candidate(
        used_payloads=[
            schedule_to_dict(schedule.payload),
            schedule_to_dict(execution.dispatch_payload),
        ],
        preferred_rank=2,
    )
    assert candidate is not None
    next_payload = mat_model.build_schedule_payload(
        base_payload=schedule_to_dict(schedule.payload),
        candidate=candidate,
    )

    assert refresh_result is not None
    assert refreshed_artifacts.output_dir != base_artifacts.output_dir
    assert refreshed_artifacts.summary["online_refresh"]["applied"] is True
    assert refreshed_artifacts.summary["online_refresh"]["closed_loop_sample_count"] == 1
    assert refreshed_artifacts.summary["n_training_rows"] == (
        base_artifacts.summary["n_training_rows"] + 1
    )
    assert next_payload["next_action"] == "run_mat_model_candidate"
    assert next_payload["mat_model_online_refresh_applied"] is True
    assert next_payload["mat_model_online_refresh_sample_count"] == 1
    assert next_payload["input_dataset_path"].endswith("online_refresh_training_data.csv")
    assert next_payload["model_version"].endswith("+online")


def test_feedback_service_calls_online_refresh_hook_when_enabled(session, monkeypatch):
    settings = Settings(
        database_url=str(session.get_bind().url),
        scheduler_enabled=False,
        auto_create_schema=False,
        mat_model_online_refresh_enabled=True,
    )
    monkeypatch.setattr("app.services.mat_model_service.get_settings", lambda: settings)

    calls = {"count": 0}

    def fake_refresh(self, current_session):
        assert current_session is session
        calls["count"] += 1
        return None

    monkeypatch.setattr(MatModelService, "refresh_candidates_from_closed_loop", fake_refresh)

    service = SchedulerService(session)
    now = utcnow()
    schedule = service.create_schedule(
        ScheduleCreate(
            name="tizrnbv-online-refresh-hook",
            target="materials/sputter-cluster-17",
            base_interval_seconds=1800,
            min_interval_seconds=300,
            max_interval_seconds=7200,
            payload={
                "sample_batch": "TZNV-ONLINE-HOOK",
                "ti_at": 25,
                "zr_at": 25,
                "nb_at": 30,
                "v_at": 20,
                "target_hardness_gpa": 14.0,
            },
        ),
        now=now,
    )

    execution = service.dispatch_due_schedules(now=now)[0][0]
    FeedbackService(session).submit_feedback(
        execution.id,
        FeedbackCreate(
            outcome="success",
            quality_score=0.95,
            latency_ms=1800,
            observation={
                "deposition_sample_id": "TZNV-ONLINE-HOOK-R01",
                "sputter_run_id": "SPT-20260327-02",
                "xrd_phase": "bcc",
                "hardness_gpa": 13.9,
                "measured_composition_at_percent": {
                    "ti_at": 24.9,
                    "zr_at": 25.0,
                    "nb_at": 30.1,
                    "v_at": 20.0,
                },
            },
        ),
        now=now + timedelta(minutes=5),
    )

    assert calls["count"] == 1


def test_manage_db_records_are_merged_into_training_rows():
    service = MatModelService()
    sample_id = "MID.CN10248.0009.S.20231017102430/0021.FARW"
    records = [
        ManageDbRecord(
            record_id="xrd-1",
            payload={
                "template_name": "高通量X射线衍射表征",
                "origin_post_data": {"关联样品MGID": [sample_id]},
                "scheduler_observation": {"xrd_phase": "bcc"},
            },
            source_url="http://manage-db/api/development-data/xrd-1",
        ),
        ManageDbRecord(
            record_id="eds-1",
            payload={
                "template_name": "高通量EDS表征",
                "origin_post_data": {"关联样品MGID": [sample_id]},
                "scheduler_observation": {
                    "measured_composition_at_percent": {
                        "ti_at": 24.8,
                        "zr_at": 25.1,
                        "nb_at": 30.2,
                        "v_at": 19.9,
                    }
                },
            },
            source_url="http://manage-db/api/development-data/eds-1",
        ),
        ManageDbRecord(
            record_id="ni-1",
            payload={
                "template_name": "高通量纳米压痕表征",
                "origin_post_data": {"关联样品MGID": [sample_id]},
                "scheduler_observation": {"hardness_gpa": 13.85},
            },
            source_url="http://manage-db/api/development-data/ni-1",
        ),
    ]

    frame, summary = service._manage_db_training_frame_from_records(records, filters={})

    assert summary["parsed_record_count"] == 3
    assert summary["training_row_count"] == 1
    row = frame.iloc[0].to_dict()
    assert row["sample_id"] == sample_id
    assert row["phase"] == "bcc"
    assert row["source"] == "manage_db_sync"
    assert row["hardness_gpa"] == pytest.approx(13.85)
    assert row["Ti"] == pytest.approx(24.8)
    assert row["Zr"] == pytest.approx(25.1)
    assert row["Nb"] == pytest.approx(30.2)
    assert row["V"] == pytest.approx(19.9)


def test_manage_db_raw_parser_extracts_structured_batch_data():
    base = Path(__file__).resolve().parents[3] / "remote_trans" / "data"

    class LocalClient:
        def download_bytes(self, resource, *, base_url=None, retry_on_unauthorized=True):
            return Path(resource).read_bytes()

    parser = ManageDbRawParser(client=LocalClient())
    records = [
        (
            "eds",
            ManageDbRecord(
                record_id="eds-batch",
                payload={
                    "原始文件": {
                        "原始数据文件": str(base / "EDS" / "eds_test" / "Ti-Zr-Nb-V成分数据.xlsx")
                    }
                },
                source_url="local://eds",
            ),
        ),
        (
            "xrd",
            ManageDbRecord(
                record_id="xrd-batch",
                payload={
                    "原始文件": {
                        "原始数据文件": str(base / "XRD" / "xrd-test" / "Ti-Zr-Nb-V结构数据.xlsx")
                    }
                },
                source_url="local://xrd",
            ),
        ),
        (
            "nanoindentation",
            ManageDbRecord(
                record_id="nano-batch",
                payload={
                    "原始文件": {
                        "原始数据文件": str(base / "NANO" / "纳米压痕_test" / "Ti-Zr-Nb-V纳米压痕数据.txt")
                    }
                },
                source_url="local://nano",
            ),
        ),
    ]

    parsed_eds = parser.parse_record(records[0][1], instrument_code="eds", sample_id="TZNV-TEST-R01-S01")
    parsed_xrd = parser.parse_record(records[1][1], instrument_code="xrd", sample_id="TZNV-TEST-R01-S01")
    parsed_nano = parser.parse_record(records[2][1], instrument_code="nanoindentation", sample_id="TZNV-TEST-R01-S01")

    assert parsed_eds is not None
    assert len(parsed_eds.sample_rows) == 169
    assert parsed_eds.summary_observation["measured_composition_at_percent"]["ti_at"] == pytest.approx(21.578422)

    assert parsed_xrd is not None
    assert len(parsed_xrd.sample_rows) == 169
    assert parsed_xrd.summary_observation["xrd_phase"] == "BCC"

    assert parsed_nano is not None
    assert len(parsed_nano.sample_rows) == 167
    assert parsed_nano.summary_observation["hardness_gpa"] == pytest.approx(6.13191)


def test_manage_db_batch_records_expand_into_training_rows(monkeypatch):
    base = Path(__file__).resolve().parents[3] / "remote_trans" / "data"
    batch_sample_id = "TZNV-TEST-R01-S01"
    service = MatModelService()

    def fake_download_bytes(self, resource, *, base_url=None, retry_on_unauthorized=True):
        return Path(resource).read_bytes()

    monkeypatch.setattr(ManageDbClient, "download_bytes", fake_download_bytes)

    records = [
        ManageDbRecord(
            record_id="xrd-batch",
            payload={
                "template_name": "高通量X射线衍射表征",
                "origin_post_data": {"关联样品MGID": [batch_sample_id]},
                "原始文件": {
                    "原始数据文件": str(base / "XRD" / "xrd-test" / "Ti-Zr-Nb-V结构数据.xlsx")
                },
            },
            source_url="local://xrd",
        ),
        ManageDbRecord(
            record_id="eds-batch",
            payload={
                "template_name": "EDS实验数据",
                "origin_post_data": {"关联样品MGID": [batch_sample_id]},
                "原始文件": {
                    "原始数据文件": str(base / "EDS" / "eds_test" / "Ti-Zr-Nb-V成分数据.xlsx")
                },
            },
            source_url="local://eds",
        ),
        ManageDbRecord(
            record_id="nano-batch",
            payload={
                "template_name": "高通量纳米压痕表征",
                "origin_post_data": {"关联样品MGID": [batch_sample_id]},
                "原始文件": {
                    "原始数据文件": str(base / "NANO" / "纳米压痕_test" / "Ti-Zr-Nb-V纳米压痕数据.txt")
                },
            },
            source_url="local://nano",
        ),
    ]

    frame, summary = service._manage_db_training_frame_from_records(records, filters={})

    assert summary["parsed_record_count"] == 3
    assert summary["training_row_count"] == 167

    center_row = frame.loc[frame["sample_id"] == f"{batch_sample_id}-R07C07"].iloc[0].to_dict()
    assert center_row["phase"] == "bcc"
    assert center_row["hardness_gpa"] == pytest.approx(6.13191)
    assert center_row["Ti"] == pytest.approx(21.578422)
    assert center_row["Zr"] == pytest.approx(16.283716)
    assert center_row["Nb"] == pytest.approx(29.87013)
    assert center_row["V"] == pytest.approx(32.267732)


def test_manage_db_sync_refreshes_overlay_before_inference(session, monkeypatch):
    settings = Settings(
        database_url=str(session.get_bind().url),
        scheduler_enabled=False,
        auto_create_schema=False,
        mat_model_online_refresh_enabled=False,
        mat_model_manage_db_sync_enabled=True,
        manage_db_base_url="http://127.0.0.1:8000",
        manage_db_username="codex",
        manage_db_password="secret",
    )
    sample_id = "MID.CN10248.0009.S.20231017102430/0021.FARW"
    records = [
        ManageDbRecord(
            record_id="xrd-2",
            payload={
                "template_name": "高通量X射线衍射表征",
                "origin_post_data": {"关联样品MGID": [sample_id]},
                "scheduler_observation": {"xrd_phase": "bcc"},
            },
            source_url="http://manage-db/api/development-data/xrd-2",
        ),
        ManageDbRecord(
            record_id="eds-2",
            payload={
                "template_name": "高通量EDS表征",
                "origin_post_data": {"关联样品MGID": [sample_id]},
                "scheduler_observation": {
                    "measured_composition_at_percent": {
                        "ti_at": 25.0,
                        "zr_at": 24.7,
                        "nb_at": 30.4,
                        "v_at": 19.9,
                    }
                },
            },
            source_url="http://manage-db/api/development-data/eds-2",
        ),
        ManageDbRecord(
            record_id="ni-2",
            payload={
                "template_name": "高通量纳米压痕表征",
                "origin_post_data": {"关联样品MGID": [sample_id]},
                "scheduler_observation": {"hardness_gpa": 14.05},
            },
            source_url="http://manage-db/api/development-data/ni-2",
        ),
    ]

    def fake_search(self, **kwargs):
        assert kwargs["template_type"] == "source"
        return ManageDbSearchResult(total=len(records), items=records)

    monkeypatch.setattr(
        "app.services.manage_db_client.ManageDbClient.search_development_data",
        fake_search,
    )

    service = MatModelService(settings)
    base_artifacts = service.base_artifacts()
    refresh_result = service.refresh_candidates_from_closed_loop(session)
    refreshed_artifacts = service.artifacts()

    assert refresh_result is not None
    assert refresh_result.manage_db_training_row_count == 1
    assert refresh_result.manage_db_downloaded_record_count == 3
    assert refreshed_artifacts.output_dir != base_artifacts.output_dir
    assert refreshed_artifacts.summary["online_refresh"]["closed_loop_sample_count"] == 0
    assert (
        refreshed_artifacts.summary["online_refresh"]["manage_db_sync"]["training_row_count"]
        == 1
    )
    assert (
        refreshed_artifacts.summary["online_refresh"]["manage_db_sync"][
            "downloaded_record_count"
        ]
        == 3
    )


def test_linked_manage_db_record_ids_drive_exact_refresh(session, monkeypatch):
    settings = Settings(
        database_url=str(session.get_bind().url),
        scheduler_enabled=False,
        auto_create_schema=False,
        mat_model_online_refresh_enabled=False,
        mat_model_manage_db_sync_enabled=True,
        manage_db_base_url="http://127.0.0.1:8000",
        manage_db_username="codex",
        manage_db_password="secret",
    )
    service = SchedulerService(session)
    now = utcnow()
    schedule = service.create_schedule(
        ScheduleCreate(
            name="linked-manage-db-refresh",
            target="materials/sputter-cluster-18",
            base_interval_seconds=1800,
            min_interval_seconds=300,
            max_interval_seconds=7200,
            payload={
                "sample_batch": "TZNV-LINKED-01",
                "deposition_chamber": "Sputter-Cluster-18",
                "xrd_component_id": "xrd-lab-18",
                "eds_component_id": "eds-lab-18",
                "nanoindentation_component_id": "nanoindentation-lab-18",
                "ti_at": 25,
                "zr_at": 25,
                "nb_at": 30,
                "v_at": 20,
                "target_hardness_gpa": 14.0,
            },
        ),
        now=now,
    )
    execution = service.dispatch_due_schedules(now=now)[0][0]
    dispatch_payload = schedule_to_dict(execution.dispatch_payload)
    batch_id = dispatch_payload["experiment_batch_id"]
    sample_id = dispatch_payload["sample_id"]
    components = dispatch_payload["instrument_components"]

    closed_loop = ClosedLoopService(session)
    closed_loop.ingest_data_package(
        DataPackageCreate(
            run_id=execution.id,
            task_id=execution.schedule_id,
            experiment_batch_id=batch_id,
            instrument_code="magnetron_sputtering",
            instrument_id=components["magnetron_sputtering"]["component_id"],
            component_id=components["magnetron_sputtering"]["component_id"],
            sample_id=sample_id,
            validation_status="validated",
            scheduler_observation={
                "deposition_sample_id": sample_id,
                "sputter_run_id": "SPT-20260329-01",
            },
        ),
        now=now + timedelta(minutes=1),
    )
    closed_loop.ingest_data_package(
        DataPackageCreate(
            run_id=execution.id,
            task_id=execution.schedule_id,
            experiment_batch_id=batch_id,
            instrument_code="xrd",
            instrument_id=components["xrd"]["component_id"],
            component_id=components["xrd"]["component_id"],
            sample_id=sample_id,
            validation_status="validated",
            manage_db_record_id="linked-xrd-001",
            standard_data_ref={
                "system": "manage_db",
                "base_url": "http://manage-db.local",
                "record_id": "linked-xrd-001",
            },
            scheduler_observation={"xrd_phase": "bcc"},
        ),
        now=now + timedelta(minutes=2),
    )
    closed_loop.ingest_data_package(
        DataPackageCreate(
            run_id=execution.id,
            task_id=execution.schedule_id,
            experiment_batch_id=batch_id,
            instrument_code="eds",
            instrument_id=components["eds"]["component_id"],
            component_id=components["eds"]["component_id"],
            sample_id=sample_id,
            validation_status="validated",
            manage_db_record_id="linked-eds-001",
            standard_data_ref={
                "system": "manage_db",
                "base_url": "http://manage-db.local",
                "record_id": "linked-eds-001",
            },
            scheduler_observation={
                "measured_composition_at_percent": {
                    "ti_at": 24.9,
                    "zr_at": 25.1,
                    "nb_at": 30.0,
                    "v_at": 20.0,
                }
            },
        ),
        now=now + timedelta(minutes=3),
    )
    closed_loop.ingest_data_package(
        DataPackageCreate(
            run_id=execution.id,
            task_id=execution.schedule_id,
            experiment_batch_id=batch_id,
            instrument_code="nanoindentation",
            instrument_id=components["nanoindentation"]["component_id"],
            component_id=components["nanoindentation"]["component_id"],
            sample_id=sample_id,
            validation_status="validated",
            manage_db_record_id="linked-ni-001",
            standard_data_ref={
                "system": "manage_db",
                "base_url": "http://manage-db.local",
                "record_id": "linked-ni-001",
            },
            scheduler_observation={"hardness_gpa": 14.25},
        ),
        now=now + timedelta(minutes=4),
    )

    session.refresh(execution)
    assert execution.status == "success"

    fetched_records = {
        "linked-xrd-001": ManageDbRecord(
            record_id="linked-xrd-001",
            payload={
                "template_name": "高通量X射线衍射表征",
                "origin_post_data": {"关联样品MGID": [sample_id]},
                "scheduler_observation": {"xrd_phase": "bcc"},
            },
            source_url="http://manage-db/api/development-data/linked-xrd-001",
        ),
        "linked-eds-001": ManageDbRecord(
            record_id="linked-eds-001",
            payload={
                "template_name": "高通量EDS表征",
                "origin_post_data": {"关联样品MGID": [sample_id]},
                "scheduler_observation": {
                    "measured_composition_at_percent": {
                        "ti_at": 24.9,
                        "zr_at": 25.1,
                        "nb_at": 30.0,
                        "v_at": 20.0,
                    }
                },
            },
            source_url="http://manage-db/api/development-data/linked-eds-001",
        ),
        "linked-ni-001": ManageDbRecord(
            record_id="linked-ni-001",
            payload={
                "template_name": "高通量纳米压痕表征",
                "origin_post_data": {"关联样品MGID": [sample_id]},
                "scheduler_observation": {"hardness_gpa": 14.25},
            },
            source_url="http://manage-db/api/development-data/linked-ni-001",
        ),
    }

    def fake_fetch(self, *, record_id, standard_ref):
        assert standard_ref["record_id"] == record_id
        return fetched_records[str(record_id)]

    def fake_search(self, **kwargs):
        return ManageDbSearchResult(total=0, items=[])

    monkeypatch.setattr(
        "app.services.manage_db_client.ManageDbClient.fetch_development_data",
        fake_fetch,
    )
    monkeypatch.setattr(
        "app.services.manage_db_client.ManageDbClient.search_development_data",
        fake_search,
    )

    mat_model = MatModelService(settings)
    refresh_result = mat_model.refresh_candidates_from_closed_loop(session)
    refreshed_artifacts = mat_model.artifacts()
    linked_sync = refreshed_artifacts.summary["online_refresh"]["manage_db_sync"][
        "linked_record_sync"
    ]
    search_sync = refreshed_artifacts.summary["online_refresh"]["manage_db_sync"]["search_sync"]

    assert refresh_result is not None
    assert refresh_result.manage_db_training_row_count == 1
    assert refresh_result.manage_db_downloaded_record_count == 3
    assert linked_sync["fetched_record_count"] == 3
    assert linked_sync["training_row_count"] == 1
    assert linked_sync["resolved_execution_count"] == 1
    assert search_sync["downloaded_record_count"] == 0
    assert refreshed_artifacts.summary["online_refresh"]["closed_loop_sample_count"] == 1


def schedule_to_dict(raw: str):
    return json.loads(raw)
