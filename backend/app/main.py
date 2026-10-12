from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse
from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.database import Database
from app.schemas import (
    CampaignRead,
    ClosedLoopRunRead,
    DataPackageCreate,
    DataPackageIngestRead,
    EdgeContextRead,
    ExecutionSamplePlanRead,
    ExecutionRead,
    FeedbackCreate,
    InstrumentUploadCreate,
    InstrumentUploadRead,
    RecommendationRead,
    ScheduleCreate,
    ScheduleRead,
    ScheduleUpdate,
    TickResult,
    campaign_to_read,
    closed_loop_event_to_read,
    data_package_to_read,
    data_package_ack_to_read,
    execution_to_read,
    execution_progress_to_read,
    recommendation_record_to_read,
    schedule_to_read,
)
from app.services.acquisition import InstrumentUploadService
from app.services.closed_loop import ClosedLoopService
from app.services.exceptions import ConflictError, DomainError, NotFoundError
from app.services.feedback import FeedbackService
from app.services.mat_model_service import MatModelService
from app.services.model_inference import ModelInferenceService
from app.services.recommendations import RecommendationService
from app.services.scheduler import BackgroundSchedulerLoop, SchedulerService
from app.services.state_projection import StateProjectionService


PROJECT_DIR = Path(__file__).resolve().parents[2]
UI_DIR = PROJECT_DIR / "ui"
UI_DIST_DIR = UI_DIR / "dist"
UI_ASSETS_DIR = UI_DIST_DIR / "assets"
UI_INDEX_FILE = UI_DIST_DIR / "index.html"


def _execution_reads_with_model_state(
    session: Session,
    executions: list,
) -> list[ExecutionRead]:
    from app.workflows.ownership import is_workflow_execution
    from app.workflows.projection import workflow_runtime_read

    execution_ids = [item.id for item in executions]
    model_map = ModelInferenceService(session).latest_for_source_execution_ids(execution_ids)
    projection = StateProjectionService(session)
    feedback_snapshot_map = projection.latest_feedback_snapshots_for_execution_ids(
        execution_ids
    )
    instrument_state_map = projection.instrument_states_for_execution_ids(execution_ids)
    return [
        execution_to_read(
            item,
            feedback_snapshot=feedback_snapshot_map.get(item.id),
            instrument_states=instrument_state_map.get(item.id, []),
            model_inference=model_map.get(item.id),
            workflow_runtime=workflow_runtime_read(session, item.id) if is_workflow_execution(session, item.id) else None,
        )
        for item in executions
    ]


def _ui_build_hint_html() -> str:
    ui_dir = UI_DIR.as_posix()
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Smart Scheduler UI</title>
    <style>
      body {{
        margin: 0;
        min-height: 100vh;
        display: grid;
        place-items: center;
        background: #f4efe7;
        color: #201814;
        font-family: "Avenir Next", "Helvetica Neue", sans-serif;
      }}
      main {{
        width: min(720px, calc(100vw - 32px));
        padding: 32px;
        border-radius: 24px;
        background: #fffaf4;
        box-shadow: 0 24px 60px rgba(70, 50, 32, 0.12);
      }}
      code, pre {{
        font-family: "SFMono-Regular", Menlo, Consolas, monospace;
      }}
      pre {{
        padding: 12px 14px;
        border-radius: 14px;
        background: #f1e7d7;
        overflow-x: auto;
      }}
    </style>
  </head>
  <body>
    <main>
      <h1>Smart Scheduler UI</h1>
      <p>前端构建产物尚未生成。调度后端已经可用，但需要先在 <code>{ui_dir}</code> 里启动或构建 Vue UI。</p>
      <p>开发模式：</p>
      <pre>cd {ui_dir}
npm install
npm run dev</pre>
      <p>构建后由 FastAPI 直接托管：</p>
      <pre>cd {ui_dir}
npm run build</pre>
    </main>
  </body>
</html>
"""


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        database = Database(resolved_settings.database_url)
        if resolved_settings.auto_create_schema:
            database.create_schema()
        with database.session() as session:
            ModelInferenceService(session).backfill_from_history()
            StateProjectionService(session).backfill_from_history()
            session.commit()

        app.state.database = database
        app.state.settings = resolved_settings
        worker = None
        worker_task = None

        if resolved_settings.scheduler_enabled and resolved_settings.scheduler_mode == "inline":
            worker = BackgroundSchedulerLoop(
                database.session_factory,
                resolved_settings.scheduler_poll_interval_seconds,
            )
            worker_task = asyncio.create_task(worker.run())
            app.state.worker = worker
            app.state.worker_task = worker_task

        try:
            yield
        finally:
            if worker is not None and worker_task is not None:
                worker.stop()
                await worker_task
            database.dispose()

    app = FastAPI(
        title="Smart Cheduler",
        version="0.1.0",
        description="Closed-loop feedback scheduling service",
        lifespan=lifespan,
    )
    @app.get("/assets/{asset_path:path}", include_in_schema=False, response_model=None)
    def ui_asset(asset_path: str):
        asset_file = (UI_ASSETS_DIR / asset_path).resolve()
        if not asset_file.is_file() or UI_ASSETS_DIR.resolve() not in asset_file.parents:
            raise HTTPException(status_code=404, detail="UI asset not found")
        return FileResponse(asset_file)

    @app.get("/", include_in_schema=False, response_model=None)
    def index():
        if UI_INDEX_FILE.exists():
            return FileResponse(UI_INDEX_FILE)
        return HTMLResponse(_ui_build_hint_html())

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/v1/schedules", response_model=ScheduleRead)
    def create_schedule(
        payload: ScheduleCreate, session: Session = Depends(get_session)
    ) -> ScheduleRead:
        schedule = SchedulerService(session).create_schedule(payload)
        return schedule_to_read(schedule)

    @app.get("/api/v1/schedules", response_model=list[ScheduleRead])
    def list_schedules(session: Session = Depends(get_session)) -> list[ScheduleRead]:
        schedules = SchedulerService(session).list_schedules()
        return [schedule_to_read(item) for item in schedules]

    @app.get("/api/v1/schedules/{schedule_id}", response_model=ScheduleRead)
    def get_schedule(schedule_id: str, session: Session = Depends(get_session)) -> ScheduleRead:
        try:
            schedule = SchedulerService(session).get_schedule(schedule_id)
        except DomainError as exc:
            raise to_http_error(exc) from exc
        return schedule_to_read(schedule)

    @app.post("/api/v1/schedules/{schedule_id}/pause", response_model=ScheduleRead)
    def pause_schedule(
        schedule_id: str, session: Session = Depends(get_session)
    ) -> ScheduleRead:
        try:
            schedule = SchedulerService(session, workflow_topic_prefix=resolved_settings.kafka.topic_prefix).pause_schedule(schedule_id)
        except DomainError as exc:
            raise to_http_error(exc) from exc
        return schedule_to_read(schedule)

    @app.post("/api/v1/schedules/{schedule_id}/resume", response_model=ScheduleRead)
    def resume_schedule(
        schedule_id: str, session: Session = Depends(get_session)
    ) -> ScheduleRead:
        try:
            schedule = SchedulerService(session, workflow_topic_prefix=resolved_settings.kafka.topic_prefix).resume_schedule(schedule_id)
        except DomainError as exc:
            raise to_http_error(exc) from exc
        return schedule_to_read(schedule)

    @app.patch("/api/v1/schedules/{schedule_id}", response_model=ScheduleRead)
    def update_schedule(
        schedule_id: str,
        payload: ScheduleUpdate,
        session: Session = Depends(get_session),
    ) -> ScheduleRead:
        try:
            schedule = SchedulerService(session).update_schedule(schedule_id, payload)
        except DomainError as exc:
            raise to_http_error(exc) from exc
        return schedule_to_read(schedule)

    @app.post("/api/v1/schedules/{schedule_id}/trigger", response_model=ExecutionRead)
    def trigger_schedule(
        schedule_id: str, session: Session = Depends(get_session)
    ) -> ExecutionRead:
        try:
            execution = SchedulerService(session, workflow_topic_prefix=resolved_settings.kafka.topic_prefix).manual_trigger(schedule_id)
        except DomainError as exc:
            raise to_http_error(exc) from exc
        return _execution_reads_with_model_state(session, [execution])[0]

    @app.get("/api/v1/executions", response_model=list[ExecutionRead])
    def list_executions(
        schedule_id: str | None = None,
        status: str | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=500),
        session: Session = Depends(get_session),
    ) -> list[ExecutionRead]:
        closed_loop_service = ClosedLoopService(session)
        executions = SchedulerService(session).list_executions(
            schedule_id=schedule_id, status=status, limit=limit
        )
        closed_loop_service.reconcile_execution_feedbacks(executions)
        closed_loop_service.refresh_executions_from_manage_db(executions)
        projection = StateProjectionService(session)
        for execution in executions:
            projection.sync_execution(execution)
        session.commit()
        return _execution_reads_with_model_state(session, executions)

    @app.get("/api/v1/executions/{execution_id}/edge-context", response_model=list[EdgeContextRead])
    def get_edge_context(
        execution_id: str,
        instrument_code: str | None = Query(default=None),
        session: Session = Depends(get_session),
    ) -> list[EdgeContextRead]:
        try:
            return ClosedLoopService(session).build_edge_context(
                execution_id,
                instrument_code=instrument_code,
            )
        except DomainError as exc:
            raise to_http_error(exc) from exc

    @app.get(
        "/api/v1/executions/{execution_id}/sample-plan",
        response_model=ExecutionSamplePlanRead,
    )
    def get_execution_sample_plan(
        execution_id: str,
        session: Session = Depends(get_session),
    ) -> ExecutionSamplePlanRead:
        try:
            return ClosedLoopService(session).build_sample_plan(execution_id)
        except DomainError as exc:
            raise to_http_error(exc) from exc

    @app.get("/api/v1/recommendations", response_model=list[RecommendationRead])
    def list_recommendations(
        request: Request,
        limit: int = Query(default=100, ge=1, le=500),
        session: Session = Depends(get_session),
    ) -> list[RecommendationRead]:
        return RecommendationService(
            session, settings=request.app.state.settings
        ).list_recommendations(limit=limit)

    @app.get("/api/v1/mat-model/space", response_model=dict[str, object])
    def get_mat_model_space(
        request: Request,
        session: Session = Depends(get_session),
    ) -> dict[str, object]:
        try:
            service = MatModelService(request.app.state.settings)
            return service.composition_space_payload()
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/v1/executions/{execution_id}/feedback", response_model=ExecutionRead)
    def submit_feedback(
        execution_id: str,
        feedback: FeedbackCreate,
        session: Session = Depends(get_session),
    ) -> ExecutionRead:
        try:
            execution = FeedbackService(session).submit_feedback(execution_id, feedback)
        except DomainError as exc:
            raise to_http_error(exc) from exc
        return _execution_reads_with_model_state(session, [execution])[0]

    @app.post("/api/v1/instrument-uploads", response_model=InstrumentUploadRead)
    def upload_instrument_data(
        payload: InstrumentUploadCreate,
        request: Request,
        session: Session = Depends(get_session),
    ) -> InstrumentUploadRead:
        try:
            service = InstrumentUploadService(
                session,
                xrd_data_dir=request.app.state.settings.xrd_data_dir,
                workflow_topic_prefix=resolved_settings.kafka.topic_prefix,
            )
            prepared = service.prepare_workflow_upload(payload)
            result = service.ingest_upload(payload, prepared_observation=prepared)
        except DomainError as exc:
            raise to_http_error(exc) from exc
        return InstrumentUploadRead(
            execution_id=result.execution.id,
            schedule_id=result.execution.schedule_id,
            experiment_batch_id=result.experiment_batch_id,
            instrument_code=result.instrument_code,
            component_id=result.component_id,
            round_completed=result.round_completed,
            observation=result.observation,
            missing_steps=result.missing_steps,
            execution=_execution_reads_with_model_state(session, [result.execution])[0],
        )

    @app.get("/api/v1/campaigns", response_model=list[CampaignRead])
    def list_campaigns(
        limit: int = Query(default=100, ge=1, le=500),
        session: Session = Depends(get_session),
    ) -> list[CampaignRead]:
        campaigns = ClosedLoopService(session).list_campaigns(limit=limit)
        return [campaign_to_read(item) for item in campaigns]

    @app.get("/api/v1/campaigns/{campaign_id}", response_model=CampaignRead)
    def get_campaign(campaign_id: str, session: Session = Depends(get_session)) -> CampaignRead:
        try:
            campaign = ClosedLoopService(session).get_campaign(campaign_id)
        except DomainError as exc:
            raise to_http_error(exc) from exc
        return campaign_to_read(campaign)

    @app.get("/api/v1/runs/{run_id}", response_model=ClosedLoopRunRead)
    def get_run(run_id: str, session: Session = Depends(get_session)) -> ClosedLoopRunRead:
        try:
            campaign, execution, data_packages, recommendations, events = ClosedLoopService(
                session
            ).build_run_view(run_id)
        except DomainError as exc:
            raise to_http_error(exc) from exc

        schedule = SchedulerService(session).get_schedule(execution.schedule_id)
        StateProjectionService(session).sync_execution(execution)
        session.commit()
        return ClosedLoopRunRead(
            campaign=None if campaign is None else campaign_to_read(campaign),
            execution=_execution_reads_with_model_state(session, [execution])[0],
            data_packages=[data_package_to_read(item) for item in data_packages],
            recommendations=[
                recommendation_record_to_read(
                    item,
                    schedule_name=schedule.name,
                    schedule_status=schedule.status,
                )
                for item in recommendations
            ],
            events=[closed_loop_event_to_read(item) for item in events],
        )

    @app.post("/api/v1/closed-loop/data-packages", response_model=DataPackageIngestRead)
    def ingest_data_package(
        payload: DataPackageCreate,
        session: Session = Depends(get_session),
    ) -> DataPackageIngestRead:
        try:
            service = ClosedLoopService(session, workflow_topic_prefix=resolved_settings.kafka.topic_prefix)
            prepared = service.prepare_workflow_data_package(payload)
            result = service.ingest_data_package(payload, prepared=prepared)
        except DomainError as exc:
            raise to_http_error(exc) from exc

        return DataPackageIngestRead(
            data_package=data_package_ack_to_read(result.data_package),
            round_completed=result.round_completed,
            missing_steps=result.missing_steps,
            execution=None
            if result.execution is None
            else execution_progress_to_read(result.execution),
        )

    @app.post("/api/v1/engine/tick", response_model=TickResult)
    def run_scheduler_tick(session: Session = Depends(get_session)) -> TickResult:
        if resolved_settings.scheduler_mode == "external":
            raise HTTPException(status_code=409, detail="automatic schedule ticks are owned by the external engine worker")
        executions, timed_out_count = SchedulerService(session).dispatch_due_schedules()
        return TickResult(
            timed_out_count=timed_out_count,
            dispatched_count=len(executions),
            executions=_execution_reads_with_model_state(session, executions),
        )

    from app.workflows.api import build_workflow_router

    app.include_router(build_workflow_router(get_session, topic_prefix=resolved_settings.kafka.topic_prefix))
    return app


def get_session(request: Request):
    session = request.app.state.database.session_factory()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def to_http_error(exc: DomainError) -> HTTPException:
    if isinstance(exc, NotFoundError):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, ConflictError):
        return HTTPException(status_code=409, detail=str(exc))
    return HTTPException(status_code=400, detail=str(exc))


app = create_app()
