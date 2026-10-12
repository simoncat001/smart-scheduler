from collections.abc import Callable
import json
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import Field, JsonValue, field_validator, model_validator
from sqlalchemy.orm import Session
from sqlalchemy import func, select

from app.models import utcnow
from app.services.exceptions import ConflictError, DomainError, NotFoundError
from app.workflows.definitions import DefinitionService, validate_graph
from app.workflows.models import MessageInbox, MessageOutbox, WorkflowDefinition, WorkflowScheduleBinding, WorkflowVersion
from app.workflows.operations import OperationService
from app.workflows.projection import workflow_runtime_read
from app.workflows.registry import NODE_TYPES
from app.workflows.schemas import StrictModel, WorkflowGraph
from app.workflows.templates import default_materials_graph


class DraftCreate(StrictModel):
    name: str = Field(min_length=1, max_length=120)
    graph: WorkflowGraph


class RevisionRequest(StrictModel):
    revision: int = Field(ge=1)


class DraftSave(RevisionRequest):
    graph: WorkflowGraph


class OperationRequest(StrictModel):
    action: Literal["approve", "retry", "cancel"]
    request_id: str = Field(min_length=1, max_length=128)
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=1, max_length=1000)
    attempt: int | None = Field(default=None, ge=1, le=10)
    outputs: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def bounded_request(self):
        if len(self.model_dump_json().encode()) > 1_000_000:
            raise ValueError("operator request exceeds 1 MB")
        return self

    @field_validator("actor", "reason", "request_id")
    @classmethod
    def not_blank(cls, value):
        if not value.strip():
            raise ValueError("operator identity, reason and request ID must not be blank")
        return value


def _draft_read(row):
    return {"id": row.id, "name": row.name, "revision": row.revision,
            "graph": json.loads(row.draft_graph_json), "created_at": row.created_at, "updated_at": row.updated_at}


def _version_read(row):
    return {"id": row.id, "definition_id": row.definition_id, "version": row.version,
            "digest": row.digest, "graph": json.loads(row.graph_json), "created_at": row.created_at}


def _definition(session, definition_id):
    row = session.get(WorkflowDefinition, definition_id)
    if row is None:
        raise NotFoundError("workflow definition not found")
    return row


class WorkflowBindingRequest(StrictModel):
    workflow_version_id: str = Field(min_length=36, max_length=36)

    @field_validator("workflow_version_id")
    @classmethod
    def canonical_uuid(cls, value):
        if str(UUID(value)) != value:
            raise ValueError("version ID must be a canonical UUID")
        return value


def _http_error(error):
    return HTTPException(status_code=404 if isinstance(error, NotFoundError) else
                         409 if isinstance(error, ConflictError) else 400, detail=str(error))


def build_workflow_router(session_dependency: Callable, *, topic_prefix="scheduler") -> APIRouter:
    router = APIRouter(prefix="/api/v1", tags=["workflows"])

    @router.get("/workflow-types")
    def read_types():
        return [{"type": entry.name, "type_version": entry.version,
                 "config_schema": entry.config_schema.model_json_schema(),
                 "executor_kind": entry.executor_kind, "adapter_ids": sorted(entry.adapter_ids)}
                for entry in NODE_TYPES.values()]

    @router.get("/workflow-templates/materials")
    def read_template():
        return {"name": "Materials characterization", "graph": default_materials_graph().model_dump(mode="json", by_alias=True)}

    @router.post("/workflows", status_code=201)
    def create_draft(request: DraftCreate, session: Session = Depends(session_dependency)):
        try:
            row = DefinitionService(session).create(request.name, request.graph)
            session.commit()
            return _draft_read(row)
        except DomainError as error:
            raise _http_error(error) from error

    @router.get("/workflows")
    def list_drafts(limit: int = Query(default=100, ge=1, le=500), session: Session = Depends(session_dependency)):
        return [_draft_read(row) for row in session.scalars(select(WorkflowDefinition)
                    .order_by(WorkflowDefinition.updated_at.desc(), WorkflowDefinition.id).limit(limit))]

    @router.get("/workflows/{definition_id}")
    def read_draft(definition_id: str, session: Session = Depends(session_dependency)):
        try:
            return _draft_read(_definition(session, definition_id))
        except DomainError as error:
            raise _http_error(error) from error

    @router.put("/workflows/{definition_id}")
    def save_draft(definition_id: str, request: DraftSave, session: Session = Depends(session_dependency)):
        try:
            row = DefinitionService(session).save(definition_id, request.revision, request.graph)
            session.commit()
            return _draft_read(row)
        except DomainError as error:
            raise _http_error(error) from error

    @router.delete("/workflows/{definition_id}", status_code=204)
    def delete_draft(definition_id: str, revision: int = Query(ge=1), session: Session = Depends(session_dependency)):
        try:
            row = DefinitionService(session)._lock_definition(definition_id, revision)
            published = session.scalar(select(WorkflowVersion.id).where(WorkflowVersion.definition_id == definition_id)
                                       .limit(1).with_for_update())
            if published:
                raise ConflictError("published workflow definitions cannot be deleted")
            session.delete(row)
            session.commit()
            return Response(status_code=204)
        except DomainError as error:
            raise _http_error(error) from error

    @router.post("/workflows/{definition_id}/validate")
    def validate_draft(definition_id: str, request: RevisionRequest, session: Session = Depends(session_dependency)):
        try:
            row = DefinitionService(session)._lock_definition(definition_id, request.revision)
            validate_graph(WorkflowGraph.model_validate_json(row.draft_graph_json))
            return {"valid": True, "revision": row.revision}
        except DomainError as error:
            raise _http_error(error) from error

    @router.post("/workflows/{definition_id}/publish", status_code=201)
    def publish_draft(definition_id: str, request: RevisionRequest, session: Session = Depends(session_dependency)):
        try:
            row = DefinitionService(session).publish(definition_id, request.revision)
            session.commit()
            return _version_read(row)
        except DomainError as error:
            raise _http_error(error) from error

    @router.get("/workflows/{definition_id}/versions")
    def list_versions(definition_id: str, session: Session = Depends(session_dependency)):
        try:
            _definition(session, definition_id)
            return [_version_read(row) for row in session.scalars(select(WorkflowVersion)
                .where(WorkflowVersion.definition_id == definition_id).order_by(WorkflowVersion.version.desc()))]
        except DomainError as error:
            raise _http_error(error) from error

    @router.get("/workflows/{definition_id}/versions/{version_id}")
    def read_version(definition_id: str, version_id: str, session: Session = Depends(session_dependency)):
        row = session.get(WorkflowVersion, version_id)
        if row is None or row.definition_id != definition_id:
            raise HTTPException(status_code=404, detail="workflow version not found")
        return _version_read(row)

    @router.put("/schedules/{schedule_id}/workflow")
    def bind_workflow(schedule_id: str, request: WorkflowBindingRequest, session: Session = Depends(session_dependency)):
        try:
            binding = DefinitionService(session).bind(schedule_id, request.workflow_version_id)
            session.commit()
            return {"schedule_id": binding.schedule_id, "workflow_version_id": binding.workflow_version_id}
        except DomainError as error:
            raise _http_error(error) from error

    @router.get("/schedules/{schedule_id}/workflow")
    def read_binding(schedule_id: str, session: Session = Depends(session_dependency)):
        binding = session.get(WorkflowScheduleBinding, schedule_id)
        if binding is None:
            raise HTTPException(status_code=404, detail="workflow binding not found")
        return {"schedule_id": binding.schedule_id, "workflow_version_id": binding.workflow_version_id}

    @router.get("/executions/{execution_id}/workflow")
    def read_workflow(execution_id: str, session: Session = Depends(session_dependency)):
        try:
            return workflow_runtime_read(session, execution_id)
        except DomainError as error:
            raise _http_error(error) from error

    @router.post("/workflow-nodes/{node_run_id}/operations")
    def operate_node(node_run_id: str, request: OperationRequest, session: Session = Depends(session_dependency)):
        try:
            result = OperationService(session, topic_prefix).apply(node_run_id, request, utcnow())
            session.commit()
            return result
        except DomainError as error:
            raise _http_error(error) from error

    @router.get("/messaging/health")
    def messaging_health(session: Session = Depends(session_dependency)):
        from app.workflows.heartbeats import worker_health

        outbox = dict(session.execute(select(MessageOutbox.status, func.count()).group_by(MessageOutbox.status)).all())
        inbox = dict(session.execute(select(MessageInbox.status, func.count())
            .where(MessageInbox.handler != "worker-heartbeat").group_by(MessageInbox.status)).all())
        return {"outbox": {**dict.fromkeys(["pending", "publishing", "sent", "expired"], 0), **outbox},
                "inbox": {**dict.fromkeys(["applied", "ignored_stale", "quarantined"], 0), **inbox},
                "broker": {"status": "unknown", "reason": "no live broker probe performed"},
                "workers": worker_health(session, topic_prefix=topic_prefix)}

    return router
