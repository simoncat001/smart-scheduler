from __future__ import annotations

from datetime import datetime
from enum import StrEnum
import json
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    BaseModel, ConfigDict, Field, JsonValue, StringConstraints,
    field_validator, model_validator,
)


Identifier = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9_-]{0,63}$")]
Schema = dict[str, JsonValue] | bool


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False, strict=True)


class PortDefinition(StrictModel):
    json_schema: Schema = Field(default_factory=dict, alias="schema")
    required: bool = True
    use_default: bool = False
    default: JsonValue = None


class RetryPolicy(StrictModel):
    max_attempts: int = Field(default=1, ge=1, le=10)
    delay_seconds: int = Field(default=30, ge=1, le=86400)


class TimeoutPolicy(StrictModel):
    delivery_seconds: int = Field(default=60, ge=1, le=86400)
    acceptance_seconds: int = Field(default=120, ge=1, le=86400)
    execution_seconds: int = Field(default=3600, ge=1, le=604800)
    validation_seconds: int = Field(default=120, ge=1, le=86400)


class ExecutorBinding(StrictModel):
    kind: Literal["device", "compute"]
    gateway_id: Identifier
    adapter_id: Identifier
    resource_id: Identifier | None = None


class NodeDefinition(StrictModel):
    id: Identifier
    type: Identifier
    type_version: int = Field(default=1, ge=1)
    label: str = Field(min_length=1, max_length=120)
    config: dict[str, JsonValue] = Field(default_factory=dict)
    inputs: dict[Identifier, PortDefinition] = Field(default_factory=dict)
    outputs: dict[Identifier, PortDefinition] = Field(default_factory=dict)
    input_values: dict[Identifier, JsonValue] = Field(default_factory=dict)
    executor: ExecutorBinding | None = None
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    timeouts: TimeoutPolicy = Field(default_factory=TimeoutPolicy)


class EdgeDefinition(StrictModel):
    source: Identifier
    target: Identifier
    source_port: Identifier | None = None
    target_port: Identifier | None = None
    route: Identifier | None = None

    @model_validator(mode="after")
    def paired_ports(self):
        if (self.source_port is None) != (self.target_port is None):
            raise ValueError("edge port bindings need both source_port and target_port")
        return self


class Position(StrictModel):
    x: float
    y: float


class WorkflowGraph(StrictModel):
    schema_version: Literal[1] = 1
    nodes: list[NodeDefinition] = Field(min_length=1, max_length=256)
    edges: list[EdgeDefinition] = Field(default_factory=list, max_length=1024)
    positions: dict[Identifier, Position] = Field(default_factory=dict)
    decision_node_id: Identifier | None = None

    @model_validator(mode="after")
    def bounded_json(self):
        encoded = json.dumps(self.model_dump(mode="json", by_alias=True), allow_nan=False)
        if len(encoded.encode("utf-8")) > 1_000_000:
            raise ValueError("workflow exceeds the 1 MB definition limit")
        return self


class EventDisposition(StrEnum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"
    IGNORED_STALE = "ignored_stale"
    QUARANTINED = "quarantined"


MessageType = Literal[
    "command.execute", "command.cancel", "command.query",
    "node.accepted", "node.started", "node.progress", "node.completed",
    "node.failed", "node.cancelled", "node.needs_attention",
]


class MessageEnvelope(StrictModel):
    schema_version: Literal[1] = 1
    message_id: str = Field(min_length=1, max_length=128)
    message_type: MessageType
    run_id: str
    workflow_version_id: str
    node_id: Identifier
    node_run_id: str
    attempt: int = Field(ge=1)
    command_id: str
    gateway_id: Identifier
    adapter_id: Identifier
    occurred_at: datetime
    expires_at: datetime | None = None
    fencing_generation: int | None = Field(default=None, ge=1)
    payload: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("run_id", "workflow_version_id", "node_run_id", "command_id")
    @classmethod
    def uuid_identity(cls, value: str) -> str:
        if str(UUID(value)) != value:
            raise ValueError("identity must be a canonical UUID")
        return value

    @field_validator("occurred_at", "expires_at")
    @classmethod
    def timezone_required(cls, value: datetime | None):
        if value is not None and value.utcoffset() is None:
            raise ValueError("message timestamps require a timezone")
        return value

    @model_validator(mode="after")
    def valid_command(self):
        if self.message_type.startswith("command."):
            if self.expires_at is None or self.expires_at <= self.occurred_at:
                raise ValueError("command expiry must be later than occurred_at")
        if len(self.model_dump_json().encode("utf-8")) > 1_000_000:
            raise ValueError("message exceeds the 1 MB envelope limit")
        json.dumps(self.payload, allow_nan=False)
        return self
