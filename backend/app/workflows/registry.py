from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Literal

import networkx as nx
from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError
from pydantic import Field, JsonValue, ValidationError as ConfigError

from app.workflows.schemas import Identifier, NodeDefinition, Schema, StrictModel


class EmptyConfig(StrictModel):
    pass


class InstrumentConfig(StrictModel):
    instrument_code: Identifier
    component_id: str | None = Field(default=None, min_length=1, max_length=255)
    action: Identifier = "execute"


class UploadConfig(StrictModel):
    instrument_code: Identifier
    component_id: str | None = Field(default=None, min_length=1, max_length=255)


class ParseConfig(StrictModel):
    parser_id: Literal["manage_db"]


class ModelConfig(StrictModel):
    model_id: str = Field(min_length=1, max_length=120)
    model_version: str = Field(min_length=1, max_length=120)
    next_round_delay_seconds: int = Field(default=3600, ge=1, le=604800)


class ConditionConfig(StrictModel):
    field: Identifier
    operator: Literal["eq", "ne", "gt", "gte", "lt", "lte", "in"]
    value: JsonValue
    match_route: Identifier
    default_route: Identifier


class JoinConfig(StrictModel):
    policy: Literal["all_success"] = "all_success"


@dataclass(frozen=True)
class NodeType:
    name: str
    version: int
    config_schema: type[StrictModel]
    executor_kind: str | None
    adapter_ids: frozenset[str]


NODE_TYPES = {
    entry.name: entry for entry in (
        NodeType("instrument_task", 1, InstrumentConfig, "device", frozenset({"simulator", "kafka_gateway"})),
        NodeType("data_upload", 1, UploadConfig, "device", frozenset({"simulator", "kafka_gateway"})),
        NodeType("data_parse", 1, ParseConfig, "compute", frozenset({"manage_db"})),
        NodeType("model_inference", 1, ModelConfig, "compute", frozenset({"mat_model"})),
        NodeType("condition", 1, ConditionConfig, None, frozenset()),
        NodeType("join", 1, JoinConfig, None, frozenset()),
        NodeType("manual_approval", 1, EmptyConfig, None, frozenset()),
    )
}


def validate_node_type(node: NodeDefinition) -> NodeType:
    entry = NODE_TYPES.get(node.type)
    if entry is None:
        raise ValueError(f"unknown node type: {node.type}")
    if entry.version != node.type_version:
        raise ValueError(f"unsupported type version for {node.id}: {node.type_version}")
    try:
        config = entry.config_schema.model_validate(node.config)
    except ConfigError as exc:
        raise ValueError(f"invalid config for {node.id}: {exc.error_count()} error(s)") from exc
    if isinstance(config, ModelConfig) and config.model_version == "unconfigured":
        raise ValueError(f"config for {node.id} needs a pinned model version")
    if isinstance(config, ConditionConfig):
        if config.match_route == config.default_route:
            raise ValueError(f"config routes for {node.id} must differ")
        if config.operator == "in" and not isinstance(config.value, list):
            raise ValueError(f"config 'in' operator for {node.id} requires a list")
    if entry.executor_kind is None:
        if node.executor is not None:
            raise ValueError(f"internal node {node.id} must not have an executor")
    else:
        if node.executor is None or node.executor.kind != entry.executor_kind:
            raise ValueError(f"node {node.id} needs a {entry.executor_kind} executor")
        if node.executor.adapter_id not in entry.adapter_ids:
            raise ValueError(f"unregistered adapter for {node.id}")
    return entry


MAX_SCHEMA_COMPLEXITY = 10_000
MAX_VALIDATION_WORK = 1_000_000
SCHEMA_MAPS = {"$defs", "properties", "dependentSchemas"}
SCHEMA_LISTS = {"allOf", "anyOf", "oneOf", "prefixItems"}
SCHEMA_VALUES = {
    "additionalProperties", "items", "contains", "not", "propertyNames",
    "if", "then", "else",
}


def _escape(token):
    return str(token).replace("~", "~0").replace("/", "~1")


def _json_tree(value):
    graph = nx.DiGraph()
    values = {}
    pending = [("#", value, 0)]
    while pending:
        path, current, depth = pending.pop()
        if depth > 64 or len(values) >= 10_000:
            raise ValueError("JSON validation complexity exceeds the limit")
        values[path] = current
        graph.add_node(path, weight=1 + (len(current) if isinstance(current, str) else 0))
        children = current.items() if isinstance(current, dict) else enumerate(current) if isinstance(current, list) else ()
        for token, child in children:
            child_path = path + "/" + _escape(token)
            graph.add_edge(path, child_path)
            pending.append((child_path, child, depth + 1))
    return graph, values


def validate_schema(schema: Schema) -> int:
    # Bound reference expansion before invoking jsonschema or holding locks for value validation.
    reference_graph, values = _json_tree(schema)
    pending, seen, targets = ["#"], set(), set()
    while pending:
        path = pending.pop()
        if path in seen:
            continue
        seen.add(path)
        current = values.get(path)
        if not isinstance(current, (dict, bool)):
            raise ValueError("schema reference or subschema target is not a schema")
        if isinstance(current, bool):
            continue
        if any(key in current for key in ("$id", "$dynamicRef", "$recursiveRef")):
            raise ValueError("schema reference scopes and dynamic references are unsupported")
        unsupported = {"pattern", "patternProperties", "unevaluatedItems", "unevaluatedProperties"}
        if unsupported & current.keys() or current.get("uniqueItems") is True:
            raise ValueError("unbounded validation keywords are unsupported in schema version 1")
        if "$ref" in current:
            reference = current["$ref"]
            if not isinstance(reference, str) or not reference.startswith("#/") or reference not in values:
                raise ValueError("schema reference must resolve to a local JSON pointer")
            reference_graph.add_edge(path, reference)
            pending.append(reference)
            targets.add(reference)
        for keyword in SCHEMA_MAPS & current.keys():
            if isinstance(current[keyword], dict):
                pending.extend(path + "/" + keyword + "/" + _escape(key) for key in current[keyword])
        for keyword in SCHEMA_LISTS & current.keys():
            if isinstance(current[keyword], list):
                pending.extend(path + "/" + keyword + "/" + str(index) for index in range(len(current[keyword])))
        pending.extend(path + "/" + keyword for keyword in SCHEMA_VALUES & current.keys())

    if not nx.is_directed_acyclic_graph(reference_graph):
        raise ValueError("recursive schema references are unsupported")
    costs = {}
    for path in reversed(list(nx.topological_sort(reference_graph))):
        costs[path] = reference_graph.nodes[path]["weight"] + sum(costs[child] for child in reference_graph[path])
        if costs[path] > MAX_SCHEMA_COMPLEXITY:
            raise ValueError("schema reference expansion complexity exceeds the limit")
    for path in {"#", *targets}:
        try:
            Draft202012Validator.check_schema(values[path])
        except SchemaError as exc:
            label = "schema reference target" if path in targets else "port schema"
            raise ValueError(f"invalid {label}") from exc
    return costs["#"]


def validate_value(schema: Schema, value: JsonValue, description: str) -> None:
    cost = validate_schema(schema)
    value_graph, _ = _json_tree(value)
    if cost * sum(node["weight"] for _, node in value_graph.nodes(data=True)) > MAX_VALIDATION_WORK:
        raise ValueError(f"validation complexity exceeds the limit for {description}")
    try:
        Draft202012Validator(schema).validate(value)
    except (ValidationError, RecursionError) as exc:
        raise ValueError(f"invalid {description}") from exc


def schemas_compatible(source: Schema, target: Schema) -> bool:
    # Reject unproven subtyping rather than letting a required runtime input fail later.
    equivalent = json.dumps(source, sort_keys=True) == json.dumps(target, sort_keys=True)
    if target is True or target == {} or equivalent:
        return True
    return source == {"type": "integer"} and target == {"type": "number"}
