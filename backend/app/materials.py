from __future__ import annotations

from copy import deepcopy
from typing import Any


QUATERNARY_KEYS = ("ti_at", "zr_at", "nb_at", "v_at")
ELEMENT_NAME_MAP = {
    "ti_at": "Ti",
    "zr_at": "Zr",
    "nb_at": "Nb",
    "v_at": "V",
}
RUNTIME_PAYLOAD_KEYS = (
    "experiment_batch_id",
    "experiment_batch_root",
    "experiment_round_index",
    "experiment_round_label",
    "sample_id",
    "sample_plan",
    "workflow_run",
)
DEFAULT_COMPOSITION_TOLERANCE_AT = 2.0
DEFAULT_INSTRUMENT_COMPONENT_SPECS = {
    "magnetron_sputtering": {
        "title": "磁控溅射数据上传组件",
        "component_key": "sputter_component_id",
        "fallback_key": "deposition_chamber",
        "default_component_id": "sputter-cluster-01",
        "step_code": "magnetron_sputtering",
        "outputs": ["deposition_sample_id", "sputter_run_id"],
    },
    "xrd": {
        "title": "XRD数据上传组件",
        "component_key": "xrd_component_id",
        "fallback_key": None,
        "default_component_id": "xrd-lab-01",
        "step_code": "xrd",
        "outputs": ["xrd_phase", "xrd_note"],
    },
    "eds": {
        "title": "EDS数据上传组件",
        "component_key": "eds_component_id",
        "fallback_key": None,
        "default_component_id": "eds-lab-01",
        "step_code": "eds",
        "outputs": ["measured_composition_at_percent"],
    },
    "nanoindentation": {
        "title": "纳米压痕数据上传组件",
        "component_key": "nanoindentation_component_id",
        "fallback_key": None,
        "default_component_id": "nanoindentation-lab-01",
        "step_code": "nanoindentation",
        "outputs": ["hardness_gpa"],
    },
}
DEFAULT_WORKFLOW_STEPS = [
    {
        "code": "magnetron_sputtering",
        "title": "磁控溅射制备",
        "output": "execution_dispatched",
        "description": "本轮任务一旦派发，即视为制备节点已启动并完成。",
        "required_feedback_fields": [],
    },
    {
        "code": "xrd",
        "title": "XRD表征结构",
        "output": "xrd_phase",
        "description": "记录相结构信息，如 bcc、amorphous 或 multiphase。",
        "required_feedback_fields": ["xrd_phase"],
    },
    {
        "code": "eds",
        "title": "EDS表征成分",
        "output": "measured_composition_at_percent",
        "description": "回填 Ti/Zr/Nb/V 的实测 at.% 组成。",
        "required_feedback_fields": ["measured_composition_at_percent"],
    },
    {
        "code": "nanoindentation",
        "title": "纳米压痕表征硬度",
        "output": "hardness_gpa",
        "description": "记录纳米压痕硬度，作为闭环优化目标。",
        "required_feedback_fields": ["hardness_gpa"],
    },
]


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _space_stats(value: Any) -> dict[str, float] | None:
    if not isinstance(value, dict):
        return None

    stats: dict[str, float] = {}
    for key in ("min", "p10", "median", "p90", "max", "mean"):
        number = _as_float(value.get(key))
        if number is not None:
            stats[key] = float(number)

    if not {"min", "max"}.issubset(stats):
        return None
    return stats


def extract_predicted_composition_space(payload: dict[str, Any]) -> dict[str, Any] | None:
    raw_space = payload.get("predicted_composition_space")
    if not isinstance(raw_space, dict):
        return None

    raw_elements = raw_space.get("elements")
    if not isinstance(raw_elements, dict):
        return None

    elements: dict[str, dict[str, float]] = {}
    for key in QUATERNARY_KEYS:
        stats = _space_stats(raw_elements.get(key))
        if stats is None:
            return None
        elements[key] = stats

    grid_shape = raw_space.get("grid_shape")
    center = raw_space.get("center_composition_at_percent")
    return {
        **raw_space,
        "elements": elements,
        "grid_shape": grid_shape if isinstance(grid_shape, dict) else {},
        "center_composition_at_percent": center if isinstance(center, dict) else {},
    }


def composition_from_space_center(space: dict[str, Any]) -> dict[str, float] | None:
    nested = space.get("center_composition_at_percent")
    if not isinstance(nested, dict):
        return None

    composition = {key: _as_float(nested.get(key)) for key in QUATERNARY_KEYS}
    if not all(value is not None for value in composition.values()):
        return None
    return {key: float(value) for key, value in composition.items()}


def extract_predicted_composition(payload: dict[str, Any]) -> dict[str, float] | None:
    candidate_nested = payload.get("mat_model_candidate_composition_at_percent")
    if isinstance(candidate_nested, dict):
        composition = {
            key: _as_float(candidate_nested.get(key))
            for key in QUATERNARY_KEYS
        }
        if all(value is not None for value in composition.values()):
            return {key: float(value) for key, value in composition.items()}

    nested = payload.get("predicted_composition_at_percent")
    if isinstance(nested, dict):
        composition = {
            key: _as_float(nested.get(key))
            for key in QUATERNARY_KEYS
        }
        if all(value is not None for value in composition.values()):
            return {key: float(value) for key, value in composition.items()}

    composition = {key: _as_float(payload.get(key)) for key in QUATERNARY_KEYS}
    if any(value is not None for value in composition.values()):
        if not all(value is not None for value in composition.values()):
            return None
        return {key: float(value) for key, value in composition.items()}

    space = extract_predicted_composition_space(payload)
    if space is not None:
        return composition_from_space_center(space)
    return None


def measured_composition_from_observation(
    observation: dict[str, Any],
) -> dict[str, float] | None:
    nested = observation.get("measured_composition_at_percent")
    if isinstance(nested, dict):
        composition = {
            key: _as_float(nested.get(key))
            for key in QUATERNARY_KEYS
        }
        if all(value is not None for value in composition.values()):
            return {key: float(value) for key, value in composition.items()}

    composition = {
        "ti_at": _as_float(observation.get("eds_ti_at")),
        "zr_at": _as_float(observation.get("eds_zr_at")),
        "nb_at": _as_float(observation.get("eds_nb_at")),
        "v_at": _as_float(observation.get("eds_v_at")),
    }
    if any(value is not None for value in composition.values()):
        if not all(value is not None for value in composition.values()):
            return None
        return {key: float(value) for key, value in composition.items()}
    return None


def workflow_steps_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    configured_steps = payload.get("workflow_steps")
    if isinstance(configured_steps, list) and configured_steps:
        return deepcopy(configured_steps)
    return deepcopy(DEFAULT_WORKFLOW_STEPS)


def instrument_components_from_payload(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    configured = payload.get("instrument_components")
    components = deepcopy(configured) if isinstance(configured, dict) else {}

    for instrument_code, spec in DEFAULT_INSTRUMENT_COMPONENT_SPECS.items():
        component = components.get(instrument_code)
        if isinstance(component, dict) and component.get("component_id"):
            component.setdefault("instrument_code", instrument_code)
            component.setdefault("title", spec["title"])
            component.setdefault("adapter_kind", "http_upload")
            component.setdefault("step_code", spec["step_code"])
            component.setdefault("outputs", deepcopy(spec["outputs"]))
            continue

        component_id = payload.get(spec["component_key"])
        if not component_id and spec["fallback_key"]:
            component_id = payload.get(spec["fallback_key"])
        if not component_id:
            component_id = spec["default_component_id"]

        components[instrument_code] = {
            "instrument_code": instrument_code,
            "title": spec["title"],
            "adapter_kind": "http_upload",
            "component_id": str(component_id),
            "step_code": spec["step_code"],
            "outputs": deepcopy(spec["outputs"]),
        }

    return components


def strip_runtime_payload_fields(payload: dict[str, Any]) -> dict[str, Any]:
    cleaned = deepcopy(payload)
    for key in RUNTIME_PAYLOAD_KEYS:
        cleaned.pop(key, None)
    return cleaned


def composition_sum(composition: dict[str, float]) -> float:
    return round(sum(composition.values()), 4)


def format_composition(composition: dict[str, float]) -> str:
    return " / ".join(
        f"{key.split('_')[0].upper()} {value:.1f}"
        for key, value in composition.items()
    ) + " at.%"


def format_composition_space(space: dict[str, Any]) -> str:
    normalized = extract_predicted_composition_space(
        {"predicted_composition_space": space}
        if "elements" in space
        else space
    )
    if normalized is None:
        return "未定义成分空间"

    elements = normalized["elements"]
    return " / ".join(
        f"{ELEMENT_NAME_MAP[key]} {elements[key]['min']:.1f}-{elements[key]['max']:.1f}"
        for key in QUATERNARY_KEYS
    ) + " at.%"


def composition_deviation(
    predicted: dict[str, float], measured: dict[str, float]
) -> tuple[float, float]:
    deltas = [abs(predicted[key] - measured[key]) for key in QUATERNARY_KEYS]
    return round(max(deltas), 3), round(sum(deltas), 3)


def _has_text(value: Any) -> bool:
    return isinstance(value, str) and value.strip() != ""


def _has_number(value: Any) -> bool:
    return _as_float(value) is not None


def _upload_completed(upload_state: Any) -> bool:
    if not isinstance(upload_state, dict):
        return False
    status = str(upload_state.get("status") or "").strip().lower()
    return bool(
        upload_state.get("uploaded_at")
        or upload_state.get("parsed_at")
        or status in {"uploaded", "parsed", "success"}
    )


def summarize_workflow_progress(
    payload: dict[str, Any], observation: dict[str, Any]
) -> dict[str, Any]:
    steps = workflow_steps_from_payload(payload)
    measured = measured_composition_from_observation(observation)
    has_xrd = _has_text(observation.get("xrd_phase")) or _has_text(observation.get("xrd_note"))
    has_hardness = _has_number(observation.get("hardness_gpa"))
    dispatch_started = any(
        [
            _has_text(payload.get("experiment_batch_id")),
            _has_text(payload.get("sample_id")),
            _has_text(payload.get("run_id")),
            payload.get("experiment_round_index") is not None,
        ]
    )
    has_deposition = any(
        [
            dispatch_started,
            _has_text(observation.get("deposition_sample_id")),
            _has_text(observation.get("sputter_run_id")),
            bool(observation.get("deposition_completed")),
        ]
    )
    instrument_uploads = (
        observation.get("instrument_uploads")
        if isinstance(observation.get("instrument_uploads"), dict)
        else {}
    )
    has_xrd_upload = _upload_completed(instrument_uploads.get("xrd"))
    has_eds_upload = _upload_completed(instrument_uploads.get("eds"))
    has_nano_upload = _upload_completed(instrument_uploads.get("nanoindentation"))

    signals = {
        "magnetron_sputtering": has_deposition,
        "xrd": has_xrd or has_xrd_upload,
        "eds": measured is not None or has_eds_upload,
        "nanoindentation": has_hardness or has_nano_upload,
    }

    missing_steps: list[dict[str, str]] = []
    completed_count = 0
    current_step: str | None = None
    current_output: str | None = None

    for step in steps:
        completed = bool(signals.get(step["code"]))
        if completed:
            step["status"] = "completed"
            completed_count += 1
            continue

        missing_steps.append({"code": step["code"], "title": step["title"]})
        if current_step is None:
            step["status"] = "current"
            current_step = step["code"]
            current_output = step["output"]
        else:
            step["status"] = "pending"

    total_steps = len(steps)
    return {
        "workflow_kind": "tizrnbv_hardness_loop",
        "steps": steps,
        "current_step": current_step,
        "current_output": current_output,
        "missing_steps": missing_steps,
        "required_feedback_fields": [
            "xrd_phase",
            "measured_composition_at_percent",
            "hardness_gpa",
        ],
        "completed_count": completed_count,
        "total_steps": total_steps,
        "completion_ratio": round(completed_count / total_steps, 3) if total_steps else 0.0,
        "is_complete": completed_count == total_steps,
    }


def build_dispatch_payload(payload: dict[str, Any]) -> dict[str, Any]:
    dispatch_payload = strip_runtime_payload_fields(payload)
    if not is_tizrnbv_hardness_loop(dispatch_payload):
        return dispatch_payload

    dispatch_payload["instrument_components"] = instrument_components_from_payload(
        dispatch_payload
    )
    dispatch_payload["workflow_run"] = summarize_workflow_progress(dispatch_payload, {})
    return dispatch_payload


def enrich_feedback_observation(
    payload: dict[str, Any], observation: dict[str, Any]
) -> dict[str, Any]:
    enriched = deepcopy(observation)
    measured = measured_composition_from_observation(enriched)
    if measured is not None:
        enriched["measured_composition_at_percent"] = {
            key: round(value, 3) for key, value in measured.items()
        }

    if is_tizrnbv_hardness_loop(payload):
        enriched["workflow_progress"] = summarize_workflow_progress(payload, enriched)
        enriched["closed_loop_ready"] = enriched["workflow_progress"]["is_complete"]

    return enriched


def is_tizrnbv_hardness_loop(payload: dict[str, Any]) -> bool:
    return payload.get("workflow_kind") == "tizrnbv_hardness_loop" or bool(
        extract_predicted_composition(payload) or extract_predicted_composition_space(payload)
    )


def validate_tizrnbv_feedback(
    payload: dict[str, Any], observation: dict[str, Any], outcome: str
) -> None:
    if not is_tizrnbv_hardness_loop(payload):
        return

    progress = summarize_workflow_progress(payload, observation)
    if outcome == "success" and not progress["is_complete"]:
        missing = " / ".join(item["title"] for item in progress["missing_steps"])
        raise ValueError(f"success 反馈必须补齐全部实验步骤，当前缺少: {missing}")


def validate_tizrnbv_payload(payload: dict[str, Any]) -> None:
    composition = extract_predicted_composition(payload)
    space = extract_predicted_composition_space(payload)
    if composition is None:
        if payload.get("workflow_kind") == "tizrnbv_hardness_loop" and space is None:
            raise ValueError("Ti/Zr/Nb/V 四元成分必须完整提供")
    else:
        total = composition_sum(composition)
        if abs(total - 100.0) > 0.2:
            raise ValueError("Ti/Zr/Nb/V at% 总和必须为 100")

    if space is not None:
        center = composition_from_space_center(space)
        if center is None:
            raise ValueError("预测成分空间必须提供中心成分")
        total = composition_sum(center)
        if abs(total - 100.0) > 0.2:
            raise ValueError("预测成分空间中心 at% 总和必须为 100")


def normalize_tizrnbv_payload(payload: dict[str, Any]) -> dict[str, Any]:
    payload = strip_runtime_payload_fields(payload)
    space = extract_predicted_composition_space(payload)
    composition = extract_predicted_composition(payload)
    if composition is None:
        return payload

    normalized = deepcopy(payload)
    normalized["workflow_kind"] = "tizrnbv_hardness_loop"
    normalized.setdefault("workflow_name", "Ti-Zr-Nb-V hardness loop")
    normalized.setdefault("measurement_metric", "hardness_gpa")
    normalized.setdefault("target_hardness_gpa", normalized.get("target_metric_value"))
    normalized.setdefault("composition_tolerance_at", DEFAULT_COMPOSITION_TOLERANCE_AT)
    normalized.setdefault("workflow_steps", deepcopy(DEFAULT_WORKFLOW_STEPS))
    normalized["instrument_components"] = instrument_components_from_payload(normalized)
    if space is not None:
        normalized["predicted_composition_space"] = {
            **space,
            "summary": space.get("summary") or format_composition_space(space),
            "center_composition_at_percent": {
                key: round(composition[key], 3) for key in QUATERNARY_KEYS
            },
        }
    normalized["predicted_composition_at_percent"] = {
        key: round(value, 3) for key, value in composition.items()
    }
    normalized["composition_summary"] = (
        format_composition_space(space) if space is not None else format_composition(composition)
    )

    for key, value in composition.items():
        normalized[key] = round(value, 3)

    return normalized
