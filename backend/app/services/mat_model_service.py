from __future__ import annotations

import csv
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
import math
from io import StringIO
from functools import lru_cache
from pathlib import Path
import re
import tempfile
from typing import Any, Iterable

import numpy as np
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from mat_model.bo import _compositions_to_features, predictive_reward, screen_candidate_space, summarize_optimal_intervals
from mat_model.config import load_config
from mat_model.data import prepare_dataset
from mat_model.experiment_selection import select_experimental_batch, summarize_experimental_batch
from mat_model.gpr import fit_best_model
from mat_model.process_mapping import map_candidates_to_process_parameters, map_interval_summary_to_process_window

from app.config import Settings, get_settings
from app.enums import ExecutionStatus
from app.materials import (
    QUATERNARY_KEYS,
    extract_predicted_composition,
    format_composition_space,
    format_composition,
    is_tizrnbv_hardness_loop,
    measured_composition_from_observation,
    normalize_tizrnbv_payload,
    strip_runtime_payload_fields,
)
from app.models import DataPackage, Execution
from app.services.manage_db_client import ManageDbClient, ManageDbClientError, ManageDbRecord
from app.services.manage_db_raw_parser import ManageDbRawParser
from app.services.exceptions import ConflictError


MODEL_TO_PAYLOAD_KEYS = {
    "Ti": "ti_at",
    "Zr": "zr_at",
    "Nb": "nb_at",
    "V": "v_at",
}
PROCESS_PARAMETER_KEYS = (
    "argon_flow_sccm",
    "pressure_pa",
    "substrate_bias_v",
    "substrate_temperature_c",
)
HIGH_THROUGHPUT_GRID_ROWS = 13
HIGH_THROUGHPUT_GRID_COLS = 13
SPACE_ELEMENT_TO_PAYLOAD_KEY = {
    "Ti": "ti_at",
    "Zr": "zr_at",
    "Nb": "nb_at",
    "V": "v_at",
}
SELECTION_ROLE_LABELS = {
    "highest_score": "首轮最高分点",
    "lowest_ti": "低 Ti 边界点",
    "highest_ti": "高 Ti 边界点",
    "lowest_v": "低 V 边界点",
    "highest_v": "高 V 边界点",
    "center_low_std": "中心低不确定性点",
}
MANAGE_DB_REVIEW_STATUSES = (
    "passed_review",
    "passed_review_preview",
    "passed_review_waiting_published",
)
MANAGE_DB_SAMPLE_REF_KEYS = (
    "sample_id",
    "deposition_sample_id",
    "experiment_batch_id",
    "样品名称",
    "MGID",
)
MANAGE_DB_RELATED_SAMPLE_KEYS = (
    "关联样品MGID",
    "associated_sample_mgid",
    "related_sample_mgid",
)
MANAGE_DB_PHASE_ALIASES = (
    "xrdphase",
    "phase",
    "物相",
    "相结构",
    "晶相",
)
MANAGE_DB_HARDNESS_ALIASES = (
    "hardnessgpa",
    "纳米压痕硬度",
    "纳米硬度",
    "平均硬度gpa",
    "硬度gpa",
)
MANAGE_DB_GRID_ROW_ALIASES = ("gridrow", "row", "行号")
MANAGE_DB_GRID_COL_ALIASES = ("gridcol", "col", "column", "列号")
NORMALIZED_KEY_PATTERN = re.compile(r"[^0-9a-z\u4e00-\u9fff]+")
SAMPLE_GRID_PATTERN = re.compile(r"R(?P<row>\d{1,2})C(?P<col>\d{1,2})", re.IGNORECASE)
ELEMENT_FIELD_ALIASES = {
    "ti_at": ("tiat", "edstiat", "钛原子百分比", "ti原子百分比", "ti"),
    "zr_at": ("zrat", "edszrat", "锆原子百分比", "zr原子百分比", "zr"),
    "nb_at": ("nbat", "edsnbat", "铌原子百分比", "nb原子百分比", "nb"),
    "v_at": ("vat", "edsvat", "钒原子百分比", "v原子百分比", "v"),
}


def _coerce_scalar(raw: str) -> Any:
    value = raw.strip()
    if value == "":
        return None
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False

    try:
        number = float(value)
    except ValueError:
        return value

    return int(number) if number.is_integer() else number


def _parse_json(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    return json.loads(raw)


def _parse_number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_phase(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    return None if not text else text


def _load_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [{key: _coerce_scalar(value) for key, value in row.items()} for row in reader]


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _resolve_output_dir(config: dict[str, Any], output_dir_override: str) -> Path:
    if output_dir_override:
        override = Path(output_dir_override).resolve()
        if (override / "summary.json").exists():
            return override

        run_name = str(config["outputs"].get("run_name") or "").strip()
        if run_name and (override / run_name / "summary.json").exists():
            return override / run_name

    output_root = Path(config["outputs"]["directory"]).resolve()
    run_name = str(config["outputs"].get("run_name") or "").strip()
    if run_name:
        return output_root / run_name
    return output_root


def _simplex_project(values: np.ndarray, target_sum: float = 100.0) -> np.ndarray:
    clipped = np.asarray(values, dtype=float).reshape(-1)
    if clipped.size == 0:
        return clipped

    sorted_values = np.sort(clipped)[::-1]
    cumulative = np.cumsum(sorted_values) - target_sum
    indices = np.arange(1, clipped.size + 1)
    valid = sorted_values - cumulative / indices > 0
    rho = indices[valid][-1]
    theta = cumulative[valid][-1] / rho
    projected = np.maximum(clipped - theta, 0.0)
    total = projected.sum()
    if total == 0:
        return np.full_like(projected, target_sum / projected.size)
    return projected * (target_sum / total)


def _space_stats_from_frame(frame: pd.DataFrame) -> dict[str, dict[str, float]]:
    stats: dict[str, dict[str, float]] = {}
    for element, payload_key in SPACE_ELEMENT_TO_PAYLOAD_KEY.items():
        series = frame[element].astype(float)
        stats[payload_key] = {
            "min": float(series.min()),
            "p10": float(series.quantile(0.10)),
            "median": float(series.quantile(0.50)),
            "p90": float(series.quantile(0.90)),
            "max": float(series.max()),
            "mean": float(series.mean()),
        }
    return stats


def _center_composition_from_elements(
    elements: dict[str, dict[str, float]],
) -> dict[str, float]:
    center = {
        key: float(stats.get("median", stats.get("mean", 0.0)))
        for key, stats in elements.items()
    }
    total = sum(center.values())
    if total <= 0:
        return {key: 25.0 for key in QUATERNARY_KEYS}
    return {key: round(value * 100.0 / total, 3) for key, value in center.items()}


def _normalized_key(value: Any) -> str:
    return NORMALIZED_KEY_PATTERN.sub("", str(value).strip().lower())


def _is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def _deep_merge_dicts(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in patch.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = _deep_merge_dicts(existing, value)
        else:
            merged[key] = value
    return merged


def _content_items_to_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, list):
        return {}

    mapped: dict[str, Any] = {}
    for item in value:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        if not title:
            continue
        content = item.get("content")
        if item.get("type") == "object":
            mapped[title] = _content_items_to_mapping(content)
        else:
            mapped[title] = content
    return mapped


def _flatten_mapping(value: Any, prefix: str = "") -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    if isinstance(value, dict):
        for key, child in value.items():
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            flattened[next_prefix] = child
            flattened.update(_flatten_mapping(child, next_prefix))
        return flattened
    if isinstance(value, list):
        for index, child in enumerate(value):
            next_prefix = f"{prefix}[{index}]"
            flattened[next_prefix] = child
            flattened.update(_flatten_mapping(child, next_prefix))
    return flattened


def _first_text(values: Iterable[Any]) -> str | None:
    for value in values:
        if isinstance(value, str):
            text = value.strip()
            if text:
                return text
    return None


def _sample_refs(value: Any) -> list[str]:
    if isinstance(value, str):
        text = value.strip()
        return [] if not text else [text]
    if isinstance(value, (list, tuple, set)):
        refs: list[str] = []
        for item in value:
            refs.extend(_sample_refs(item))
        return refs
    return []


@dataclass(slots=True)
class MatModelArtifacts:
    config: dict[str, Any]
    resolved_config_path: Path
    output_dir: Path
    resolved_runtime_config: dict[str, Any]
    summary: dict[str, Any]
    optimal_intervals: dict[str, Any]
    process_window: dict[str, Any]
    experimental_batch: list[dict[str, Any]]
    recommended_candidates: list[dict[str, Any]]

    @property
    def run_name(self) -> str:
        return self.output_dir.name

    @property
    def online_refresh_metadata(self) -> dict[str, Any] | None:
        refresh = self.summary.get("online_refresh")
        return refresh if isinstance(refresh, dict) else None

    @property
    def model_version(self) -> str:
        refresh = self.online_refresh_metadata
        if refresh and refresh.get("applied"):
            return f"mat_model/{self.output_dir.name}+online"
        return f"mat_model/{self.output_dir.name}"

    @property
    def input_dataset_path(self) -> str | None:
        data_config = self.resolved_runtime_config.get("data")
        if not isinstance(data_config, dict):
            return None
        path = data_config.get("path")
        return None if path in (None, "") else str(path)


@dataclass(slots=True)
class MatModelCandidate:
    source: str
    rank: int | None
    selection_role: str | None
    row: dict[str, Any]

    @property
    def composition(self) -> dict[str, float]:
        return {
            MODEL_TO_PAYLOAD_KEYS[element]: float(self.row[element])
            for element in MODEL_TO_PAYLOAD_KEYS
        }

    @property
    def predicted_hardness_gpa(self) -> float | None:
        value = self.row.get("predicted_hardness_gpa")
        return None if value is None else float(value)

    @property
    def predicted_std_gpa(self) -> float | None:
        value = self.row.get("predicted_std_gpa")
        return None if value is None else float(value)

    @property
    def reward(self) -> float | None:
        value = self.row.get("reward")
        return None if value is None else float(value)

    @property
    def signature(self) -> tuple[float, float, float, float]:
        composition = self.composition
        return tuple(round(float(composition[key]), 3) for key in QUATERNARY_KEYS)

    @property
    def composition_summary(self) -> str:
        return format_composition(self.composition)

    @property
    def role_label(self) -> str | None:
        if not self.selection_role:
            return None
        return SELECTION_ROLE_LABELS.get(self.selection_role, self.selection_role)


@dataclass(slots=True)
class MatModelRefreshResult:
    output_dir: Path
    training_row_count: int
    closed_loop_sample_count: int
    reliable_candidate_count: int
    manage_db_training_row_count: int = 0
    manage_db_downloaded_record_count: int = 0


@lru_cache(maxsize=8)
def _load_artifacts(config_path: str, output_dir: str) -> MatModelArtifacts:
    config, resolved_config_path = load_config(config_path)
    resolved_output_dir = _resolve_output_dir(config, output_dir)

    required_files = {
        "resolved_runtime_config": resolved_output_dir / "resolved_config.json",
        "summary": resolved_output_dir / "summary.json",
        "optimal_intervals": resolved_output_dir / "optimal_intervals.json",
        "process_window": resolved_output_dir / "process_window.json",
        "experimental_batch": resolved_output_dir / "experimental_batch.csv",
        "recommended_candidates": resolved_output_dir / "recommended_process_candidates.csv",
    }
    missing = [name for name, path in required_files.items() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            f"mat_model artifacts are missing in {resolved_output_dir}: {', '.join(missing)}"
        )

    return MatModelArtifacts(
        config=config,
        resolved_config_path=resolved_config_path,
        output_dir=resolved_output_dir,
        resolved_runtime_config=_read_json(required_files["resolved_runtime_config"]),
        summary=_read_json(required_files["summary"]),
        optimal_intervals=_read_json(required_files["optimal_intervals"]),
        process_window=_read_json(required_files["process_window"]),
        experimental_batch=_load_csv_rows(required_files["experimental_batch"]),
        recommended_candidates=_load_csv_rows(required_files["recommended_candidates"]),
    )


class MatModelService:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def _workflow_snapshot(self):
        config_path = Path(self.settings.mat_model_config_path).expanduser().resolve()
        root = Path(self.settings.mat_model_output_dir).expanduser().resolve()
        names = ("resolved_config.json", "summary.json", "optimal_intervals.json", "process_window.json",
                 "experimental_batch.csv", "recommended_process_candidates.csv")
        paths = {"config": config_path, **{name: root / name for name in names}}
        snapshot, digest = {}, hashlib.sha256()
        for name, path in sorted(paths.items()):
            if path.stat().st_size > 64_000_000:
                raise ConflictError("pinned model artifact exceeds 64 MB")
            data = path.read_bytes()
            snapshot[name] = data
            digest.update(name.encode())
            digest.update(len(data).to_bytes(8, "big"))
            digest.update(data)
        return config_path, root, snapshot, "mat-sha256:" + digest.hexdigest()

    def workflow_model_version(self):
        return self._workflow_snapshot()[-1]

    def workflow_artifacts(self, model_id, model_version):
        if model_id != "mat-materials":
            raise ConflictError("unregistered pinned workflow model")
        path, root, snapshot, version = self._workflow_snapshot()
        if version != model_version:
            raise ConflictError("pinned model version no longer matches configured artifacts")
        # Parse the exact hashed bytes, not a second file read or mutable cached artifacts.
        return MatModelArtifacts(
            config=json.loads(snapshot["config"]), resolved_config_path=path, output_dir=root,
            resolved_runtime_config=json.loads(snapshot["resolved_config.json"]), summary=json.loads(snapshot["summary.json"]),
            optimal_intervals=json.loads(snapshot["optimal_intervals.json"]), process_window=json.loads(snapshot["process_window.json"]),
            experimental_batch=[{key: _coerce_scalar(value) for key, value in row.items()}
                for row in csv.DictReader(StringIO(snapshot["experimental_batch.csv"].decode("utf-8-sig")))],
            recommended_candidates=[{key: _coerce_scalar(value) for key, value in row.items()}
                for row in csv.DictReader(StringIO(snapshot["recommended_process_candidates.csv"].decode("utf-8-sig")))],
        )

    def recommend_for_workflow(self, *, model_id, model_version, execution_id, base_payload, inputs, used_payloads):
        artifacts = self.workflow_artifacts(model_id, model_version)
        candidate = None
        if inputs:
            if not {"xrd", "eds", "nanoindentation"}.issubset(inputs) or not all(
                isinstance(inputs[name], dict) for name in ("xrd", "eds", "nanoindentation")
            ):
                raise ConflictError("MAT workflow requires named XRD/EDS/nanoindentation inputs")
            hardness = inputs["nanoindentation"].get("hardness_gpa")
            measured = measured_composition_from_observation(inputs["eds"])
            if (not inputs["xrd"].get("xrd_phase") or measured is None or isinstance(hardness, bool) or
                    not isinstance(hardness, (int, float)) or not math.isfinite(hardness) or hardness <= 0):
                raise ConflictError("MAT workflow characterization inputs are incomplete")
            target = float(base_payload.get("target_hardness_gpa") or 13)
            if hardness >= target:
                candidate = self.pick_local_candidate(anchor_composition=measured, used_payloads=used_payloads, artifacts=artifacts)
        candidate = candidate or self.pick_next_candidate(used_payloads=used_payloads, artifacts=artifacts)
        result = {"status": "exhausted" if candidate is None else "candidate", "source_execution_id": execution_id,
                  "model_id": model_id, "model_version": model_version,
                  "summary": "Pinned MAT candidate pool exhausted" if candidate is None else candidate.composition_summary,
                  "confidence": self.confidence_for_candidate(candidate)}
        if candidate is not None:
            payload = self.build_schedule_payload(base_payload=base_payload, candidate=candidate, artifacts=artifacts)
            for key in ("run_id", "workflow_version_id", "used_model_payloads", "schedule_payload_digest"):
                payload.pop(key, None)
            payload["model_version"] = model_version
            result["next_payload"] = payload
        return result

    def artifacts(self) -> MatModelArtifacts:
        return _load_artifacts(
            str(Path(self.settings.mat_model_config_path).resolve()),
            str(self._effective_output_dir()),
        )

    def base_artifacts(self) -> MatModelArtifacts:
        return _load_artifacts(
            str(Path(self.settings.mat_model_config_path).resolve()),
            str(self._base_output_dir()),
        )

    def composition_space_payload(self, *, artifacts: MatModelArtifacts | None = None) -> dict[str, Any]:
        artifacts = artifacts or self.artifacts()
        elements = self._composition_space_elements(artifacts)
        center = _center_composition_from_elements(elements)
        batch_plan = self._generate_high_throughput_batch_plan(artifacts)
        return {
            "source": "mat_model",
            "model_version": artifacts.model_version,
            "summary": format_composition_space({"elements": elements}),
            "selection_rule": deepcopy(artifacts.optimal_intervals.get("selection_rule", {})),
            "candidate_pool_size": artifacts.summary.get("n_reliable_candidates"),
            "center_composition_at_percent": center,
            "predicted_composition_space": {
                "space_kind": "high_throughput_interval",
                "source": "mat_model",
                "summary": format_composition_space({"elements": elements}),
                "grid_shape": {
                    "rows": HIGH_THROUGHPUT_GRID_ROWS,
                    "cols": HIGH_THROUGHPUT_GRID_COLS,
                    "total_samples": HIGH_THROUGHPUT_GRID_ROWS * HIGH_THROUGHPUT_GRID_COLS,
                },
                "center_composition_at_percent": center,
                "elements": elements,
            },
            "predicted_batch_plan": batch_plan,
            "process_window": deepcopy(artifacts.process_window),
        }

    @staticmethod
    def _composition_space_elements(
        artifacts: MatModelArtifacts,
    ) -> dict[str, dict[str, float]]:
        raw_elements = artifacts.optimal_intervals.get("elements", {})
        elements: dict[str, dict[str, float]] = {}
        for element, payload_key in SPACE_ELEMENT_TO_PAYLOAD_KEY.items():
            stats = raw_elements.get(element)
            if not isinstance(stats, dict):
                continue
            elements[payload_key] = {
                key: float(value)
                for key, value in stats.items()
                if key in {"min", "p10", "median", "p90", "max", "mean"} and value is not None
            }
        return elements

    @staticmethod
    def _snake_index(row_index: int, col_index: int) -> int:
        zero_based_row = row_index - 1
        zero_based_col = col_index - 1
        if zero_based_row % 2 == 0:
            return zero_based_row * HIGH_THROUGHPUT_GRID_COLS + zero_based_col + 1
        return (
            zero_based_row * HIGH_THROUGHPUT_GRID_COLS
            + (HIGH_THROUGHPUT_GRID_COLS - zero_based_col)
        )

    def _generate_high_throughput_batch_plan(
        self,
        artifacts: MatModelArtifacts,
    ) -> dict[str, Any]:
        candidate_frame = pd.DataFrame(artifacts.recommended_candidates)
        if candidate_frame.empty:
            return {
                "design_kind": "high_throughput_grid",
                "ordering": "snake",
                "rows": HIGH_THROUGHPUT_GRID_ROWS,
                "cols": HIGH_THROUGHPUT_GRID_COLS,
                "total_samples": HIGH_THROUGHPUT_GRID_ROWS * HIGH_THROUGHPUT_GRID_COLS,
                "elements": {},
                "preview": [],
                "samples": [],
            }

        pool = candidate_frame.head(min(512, len(candidate_frame))).copy()
        compositions = pool[list(MODEL_TO_PAYLOAD_KEYS)].astype(float)
        values = compositions.to_numpy(dtype=float)
        center = values.mean(axis=0)
        centered = values - center

        _, _, vh = np.linalg.svd(centered, full_matrices=False)
        direction_a = vh[0]
        direction_b = vh[1] if vh.shape[0] > 1 else np.roll(direction_a, 1)
        projections = centered @ np.vstack([direction_a, direction_b]).T
        axis_a_min, axis_a_max = float(projections[:, 0].min()), float(projections[:, 0].max())
        axis_b_min, axis_b_max = float(projections[:, 1].min()), float(projections[:, 1].max())

        samples: list[dict[str, Any]] = []
        for row_index in range(1, HIGH_THROUGHPUT_GRID_ROWS + 1):
            row_fraction = (
                0.0
                if HIGH_THROUGHPUT_GRID_ROWS == 1
                else (row_index - 1) / (HIGH_THROUGHPUT_GRID_ROWS - 1)
            )
            axis_a = axis_a_min + (axis_a_max - axis_a_min) * row_fraction
            for col_index in range(1, HIGH_THROUGHPUT_GRID_COLS + 1):
                col_fraction = (
                    0.0
                    if HIGH_THROUGHPUT_GRID_COLS == 1
                    else (col_index - 1) / (HIGH_THROUGHPUT_GRID_COLS - 1)
                )
                axis_b = axis_b_min + (axis_b_max - axis_b_min) * col_fraction
                projected = center + axis_a * direction_a + axis_b * direction_b
                composition = _simplex_project(projected)
                sample = {
                    "sample_id": f"R{row_index:02d}C{col_index:02d}",
                    "grid_row": row_index,
                    "grid_col": col_index,
                    "snake_index": self._snake_index(row_index, col_index),
                }
                for element_index, element in enumerate(MODEL_TO_PAYLOAD_KEYS):
                    sample[SPACE_ELEMENT_TO_PAYLOAD_KEY[element]] = round(
                        float(composition[element_index]), 3
                    )
                samples.append(sample)

        sample_frame = pd.DataFrame(samples)
        sample_frame["Ti"] = sample_frame["ti_at"]
        sample_frame["Zr"] = sample_frame["zr_at"]
        sample_frame["Nb"] = sample_frame["nb_at"]
        sample_frame["V"] = sample_frame["v_at"]
        element_stats = _space_stats_from_frame(sample_frame)
        preview_ids = {
            "R01C01",
            "R01C07",
            "R01C13",
            "R07C01",
            "R07C07",
            "R07C13",
            "R13C01",
            "R13C07",
            "R13C13",
        }
        preview = [
            sample
            for sample in samples
            if sample["sample_id"] in preview_ids
        ]

        return {
            "design_kind": "high_throughput_grid",
            "ordering": "snake",
            "rows": HIGH_THROUGHPUT_GRID_ROWS,
            "cols": HIGH_THROUGHPUT_GRID_COLS,
            "total_samples": HIGH_THROUGHPUT_GRID_ROWS * HIGH_THROUGHPUT_GRID_COLS,
            "summary": format_composition_space({"elements": element_stats}),
            "elements": element_stats,
            "preview": preview,
            "samples": samples,
        }

    def _pre_inference_refresh_enabled(self) -> bool:
        manage_db_configured = bool(
            self.settings.mat_model_manage_db_sync_enabled
            and self.settings.manage_db_base_url
            and self.settings.manage_db_username
            and self.settings.manage_db_password
        )
        return bool(
            self.settings.mat_model_online_refresh_enabled
            or manage_db_configured
        )

    def _manage_db_training_frame(
        self,
        *,
        config: dict[str, Any],
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        summary: dict[str, Any] = {
            "enabled": bool(self.settings.mat_model_manage_db_sync_enabled),
            "configured": False,
            "downloaded_record_count": 0,
            "parsed_record_count": 0,
            "training_row_count": 0,
            "partial_sample_count": 0,
            "complete_sample_count": 0,
            "skipped_sample_count": 0,
            "total_available_record_count": 0,
            "truncated": False,
        }
        if not self.settings.mat_model_manage_db_sync_enabled:
            summary["skipped_reason"] = "disabled"
            return pd.DataFrame(), summary

        if not (
            self.settings.manage_db_base_url
            and self.settings.manage_db_username
            and self.settings.manage_db_password
        ):
            summary["skipped_reason"] = "missing_credentials"
            return pd.DataFrame(), summary

        summary["configured"] = True
        client = ManageDbClient(self.settings)
        filters = config.get("data", {}).get("filters", {})
        max_records = max(1, int(self.settings.mat_model_manage_db_sync_max_records))
        page_size = max(1, int(self.settings.mat_model_manage_db_sync_page_size))
        start = 0
        total_available = 0
        downloaded_records: list[ManageDbRecord] = []

        try:
            while len(downloaded_records) < max_records:
                remaining = max_records - len(downloaded_records)
                page = client.search_development_data(
                    data_category=self.settings.mat_model_manage_db_sync_data_category,
                    template_type=self.settings.mat_model_manage_db_sync_template_type,
                    review_statuses=list(MANAGE_DB_REVIEW_STATUSES),
                    start=start,
                    size=min(page_size, remaining),
                )
                total_available = max(total_available, page.total)
                if not page.items:
                    break
                downloaded_records.extend(page.items)
                start += len(page.items)
                if len(page.items) < min(page_size, remaining) or start >= page.total:
                    break
        except ManageDbClientError as exc:
            summary["error"] = str(exc)
            return pd.DataFrame(), summary

        frame, parsed_summary = self._manage_db_training_frame_from_records(
            downloaded_records,
            filters=filters,
        )
        summary.update(parsed_summary)
        summary["downloaded_record_count"] = len(downloaded_records)
        summary["total_available_record_count"] = max(total_available, len(downloaded_records))
        summary["truncated"] = total_available > len(downloaded_records)
        return frame, summary

    def _manage_db_training_frame_from_records(
        self,
        records: list[ManageDbRecord],
        *,
        filters: dict[str, Any],
    ) -> tuple[pd.DataFrame, dict[str, int]]:
        aggregated: dict[str, dict[str, Any]] = {}
        parsed_record_count = 0
        raw_parser = ManageDbRawParser(self.settings)

        for record in records:
            payload = self._merged_manage_db_payload(record.payload)
            sample_id = self._sample_id_from_manage_db_payload(payload)
            instrument_code = raw_parser.infer_instrument_code(record.payload)
            parsed_dataset = None
            if instrument_code:
                try:
                    parsed_dataset = raw_parser.parse_record(
                        record,
                        instrument_code=instrument_code,
                        sample_id=sample_id,
                    )
                except ManageDbClientError:
                    parsed_dataset = None

            if parsed_dataset is not None and parsed_dataset.sample_rows:
                record_emitted = False
                for parsed_row in parsed_dataset.sample_rows:
                    row_sample_id = str(parsed_row.get("sample_id") or "").strip()
                    if not row_sample_id:
                        continue
                    grid_row = self._coerce_optional_int(parsed_row.get("grid_row"))
                    grid_col = self._coerce_optional_int(parsed_row.get("grid_col"))
                    measured = self._measured_composition_from_manage_db_payload(parsed_row)
                    hardness = self._hardness_from_manage_db_payload(parsed_row)
                    phase = self._phase_from_manage_db_payload(parsed_row)
                    if measured is None and hardness is None and phase is None:
                        continue
                    record_emitted = True
                    self._apply_manage_db_training_signal(
                        aggregated,
                        sample_id=row_sample_id,
                        grid_row=grid_row,
                        grid_col=grid_col,
                        phase=phase,
                        measured=measured,
                        hardness=hardness,
                        record_id=record.record_id,
                    )
                if record_emitted:
                    parsed_record_count += 1
                    continue

            if sample_id is None:
                continue

            measured = self._measured_composition_from_manage_db_payload(payload)
            hardness = self._hardness_from_manage_db_payload(payload)
            phase = self._phase_from_manage_db_payload(payload)
            if measured is None and hardness is None and phase is None:
                continue

            parsed_record_count += 1
            grid_row, grid_col = self._grid_position_from_manage_db_payload(payload, sample_id)
            self._apply_manage_db_training_signal(
                aggregated,
                sample_id=sample_id,
                grid_row=grid_row,
                grid_col=grid_col,
                phase=phase,
                measured=measured,
                hardness=hardness,
                record_id=record.record_id,
            )

        rows: list[dict[str, Any]] = []
        complete_sample_count = 0
        for aggregate in aggregated.values():
            measured = aggregate.get("measured")
            hardness = aggregate.get("hardness_gpa")
            phase = aggregate.get("phase")
            if measured is None or hardness is None or phase is None:
                continue
            complete_sample_count += 1
            row = {
                "sample_id": aggregate["sample_id"],
                "grid_row": aggregate.get("grid_row"),
                "grid_col": aggregate.get("grid_col"),
                "phase": phase,
                "Ti": round(float(measured["ti_at"]), 6),
                "Zr": round(float(measured["zr_at"]), 6),
                "Nb": round(float(measured["nb_at"]), 6),
                "V": round(float(measured["v_at"]), 6),
                "hardness_mpa_raw": round(float(hardness) * 1000.0, 6),
                "source": "manage_db_sync",
                "composition_sum": round(sum(float(measured[key]) for key in QUATERNARY_KEYS), 6),
                "hardness_is_nan": False,
                "hardness_is_instrument_floor": False,
                "hardness_valid": True,
                "hardness_gpa": round(float(hardness), 6),
                "notes": "manage_db_record_ids=" + ",".join(aggregate["record_ids"]),
            }
            if self._passes_filters(row, filters):
                rows.append(row)

        frame = (
            pd.DataFrame(rows)
            .drop_duplicates(subset=["sample_id"], keep="last")
            .reset_index(drop=True)
            if rows
            else pd.DataFrame()
        )
        return frame, {
            "parsed_record_count": parsed_record_count,
            "training_row_count": int(len(frame)),
            "partial_sample_count": int(max(0, len(aggregated) - complete_sample_count)),
            "complete_sample_count": int(complete_sample_count),
            "skipped_sample_count": int(len(aggregated) - complete_sample_count),
        }

    @staticmethod
    def _coerce_optional_int(value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _apply_manage_db_training_signal(
        aggregated: dict[str, dict[str, Any]],
        *,
        sample_id: str,
        grid_row: int | None,
        grid_col: int | None,
        phase: str | None,
        measured: dict[str, float] | None,
        hardness: float | None,
        record_id: str,
    ) -> None:
        aggregate = aggregated.setdefault(
            sample_id,
            {
                "sample_id": sample_id,
                "grid_row": grid_row,
                "grid_col": grid_col,
                "phase": None,
                "measured": None,
                "hardness_gpa": None,
                "record_ids": [],
            },
        )
        if aggregate.get("grid_row") is None and grid_row is not None:
            aggregate["grid_row"] = grid_row
        if aggregate.get("grid_col") is None and grid_col is not None:
            aggregate["grid_col"] = grid_col
        if phase is not None:
            aggregate["phase"] = phase
        if measured is not None:
            aggregate["measured"] = measured
        if hardness is not None:
            aggregate["hardness_gpa"] = hardness
        if record_id not in aggregate["record_ids"]:
            aggregate["record_ids"].append(record_id)

    def _linked_manage_db_training_frame(
        self,
        session: Session,
        *,
        config: dict[str, Any],
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        summary: dict[str, Any] = {
            "enabled": bool(self.settings.mat_model_manage_db_sync_enabled),
            "configured": False,
            "eligible_execution_count": 0,
            "linked_package_count": 0,
            "resolved_execution_count": 0,
            "fetched_record_count": 0,
            "parsed_record_count": 0,
            "training_row_count": 0,
        }
        if not self.settings.mat_model_manage_db_sync_enabled:
            return pd.DataFrame(), summary
        if not (
            self.settings.manage_db_base_url
            and self.settings.manage_db_username
            and self.settings.manage_db_password
        ):
            return pd.DataFrame(), summary

        summary["configured"] = True
        filters = config.get("data", {}).get("filters", {})
        eligible_execution_ids: list[str] = []
        executions = list(
            session.scalars(
                select(Execution)
                .where(Execution.status == ExecutionStatus.SUCCESS.value)
                .order_by(Execution.closed_at.asc(), Execution.dispatched_at.asc())
            )
        )
        for execution in executions:
            dispatch_payload = _parse_json(execution.dispatch_payload)
            if not is_tizrnbv_hardness_loop(dispatch_payload):
                continue
            observation = _parse_json(execution.feedback_observation)
            if not bool(observation.get("closed_loop_ready")):
                continue
            eligible_execution_ids.append(execution.id)

        summary["eligible_execution_count"] = int(len(eligible_execution_ids))
        if not eligible_execution_ids:
            return pd.DataFrame(), summary

        linked_packages = list(
            session.scalars(
                select(DataPackage)
                .where(
                    DataPackage.execution_id.in_(eligible_execution_ids),
                    DataPackage.manage_db_record_id.is_not(None),
                )
                .order_by(DataPackage.created_at.asc())
            )
        )
        summary["linked_package_count"] = int(len(linked_packages))
        if not linked_packages:
            return pd.DataFrame(), summary

        client = ManageDbClient(self.settings)
        seen_record_ids: set[str] = set()
        resolved_execution_ids: set[str] = set()
        fetched_records: list[ManageDbRecord] = []
        fetch_errors: list[str] = []
        for data_package in linked_packages:
            record_id = str(data_package.manage_db_record_id or "").strip()
            if not record_id or record_id in seen_record_ids:
                continue

            try:
                record = client.fetch_development_data(
                    record_id=record_id,
                    standard_ref=_parse_json(data_package.standard_data_ref_json),
                )
            except ManageDbClientError as exc:
                fetch_errors.append(f"{record_id}: {exc}")
                continue
            if record is None:
                continue

            seen_record_ids.add(record_id)
            resolved_execution_ids.add(data_package.execution_id)
            fetched_records.append(
                ManageDbRecord(
                    record_id=record.record_id,
                    payload=self._linked_manage_db_payload(data_package, record.payload),
                    source_url=record.source_url,
                    base_url=record.base_url,
                )
            )

        summary["resolved_execution_count"] = int(len(resolved_execution_ids))
        summary["fetched_record_count"] = int(len(fetched_records))
        if fetch_errors:
            summary["errors"] = fetch_errors[:10]
        if not fetched_records:
            return pd.DataFrame(), summary

        frame, parsed_summary = self._manage_db_training_frame_from_records(
            fetched_records,
            filters=filters,
        )
        summary.update(parsed_summary)
        return frame, summary

    def _linked_manage_db_payload(
        self,
        data_package: DataPackage,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        merged = dict(payload)
        sample_id = str(data_package.sample_id or data_package.experiment_batch_id or "").strip()
        if sample_id:
            merged.setdefault("sample_id", sample_id)
            merged.setdefault("deposition_sample_id", sample_id)

            origin_post_data = merged.get("origin_post_data")
            if isinstance(origin_post_data, dict):
                related = origin_post_data.get("关联样品MGID")
                if not related:
                    origin_post_data = dict(origin_post_data)
                    origin_post_data["关联样品MGID"] = [sample_id]
                    merged["origin_post_data"] = origin_post_data

        experiment_batch_id = str(data_package.experiment_batch_id or "").strip()
        if experiment_batch_id:
            merged.setdefault("experiment_batch_id", experiment_batch_id)

        scheduler_observation = _parse_json(data_package.scheduler_observation_json)
        if scheduler_observation:
            merged["scheduler_observation"] = _deep_merge_dicts(
                scheduler_observation,
                merged.get("scheduler_observation")
                if isinstance(merged.get("scheduler_observation"), dict)
                else {},
            )
        return merged

    def _merged_manage_db_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        merged = dict(payload)
        for key in ("scheduler_observation", "result_data", "process_data", "origin_post_data"):
            nested = payload.get(key)
            if isinstance(nested, dict):
                merged = _deep_merge_dicts(merged, nested)
        content_tree = _content_items_to_mapping(payload.get("data_content"))
        if content_tree:
            merged = _deep_merge_dicts(merged, content_tree)
        return merged

    def _sample_id_from_manage_db_payload(self, payload: dict[str, Any]) -> str | None:
        for key in (*MANAGE_DB_RELATED_SAMPLE_KEYS, *MANAGE_DB_SAMPLE_REF_KEYS):
            text = _first_text(_sample_refs(payload.get(key)))
            if text:
                return text

        flattened = _flatten_mapping(payload)
        for path, value in flattened.items():
            normalized_path = _normalized_key(path)
            if normalized_path in {_normalized_key(item) for item in MANAGE_DB_RELATED_SAMPLE_KEYS}:
                text = _first_text(_sample_refs(value))
                if text:
                    return text
        for path, value in flattened.items():
            normalized_path = _normalized_key(path)
            if normalized_path in {_normalized_key(item) for item in MANAGE_DB_SAMPLE_REF_KEYS}:
                text = _first_text(_sample_refs(value))
                if text:
                    return text
        return None

    def _grid_position_from_manage_db_payload(
        self,
        payload: dict[str, Any],
        sample_id: str,
    ) -> tuple[int | None, int | None]:
        grid_row = self._aliased_int_from_payload(payload, MANAGE_DB_GRID_ROW_ALIASES)
        grid_col = self._aliased_int_from_payload(payload, MANAGE_DB_GRID_COL_ALIASES)
        if grid_row is not None or grid_col is not None:
            return grid_row, grid_col

        matched = SAMPLE_GRID_PATTERN.search(sample_id)
        if matched is None:
            return None, None
        return int(matched.group("row")), int(matched.group("col"))

    def _phase_from_manage_db_payload(self, payload: dict[str, Any]) -> str | None:
        direct = _normalize_phase(payload.get("xrd_phase") or payload.get("phase"))
        if direct is not None:
            return direct
        return _normalize_phase(self._aliased_scalar_from_payload(payload, MANAGE_DB_PHASE_ALIASES))

    def _hardness_from_manage_db_payload(self, payload: dict[str, Any]) -> float | None:
        direct = _parse_number(payload.get("hardness_gpa"))
        if direct is not None and 0.0 < direct < 100.0:
            return round(float(direct), 6)

        aliased_value = self._aliased_scalar_from_payload(payload, MANAGE_DB_HARDNESS_ALIASES)
        hardness = _parse_number(aliased_value)
        if hardness is None:
            return None
        if hardness >= 1000.0:
            hardness /= 1000.0
        if hardness <= 0.0 or hardness >= 100.0:
            return None
        return round(float(hardness), 6)

    def _measured_composition_from_manage_db_payload(
        self,
        payload: dict[str, Any],
    ) -> dict[str, float] | None:
        direct_candidates = [
            payload.get("measured_composition_at_percent"),
            payload.get("composition_at_percent"),
        ]
        for direct in direct_candidates:
            composition = self._coerce_quaternary_mapping(direct)
            if composition is not None:
                return composition

        direct_payload = self._coerce_quaternary_mapping(payload)
        if direct_payload is not None:
            return direct_payload

        flattened = {
            _normalized_key(path): value
            for path, value in _flatten_mapping(payload).items()
            if _is_scalar(value)
        }
        composition: dict[str, float] = {}
        for payload_key, aliases in ELEMENT_FIELD_ALIASES.items():
            value = None
            for alias in aliases:
                for normalized_path, candidate in flattened.items():
                    if normalized_path == alias or normalized_path.endswith(alias):
                        parsed = _parse_number(candidate)
                        if parsed is not None:
                            value = parsed
                            break
                if value is not None:
                    break
            if value is None:
                return None
            composition[payload_key] = float(value)
        return self._normalize_quaternary_composition(composition)

    def _coerce_quaternary_mapping(self, value: Any) -> dict[str, float] | None:
        if not isinstance(value, dict):
            return None

        raw: dict[str, float] = {}
        key_map = {
            "ti_at": ("ti_at", "Ti", "ti"),
            "zr_at": ("zr_at", "Zr", "zr"),
            "nb_at": ("nb_at", "Nb", "nb"),
            "v_at": ("v_at", "V", "v"),
        }
        for payload_key, aliases in key_map.items():
            parsed_value = None
            for alias in aliases:
                parsed_value = _parse_number(value.get(alias))
                if parsed_value is not None:
                    break
            if parsed_value is None:
                return None
            raw[payload_key] = float(parsed_value)
        return self._normalize_quaternary_composition(raw)

    @staticmethod
    def _normalize_quaternary_composition(
        composition: dict[str, float],
    ) -> dict[str, float] | None:
        total = sum(float(composition[key]) for key in QUATERNARY_KEYS)
        if total <= 0:
            return None
        return {
            key: round(float(composition[key]) * 100.0 / total, 6)
            for key in QUATERNARY_KEYS
        }

    def _aliased_scalar_from_payload(
        self,
        payload: dict[str, Any],
        aliases: tuple[str, ...],
    ) -> Any:
        normalized_aliases = {_normalized_key(alias) for alias in aliases}
        flattened = _flatten_mapping(payload)
        for path, value in flattened.items():
            if not _is_scalar(value):
                continue
            normalized_path = _normalized_key(path)
            if any(
                normalized_path == alias or normalized_path.endswith(alias)
                for alias in normalized_aliases
            ):
                if value not in (None, ""):
                    return value
        return None

    def _aliased_int_from_payload(
        self,
        payload: dict[str, Any],
        aliases: tuple[str, ...],
    ) -> int | None:
        value = self._aliased_scalar_from_payload(payload, aliases)
        parsed = _parse_number(value)
        if parsed is None:
            return None
        return int(parsed)

    @staticmethod
    def _dynamic_rows_fingerprint(frame: pd.DataFrame) -> str:
        if frame.empty:
            return ""
        normalized = frame.replace({np.nan: None}).sort_values(
            by=["sample_id"],
            kind="mergesort",
        )
        payload = json.dumps(
            normalized.to_dict(orient="records"),
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()

    def refresh_candidates_from_closed_loop(
        self,
        session: Session,
    ) -> MatModelRefreshResult | None:
        base_artifacts = self.base_artifacts()
        closed_loop_frame = self._closed_loop_training_frame(
            session,
            config=base_artifacts.config,
        )
        linked_manage_db_frame, linked_manage_db_sync = self._linked_manage_db_training_frame(
            session,
            config=base_artifacts.config,
        )
        search_manage_db_frame, search_manage_db_sync = self._manage_db_training_frame(
            config=base_artifacts.config,
        )
        manage_db_frames = [
            frame
            for frame in (search_manage_db_frame, linked_manage_db_frame)
            if not frame.empty
        ]
        if manage_db_frames:
            manage_db_frame = pd.concat(manage_db_frames, ignore_index=True, sort=False)
            if "sample_id" in manage_db_frame.columns:
                manage_db_frame = manage_db_frame.drop_duplicates(
                    subset=["sample_id"],
                    keep="last",
                ).reset_index(drop=True)
        else:
            manage_db_frame = pd.DataFrame()

        manage_db_sync = {
            "enabled": bool(
                search_manage_db_sync.get("enabled") or linked_manage_db_sync.get("enabled")
            ),
            "configured": bool(
                search_manage_db_sync.get("configured") or linked_manage_db_sync.get("configured")
            ),
            "downloaded_record_count": int(
                (search_manage_db_sync.get("downloaded_record_count") or 0)
                + (linked_manage_db_sync.get("fetched_record_count") or 0)
            ),
            "parsed_record_count": int(
                (search_manage_db_sync.get("parsed_record_count") or 0)
                + (linked_manage_db_sync.get("parsed_record_count") or 0)
            ),
            "training_row_count": int(len(manage_db_frame)),
            "search_sync": search_manage_db_sync,
            "linked_record_sync": linked_manage_db_sync,
        }
        should_refresh = bool(
            self.settings.mat_model_online_refresh_enabled
            or not closed_loop_frame.empty
            or not manage_db_frame.empty
        )
        if not should_refresh:
            return None
        if closed_loop_frame.empty and manage_db_frame.empty:
            return None

        closed_loop_frame = closed_loop_frame.dropna(axis=1, how="all")
        manage_db_frame = manage_db_frame.dropna(axis=1, how="all")

        base_training_path = base_artifacts.output_dir / "cleaned_training_data.csv"
        if not base_training_path.exists():
            raise FileNotFoundError(f"cleaned training data not found: {base_training_path}")

        base_training = pd.read_csv(base_training_path)
        dynamic_frames = [frame for frame in (closed_loop_frame, manage_db_frame) if not frame.empty]
        dynamic_frame = pd.concat(dynamic_frames, ignore_index=True, sort=False)
        data_fingerprint = self._dynamic_rows_fingerprint(dynamic_frame)
        combined_training = pd.concat(
            [base_training, *dynamic_frames],
            ignore_index=True,
            sort=False,
        )
        if "sample_id" in combined_training.columns:
            combined_training = combined_training.drop_duplicates(
                subset=["sample_id"],
                keep="last",
            ).reset_index(drop=True)

        overlay_dir = self._overlay_output_dir(base_artifacts)
        overlay_dir.mkdir(parents=True, exist_ok=True)
        training_dataset_path = overlay_dir / "online_refresh_training_data.csv"
        delta_path = overlay_dir / "online_refresh_delta.csv"

        existing_summary_path = overlay_dir / "summary.json"
        if existing_summary_path.exists():
            required_overlay_files = (
                overlay_dir / "resolved_config.json",
                overlay_dir / "optimal_intervals.json",
                overlay_dir / "process_window.json",
                overlay_dir / "experimental_batch.csv",
                overlay_dir / "recommended_process_candidates.csv",
            )
            existing_summary = _read_json(existing_summary_path)
            existing_refresh = (
                existing_summary.get("online_refresh")
                if isinstance(existing_summary.get("online_refresh"), dict)
                else {}
            )
            if existing_refresh.get("data_fingerprint") == data_fingerprint and all(
                path.exists() for path in required_overlay_files
            ):
                return MatModelRefreshResult(
                    output_dir=overlay_dir,
                    training_row_count=int(existing_summary.get("n_training_rows") or 0),
                    closed_loop_sample_count=int(
                        existing_refresh.get("closed_loop_sample_count") or 0
                    ),
                    reliable_candidate_count=int(
                        existing_summary.get("n_reliable_candidates") or 0
                    ),
                    manage_db_training_row_count=int(
                        ((existing_refresh.get("manage_db_sync") or {}).get("training_row_count"))
                        or 0
                    ),
                    manage_db_downloaded_record_count=int(
                        (
                            (existing_refresh.get("manage_db_sync") or {}).get(
                                "downloaded_record_count"
                            )
                        )
                        or 0
                    ),
                )

        combined_training.to_csv(training_dataset_path, index=False)
        dynamic_frame.to_csv(delta_path, index=False)

        runtime_config = deepcopy(base_artifacts.config)
        runtime_config["data"]["path"] = str(training_dataset_path)
        runtime_config["gpr"] = deepcopy(runtime_config["gpr"])
        best_kernel = self._refresh_kernel(base_artifacts)
        if best_kernel:
            runtime_config["gpr"]["candidate_kernels"] = [best_kernel]
        runtime_config["gpr"]["cv_folds"] = min(int(runtime_config["gpr"]["cv_folds"]), 3)

        dataset = prepare_dataset(runtime_config)
        feature_matrix = dataset.feature_frame.to_numpy(dtype=float)
        target_values = dataset.target.to_numpy(dtype=float)
        model, cv_summary, oof_table = fit_best_model(
            feature_matrix,
            target_values,
            runtime_config["gpr"],
        )

        screened_candidates = self._rescored_candidates(
            model=model,
            composition_columns=dataset.composition_columns,
            preprocessing_config=runtime_config["preprocessing"],
            bo_config=runtime_config["bayes_optimization"],
            random_state=int(runtime_config["gpr"]["random_state"]),
        )
        optimal_intervals = summarize_optimal_intervals(
            screened_candidates=screened_candidates,
            composition_columns=dataset.composition_columns,
            bo_config=runtime_config["bayes_optimization"],
            top_k=int(runtime_config["outputs"]["top_k"]),
        )

        recommended_candidates = self._mapped_recommended_candidates(
            screened_candidates=screened_candidates,
            composition_columns=dataset.composition_columns,
            runtime_config=runtime_config,
        )
        batch_size = min(6, len(recommended_candidates))
        experimental_batch = (
            select_experimental_batch(
                candidates=recommended_candidates,
                batch_size=batch_size,
                pool_size=min(20, len(recommended_candidates)),
            )
            if batch_size > 0
            else pd.DataFrame()
        )
        batch_summary = (
            summarize_experimental_batch(experimental_batch)
            if not experimental_batch.empty
            else {"batch_size": 0}
        )
        process_window = map_interval_summary_to_process_window(
            interval_summary=optimal_intervals,
            process_config=runtime_config["process_mapping"],
        )

        cv_oof = dataset.model_table.reset_index(drop=True)
        cv_oof = pd.concat([cv_oof, oof_table.reset_index(drop=True)], axis=1)

        dataset.model_table.to_csv(overlay_dir / "cleaned_training_data.csv", index=False)
        dataset.dropped_rows.to_csv(overlay_dir / "dropped_rows.csv", index=False)
        cv_summary.to_csv(overlay_dir / "cv_summary.csv", index=False)
        cv_oof.to_csv(overlay_dir / "cv_oof_predictions.csv", index=False)
        screened_candidates.to_csv(overlay_dir / "screened_candidates.csv", index=False)
        recommended_candidates.to_csv(
            overlay_dir / "recommended_process_candidates.csv",
            index=False,
        )
        experimental_batch.to_csv(overlay_dir / "experimental_batch.csv", index=False)

        refresh_metadata = {
            "applied": True,
            "closed_loop_sample_count": int(len(closed_loop_frame)),
            "manage_db_sync": manage_db_sync,
            "data_fingerprint": data_fingerprint,
            "generated_at": pd.Timestamp.utcnow().isoformat(),
            "base_output_dir": str(base_artifacts.output_dir),
            "dataset_path": str(training_dataset_path),
            "delta_path": str(delta_path),
        }
        resolved_config = {
            **runtime_config,
            "_resolved_from": str(base_artifacts.resolved_config_path),
            "_output_dir": str(overlay_dir),
            "online_refresh": refresh_metadata,
        }
        summary = {
            "output_dir": str(overlay_dir),
            "n_raw_rows": int(len(dataset.raw_table)),
            "n_training_rows": int(len(dataset.model_table)),
            "n_dropped_rows": int(len(dataset.dropped_rows)),
            "best_kernel": model.kernel_name,
            "optimized_kernel": model.to_dict()["optimized_kernel"],
            "cv_rmse_mean": float(cv_summary.iloc[0]["rmse_mean"]),
            "cv_mae_mean": float(cv_summary.iloc[0]["mae_mean"]),
            "cv_r2_mean": float(cv_summary.iloc[0]["r2_mean"]),
            "n_reliable_candidates": int((screened_candidates["is_reliable"]).sum()),
            "online_refresh": refresh_metadata,
        }

        _write_json(overlay_dir / "resolved_config.json", resolved_config)
        _write_json(overlay_dir / "optimal_intervals.json", optimal_intervals)
        _write_json(overlay_dir / "process_window.json", process_window)
        _write_json(overlay_dir / "experimental_batch_summary.json", batch_summary)
        _write_json(overlay_dir / "summary.json", summary)
        _load_artifacts.cache_clear()

        return MatModelRefreshResult(
            output_dir=overlay_dir,
            training_row_count=int(len(dataset.model_table)),
            closed_loop_sample_count=int(len(closed_loop_frame)),
            reliable_candidate_count=int((screened_candidates["is_reliable"]).sum()),
            manage_db_training_row_count=int(manage_db_sync.get("training_row_count") or 0),
            manage_db_downloaded_record_count=int(
                manage_db_sync.get("downloaded_record_count") or 0
            ),
        )

    def build_schedule_payload(
        self, *, base_payload: dict[str, Any], candidate: MatModelCandidate, artifacts: MatModelArtifacts | None = None
    ) -> dict[str, Any]:
        artifacts = artifacts or self.artifacts()
        space_payload = self.composition_space_payload(artifacts=artifacts)
        payload = strip_runtime_payload_fields(base_payload)
        payload.update(space_payload["center_composition_at_percent"])
        payload["predicted_composition_at_percent"] = {
            key: round(value, 3)
            for key, value in space_payload["center_composition_at_percent"].items()
        }
        payload["predicted_composition_space"] = deepcopy(space_payload["predicted_composition_space"])
        payload["predicted_batch_plan"] = deepcopy(space_payload["predicted_batch_plan"])
        payload["composition_summary"] = space_payload["summary"]
        payload["workflow_kind"] = "tizrnbv_hardness_loop"
        payload["workflow_name"] = "Ti-Zr-Nb-V hardness loop"
        payload["model_version"] = artifacts.model_version
        payload["next_action"] = "run_mat_model_candidate"
        payload["composition_source"] = "mat_model"
        payload["mat_model_candidate_composition_at_percent"] = {
            key: round(value, 3) for key, value in candidate.composition.items()
        }
        payload["mat_model_candidate_summary"] = candidate.composition_summary
        payload["mat_model_candidate_source"] = candidate.source
        payload["mat_model_candidate_rank"] = candidate.rank
        payload["mat_model_selection_role"] = candidate.selection_role
        payload["mat_model_selection_role_label"] = candidate.role_label
        payload["mat_model_predicted_hardness_gpa"] = _rounded(candidate.predicted_hardness_gpa)
        payload["mat_model_predicted_std_gpa"] = _rounded(candidate.predicted_std_gpa)
        payload["mat_model_reward"] = _rounded(candidate.reward, digits=4)
        payload["mat_model_config_path"] = str(artifacts.resolved_config_path)
        payload["mat_model_output_dir"] = str(artifacts.output_dir)
        payload["input_dataset_path"] = artifacts.input_dataset_path
        payload["input_dataset_version"] = artifacts.input_dataset_path
        payload["mat_model_cv_rmse_gpa"] = _rounded(artifacts.summary.get("cv_rmse_mean"), digits=3)
        payload["mat_model_cv_r2"] = _rounded(artifacts.summary.get("cv_r2_mean"), digits=3)
        payload["mat_model_reliable_candidate_count"] = artifacts.summary.get(
            "n_reliable_candidates"
        )

        refresh = artifacts.online_refresh_metadata
        if refresh and refresh.get("applied"):
            payload["mat_model_online_refresh_applied"] = True
            payload["mat_model_online_refresh_sample_count"] = refresh.get(
                "closed_loop_sample_count"
            )
            payload["mat_model_online_refresh_generated_at"] = refresh.get("generated_at")
            manage_db_sync = refresh.get("manage_db_sync")
            if isinstance(manage_db_sync, dict):
                payload["mat_model_manage_db_training_row_count"] = manage_db_sync.get(
                    "training_row_count"
                )
                payload["mat_model_manage_db_downloaded_record_count"] = manage_db_sync.get(
                    "downloaded_record_count"
                )

        power_plan = {
            element: _rounded(candidate.row.get(f"{element}_power_w"), digits=3)
            for element in MODEL_TO_PAYLOAD_KEYS
            if candidate.row.get(f"{element}_power_w") is not None
        }
        power_share_plan = {
            element: _rounded(candidate.row.get(f"{element}_power_share"), digits=4)
            for element in MODEL_TO_PAYLOAD_KEYS
            if candidate.row.get(f"{element}_power_share") is not None
        }
        if power_plan:
            payload["sputter_power_plan_w"] = power_plan
        if power_share_plan:
            payload["sputter_power_share"] = power_share_plan

        for key in PROCESS_PARAMETER_KEYS:
            if candidate.row.get(key) is not None:
                payload[key] = candidate.row[key]

        payload["high_throughput_batch_size"] = space_payload["predicted_batch_plan"]["total_samples"]
        payload["high_throughput_grid_rows"] = space_payload["predicted_batch_plan"]["rows"]
        payload["high_throughput_grid_cols"] = space_payload["predicted_batch_plan"]["cols"]
        payload.setdefault("objective", "maximize hardness with the merged mat_model pipeline")
        return normalize_tizrnbv_payload(payload)

    def pick_next_candidate(
        self,
        *,
        used_payloads: Iterable[dict[str, Any]],
        preferred_rank: int | None = None,
        artifacts: MatModelArtifacts | None = None,
    ) -> MatModelCandidate | None:
        artifacts = artifacts or self.artifacts()
        used_signatures = self._used_signatures(used_payloads)

        experimental_batch = [
            self._candidate_from_row(
                row,
                source="experimental_batch",
                default_rank=int(row.get("batch_rank") or index),
            )
            for index, row in enumerate(artifacts.experimental_batch, start=1)
        ]
        if preferred_rank is not None:
            for candidate in experimental_batch:
                if candidate.rank == preferred_rank and candidate.signature not in used_signatures:
                    return candidate

        for candidate in experimental_batch:
            if candidate.signature not in used_signatures:
                return candidate

        for index, row in enumerate(artifacts.recommended_candidates, start=1):
            candidate = self._candidate_from_row(
                row,
                source="recommended_process_candidates",
                default_rank=index,
            )
            if candidate.signature not in used_signatures:
                return candidate
        return None

    def pick_local_candidate(
        self,
        *,
        anchor_composition: dict[str, float],
        used_payloads: Iterable[dict[str, Any]],
        artifacts: MatModelArtifacts | None = None,
    ) -> MatModelCandidate | None:
        artifacts = artifacts or self.artifacts()
        used_signatures = self._used_signatures(used_payloads)
        candidates = [
            self._candidate_from_row(
                row,
                source="recommended_process_candidates",
                default_rank=index,
            )
            for index, row in enumerate(artifacts.recommended_candidates, start=1)
        ]
        anchor = tuple(float(anchor_composition[key]) for key in QUATERNARY_KEYS)
        nearest: tuple[float, float, MatModelCandidate] | None = None
        for candidate in candidates:
            if candidate.signature in used_signatures:
                continue
            distance = math.sqrt(
                sum(
                    (candidate.composition[key] - anchor_value) ** 2
                    for key, anchor_value in zip(QUATERNARY_KEYS, anchor)
                )
            )
            std = candidate.predicted_std_gpa or 0.0
            current = (distance, std, candidate)
            if nearest is None or current < nearest:
                nearest = current
        return None if nearest is None else nearest[2]

    @staticmethod
    def confidence_for_candidate(candidate: MatModelCandidate | None) -> float:
        if candidate is None or candidate.predicted_std_gpa is None:
            return 0.52
        return round(max(0.45, min(0.92, 0.92 - candidate.predicted_std_gpa * 0.14)), 2)

    @staticmethod
    def _candidate_from_row(
        row: dict[str, Any], *, source: str, default_rank: int
    ) -> MatModelCandidate:
        rank_value = row.get("batch_rank") or row.get("candidate_rank") or default_rank
        rank = None if rank_value is None else int(rank_value)
        selection_role = row.get("selection_role")
        selection_role = None if selection_role is None else str(selection_role)
        return MatModelCandidate(
            source=source,
            rank=rank,
            selection_role=selection_role,
            row=row,
        )

    @staticmethod
    def _used_signatures(
        payloads: Iterable[dict[str, Any]],
    ) -> set[tuple[float, float, float, float]]:
        used: set[tuple[float, float, float, float]] = set()
        for payload in payloads:
            candidate_composition = payload.get("mat_model_candidate_composition_at_percent")
            if isinstance(candidate_composition, dict):
                composition = {
                    key: float(candidate_composition[key])
                    for key in QUATERNARY_KEYS
                    if candidate_composition.get(key) is not None
                }
                if len(composition) != len(QUATERNARY_KEYS):
                    composition = extract_predicted_composition(payload)
            else:
                composition = extract_predicted_composition(payload)
            if composition is None:
                continue
            used.add(tuple(round(float(composition[key]), 3) for key in QUATERNARY_KEYS))
        return used

    def _effective_output_dir(self) -> Path:
        base_output_dir = self._base_output_dir()
        overlay_dir = self._overlay_output_dir_from_path(base_output_dir)
        required_overlay_files = (
            overlay_dir / "resolved_config.json",
            overlay_dir / "summary.json",
            overlay_dir / "optimal_intervals.json",
            overlay_dir / "process_window.json",
            overlay_dir / "experimental_batch.csv",
            overlay_dir / "recommended_process_candidates.csv",
        )
        if all(path.exists() for path in required_overlay_files):
            return overlay_dir
        return base_output_dir

    def _base_output_dir(self) -> Path:
        config, _ = load_config(str(Path(self.settings.mat_model_config_path).resolve()))
        return _resolve_output_dir(config, str(Path(self.settings.mat_model_output_dir).resolve()))

    def _overlay_output_dir(self, artifacts: MatModelArtifacts) -> Path:
        return self._overlay_output_dir_from_path(artifacts.output_dir)

    def _overlay_output_dir_from_path(self, base_output_dir: Path) -> Path:
        key = hashlib.sha1(
            "|".join(
                [
                    str(Path(self.settings.mat_model_config_path).resolve()),
                    str(base_output_dir.resolve()),
                    self.settings.database_url,
                ]
            ).encode("utf-8")
        ).hexdigest()[:12]
        return Path(tempfile.gettempdir()) / "smart_scheduler_mat_model" / key / base_output_dir.name

    def _refresh_kernel(self, artifacts: MatModelArtifacts) -> str | None:
        best_kernel = artifacts.summary.get("best_kernel")
        return None if best_kernel in (None, "") else str(best_kernel)

    def _rescored_candidates(
        self,
        *,
        model,
        composition_columns: list[str],
        preprocessing_config: dict[str, Any],
        bo_config: dict[str, Any],
        random_state: int,
    ) -> pd.DataFrame:
        source_path = self._screened_candidates_source_path()
        if source_path is not None and source_path.exists():
            source_frame = pd.read_csv(source_path)
        else:
            source_frame = screen_candidate_space(
                model=model,
                preprocessing_config=preprocessing_config,
                bo_config=bo_config,
                random_state=random_state,
            )

        candidates = source_frame[composition_columns].copy().drop_duplicates().reset_index(drop=True)
        feature_frame = _compositions_to_features(candidates, preprocessing_config)
        mean, std = model.predict(feature_frame.to_numpy(dtype=float), return_std=True)
        reward = predictive_reward(mean, std, bo_config)

        rescored = candidates.copy()
        rescored["predicted_hardness_gpa"] = mean
        rescored["predicted_std_gpa"] = std
        rescored["reward"] = reward
        rescored["is_reliable"] = (
            (rescored["predicted_hardness_gpa"] >= float(bo_config["hardness_target"]))
            & (rescored["predicted_std_gpa"] <= float(bo_config["sigma_threshold"]))
        )
        return rescored.sort_values(
            by=["reward", "predicted_hardness_gpa"],
            ascending=[False, False],
        ).reset_index(drop=True)

    def _mapped_recommended_candidates(
        self,
        *,
        screened_candidates: pd.DataFrame,
        composition_columns: list[str],
        runtime_config: dict[str, Any],
    ) -> pd.DataFrame:
        top_k = int(runtime_config["outputs"]["top_k"])
        reliable_candidates = screened_candidates[screened_candidates["is_reliable"] == True].copy()
        if reliable_candidates.empty:
            reliable_candidates = screened_candidates.head(top_k).copy()
        else:
            reliable_candidates = reliable_candidates.head(top_k).copy()

        if not runtime_config["process_mapping"]["enabled"]:
            return reliable_candidates.reset_index(drop=True)

        mapped = map_candidates_to_process_parameters(
            candidates=reliable_candidates,
            composition_columns=composition_columns,
            process_config=runtime_config["process_mapping"],
        )
        return mapped.reset_index(drop=True)

    def _screened_candidates_source_path(self) -> Path | None:
        for output_dir in (self._effective_output_dir(), self._base_output_dir()):
            candidate_path = output_dir / "screened_candidates.csv"
            if candidate_path.exists():
                return candidate_path
        return None

    def _closed_loop_training_frame(
        self,
        session: Session,
        *,
        config: dict[str, Any],
    ) -> pd.DataFrame:
        filters = config.get("data", {}).get("filters", {})
        rows: list[dict[str, Any]] = []
        executions = list(
            session.scalars(
                select(Execution)
                .where(Execution.status == ExecutionStatus.SUCCESS.value)
                .order_by(Execution.closed_at.asc(), Execution.dispatched_at.asc())
            )
        )
        eligible_executions: list[Execution] = []
        for execution in executions:
            dispatch_payload = _parse_json(execution.dispatch_payload)
            if not is_tizrnbv_hardness_loop(dispatch_payload):
                continue

            observation = _parse_json(execution.feedback_observation)
            if not bool(observation.get("closed_loop_ready")):
                continue
            eligible_executions.append(execution)

        data_packages_by_execution: dict[str, list[DataPackage]] = {}
        if eligible_executions:
            execution_ids = [execution.id for execution in eligible_executions]
            linked_packages = list(
                session.scalars(
                    select(DataPackage)
                    .where(DataPackage.execution_id.in_(execution_ids))
                    .order_by(DataPackage.created_at.asc())
                )
            )
            for data_package in linked_packages:
                data_packages_by_execution.setdefault(data_package.execution_id, []).append(
                    data_package
                )

        for execution in eligible_executions:
            dispatch_payload = _parse_json(execution.dispatch_payload)
            batch_rows = self._closed_loop_batch_training_rows(
                data_packages=data_packages_by_execution.get(execution.id, []),
                execution=execution,
                filters=filters,
            )
            if batch_rows:
                rows.extend(batch_rows)
                continue

            observation = _parse_json(execution.feedback_observation)

            measured = measured_composition_from_observation(observation)
            hardness = _parse_number(observation.get("hardness_gpa"))
            phase = _normalize_phase(observation.get("xrd_phase"))
            if measured is None or hardness is None or phase is None:
                continue

            row = {
                "sample_id": str(
                    observation.get("deposition_sample_id")
                    or dispatch_payload.get("experiment_batch_id")
                    or execution.id
                ),
                "grid_row": None,
                "grid_col": None,
                "phase": phase,
                "Ti": round(float(measured["ti_at"]), 6),
                "Zr": round(float(measured["zr_at"]), 6),
                "Nb": round(float(measured["nb_at"]), 6),
                "V": round(float(measured["v_at"]), 6),
                "hardness_mpa_raw": round(float(hardness) * 1000.0, 6),
                "source": "closed_loop_feedback",
                "composition_sum": round(sum(float(measured[key]) for key in QUATERNARY_KEYS), 6),
                "hardness_is_nan": False,
                "hardness_is_instrument_floor": False,
                "hardness_valid": True,
                "hardness_gpa": round(float(hardness), 6),
                "notes": f"schedule_id={execution.schedule_id};execution_id={execution.id}",
                "_online_source_execution_id": execution.id,
            }
            if self._passes_filters(row, filters):
                rows.append(row)

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).drop_duplicates(subset=["sample_id"], keep="last").reset_index(
            drop=True
        )

    def _closed_loop_batch_training_rows(
        self,
        *,
        data_packages: list[DataPackage],
        execution: Execution,
        filters: dict[str, Any],
    ) -> list[dict[str, Any]]:
        aggregated: dict[str, dict[str, Any]] = {}
        package_ids: list[str] = []

        for data_package in data_packages:
            scheduler_observation = _parse_json(data_package.scheduler_observation_json)
            if not scheduler_observation:
                continue

            package_ids.append(data_package.id)
            instrument_code = str(data_package.instrument_code or "").strip().lower()
            grid_key = {
                "eds": "eds_batch_grid",
                "nanoindentation": "nanoindentation_batch_grid",
                "xrd": "xrd_phase_grid",
            }.get(instrument_code)
            batch_grid = (
                scheduler_observation.get(grid_key)
                if grid_key and isinstance(scheduler_observation.get(grid_key), dict)
                else None
            )
            samples = batch_grid.get("samples") if isinstance(batch_grid, dict) else None
            if not isinstance(samples, list):
                continue

            for sample in samples:
                if not isinstance(sample, dict):
                    continue
                sample_id = str(sample.get("sample_id") or "").strip()
                if not sample_id:
                    continue
                aggregate = aggregated.setdefault(
                    sample_id,
                    {
                        "sample_id": sample_id,
                        "grid_row": self._coerce_optional_int(sample.get("grid_row")),
                        "grid_col": self._coerce_optional_int(sample.get("grid_col")),
                        "phase": None,
                        "measured": None,
                        "hardness_gpa": None,
                    },
                )
                if aggregate.get("grid_row") is None:
                    aggregate["grid_row"] = self._coerce_optional_int(sample.get("grid_row"))
                if aggregate.get("grid_col") is None:
                    aggregate["grid_col"] = self._coerce_optional_int(sample.get("grid_col"))

                if instrument_code == "eds":
                    measured = self._measured_composition_from_manage_db_payload(sample)
                    if measured is not None:
                        aggregate["measured"] = measured
                elif instrument_code == "nanoindentation":
                    hardness = self._hardness_from_manage_db_payload(sample)
                    if hardness is not None:
                        aggregate["hardness_gpa"] = hardness
                elif instrument_code == "xrd":
                    phase = _normalize_phase(sample.get("xrd_phase"))
                    if phase is not None:
                        aggregate["phase"] = phase

        rows: list[dict[str, Any]] = []
        package_note = ",".join(package_ids)
        for aggregate in aggregated.values():
            measured = aggregate.get("measured")
            hardness = aggregate.get("hardness_gpa")
            phase = aggregate.get("phase")
            if measured is None or hardness is None or phase is None:
                continue

            row = {
                "sample_id": aggregate["sample_id"],
                "grid_row": aggregate.get("grid_row"),
                "grid_col": aggregate.get("grid_col"),
                "phase": phase,
                "Ti": round(float(measured["ti_at"]), 6),
                "Zr": round(float(measured["zr_at"]), 6),
                "Nb": round(float(measured["nb_at"]), 6),
                "V": round(float(measured["v_at"]), 6),
                "hardness_mpa_raw": round(float(hardness) * 1000.0, 6),
                "source": "closed_loop_batch_feedback",
                "composition_sum": round(sum(float(measured[key]) for key in QUATERNARY_KEYS), 6),
                "hardness_is_nan": False,
                "hardness_is_instrument_floor": False,
                "hardness_valid": True,
                "hardness_gpa": round(float(hardness), 6),
                "notes": (
                    f"schedule_id={execution.schedule_id};execution_id={execution.id};"
                    f"data_package_ids={package_note}"
                ),
                "_online_source_execution_id": execution.id,
            }
            if self._passes_filters(row, filters):
                rows.append(row)
        return rows

    @staticmethod
    def _passes_filters(row: dict[str, Any], filters: dict[str, Any]) -> bool:
        for column, allowed in filters.items():
            value = row.get(column)
            if isinstance(value, str):
                value = value.lower()
            if isinstance(allowed, list):
                normalized_allowed = [
                    item.lower() if isinstance(item, str) else item for item in allowed
                ]
                if value not in normalized_allowed:
                    return False
            else:
                normalized_allowed = allowed.lower() if isinstance(allowed, str) else allowed
                if value != normalized_allowed:
                    return False
        return True


def _rounded(value: Any, *, digits: int = 2) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)
