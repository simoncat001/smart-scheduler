from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any


DEFAULT_CONFIG: dict[str, Any] = {
    "data": {
        "path": "",
        "sheet_name": None,
        "composition_columns": [],
        "target_column": "",
        "id_column": None,
        "filters": {},
        "extra_keep_columns": [],
    },
    "preprocessing": {
        "closure_target": 100.0,
        "composition_min": 0.0,
        "composition_max": 100.0,
        "use_clr": True,
        "zero_replacement": 1e-6,
        "outlier_method": "isolation_forest",
        "outlier_fraction": 0.08,
        "hardness_iqr_multiplier": 1.5,
    },
    "gpr": {
        "candidate_kernels": ["matern32", "matern52", "rbf", "rq"],
        "cv_folds": 5,
        "alpha": 1e-6,
        "n_restarts_optimizer": 10,
        "normalize_y": True,
        "random_state": 42,
    },
    "bayes_optimization": {
        "bounds": {},
        "fixed_sum": 100.0,
        "n_random_starts": 20,
        "n_iterations": 40,
        "n_acquisition_candidates": 5000,
        "n_screening_candidates": 20000,
        "acquisition": "ei",
        "acquisition_weight": 0.25,
        "xi": 0.01,
        "kappa": 1.96,
        "hardness_target": 6.0,
        "sigma_threshold": 0.5,
        "hardness_reward_weight": 1.0,
        "confidence_reward_weight": 0.5,
        "confidence_penalty_weight": 1.0,
        "below_target_penalty_scale": 20.0,
        "invalid_penalty": -100.0,
    },
    "outputs": {
        "directory": "outputs",
        "run_name": None,
        "top_k": 20,
    },
    "process_mapping": {
        "enabled": False,
        "mode": "equal_yield_power_share",
        "total_power_w": 200.0,
        "fixed_parameters": {
            "argon_flow_sccm": 20.0,
            "pressure_pa": 0.6,
            "substrate_bias_v": 0.0,
            "substrate_temperature_c": 25.0,
        },
    },
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _resolve_path(raw_path: str, base_dir: Path) -> str:
    path = Path(raw_path)
    if not raw_path:
        return raw_path
    if path.is_absolute():
        return str(path)
    return str((base_dir / path).resolve())


def load_config(config_path: str | Path) -> tuple[dict[str, Any], Path]:
    resolved_path = Path(config_path).resolve()
    with resolved_path.open("r", encoding="utf-8") as handle:
        user_config = json.load(handle)

    config = _deep_merge(DEFAULT_CONFIG, user_config)
    config_dir = resolved_path.parent

    if config["data"]["path"]:
        config["data"]["path"] = _resolve_path(config["data"]["path"], config_dir)

    output_dir = config["outputs"]["directory"]
    config["outputs"]["directory"] = _resolve_path(output_dir, config_dir)

    return config, resolved_path
