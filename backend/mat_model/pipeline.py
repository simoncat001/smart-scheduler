from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import joblib
import matplotlib
import numpy as np
import pandas as pd

from .bo import run_bayesian_optimization, screen_candidate_space, summarize_optimal_intervals
from .config import load_config
from .data import prepare_dataset
from .gpr import fit_best_model
from .process_mapping import map_candidates_to_process_parameters, map_interval_summary_to_process_window


matplotlib.use("Agg")
import matplotlib.pyplot as plt


def _json_default(value: Any):
    if isinstance(value, (np.floating, np.integer)):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable.")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=_json_default),
        encoding="utf-8",
    )


def _plot_bo_convergence(history: pd.DataFrame, output_path: Path) -> None:
    if history.empty:
        return
    plt.figure(figsize=(8, 4.5))
    trace = history.loc[history["stage"] == "bo", "score"].to_numpy(dtype=float)
    if trace.size == 0:
        trace = history["score"].to_numpy(dtype=float)
    best_so_far = np.maximum.accumulate(trace)
    plt.plot(np.arange(1, len(trace) + 1), trace, marker="o", linewidth=1.2, label="Iteration Score")
    plt.plot(np.arange(1, len(best_so_far) + 1), best_so_far, linewidth=2.0, label="Best Score So Far")
    plt.xlabel("Iteration")
    plt.ylabel("Score")
    plt.title("Bayesian Optimization Convergence")
    plt.grid(alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=180)
    plt.close()


def run_pipeline(config_path: str | Path) -> dict[str, Any]:
    config, resolved_config_path = load_config(config_path)
    random_state = int(config["gpr"]["random_state"])

    raw_output_dir = Path(config["outputs"]["directory"])
    run_name = config["outputs"]["run_name"] or datetime.now().strftime("run_%Y%m%d_%H%M%S")
    output_dir = raw_output_dir / run_name
    output_dir.mkdir(parents=True, exist_ok=True)

    dataset = prepare_dataset(config)
    X = dataset.feature_frame.to_numpy(dtype=float)
    y = dataset.target.to_numpy(dtype=float)

    model, cv_summary, oof_table = fit_best_model(X, y, config["gpr"])
    train_mean, train_std = model.predict(X, return_std=True)

    train_predictions = dataset.model_table.copy()
    train_predictions["train_prediction_gpa"] = train_mean
    train_predictions["train_prediction_std_gpa"] = train_std
    train_predictions["train_residual_gpa"] = y - train_mean

    bo_history = run_bayesian_optimization(
        base_model=model,
        training_X=X,
        training_y=y,
        preprocessing_config=config["preprocessing"],
        bo_config=config["bayes_optimization"],
        random_state=random_state,
    )
    screened_candidates = screen_candidate_space(
        model=model,
        preprocessing_config=config["preprocessing"],
        bo_config=config["bayes_optimization"],
        random_state=random_state,
    )
    interval_summary = summarize_optimal_intervals(
        screened_candidates=screened_candidates,
        composition_columns=dataset.composition_columns,
        bo_config=config["bayes_optimization"],
        top_k=int(config["outputs"]["top_k"]),
    )

    cv_oof = dataset.model_table.copy()
    cv_oof = cv_oof.reset_index(drop=True)
    cv_oof = pd.concat([cv_oof, oof_table.reset_index(drop=True)], axis=1)

    dataset.model_table.to_csv(output_dir / "cleaned_training_data.csv", index=False)
    dataset.dropped_rows.to_csv(output_dir / "dropped_rows.csv", index=False)
    train_predictions.to_csv(output_dir / "train_predictions.csv", index=False)
    cv_summary.to_csv(output_dir / "cv_summary.csv", index=False)
    cv_oof.to_csv(output_dir / "cv_oof_predictions.csv", index=False)
    bo_history.to_csv(output_dir / "bo_history.csv", index=False)
    screened_candidates.to_csv(output_dir / "screened_candidates.csv", index=False)
    _plot_bo_convergence(bo_history, output_dir / "bo_convergence.png")

    if config["process_mapping"]["enabled"]:
        reliable_candidates = screened_candidates[screened_candidates["is_reliable"]].copy()
        if reliable_candidates.empty:
            reliable_candidates = screened_candidates.head(int(config["outputs"]["top_k"])).copy()
        else:
            reliable_candidates = reliable_candidates.head(int(config["outputs"]["top_k"])).copy()

        mapped_candidates = map_candidates_to_process_parameters(
            candidates=reliable_candidates,
            composition_columns=dataset.composition_columns,
            process_config=config["process_mapping"],
        )
        mapped_candidates.to_csv(output_dir / "recommended_process_candidates.csv", index=False)
        process_window = map_interval_summary_to_process_window(
            interval_summary=interval_summary,
            process_config=config["process_mapping"],
        )
        _write_json(output_dir / "process_window.json", process_window)

    joblib.dump(
        {
            "model": model,
            "feature_columns": dataset.feature_columns,
            "composition_columns": dataset.composition_columns,
            "target_column": dataset.target_column,
            "config": config,
        },
        output_dir / "gpr_model.joblib",
    )

    resolved_config = {
        **config,
        "_resolved_from": str(resolved_config_path),
        "_output_dir": str(output_dir),
    }
    _write_json(output_dir / "resolved_config.json", resolved_config)
    _write_json(output_dir / "optimal_intervals.json", interval_summary)

    summary = {
        "output_dir": str(output_dir),
        "n_raw_rows": int(len(dataset.raw_table)),
        "n_training_rows": int(len(dataset.model_table)),
        "n_dropped_rows": int(len(dataset.dropped_rows)),
        "best_kernel": model.kernel_name,
        "optimized_kernel": model.to_dict()["optimized_kernel"],
        "cv_rmse_mean": float(cv_summary.iloc[0]["rmse_mean"]),
        "cv_mae_mean": float(cv_summary.iloc[0]["mae_mean"]),
        "cv_r2_mean": float(cv_summary.iloc[0]["r2_mean"]),
        "n_reliable_candidates": int(interval_summary["n_reliable_candidates"]),
    }
    _write_json(output_dir / "summary.json", summary)
    return summary
