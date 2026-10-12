from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import norm

from .composition import clr_transform, normalize_compositions, replace_zeros_and_renormalize, sample_bounded_simplex
from .gpr import GPRSurrogate


def _compositions_to_features(
    composition_frame: pd.DataFrame,
    preprocessing_config: dict[str, Any],
) -> pd.DataFrame:
    normalized = normalize_compositions(
        composition_frame,
        closure_target=float(preprocessing_config["closure_target"]),
    )
    normalized = replace_zeros_and_renormalize(
        normalized,
        zero_replacement=float(preprocessing_config["zero_replacement"]),
        closure_target=float(preprocessing_config["closure_target"]),
    )
    if preprocessing_config["use_clr"]:
        return clr_transform(normalized)
    return normalized


def predictive_reward(
    mean: np.ndarray,
    std: np.ndarray,
    bo_config: dict[str, Any],
    valid_mask: np.ndarray | None = None,
) -> np.ndarray:
    if valid_mask is None:
        valid_mask = np.ones_like(mean, dtype=bool)

    reward = np.full_like(mean, float(bo_config["invalid_penalty"]), dtype=float)
    target = float(bo_config["hardness_target"])
    sigma_threshold = float(bo_config["sigma_threshold"])

    below_target = mean < target
    valid_above_target = valid_mask & ~below_target
    valid_below_target = valid_mask & below_target

    reward[valid_below_target] = float(bo_config["invalid_penalty"]) - (
        target - mean[valid_below_target]
    ) * float(bo_config["below_target_penalty_scale"])

    if np.any(valid_above_target):
        hardness_gain = (mean[valid_above_target] - target) * float(bo_config["hardness_reward_weight"])
        confidence_bonus = np.maximum(sigma_threshold - std[valid_above_target], 0.0) * float(
            bo_config["confidence_reward_weight"]
        )
        confidence_penalty = np.maximum(std[valid_above_target] - sigma_threshold, 0.0) * float(
            bo_config["confidence_penalty_weight"]
        )
        reward[valid_above_target] = hardness_gain + confidence_bonus - confidence_penalty

    return reward


def expected_improvement(mean: np.ndarray, std: np.ndarray, incumbent: float, xi: float) -> np.ndarray:
    safe_std = np.maximum(std, 1e-12)
    improvement = mean - incumbent - xi
    z_score = improvement / safe_std
    ei = improvement * norm.cdf(z_score) + safe_std * norm.pdf(z_score)
    ei[std < 1e-12] = 0.0
    return ei


def upper_confidence_bound(mean: np.ndarray, std: np.ndarray, kappa: float) -> np.ndarray:
    return mean + kappa * std


def _candidate_key(values: np.ndarray, digits: int = 6) -> tuple[float, ...]:
    return tuple(np.round(values.astype(float), digits))


def _clone_surrogate(model: GPRSurrogate) -> GPRSurrogate:
    return GPRSurrogate(
        kernel_name=model.kernel_name,
        alpha=model.alpha,
        normalize_y=model.normalize_y,
        n_restarts_optimizer=model.n_restarts_optimizer,
        random_state=model.random_state,
    )


def _sample_unique_pool(
    bounds: dict[str, list[float]],
    total: float,
    target_count: int,
    random_state: int,
    seen: set[tuple[float, ...]],
) -> pd.DataFrame:
    pool = sample_bounded_simplex(
        bounds=bounds,
        total=total,
        n_samples=max(target_count * 2, target_count),
        random_state=random_state,
    )
    keep_mask = [
        _candidate_key(row.to_numpy()) not in seen
        for _, row in pool.iterrows()
    ]
    unique_pool = pool.loc[keep_mask].drop_duplicates().reset_index(drop=True)
    if len(unique_pool) < target_count:
        return unique_pool
    return unique_pool.iloc[:target_count].copy()


def run_bayesian_optimization(
    base_model: GPRSurrogate,
    training_X: np.ndarray,
    training_y: np.ndarray,
    preprocessing_config: dict[str, Any],
    bo_config: dict[str, Any],
    random_state: int,
) -> pd.DataFrame:
    bounds = bo_config["bounds"]
    total = float(bo_config["fixed_sum"])
    acquisition_name = str(bo_config["acquisition"]).lower()

    history_rows: list[dict[str, float | int | str]] = []
    seen: set[tuple[float, ...]] = set()

    state_X = training_X.copy()
    state_y = training_y.copy()

    n_random_starts = int(bo_config["n_random_starts"])
    if n_random_starts > 0:
        initial_pool = _sample_unique_pool(
            bounds=bounds,
            total=total,
            target_count=n_random_starts,
            random_state=random_state + 101,
            seen=seen,
        )
        init_features = _compositions_to_features(initial_pool, preprocessing_config).to_numpy(dtype=float)
        init_mean, init_std = base_model.predict(init_features, return_std=True)
        init_reward = predictive_reward(init_mean, init_std, bo_config)
        for row_idx, (_, composition) in enumerate(initial_pool.iterrows()):
            comp_values = composition.to_numpy(dtype=float)
            seen.add(_candidate_key(comp_values))
            history_rows.append(
                {
                    "stage": "random_start",
                    "iteration": row_idx,
                    **composition.to_dict(),
                    "predicted_hardness_gpa": float(init_mean[row_idx]),
                    "predicted_std_gpa": float(init_std[row_idx]),
                    "reward": float(init_reward[row_idx]),
                    "acquisition": np.nan,
                    "score": float(init_reward[row_idx]),
                }
            )
        state_X = np.vstack([state_X, init_features])
        state_y = np.concatenate([state_y, init_mean])

    state_model = _clone_surrogate(base_model).fit(state_X, state_y)

    for iteration in range(int(bo_config["n_iterations"])):
        pool = _sample_unique_pool(
            bounds=bounds,
            total=total,
            target_count=int(bo_config["n_acquisition_candidates"]),
            random_state=random_state + 1000 + iteration,
            seen=seen,
        )
        if pool.empty:
            break

        feature_frame = _compositions_to_features(pool, preprocessing_config)
        candidate_X = feature_frame.to_numpy(dtype=float)
        mean, std = state_model.predict(candidate_X, return_std=True)
        reward = predictive_reward(mean, std, bo_config)

        incumbent = float(np.max(state_y))
        if acquisition_name == "ei":
            acquisition = expected_improvement(mean, std, incumbent=incumbent, xi=float(bo_config["xi"]))
        elif acquisition_name == "ucb":
            acquisition = upper_confidence_bound(mean, std, kappa=float(bo_config["kappa"]))
        else:
            raise ValueError(f"Unsupported acquisition function: {acquisition_name}")

        score = reward + float(bo_config["acquisition_weight"]) * acquisition
        best_idx = int(np.argmax(score))

        selected_comp = pool.iloc[best_idx]
        selected_X = candidate_X[[best_idx]]
        selected_mean, selected_std = base_model.predict(selected_X, return_std=True)
        seen.add(_candidate_key(selected_comp.to_numpy(dtype=float)))

        history_rows.append(
            {
                "stage": "bo",
                "iteration": iteration,
                **selected_comp.to_dict(),
                "predicted_hardness_gpa": float(selected_mean[0]),
                "predicted_std_gpa": float(selected_std[0]),
                "reward": float(reward[best_idx]),
                "acquisition": float(acquisition[best_idx]),
                "score": float(score[best_idx]),
            }
        )

        state_X = np.vstack([state_X, selected_X])
        state_y = np.concatenate([state_y, selected_mean])
        state_model = _clone_surrogate(base_model).fit(state_X, state_y)

    return pd.DataFrame(history_rows)


def screen_candidate_space(
    model: GPRSurrogate,
    preprocessing_config: dict[str, Any],
    bo_config: dict[str, Any],
    random_state: int,
) -> pd.DataFrame:
    pool = sample_bounded_simplex(
        bounds=bo_config["bounds"],
        total=float(bo_config["fixed_sum"]),
        n_samples=int(bo_config["n_screening_candidates"]),
        random_state=random_state + 9999,
    )
    feature_frame = _compositions_to_features(pool, preprocessing_config)
    candidate_X = feature_frame.to_numpy(dtype=float)
    mean, std = model.predict(candidate_X, return_std=True)
    reward = predictive_reward(mean, std, bo_config)

    screened = pool.copy()
    screened["predicted_hardness_gpa"] = mean
    screened["predicted_std_gpa"] = std
    screened["reward"] = reward
    screened["is_reliable"] = (
        (screened["predicted_hardness_gpa"] >= float(bo_config["hardness_target"]))
        & (screened["predicted_std_gpa"] <= float(bo_config["sigma_threshold"]))
    )
    return screened.sort_values(by=["reward", "predicted_hardness_gpa"], ascending=[False, False]).reset_index(drop=True)


def summarize_optimal_intervals(
    screened_candidates: pd.DataFrame,
    composition_columns: list[str],
    bo_config: dict[str, Any],
    top_k: int,
) -> dict[str, Any]:
    reliable = screened_candidates[screened_candidates["is_reliable"]].copy()
    used_fallback = False
    if reliable.empty:
        reliable = screened_candidates.head(top_k).copy()
        used_fallback = True
    else:
        reliable = reliable.head(top_k).copy()

    summary: dict[str, Any] = {
        "n_reliable_candidates": int((screened_candidates["is_reliable"]).sum()),
        "used_fallback_top_k": used_fallback,
        "selection_rule": {
            "hardness_target_gpa": float(bo_config["hardness_target"]),
            "sigma_threshold_gpa": float(bo_config["sigma_threshold"]),
            "top_k": int(top_k),
        },
        "elements": {},
    }

    for column in composition_columns:
        values = reliable[column].to_numpy(dtype=float)
        summary["elements"][column] = {
            "min": float(np.min(values)),
            "p10": float(np.quantile(values, 0.10)),
            "median": float(np.median(values)),
            "p90": float(np.quantile(values, 0.90)),
            "max": float(np.max(values)),
            "mean": float(np.mean(values)),
        }

    return summary
