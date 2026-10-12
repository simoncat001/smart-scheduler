from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np
import pandas as pd


def normalize_compositions(
    frame: pd.DataFrame,
    closure_target: float = 100.0,
) -> pd.DataFrame:
    totals = frame.sum(axis=1)
    if (totals <= 0).any():
        raise ValueError("Composition rows with non-positive total cannot be normalized.")
    normalized = frame.div(totals, axis=0) * closure_target
    return normalized


def replace_zeros_and_renormalize(
    frame: pd.DataFrame,
    zero_replacement: float,
    closure_target: float,
) -> pd.DataFrame:
    adjusted = frame.copy()
    adjusted[adjusted <= 0] = zero_replacement
    return normalize_compositions(adjusted, closure_target=closure_target)


def clr_transform(frame: pd.DataFrame) -> pd.DataFrame:
    if (frame <= 0).any().any():
        raise ValueError("CLR transform requires strictly positive compositions.")
    values = frame.to_numpy(dtype=float)
    geometric_mean = np.exp(np.mean(np.log(values), axis=1, keepdims=True))
    clr_values = np.log(values / geometric_mean)
    return pd.DataFrame(
        clr_values,
        columns=[f"clr_{column}" for column in frame.columns],
        index=frame.index,
    )


def inverse_clr_transform(frame: pd.DataFrame, closure_target: float = 100.0) -> pd.DataFrame:
    values = np.exp(frame.to_numpy(dtype=float))
    normalized = values / values.sum(axis=1, keepdims=True) * closure_target
    columns = [column.removeprefix("clr_") for column in frame.columns]
    return pd.DataFrame(normalized, columns=columns, index=frame.index)


def is_valid_composition(
    composition: Sequence[float],
    lower: Sequence[float],
    upper: Sequence[float],
    total: float,
    atol: float = 1e-6,
) -> bool:
    values = np.asarray(composition, dtype=float)
    lower_arr = np.asarray(lower, dtype=float)
    upper_arr = np.asarray(upper, dtype=float)
    if values.shape != lower_arr.shape or values.shape != upper_arr.shape:
        raise ValueError("Composition and bounds must have the same shape.")
    if np.any(values < lower_arr - atol) or np.any(values > upper_arr + atol):
        return False
    return abs(values.sum() - total) <= atol


def sample_bounded_simplex(
    bounds: Mapping[str, Sequence[float]],
    total: float,
    n_samples: int,
    random_state: int,
    max_attempt_factor: int = 200,
) -> pd.DataFrame:
    if not bounds:
        raise ValueError("Bayesian optimization bounds cannot be empty.")

    elements = list(bounds.keys())
    lower = np.asarray([bounds[element][0] for element in elements], dtype=float)
    upper = np.asarray([bounds[element][1] for element in elements], dtype=float)

    if np.any(lower < 0):
        raise ValueError("Lower bounds must be non-negative for compositional sampling.")
    if np.any(upper <= lower):
        raise ValueError("Each upper bound must be larger than its lower bound.")

    free_budget = total - lower.sum()
    if free_budget < -1e-9:
        raise ValueError("Sum of lower bounds exceeds the fixed composition total.")
    if total > upper.sum() + 1e-9:
        raise ValueError("Sum of upper bounds is smaller than the fixed composition total.")

    if abs(free_budget) <= 1e-12:
        return pd.DataFrame([lower] * n_samples, columns=elements)

    rng = np.random.default_rng(random_state)
    samples: list[np.ndarray] = []
    attempts = 0
    max_attempts = max(n_samples * max_attempt_factor, 1000)

    while len(samples) < n_samples and attempts < max_attempts:
        raw = rng.dirichlet(np.ones(len(elements)))
        candidate = lower + raw * free_budget
        if np.all(candidate <= upper + 1e-9):
            samples.append(candidate)
        attempts += 1

    if len(samples) < n_samples:
        raise ValueError(
            "Unable to sample enough feasible compositions under the given bounded simplex. "
            "Please relax the bounds or reduce the requested sample count."
        )

    sampled = pd.DataFrame(samples, columns=elements)
    return normalize_compositions(sampled, closure_target=total)
