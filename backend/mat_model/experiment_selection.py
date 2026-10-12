from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


COMPOSITION_COLUMNS = ["Ti", "Zr", "Nb", "V"]


def _pick_unique_index(frame: pd.DataFrame, selected_indices: set[int], by: list[str], ascending: list[bool]) -> int:
    ordered = frame.sort_values(by=by, ascending=ascending)
    for index in ordered.index:
        if int(index) not in selected_indices:
            return int(index)
    raise ValueError("Unable to select a unique candidate for the requested role.")


def _pick_center_candidate(frame: pd.DataFrame, selected_indices: set[int]) -> int:
    center = frame[COMPOSITION_COLUMNS].median(axis=0)
    composition_scale = frame[COMPOSITION_COLUMNS].std(axis=0).replace(0, 1.0)
    normalized = (frame[COMPOSITION_COLUMNS] - center) / composition_scale
    frame = frame.copy()
    frame["_center_distance"] = np.sqrt((normalized**2).sum(axis=1))
    return _pick_unique_index(
        frame=frame,
        selected_indices=selected_indices,
        by=["_center_distance", "predicted_std_gpa", "reward"],
        ascending=[True, True, False],
    )


def _select_anchor_indices(frame: pd.DataFrame, batch_size: int) -> list[tuple[str, int]]:
    anchor_specs: list[tuple[str, list[str], list[bool]]] = [
        ("highest_score", ["reward", "predicted_hardness_gpa"], [False, False]),
        ("lowest_ti", ["Ti", "reward"], [True, False]),
        ("highest_ti", ["Ti", "reward"], [False, False]),
        ("lowest_v", ["V", "reward"], [True, False]),
        ("highest_v", ["V", "reward"], [False, False]),
    ]

    selected_indices: set[int] = set()
    selections: list[tuple[str, int]] = []

    for role, by, ascending in anchor_specs[: min(batch_size, len(anchor_specs))]:
        index = _pick_unique_index(frame=frame, selected_indices=selected_indices, by=by, ascending=ascending)
        selected_indices.add(index)
        selections.append((role, index))

    if batch_size >= 6:
        index = _pick_center_candidate(frame=frame, selected_indices=selected_indices)
        selected_indices.add(index)
        selections.append(("center_low_std", index))

    return selections


def _composition_distance_matrix(frame: pd.DataFrame) -> np.ndarray:
    values = frame[COMPOSITION_COLUMNS].to_numpy(dtype=float)
    scale = np.std(values, axis=0)
    scale[scale == 0] = 1.0
    normalized = values / scale
    deltas = normalized[:, None, :] - normalized[None, :, :]
    return np.sqrt(np.sum(deltas**2, axis=2))


def _select_diverse_fillers(frame: pd.DataFrame, selected: list[tuple[str, int]], batch_size: int) -> list[tuple[str, int]]:
    if len(selected) >= batch_size:
        return selected

    distance_matrix = _composition_distance_matrix(frame)
    selected_indices = [index for _, index in selected]
    selected_set = set(selected_indices)

    while len(selected) < batch_size:
        best_index = None
        best_score = None
        for index in frame.index:
            int_index = int(index)
            if int_index in selected_set:
                continue
            min_distance = float(np.min(distance_matrix[int_index, selected_indices]))
            quality = float(frame.loc[index, "reward"])
            score = (min_distance, quality)
            if best_score is None or score > best_score:
                best_score = score
                best_index = int_index

        if best_index is None:
            break
        selected.append((f"diverse_fill_{len(selected) + 1}", best_index))
        selected_indices.append(best_index)
        selected_set.add(best_index)

    return selected


def select_experimental_batch(
    candidates: pd.DataFrame,
    batch_size: int = 6,
    pool_size: int = 20,
) -> pd.DataFrame:
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1.")

    reliable = candidates[candidates["is_reliable"] == True].copy()
    if reliable.empty:
        raise ValueError("No reliable candidates are available for batch selection.")

    pool = reliable.head(pool_size).copy().reset_index(drop=True)
    anchored = _select_anchor_indices(pool, batch_size=batch_size)
    selected = _select_diverse_fillers(pool, anchored, batch_size=batch_size)

    batch = pool.loc[[index for _, index in selected]].copy().reset_index(drop=True)
    batch.insert(0, "selection_role", [role for role, _ in selected])
    batch.insert(1, "batch_rank", np.arange(1, len(batch) + 1))
    return batch


def summarize_experimental_batch(batch: pd.DataFrame) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "batch_size": int(len(batch)),
        "predicted_hardness_gpa": {
            "min": float(batch["predicted_hardness_gpa"].min()),
            "max": float(batch["predicted_hardness_gpa"].max()),
            "mean": float(batch["predicted_hardness_gpa"].mean()),
        },
        "predicted_std_gpa": {
            "min": float(batch["predicted_std_gpa"].min()),
            "max": float(batch["predicted_std_gpa"].max()),
            "mean": float(batch["predicted_std_gpa"].mean()),
        },
        "elements": {},
    }
    for column in COMPOSITION_COLUMNS:
        summary["elements"][column] = {
            "min": float(batch[column].min()),
            "max": float(batch[column].max()),
            "mean": float(batch[column].mean()),
        }
    return summary


def write_experimental_batch(
    candidates_csv: str | Path,
    output_csv: str | Path,
    summary_json: str | Path,
    batch_size: int = 6,
    pool_size: int = 20,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    candidates = pd.read_csv(candidates_csv)
    batch = select_experimental_batch(candidates=candidates, batch_size=batch_size, pool_size=pool_size)
    summary = summarize_experimental_batch(batch)

    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    batch.to_csv(output_path, index=False)

    summary_path = Path(summary_json)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return batch, summary
