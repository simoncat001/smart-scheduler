from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

from .composition import clr_transform, normalize_compositions, replace_zeros_and_renormalize


@dataclass(slots=True)
class PreparedDataset:
    raw_table: pd.DataFrame
    model_table: pd.DataFrame
    dropped_rows: pd.DataFrame
    normalized_compositions: pd.DataFrame
    feature_frame: pd.DataFrame
    target: pd.Series
    composition_columns: list[str]
    feature_columns: list[str]
    target_column: str


def load_tabular_data(path: str, sheet_name: str | None = None) -> pd.DataFrame:
    data_path = Path(path)
    suffix = data_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(data_path)
    if suffix in {".xlsx", ".xls"}:
        if sheet_name is None:
            return pd.read_excel(data_path)
        return pd.read_excel(data_path, sheet_name=sheet_name)
    raise ValueError(f"Unsupported data format: {suffix}")


def _apply_filters(frame: pd.DataFrame, filters: dict[str, Any]) -> pd.DataFrame:
    filtered = frame.copy()
    for column, allowed_values in filters.items():
        if column not in filtered.columns:
            raise ValueError(f"Filter column '{column}' does not exist in the dataset.")
        if isinstance(allowed_values, list):
            filtered = filtered[filtered[column].isin(allowed_values)]
        else:
            filtered = filtered[filtered[column] == allowed_values]
    return filtered


def _build_outlier_mask(
    feature_frame: pd.DataFrame,
    target: pd.Series,
    preprocessing_config: dict[str, Any],
    random_state: int,
) -> np.ndarray:
    method = preprocessing_config["outlier_method"].lower()
    if method == "none":
        return np.zeros(len(feature_frame), dtype=bool)

    if method == "iqr":
        multiplier = preprocessing_config["hardness_iqr_multiplier"]
        q1 = float(target.quantile(0.25))
        q3 = float(target.quantile(0.75))
        iqr = q3 - q1
        lower = q1 - multiplier * iqr
        upper = q3 + multiplier * iqr
        return ((target < lower) | (target > upper)).to_numpy()

    if method == "isolation_forest":
        if len(feature_frame) < 10:
            return _build_outlier_mask(feature_frame, target, {**preprocessing_config, "outlier_method": "iqr"}, random_state)

        contamination = float(preprocessing_config["outlier_fraction"])
        if not 0 < contamination < 0.5:
            raise ValueError("IsolationForest contamination must be in the interval (0, 0.5).")

        merged = feature_frame.copy()
        merged[target.name] = target.to_numpy()
        forest = IsolationForest(
            contamination=contamination,
            n_estimators=300,
            random_state=random_state,
        )
        labels = forest.fit_predict(merged.to_numpy(dtype=float))
        return labels == -1

    raise ValueError(f"Unsupported outlier method: {method}")


def prepare_dataset(config: dict[str, Any]) -> PreparedDataset:
    data_config = config["data"]
    preprocessing_config = config["preprocessing"]
    gpr_config = config["gpr"]

    frame = load_tabular_data(
        path=data_config["path"],
        sheet_name=data_config.get("sheet_name"),
    )
    frame = _apply_filters(frame, data_config.get("filters", {}))
    if frame.empty:
        raise ValueError("The dataset is empty after applying filters.")

    composition_columns = list(data_config["composition_columns"])
    target_column = data_config["target_column"]
    required_columns = composition_columns + [target_column]
    missing_columns = [column for column in required_columns if column not in frame.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns in dataset: {missing_columns}")

    working = frame.copy()
    working["_row_id"] = np.arange(len(working))

    for column in required_columns:
        working[column] = pd.to_numeric(working[column], errors="coerce")

    drop_reason = pd.Series("", index=working.index, dtype="object")
    numeric_missing_mask = working[required_columns].isna().any(axis=1)
    drop_reason.loc[numeric_missing_mask] = "missing_numeric_values"

    if "id_column" in data_config and data_config["id_column"] and data_config["id_column"] not in working.columns:
        raise ValueError(f"id_column '{data_config['id_column']}' does not exist in the dataset.")

    closure_target = float(preprocessing_config["closure_target"])
    zero_replacement = float(preprocessing_config["zero_replacement"])

    valid_numeric = working.loc[~numeric_missing_mask].copy()
    raw_compositions = valid_numeric[composition_columns]
    raw_sum = raw_compositions.sum(axis=1)

    non_positive_sum_mask = raw_sum <= 0
    if non_positive_sum_mask.any():
        drop_reason.loc[valid_numeric.index[non_positive_sum_mask]] = "non_positive_composition_sum"
        valid_numeric = valid_numeric.loc[~non_positive_sum_mask].copy()
        raw_compositions = valid_numeric[composition_columns]
        raw_sum = raw_compositions.sum(axis=1)

    normalized = normalize_compositions(raw_compositions, closure_target=closure_target)
    normalized = replace_zeros_and_renormalize(
        normalized,
        zero_replacement=zero_replacement,
        closure_target=closure_target,
    )

    composition_min = float(preprocessing_config["composition_min"])
    composition_max = float(preprocessing_config["composition_max"])
    invalid_range_mask = ((normalized < composition_min) | (normalized > composition_max)).any(axis=1)
    if invalid_range_mask.any():
        drop_reason.loc[normalized.index[invalid_range_mask]] = "training_composition_out_of_range"
        valid_numeric = valid_numeric.loc[~invalid_range_mask].copy()
        normalized = normalized.loc[~invalid_range_mask].copy()

    if preprocessing_config["use_clr"]:
        feature_frame = clr_transform(normalized)
    else:
        feature_frame = normalized.copy()

    target = valid_numeric[target_column].copy()
    feature_frame.index = valid_numeric.index
    normalized.index = valid_numeric.index

    outlier_mask = _build_outlier_mask(
        feature_frame=feature_frame,
        target=target,
        preprocessing_config=preprocessing_config,
        random_state=int(gpr_config["random_state"]),
    )
    if outlier_mask.any():
        outlier_index = feature_frame.index[outlier_mask]
        drop_reason.loc[outlier_index] = "outlier_detected"

    kept_index = feature_frame.index[~outlier_mask]
    model_table = valid_numeric.loc[kept_index].copy()
    normalized = normalized.loc[kept_index].copy()
    feature_frame = feature_frame.loc[kept_index].copy()
    target = target.loc[kept_index].copy()

    model_table["composition_sum_raw"] = raw_sum.loc[kept_index]
    for column in composition_columns:
        model_table[f"{column}_normalized"] = normalized[column]
    model_table[target_column] = target

    dropped_rows = working.loc[drop_reason != ""].copy()
    dropped_rows["drop_reason"] = drop_reason.loc[drop_reason != ""]

    extra_keep_columns = data_config.get("extra_keep_columns", [])
    for column in extra_keep_columns:
        if column not in model_table.columns:
            raise ValueError(f"extra_keep_columns references missing column '{column}'.")

    return PreparedDataset(
        raw_table=frame,
        model_table=model_table,
        dropped_rows=dropped_rows,
        normalized_compositions=normalized,
        feature_frame=feature_frame,
        target=target,
        composition_columns=composition_columns,
        feature_columns=list(feature_frame.columns),
        target_column=target_column,
    )
