from __future__ import annotations

from typing import Any

import pandas as pd


def map_candidates_to_process_parameters(
    candidates: pd.DataFrame,
    composition_columns: list[str],
    process_config: dict[str, Any],
) -> pd.DataFrame:
    if candidates.empty:
        return pd.DataFrame()

    mode = str(process_config["mode"]).lower()
    if mode != "equal_yield_power_share":
        raise ValueError(f"Unsupported process mapping mode: {mode}")

    total_power = float(process_config["total_power_w"])
    mapped = candidates.copy()
    total_fraction = mapped[composition_columns].sum(axis=1)
    for column in composition_columns:
        share_column = f"{column}_power_share"
        power_column = f"{column}_power_w"
        mapped[share_column] = mapped[column] / total_fraction
        mapped[power_column] = mapped[share_column] * total_power

    fixed_parameters = process_config.get("fixed_parameters", {})
    for key, value in fixed_parameters.items():
        mapped[key] = value

    return mapped


def map_interval_summary_to_process_window(
    interval_summary: dict[str, Any],
    process_config: dict[str, Any],
) -> dict[str, Any]:
    mode = str(process_config["mode"]).lower()
    if mode != "equal_yield_power_share":
        raise ValueError(f"Unsupported process mapping mode: {mode}")

    total_power = float(process_config["total_power_w"])
    process_window = {
        "assumption": (
            "Assume each target has the same deposition efficiency, so the target power "
            "share is proportional to the desired atomic fraction."
        ),
        "mode": mode,
        "total_power_w": total_power,
        "fixed_parameters": process_config.get("fixed_parameters", {}),
        "targets": {},
    }

    for element, stats in interval_summary["elements"].items():
        process_window["targets"][element] = {
            "power_share_min": stats["min"] / 100.0,
            "power_share_p10": stats["p10"] / 100.0,
            "power_share_median": stats["median"] / 100.0,
            "power_share_p90": stats["p90"] / 100.0,
            "power_share_max": stats["max"] / 100.0,
            "power_w_min": total_power * stats["min"] / 100.0,
            "power_w_p10": total_power * stats["p10"] / 100.0,
            "power_w_median": total_power * stats["median"] / 100.0,
            "power_w_p90": total_power * stats["p90"] / 100.0,
            "power_w_max": total_power * stats["max"] / 100.0,
        }

    return process_window
