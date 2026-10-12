from __future__ import annotations

from pathlib import Path

import numpy as np

from app.xrd_parser import (
    analyze_xrd_pair,
    build_snake_matrix,
    discover_xrd_pairs,
    resolve_xrd_pair,
)


def _synthetic_trace(
    x_values: np.ndarray,
    peaks: list[tuple[float, float, float]],
    *,
    baseline: float = 0.1,
) -> np.ndarray:
    y_values = baseline + 0.002 * (x_values - float(x_values.min()))
    for amplitude, center, sigma in peaks:
        y_values = y_values + amplitude * np.exp(-((x_values - center) ** 2) / (2.0 * sigma**2))
    return y_values


def _write_xy_file(path: Path, x_values: np.ndarray, y_values: np.ndarray) -> None:
    lines = ['Id: "" Comment: "synthetic test"']
    lines.extend(f"{x_value:.6f} {y_value:.9f}" for x_value, y_value in zip(x_values, y_values))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_build_snake_matrix_supports_serpentine_layout():
    matrix = build_snake_matrix(
        ["s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"],
        row_num=3,
        col_num=3,
    )
    assert matrix == [
        ["s0", "s1", "s2"],
        ["s5", "s4", "s3"],
        ["s6", "s7", "s8"],
    ]


def test_discover_xrd_pairs_matches_low_and_high_angle_files(tmp_path):
    (tmp_path / "40-001-0000_exported.xy").write_text("header\n30 1\n31 2\n32 1\n", encoding="utf-8")
    (tmp_path / "60-001-0000_exported.xy").write_text("header\n45 1\n46 2\n47 1\n", encoding="utf-8")
    (tmp_path / "40-002-0000_exported.xy").write_text("header\n30 1\n31 2\n32 1\n", encoding="utf-8")

    pairs = discover_xrd_pairs(tmp_path)

    assert len(pairs) == 1
    assert pairs[0].sample_id == "001"
    assert pairs[0].low_angle_file.name == "40-001-0000_exported.xy"
    assert pairs[0].high_angle_file.name == "60-001-0000_exported.xy"


def test_resolve_xrd_pair_finds_sample_by_directory_and_id(tmp_path):
    (tmp_path / "40-007-0000_exported.xy").write_text("header\n30 1\n31 2\n32 1\n", encoding="utf-8")
    (tmp_path / "60-007-0000_exported.xy").write_text("header\n45 1\n46 2\n47 1\n", encoding="utf-8")

    pair = resolve_xrd_pair(tmp_path, "7")

    assert pair.sample_id == "007"
    assert pair.low_angle_file.name == "40-007-0000_exported.xy"
    assert pair.high_angle_file.name == "60-007-0000_exported.xy"


def test_analyze_xrd_pair_classifies_bcc_from_raw_files(tmp_path):
    low_x = np.linspace(25.0, 43.0, 1200)
    high_x = np.linspace(45.0, 75.0, 1600)
    low_y = _synthetic_trace(low_x, [(8.0, 39.0, 0.18)], baseline=0.12)
    high_y = _synthetic_trace(high_x, [(6.0, 57.0, 0.2), (5.0, 70.0, 0.22)], baseline=0.1)

    low_file = tmp_path / "40-007-0000_exported.xy"
    high_file = tmp_path / "60-007-0000_exported.xy"
    _write_xy_file(low_file, low_x, low_y)
    _write_xy_file(high_file, high_x, high_y)

    analysis = analyze_xrd_pair(low_file, high_file)

    assert analysis.sample_id == "007"
    assert analysis.phase == "bcc"
    assert analysis.bcc_peaks["(110)"] is not None
    assert analysis.bcc_peaks["(200)"] is not None
    assert analysis.bcc_peaks["(211)"] is not None
    assert abs(analysis.bcc_peaks["(110)"].center - 39.0) < 0.3
    assert abs(analysis.bcc_peaks["(200)"].center - 57.0) < 0.3
    assert abs(analysis.bcc_peaks["(211)"].center - 70.0) < 0.3
