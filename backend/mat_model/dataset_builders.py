from __future__ import annotations

from pathlib import Path

import pandas as pd


def build_initial_grid_dataset(
    composition_path: Path,
    structure_path: Path,
    hardness_path: Path,
    output_path: Path,
    invalid_hardness_mpa: float = 979.592,
) -> pd.DataFrame:
    composition_book = pd.ExcelFile(composition_path)
    composition_frames = {
        sheet.replace("_Content", ""): pd.read_excel(composition_path, sheet_name=sheet, header=None)
        for sheet in composition_book.sheet_names
    }
    phase_frame = pd.read_excel(structure_path, header=None)
    hardness_frame = pd.read_csv(hardness_path, sep="\t", header=None)

    rows: list[dict[str, object]] = []
    for row_idx in range(hardness_frame.shape[0]):
        for col_idx in range(hardness_frame.shape[1]):
            hardness_mpa = hardness_frame.iat[row_idx, col_idx]
            rows.append(
                {
                    "sample_id": f"R{row_idx + 1:02d}C{col_idx + 1:02d}",
                    "grid_row": row_idx + 1,
                    "grid_col": col_idx + 1,
                    "phase": str(phase_frame.iat[row_idx, col_idx]).strip().lower(),
                    "Ti": float(composition_frames["Ti"].iat[row_idx, col_idx]),
                    "Zr": float(composition_frames["Zr"].iat[row_idx, col_idx]),
                    "Nb": float(composition_frames["Nb"].iat[row_idx, col_idx]),
                    "V": float(composition_frames["V"].iat[row_idx, col_idx]),
                    "hardness_mpa_raw": hardness_mpa,
                    "source": "initial_grid",
                }
            )

    dataset = pd.DataFrame(rows)
    dataset["composition_sum"] = dataset[["Ti", "Zr", "Nb", "V"]].sum(axis=1)
    dataset["hardness_is_nan"] = dataset["hardness_mpa_raw"].isna()
    dataset["hardness_is_instrument_floor"] = dataset["hardness_mpa_raw"].eq(invalid_hardness_mpa)
    dataset["hardness_valid"] = ~(dataset["hardness_is_nan"] | dataset["hardness_is_instrument_floor"])
    dataset["hardness_gpa"] = dataset["hardness_mpa_raw"] / 1000.0
    dataset["notes"] = ""
    dataset.loc[dataset["hardness_is_nan"], "notes"] = "missing_hardness"
    dataset.loc[dataset["hardness_is_instrument_floor"], "notes"] = "instrument_floor_filtered"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_csv(output_path, index=False)
    return dataset


def build_literature_dataset(
    literature_path: Path,
    output_path: Path,
    sheet_name: str = "Sheet1",
) -> pd.DataFrame:
    raw = pd.read_excel(literature_path, sheet_name=sheet_name, engine="xlrd")
    hardness_gpa = pd.to_numeric(raw["纳米压痕硬度"], errors="coerce")

    dataset = pd.DataFrame(
        {
            "sample_id": [f"LIT_{idx + 1:03d}" for idx in range(len(raw))],
            "grid_row": pd.NA,
            "grid_col": pd.NA,
            "phase": raw["合金结构"].astype(str).str.strip().str.lower(),
            "Ti": pd.to_numeric(raw["钛Ti百分比"], errors="coerce"),
            "Zr": pd.to_numeric(raw["锆Zr百分比"], errors="coerce"),
            "Nb": pd.to_numeric(raw["铌Nb百分比"], errors="coerce"),
            "V": pd.to_numeric(raw["钒V百分比"], errors="coerce"),
            "hardness_mpa_raw": hardness_gpa * 1000.0,
            "source": "literature",
        }
    )
    dataset["composition_sum"] = dataset[["Ti", "Zr", "Nb", "V"]].sum(axis=1)
    dataset["hardness_is_nan"] = hardness_gpa.isna()
    dataset["hardness_is_instrument_floor"] = False
    dataset["hardness_valid"] = ~dataset["hardness_is_nan"]
    dataset["hardness_gpa"] = hardness_gpa
    dataset["notes"] = ""
    dataset.loc[dataset["hardness_is_nan"], "notes"] = "missing_literature_hardness"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_csv(output_path, index=False)
    return dataset


def build_combined_dataset(
    initial_dataset_path: Path,
    literature_dataset_path: Path,
    output_path: Path,
) -> pd.DataFrame:
    initial = pd.read_csv(initial_dataset_path)
    literature = pd.read_csv(literature_dataset_path)

    all_columns = []
    for column in list(initial.columns) + list(literature.columns):
        if column not in all_columns:
            all_columns.append(column)

    initial = initial.reindex(columns=all_columns)
    literature = literature.reindex(columns=all_columns)
    combined = pd.concat([initial, literature], ignore_index=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(output_path, index=False)
    return combined
