from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO, StringIO
import json
from pathlib import Path
import re
from typing import Any
import zipfile

import pandas as pd

from app.config import Settings, get_settings
from app.materials import QUATERNARY_KEYS
from app.services.manage_db_client import ManageDbClient, ManageDbClientError, ManageDbRecord


RAW_FILE_TITLE_HINTS = (
    "原始数据文件",
    "表征原始数据文件",
    "原始数据对应列表",
    "表征原始数据列表",
    "表征原始数据对应列表",
)
INSTRUMENT_NAME_HINTS = {
    "eds": ("eds", "能谱", "成分"),
    "xrd": ("xrd", "射线衍射", "结构"),
    "nanoindentation": ("nano", "纳米压痕", "硬度"),
}
DOWNLOAD_REF_PATTERN = re.compile(r"/api/download/[^\s\"']+")
GRID_SAMPLE_PATTERN = re.compile(r"R(?P<row>\d{1,2})C(?P<col>\d{1,2})", re.IGNORECASE)
PHASE_TEXT_PATTERN = re.compile(r"[A-Za-z\u4e00-\u9fff0-9+\-_/]+")


@dataclass(slots=True)
class ParsedManageDbDataset:
    instrument_code: str
    summary_observation: dict[str, Any]
    sample_rows: list[dict[str, Any]]
    raw_resources: list[str]


@dataclass(slots=True)
class _ResourceBlob:
    name: str
    content: bytes


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
    elif isinstance(value, list):
        for index, child in enumerate(value):
            next_prefix = f"{prefix}[{index}]"
            flattened[next_prefix] = child
            flattened.update(_flatten_mapping(child, next_prefix))
    return flattened


def _normalize_key(value: Any) -> str:
    return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", str(value or "").strip().lower())


def _parse_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return float(number)


def _decode_text(raw: bytes) -> str:
    for encoding in ("utf-8", "utf-8-sig", "gb18030", "gbk", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _trim_frame(frame: pd.DataFrame) -> pd.DataFrame:
    trimmed = frame.dropna(axis=0, how="all").dropna(axis=1, how="all")
    return trimmed.reset_index(drop=True)


def _first_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, (list, tuple)):
        for item in value:
            text = _first_text(item)
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


def _resource_candidates(value: Any) -> list[str]:
    candidates: list[str] = []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return candidates
        if text.startswith("file:"):
            text = text[5:]
            if text.startswith("http://") or text.startswith("https://") or text.startswith("/"):
                candidates.append(text.rstrip(":"))
                return candidates
        if text.startswith("http://") or text.startswith("https://") or text.startswith("/"):
            candidates.append(text)
            return candidates
        for matched in DOWNLOAD_REF_PATTERN.findall(text):
            candidates.append(matched)
        return candidates
    if isinstance(value, dict):
        name_value = value.get("name")
        candidates.extend(_resource_candidates(name_value))
    if isinstance(value, (list, tuple)):
        for item in value:
            candidates.extend(_resource_candidates(item))
    return candidates


def _grid_dimensions(rows: list[dict[str, Any]]) -> tuple[int, int]:
    max_row = max((int(row.get("grid_row") or 0) for row in rows), default=0)
    max_col = max((int(row.get("grid_col") or 0) for row in rows), default=0)
    return max_row, max_col


def _choose_center_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    max_row, max_col = _grid_dimensions(rows)
    center_row = (max_row + 1) / 2 if max_row else 1
    center_col = (max_col + 1) / 2 if max_col else 1
    scored = sorted(
        rows,
        key=lambda item: (
            abs((item.get("grid_row") or 1) - center_row)
            + abs((item.get("grid_col") or 1) - center_col),
            item.get("grid_row") or 1,
            item.get("grid_col") or 1,
        ),
    )
    return scored[0]


def _normalize_composition(values: dict[str, float]) -> dict[str, float] | None:
    total = sum(float(values[key]) for key in QUATERNARY_KEYS)
    if total <= 0:
        return None
    return {
        key: round(float(values[key]) * 100.0 / total, 6)
        for key in QUATERNARY_KEYS
    }


def _grid_sample_id(base_sample_id: str, row_index: int, col_index: int) -> str:
    grid_code = f"R{row_index:02d}C{col_index:02d}"
    return f"{base_sample_id}-{grid_code}" if base_sample_id else grid_code


class ManageDbRawParser:
    def __init__(
        self,
        settings: Settings | None = None,
        *,
        client: ManageDbClient | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.client = client or ManageDbClient(self.settings)

    def parse_record(
        self,
        record: ManageDbRecord,
        *,
        instrument_code: str,
        sample_id: str | None = None,
        experiment_batch_id: str | None = None,
    ) -> ParsedManageDbDataset | None:
        merged = self._merged_payload(record.payload)
        resources = self._raw_resource_refs(merged)
        if not resources:
            return None

        blobs = self._download_resources(resources, base_url=record.base_url)
        if not blobs:
            return None

        base_sample_id = (
            sample_id
            or self._sample_id_from_payload(merged)
            or experiment_batch_id
            or record.record_id
        )

        if instrument_code == "eds":
            return self._parse_eds(blobs, base_sample_id)
        if instrument_code == "xrd":
            return self._parse_xrd(blobs, base_sample_id)
        if instrument_code == "nanoindentation":
            return self._parse_nanoindentation(blobs, base_sample_id)
        return None

    def infer_instrument_code(self, payload: dict[str, Any]) -> str | None:
        merged = self._merged_payload(payload)
        candidates = [
            merged.get("template_name"),
            merged.get("实验仪器名称"),
            merged.get("仪器名称"),
            merged.get("template_type"),
            merged.get("样品名称"),
        ]
        flattened = _flatten_mapping(merged)
        candidates.extend(flattened.values())
        for instrument_code, aliases in INSTRUMENT_NAME_HINTS.items():
            for candidate in candidates:
                if not isinstance(candidate, str):
                    continue
                normalized = candidate.strip().lower()
                if any(alias in normalized for alias in aliases):
                    return instrument_code
        return None

    @staticmethod
    def _merged_payload(payload: dict[str, Any]) -> dict[str, Any]:
        merged = dict(payload)
        for key in ("scheduler_observation", "result_data", "process_data", "origin_post_data"):
            nested = payload.get(key)
            if isinstance(nested, dict):
                merged = _deep_merge_dicts(merged, nested)
        raw_files = payload.get("raw_files")
        if isinstance(raw_files, dict) and raw_files:
            existing_raw = merged.get("原始文件")
            if isinstance(existing_raw, dict):
                merged["原始文件"] = _deep_merge_dicts(existing_raw, raw_files)
            else:
                merged["原始文件"] = dict(raw_files)
        content_tree = _content_items_to_mapping(payload.get("data_content"))
        if content_tree:
            merged = _deep_merge_dicts(merged, content_tree)
        return merged

    @staticmethod
    def _sample_id_from_payload(payload: dict[str, Any]) -> str | None:
        for key in (
            "关联样品MGID",
            "sample_id",
            "deposition_sample_id",
            "experiment_batch_id",
            "样品名称",
            "MGID",
        ):
            text = _first_text(_sample_refs(payload.get(key)))
            if text:
                return text
        return None

    def _raw_resource_refs(self, payload: dict[str, Any]) -> list[str]:
        refs: list[str] = []
        raw_section = payload.get("原始文件")
        if isinstance(raw_section, dict):
            for key in RAW_FILE_TITLE_HINTS:
                refs.extend(_resource_candidates(raw_section.get(key)))
        flattened = _flatten_mapping(payload)
        for path, value in flattened.items():
            normalized_path = _normalize_key(path)
            if any(_normalize_key(hint) in normalized_path for hint in RAW_FILE_TITLE_HINTS):
                refs.extend(_resource_candidates(value))

        deduped: list[str] = []
        seen: set[str] = set()
        for ref in refs:
            if ref and ref not in seen:
                deduped.append(ref)
                seen.add(ref)
        return deduped

    def _download_resources(self, resources: list[str], *, base_url: str | None) -> list[_ResourceBlob]:
        blobs: list[_ResourceBlob] = []
        for ref in resources:
            try:
                raw = self.client.download_bytes(ref, base_url=base_url)
            except ManageDbClientError:
                continue
            name = Path(ref.split("?")[0]).name or "resource.bin"
            suffix = Path(name).suffix.lower()
            if suffix not in {".xlsx", ".xlsm", ".xlsb"} and zipfile.is_zipfile(BytesIO(raw)):
                with zipfile.ZipFile(BytesIO(raw)) as archive:
                    for member in archive.infolist():
                        if member.is_dir():
                            continue
                        blobs.append(
                            _ResourceBlob(
                                name=Path(member.filename).name,
                                content=archive.read(member),
                            )
                        )
                continue
            blobs.append(_ResourceBlob(name=name, content=raw))
        return blobs

    def _parse_eds(
        self,
        blobs: list[_ResourceBlob],
        base_sample_id: str,
    ) -> ParsedManageDbDataset | None:
        workbook = next(
            (blob for blob in blobs if Path(blob.name).suffix.lower() in {".xlsx", ".xls"}),
            None,
        )
        if workbook is None:
            return None

        excel = pd.ExcelFile(BytesIO(workbook.content))
        sheet_frames: dict[str, pd.DataFrame] = {}
        for sheet_name in excel.sheet_names:
            normalized = _normalize_key(sheet_name)
            frame = _trim_frame(
                excel.parse(sheet_name, header=None).apply(pd.to_numeric, errors="coerce")
            )
            if frame.empty:
                continue
            if "ti" in normalized:
                sheet_frames["ti_at"] = frame
            elif "zr" in normalized:
                sheet_frames["zr_at"] = frame
            elif "nb" in normalized:
                sheet_frames["nb_at"] = frame
            elif normalized.startswith("v") or "vcontent" in normalized:
                sheet_frames["v_at"] = frame
        if set(sheet_frames) != set(QUATERNARY_KEYS):
            return None

        row_count = min(frame.shape[0] for frame in sheet_frames.values())
        col_count = min(frame.shape[1] for frame in sheet_frames.values())
        samples: list[dict[str, Any]] = []
        for row_index in range(row_count):
            for col_index in range(col_count):
                composition = {
                    key: _parse_number(sheet_frames[key].iat[row_index, col_index])
                    for key in QUATERNARY_KEYS
                }
                if any(value is None for value in composition.values()):
                    continue
                normalized = _normalize_composition({key: float(value) for key, value in composition.items() if value is not None})
                if normalized is None:
                    continue
                samples.append(
                    {
                        "sample_id": _grid_sample_id(base_sample_id, row_index + 1, col_index + 1),
                        "grid_row": row_index + 1,
                        "grid_col": col_index + 1,
                        "measured_composition_at_percent": normalized,
                    }
                )

        center = _choose_center_row(samples)
        if center is None:
            return None
        return ParsedManageDbDataset(
            instrument_code="eds",
            summary_observation={
                "measured_composition_at_percent": dict(center["measured_composition_at_percent"]),
                "eds_batch_grid": {
                    "rows": row_count,
                    "cols": col_count,
                    "total_samples": len(samples),
                    "samples": samples,
                },
            },
            sample_rows=samples,
            raw_resources=[blob.name for blob in blobs],
        )

    def _parse_nanoindentation(
        self,
        blobs: list[_ResourceBlob],
        base_sample_id: str,
    ) -> ParsedManageDbDataset | None:
        matrix_blob = self._best_numeric_matrix_blob(blobs)
        if matrix_blob is None:
            return None
        frame = self._numeric_frame_from_blob(matrix_blob)
        if frame is None or frame.empty:
            return None

        samples: list[dict[str, Any]] = []
        row_count, col_count = frame.shape
        for row_index in range(row_count):
            for col_index in range(col_count):
                value = _parse_number(frame.iat[row_index, col_index])
                if value is None:
                    continue
                hardness_gpa = float(value) / 1000.0 if value >= 100.0 else float(value)
                samples.append(
                    {
                        "sample_id": _grid_sample_id(base_sample_id, row_index + 1, col_index + 1),
                        "grid_row": row_index + 1,
                        "grid_col": col_index + 1,
                        "hardness_gpa": round(hardness_gpa, 6),
                    }
                )

        center = _choose_center_row(samples)
        if center is None:
            return None
        return ParsedManageDbDataset(
            instrument_code="nanoindentation",
            summary_observation={
                "hardness_gpa": float(center["hardness_gpa"]),
                "nanoindentation_batch_grid": {
                    "rows": row_count,
                    "cols": col_count,
                    "total_samples": len(samples),
                    "samples": samples,
                },
            },
            sample_rows=samples,
            raw_resources=[blob.name for blob in blobs],
        )

    def _parse_xrd(
        self,
        blobs: list[_ResourceBlob],
        base_sample_id: str,
    ) -> ParsedManageDbDataset | None:
        frame = None
        for blob in blobs:
            suffix = Path(blob.name).suffix.lower()
            if suffix in {".xlsx", ".xls"}:
                excel = pd.ExcelFile(BytesIO(blob.content))
                parsed = _trim_frame(excel.parse(excel.sheet_names[0], header=None))
                if not parsed.empty:
                    frame = parsed
                    break
            elif suffix in {".csv", ".txt"}:
                text = _decode_text(blob.content)
                parsed = _trim_frame(pd.read_csv(StringIO(text), header=None))
                if not parsed.empty:
                    frame = parsed
                    break
        if frame is None or frame.empty:
            return None

        samples: list[dict[str, Any]] = []
        row_count, col_count = frame.shape
        for row_index in range(row_count):
            for col_index in range(col_count):
                raw_value = frame.iat[row_index, col_index]
                if raw_value is None or (isinstance(raw_value, float) and pd.isna(raw_value)):
                    continue
                text = str(raw_value).strip()
                if not text or not PHASE_TEXT_PATTERN.search(text):
                    continue
                samples.append(
                    {
                        "sample_id": _grid_sample_id(base_sample_id, row_index + 1, col_index + 1),
                        "grid_row": row_index + 1,
                        "grid_col": col_index + 1,
                        "xrd_phase": text,
                    }
                )

        center = _choose_center_row(samples)
        if center is None:
            return None
        return ParsedManageDbDataset(
            instrument_code="xrd",
            summary_observation={
                "xrd_phase": str(center["xrd_phase"]),
                "xrd_phase_grid": {
                    "rows": row_count,
                    "cols": col_count,
                    "total_samples": len(samples),
                    "samples": samples,
                },
            },
            sample_rows=samples,
            raw_resources=[blob.name for blob in blobs],
        )

    def _best_numeric_matrix_blob(self, blobs: list[_ResourceBlob]) -> _ResourceBlob | None:
        best_blob: _ResourceBlob | None = None
        best_score = -1
        for blob in blobs:
            suffix = Path(blob.name).suffix.lower()
            if suffix not in {".txt", ".csv", ".dat", ".log", ""}:
                continue
            text = _decode_text(blob.content)
            numbers = re.findall(r"(?:NaN|[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)", text, flags=re.IGNORECASE)
            if len(numbers) > best_score:
                best_score = len(numbers)
                best_blob = blob
        return best_blob

    def _numeric_frame_from_blob(self, blob: _ResourceBlob) -> pd.DataFrame | None:
        text = _decode_text(blob.content)
        try:
            frame = pd.read_csv(
                StringIO(text),
                header=None,
                sep=r"[\s,\t]+",
                engine="python",
            )
        except Exception:
            return None
        frame = _trim_frame(frame.apply(pd.to_numeric, errors="coerce"))
        return None if frame.empty else frame
