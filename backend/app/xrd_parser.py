from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import csv
import math
import re
from typing import Iterable

import numpy as np
from openpyxl import Workbook
from scipy import ndimage
from scipy.optimize import curve_fit
from scipy.signal import savgol_filter


DEFAULT_LOW_ANGLE_PATTERNS = ("40-*-0000_exported.xy", "siyuan1-*-0000_exported.xy")
DEFAULT_HIGH_ANGLE_PATTERNS = ("60-*-0000_exported.xy",)
SAMPLE_ID_RE = re.compile(r"(?P<sample>\d{3})-0000_exported\.xy$", re.IGNORECASE)
SAMPLE_ID_VALUE_RE = re.compile(r"(?P<sample>\d{1,3})")


class XrdParsingError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class PeakSpec:
    label: str
    center: float
    tolerance: float
    fit_range: tuple[float, float]
    amplitude_guess: float
    sigma_guess: float


@dataclass(frozen=True, slots=True)
class XrdFilePair:
    sample_id: str
    low_angle_file: Path
    high_angle_file: Path


@dataclass(frozen=True, slots=True)
class PeakFitResult:
    label: str
    amplitude: float
    center: float
    sigma: float
    matched: bool
    fit_range: tuple[float, float]

    def to_dict(self) -> dict[str, float | bool | str]:
        return {
            "label": self.label,
            "amplitude": round(self.amplitude, 6),
            "center": round(self.center, 6),
            "sigma": round(self.sigma, 6),
            "matched": self.matched,
            "fit_start": round(self.fit_range[0], 6),
            "fit_end": round(self.fit_range[1], 6),
        }


@dataclass(frozen=True, slots=True)
class XrdSampleAnalysis:
    sample_id: str | None
    low_angle_file: Path
    high_angle_file: Path
    phase: str
    note: str
    bcc_peaks: dict[str, PeakFitResult | None]
    fcc_peaks: dict[str, PeakFitResult | None]

    @property
    def peak_positions(self) -> dict[str, float]:
        positions: dict[str, float] = {}
        for family, fits in (("bcc", self.bcc_peaks), ("fcc", self.fcc_peaks)):
            for label, fit in fits.items():
                if fit is None:
                    continue
                key = f"{family}_{label.strip('()').lower()}"
                positions[key] = round(fit.center, 4)
        return positions

    def to_dict(self) -> dict[str, object]:
        return {
            "sample_id": self.sample_id,
            "phase": self.phase,
            "note": self.note,
            "low_angle_file": str(self.low_angle_file),
            "high_angle_file": str(self.high_angle_file),
            "bcc_peaks": {
                label: fit.to_dict() if fit is not None else None
                for label, fit in self.bcc_peaks.items()
            },
            "fcc_peaks": {
                label: fit.to_dict() if fit is not None else None
                for label, fit in self.fcc_peaks.items()
            },
            "peak_positions": self.peak_positions,
        }

BCC_PEAKS = {
    "(110)": PeakSpec("(110)", center=39.0, tolerance=3.0, fit_range=(36.0, 42.0), amplitude_guess=3.0, sigma_guess=0.6),
    "(200)": PeakSpec("(200)", center=57.0, tolerance=3.0, fit_range=(54.0, 60.0), amplitude_guess=3.0, sigma_guess=0.6),
    "(211)": PeakSpec("(211)", center=70.0, tolerance=2.0, fit_range=(68.0, 72.0), amplitude_guess=2.0, sigma_guess=0.5),
}
FCC_PEAKS = {
    "(111)": PeakSpec("(111)", center=35.0, tolerance=3.0, fit_range=(32.0, 38.0), amplitude_guess=3.0, sigma_guess=0.6),
    "(200)": PeakSpec("(200)", center=40.0, tolerance=3.0, fit_range=(38.0, 42.0), amplitude_guess=3.0, sigma_guess=0.6),
    "(220)": PeakSpec("(220)", center=58.0, tolerance=2.0, fit_range=(56.0, 60.0), amplitude_guess=2.0, sigma_guess=0.5),
}


def extract_sample_id(path: str | Path) -> str | None:
    match = SAMPLE_ID_RE.search(Path(path).name)
    return match.group("sample") if match else None


def normalize_sample_id(sample_id: str | int) -> str:
    text = str(sample_id).strip()
    match = SAMPLE_ID_VALUE_RE.search(text)
    if not match:
        raise XrdParsingError(f"invalid XRD sample id: {sample_id}")
    return match.group("sample").zfill(3)


def load_xy_file(path: str | Path, *, skiprows: int = 1) -> tuple[np.ndarray, np.ndarray]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.exists():
        raise XrdParsingError(f"XRD raw file not found: {resolved}")

    try:
        data = np.loadtxt(resolved, encoding="utf-8", skiprows=skiprows)
    except Exception as exc:
        raise XrdParsingError(f"failed to load XRD raw file {resolved}: {exc}") from exc

    if data.ndim != 2 or data.shape[1] < 2:
        raise XrdParsingError(f"XRD raw file must contain at least two columns: {resolved}")

    x = np.asarray(data[:, 0], dtype=float)
    y = np.asarray(data[:, 1], dtype=float)
    if len(x) != len(y) or len(x) < 5:
        raise XrdParsingError(f"XRD raw file has insufficient points: {resolved}")
    return x, y


def cut_range(
    x_values: np.ndarray,
    y_values: np.ndarray,
    start: float,
    end: float,
) -> tuple[np.ndarray, np.ndarray]:
    mask = (x_values > start) & (x_values < end)
    return x_values[mask], y_values[mask]


def gaussian(x_values: np.ndarray, amplitude: float, center: float, sigma: float) -> np.ndarray:
    return amplitude * np.exp(-((x_values - center) ** 2) / (2.0 * sigma**2))


def _valid_window(length: int, preferred: int, polyorder: int) -> int | None:
    if length <= polyorder + 2:
        return None

    window = min(preferred, length)
    if window % 2 == 0:
        window -= 1
    minimum = polyorder + 2
    if minimum % 2 == 0:
        minimum += 1
    if window < minimum:
        window = minimum
    if window > length:
        window = length if length % 2 == 1 else length - 1
    if window <= polyorder or window < 3:
        return None
    return window


def smooth_signal(y_values: np.ndarray, *, preferred_window: int = 101, polyorder: int = 3) -> np.ndarray:
    window = _valid_window(len(y_values), preferred_window, polyorder)
    if window is None:
        return y_values.astype(float, copy=True)
    return savgol_filter(y_values, window_length=window, polyorder=polyorder)


def estimate_baseline(y_values: np.ndarray, *, preferred_window: int = 301) -> np.ndarray:
    if len(y_values) < 5:
        return np.full_like(y_values, np.min(y_values) if len(y_values) else 0.0, dtype=float)

    size = min(preferred_window, len(y_values))
    if size < 3:
        size = 3
    if size % 2 == 0:
        size -= 1
    size = max(3, size)

    opened = ndimage.maximum_filter1d(
        ndimage.minimum_filter1d(y_values, size=size, mode="nearest"),
        size=size,
        mode="nearest",
    )
    window = _valid_window(len(opened), min(size, 101), polyorder=2)
    if window is None:
        return opened.astype(float, copy=False)
    return savgol_filter(opened, window_length=window, polyorder=2)


def preprocess_scan(
    x_values: np.ndarray,
    y_values: np.ndarray,
    *,
    scan_range: tuple[float, float],
    smooth_window: int = 101,
    baseline_window: int = 301,
) -> tuple[np.ndarray, np.ndarray]:
    x_cut, y_cut = cut_range(x_values, y_values, scan_range[0], scan_range[1])
    if len(x_cut) < 5:
        raise XrdParsingError(
            f"not enough XRD points in range {scan_range[0]}-{scan_range[1]} deg"
        )
    smoothed = smooth_signal(y_cut, preferred_window=smooth_window)
    baseline = estimate_baseline(smoothed, preferred_window=baseline_window)
    corrected = np.maximum(smoothed - baseline, 0.0)
    return x_cut, corrected


def fit_peak(
    x_values: np.ndarray,
    y_values: np.ndarray,
    spec: PeakSpec,
) -> PeakFitResult | None:
    x_fit, y_fit = cut_range(x_values, y_values, spec.fit_range[0], spec.fit_range[1])
    if len(x_fit) < 5:
        return None

    amplitude_guess = max(float(np.max(y_fit)), spec.amplitude_guess)
    if amplitude_guess <= 0:
        return None

    center_guess = float(x_fit[int(np.argmax(y_fit))])
    center_guess = min(max(center_guess, spec.fit_range[0]), spec.fit_range[1])
    sigma_guess = max(spec.sigma_guess, 0.05)

    try:
        popt, _ = curve_fit(
            gaussian,
            x_fit,
            y_fit,
            p0=[amplitude_guess, center_guess, sigma_guess],
            bounds=(
                [0.0, spec.fit_range[0], 0.01],
                [math.inf, spec.fit_range[1], 10.0],
            ),
            maxfev=20000,
        )
    except Exception:
        return None

    amplitude, center, sigma = (float(value) for value in popt)
    return PeakFitResult(
        label=spec.label,
        amplitude=amplitude,
        center=center,
        sigma=sigma,
        matched=abs(center - spec.center) <= spec.tolerance,
        fit_range=spec.fit_range,
    )


def classify_phase(
    bcc_fits: dict[str, PeakFitResult | None],
    fcc_fits: dict[str, PeakFitResult | None],
) -> str:
    bcc_valid = sum(1 for fit in bcc_fits.values() if fit is not None and fit.matched)
    fcc_valid = sum(1 for fit in fcc_fits.values() if fit is not None and fit.matched)

    if bcc_valid == len(BCC_PEAKS) and fcc_valid == len(FCC_PEAKS):
        bcc_deviation = family_deviation_score(bcc_fits, BCC_PEAKS)
        fcc_deviation = family_deviation_score(fcc_fits, FCC_PEAKS)
        return "bcc" if bcc_deviation <= fcc_deviation else "fcc"

    if bcc_valid == len(BCC_PEAKS):
        return "bcc"

    if fcc_valid == len(FCC_PEAKS):
        return "fcc"

    return "other"


def family_deviation_score(
    fits: dict[str, PeakFitResult | None],
    specs: dict[str, PeakSpec],
) -> float:
    score = 0.0
    for label, spec in specs.items():
        fit = fits.get(label)
        if fit is None:
            return math.inf
        score += abs(fit.center - spec.center)
    return score


def summarize_phase_note(
    phase: str,
    bcc_fits: dict[str, PeakFitResult | None],
    fcc_fits: dict[str, PeakFitResult | None],
) -> str:
    bcc_valid = sum(1 for fit in bcc_fits.values() if fit is not None and fit.matched)
    fcc_valid = sum(1 for fit in fcc_fits.values() if fit is not None and fit.matched)
    bcc_score = family_deviation_score(bcc_fits, BCC_PEAKS)
    fcc_score = family_deviation_score(fcc_fits, FCC_PEAKS)
    return (
        "auto parsed from raw XRD files; "
        f"phase={phase}; "
        f"matched bcc={bcc_valid}/3 (deviation={bcc_score:.4f}); "
        f"matched fcc={fcc_valid}/3 (deviation={fcc_score:.4f})"
    )


def analyze_xrd_pair(
    low_angle_file: str | Path,
    high_angle_file: str | Path,
) -> XrdSampleAnalysis:
    low_path = Path(low_angle_file).expanduser().resolve()
    high_path = Path(high_angle_file).expanduser().resolve()

    low_sample_id = extract_sample_id(low_path)
    high_sample_id = extract_sample_id(high_path)
    if low_sample_id and high_sample_id and low_sample_id != high_sample_id:
        raise XrdParsingError(
            f"sample id mismatch between {low_path.name} and {high_path.name}"
        )

    low_x, low_y = load_xy_file(low_path)
    high_x, high_y = load_xy_file(high_path)

    low_processed_x, low_processed_y = preprocess_scan(low_x, low_y, scan_range=(30.0, 43.0))
    high_processed_x, high_processed_y = preprocess_scan(high_x, high_y, scan_range=(43.0, 74.0))

    bcc_fits = {
        "(110)": fit_peak(low_processed_x, low_processed_y, BCC_PEAKS["(110)"]),
        "(200)": fit_peak(high_processed_x, high_processed_y, BCC_PEAKS["(200)"]),
        "(211)": fit_peak(high_processed_x, high_processed_y, BCC_PEAKS["(211)"]),
    }
    fcc_fits = {
        "(111)": fit_peak(low_processed_x, low_processed_y, FCC_PEAKS["(111)"]),
        "(200)": fit_peak(low_processed_x, low_processed_y, FCC_PEAKS["(200)"]),
        "(220)": fit_peak(high_processed_x, high_processed_y, FCC_PEAKS["(220)"]),
    }

    phase = classify_phase(bcc_fits, fcc_fits)
    note = summarize_phase_note(phase, bcc_fits, fcc_fits)
    return XrdSampleAnalysis(
        sample_id=low_sample_id or high_sample_id,
        low_angle_file=low_path,
        high_angle_file=high_path,
        phase=phase,
        note=note,
        bcc_peaks=bcc_fits,
        fcc_peaks=fcc_fits,
    )


def build_snake_matrix(
    phase_list: list[str],
    *,
    row_num: int = 13,
    col_num: int = 13,
) -> list[list[str]]:
    matrix: list[list[str]] = []
    for row_index in range(row_num):
        row_data: list[str] = []
        for col_index in range(col_num):
            if row_index % 2 == 0:
                index = row_index * col_num + col_index
            else:
                index = row_index * col_num + (col_num - 1 - col_index)
            row_data.append(phase_list[index] if index < len(phase_list) else "other")
        matrix.append(row_data)
    return matrix


def _collect_by_patterns(directory: Path, patterns: Iterable[str]) -> dict[str, Path]:
    matches: dict[str, Path] = {}
    for pattern in patterns:
        for path in sorted(directory.glob(pattern)):
            sample_id = extract_sample_id(path)
            if sample_id is None:
                continue
            if sample_id in matches:
                raise XrdParsingError(f"duplicate XRD file for sample {sample_id}: {path}")
            matches[sample_id] = path.resolve()
    return matches


def discover_xrd_pairs(
    directory: str | Path,
    *,
    low_patterns: Iterable[str] = DEFAULT_LOW_ANGLE_PATTERNS,
    high_patterns: Iterable[str] = DEFAULT_HIGH_ANGLE_PATTERNS,
) -> list[XrdFilePair]:
    resolved = Path(directory).expanduser().resolve()
    if not resolved.exists():
        raise XrdParsingError(f"XRD directory not found: {resolved}")

    low_map = _collect_by_patterns(resolved, low_patterns)
    high_map = _collect_by_patterns(resolved, high_patterns)
    sample_ids = sorted(set(low_map) & set(high_map), key=int)

    return [
        XrdFilePair(
            sample_id=sample_id,
            low_angle_file=low_map[sample_id],
            high_angle_file=high_map[sample_id],
        )
        for sample_id in sample_ids
    ]


def resolve_xrd_pair(
    directory: str | Path,
    sample_id: str | int,
    *,
    low_patterns: Iterable[str] = DEFAULT_LOW_ANGLE_PATTERNS,
    high_patterns: Iterable[str] = DEFAULT_HIGH_ANGLE_PATTERNS,
) -> XrdFilePair:
    normalized_sample_id = normalize_sample_id(sample_id)
    resolved_directory = Path(directory).expanduser().resolve()
    if not resolved_directory.exists():
        raise XrdParsingError(f"XRD directory not found: {resolved_directory}")

    pairs = discover_xrd_pairs(
        resolved_directory,
        low_patterns=low_patterns,
        high_patterns=high_patterns,
    )
    for pair in pairs:
        if pair.sample_id == normalized_sample_id:
            return pair

    raise XrdParsingError(
        f"XRD pair not found for sample {normalized_sample_id} in {resolved_directory}"
    )


def analyze_xrd_directory(
    directory: str | Path,
    *,
    low_patterns: Iterable[str] = DEFAULT_LOW_ANGLE_PATTERNS,
    high_patterns: Iterable[str] = DEFAULT_HIGH_ANGLE_PATTERNS,
) -> list[XrdSampleAnalysis]:
    return [
        analyze_xrd_pair(pair.low_angle_file, pair.high_angle_file)
        for pair in discover_xrd_pairs(directory, low_patterns=low_patterns, high_patterns=high_patterns)
    ]


def write_phase_results_csv(
    analyses: Iterable[XrdSampleAnalysis],
    output_path: str | Path,
) -> Path:
    resolved = Path(output_path).expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)

    with resolved.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "sample_id",
                "phase",
                "bcc_110",
                "bcc_200",
                "bcc_211",
                "fcc_111",
                "fcc_200",
                "fcc_220",
                "low_angle_file",
                "high_angle_file",
                "note",
            ]
        )
        for analysis in analyses:
            writer.writerow(
                [
                    analysis.sample_id or "",
                    analysis.phase,
                    _peak_center(analysis.bcc_peaks.get("(110)")),
                    _peak_center(analysis.bcc_peaks.get("(200)")),
                    _peak_center(analysis.bcc_peaks.get("(211)")),
                    _peak_center(analysis.fcc_peaks.get("(111)")),
                    _peak_center(analysis.fcc_peaks.get("(200)")),
                    _peak_center(analysis.fcc_peaks.get("(220)")),
                    str(analysis.low_angle_file),
                    str(analysis.high_angle_file),
                    analysis.note,
                ]
            )
    return resolved


def write_snake_matrix_xlsx(
    matrix: list[list[str]],
    output_path: str | Path,
) -> Path:
    resolved = Path(output_path).expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)

    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "phase_snake"
    for row_index, row in enumerate(matrix, start=1):
        for col_index, value in enumerate(row, start=1):
            worksheet.cell(row=row_index, column=col_index, value=value)
    workbook.save(resolved)
    return resolved


def phase_counts(analyses: Iterable[XrdSampleAnalysis]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for analysis in analyses:
        counts[analysis.phase] = counts.get(analysis.phase, 0) + 1
    return counts


def _peak_center(fit: PeakFitResult | None) -> str:
    return "-" if fit is None else f"{fit.center:.4f}"
