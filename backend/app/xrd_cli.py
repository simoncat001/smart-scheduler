from __future__ import annotations

import argparse
from pathlib import Path

from app.xrd_parser import (
    DEFAULT_HIGH_ANGLE_PATTERNS,
    DEFAULT_LOW_ANGLE_PATTERNS,
    analyze_xrd_directory,
    analyze_xrd_pair,
    build_snake_matrix,
    phase_counts,
    write_phase_results_csv,
    write_snake_matrix_xlsx,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Parse raw XRD .xy files and classify phases for Ti-Zr-Nb-V samples."
    )
    parser.add_argument("--input-dir", help="Directory containing raw XRD .xy files.")
    parser.add_argument("--low-file", help="Low-angle XRD file for a single sample.")
    parser.add_argument("--high-file", help="High-angle XRD file for a single sample.")
    parser.add_argument(
        "--low-pattern",
        action="append",
        default=[],
        help="Glob pattern for low-angle files. Can be passed multiple times.",
    )
    parser.add_argument(
        "--high-pattern",
        action="append",
        default=[],
        help="Glob pattern for high-angle files. Can be passed multiple times.",
    )
    parser.add_argument(
        "--output-csv",
        help="CSV output path for batch mode. Defaults to <input-dir>/phase_judge_result.csv.",
    )
    parser.add_argument(
        "--output-xlsx",
        help="Excel output path for batch mode. Defaults to <input-dir>/phase_snake_arrangement.xlsx.",
    )
    parser.add_argument("--rows", type=int, default=13, help="Row count for snake layout.")
    parser.add_argument("--cols", type=int, default=13, help="Column count for snake layout.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.low_file and args.high_file:
        analysis = analyze_xrd_pair(args.low_file, args.high_file)
        print(f"sample_id: {analysis.sample_id or '-'}")
        print(f"phase: {analysis.phase}")
        print(f"note: {analysis.note}")
        for family_name, family in (("bcc", analysis.bcc_peaks), ("fcc", analysis.fcc_peaks)):
            for label, fit in family.items():
                if fit is None:
                    print(f"{family_name} {label}: -")
                    continue
                print(
                    f"{family_name} {label}: center={fit.center:.4f}, sigma={fit.sigma:.4f}, matched={fit.matched}"
                )
        return 0

    if not args.input_dir:
        parser.error("Provide either --input-dir for batch mode or --low-file with --high-file.")

    input_dir = Path(args.input_dir).expanduser().resolve()
    low_patterns = tuple(args.low_pattern) if args.low_pattern else DEFAULT_LOW_ANGLE_PATTERNS
    high_patterns = tuple(args.high_pattern) if args.high_pattern else DEFAULT_HIGH_ANGLE_PATTERNS
    analyses = analyze_xrd_directory(
        input_dir,
        low_patterns=low_patterns,
        high_patterns=high_patterns,
    )

    output_csv = Path(args.output_csv).expanduser().resolve() if args.output_csv else input_dir / "phase_judge_result.csv"
    output_xlsx = Path(args.output_xlsx).expanduser().resolve() if args.output_xlsx else input_dir / "phase_snake_arrangement.xlsx"

    write_phase_results_csv(analyses, output_csv)
    snake_matrix = build_snake_matrix([analysis.phase for analysis in analyses], row_num=args.rows, col_num=args.cols)
    write_snake_matrix_xlsx(snake_matrix, output_xlsx)

    print(f"parsed_samples: {len(analyses)}")
    print(f"output_csv: {output_csv}")
    print(f"output_xlsx: {output_xlsx}")
    for phase, count in sorted(phase_counts(analyses).items()):
        print(f"{phase}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
