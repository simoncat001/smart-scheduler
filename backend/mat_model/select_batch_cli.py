from __future__ import annotations

import argparse
import json
from pathlib import Path

from .experiment_selection import write_experimental_batch


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Select a diverse first-round experimental batch.")
    parser.add_argument(
        "--candidates",
        required=True,
        help="Path to the recommended_process_candidates.csv file.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to the selected batch CSV file.",
    )
    parser.add_argument(
        "--summary",
        required=True,
        help="Path to the batch summary JSON file.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=6,
        help="Number of experiments to select.",
    )
    parser.add_argument(
        "--pool-size",
        type=int,
        default=20,
        help="How many top reliable candidates to consider as the selection pool.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    batch, summary = write_experimental_batch(
        candidates_csv=Path(args.candidates),
        output_csv=Path(args.output),
        summary_json=Path(args.summary),
        batch_size=args.batch_size,
        pool_size=args.pool_size,
    )
    print(
        json.dumps(
            {"summary": summary, "selected_roles": batch["selection_role"].tolist()},
            indent=2,
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
