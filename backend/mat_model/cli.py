from __future__ import annotations

import argparse
import json

from .pipeline import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the quaternary alloy hardness modeling pipeline.",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to the JSON configuration file.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    summary = run_pipeline(args.config)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
