"""Command line interface for the tracker."""

from __future__ import annotations

import argparse
from pathlib import Path

from flight_price_tracker.run import run_once


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser.

    Returns:
        The configured argument parser.
    """
    parser = argparse.ArgumentParser(prog="flight-price-tracker")
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Fetch prices, persist to parquet, and generate report")
    run_p.add_argument("--config", type=Path, default=Path("config.yaml"))

    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CLI.

    Args:
        argv: Optional list of CLI arguments. If None, argparse reads from sys.argv.

    Returns:
        Process exit code.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "run":
        run_once(config_path=args.config)
        return 0

    raise AssertionError(f"Unhandled command: {args.command}")
