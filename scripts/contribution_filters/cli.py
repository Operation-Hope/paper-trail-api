"""Command-line interface for contribution filters."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from .exceptions import ContributionFilterError
from .extractor import (
    extract_organizational_contributions,
    extract_raw_organizational_contributions,
    extract_recipient_aggregates,
)
from .schema import (
    ALL_CYCLES,
    MAX_CYCLE,
    MIN_CYCLE,
    get_organizational_filename,
    get_raw_organizational_filename,
    get_recipient_aggregates_filename,
)

logger = logging.getLogger(__name__)


def _setup_logging() -> None:
    """Configure logging for CLI usage."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def main(argv: list[str] | None = None) -> int:
    """Main entry point for the CLI."""
    _setup_logging()

    parser = argparse.ArgumentParser(
        description="Extract filtered contribution datasets from DIME data",
        epilog="""
Examples:
  %(prog)s output/ --cycle 2020
  %(prog)s output/ --all
  %(prog)s output/ --start-cycle 2000 --end-cycle 2020
  %(prog)s output/ --cycle 2020 --output-type aggregates
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "output_dir",
        type=Path,
        help="Output directory for parquet files",
    )

    # Cycle selection (mutually exclusive group)
    cycle_group = parser.add_mutually_exclusive_group(required=True)
    cycle_group.add_argument(
        "--cycle",
        type=int,
        help=f"Single election cycle (even year {MIN_CYCLE}-{MAX_CYCLE})",
    )
    cycle_group.add_argument(
        "--all",
        action="store_true",
        help=f"Process all cycles ({MIN_CYCLE}-{MAX_CYCLE})",
    )
    cycle_group.add_argument(
        "--start-cycle",
        type=int,
        dest="start_cycle",
        help="Start of cycle range (use with --end-cycle)",
    )

    parser.add_argument(
        "--end-cycle",
        type=int,
        dest="end_cycle",
        help="End of cycle range (use with --start-cycle)",
    )

    parser.add_argument(
        "--output-type",
        choices=["organizational", "aggregates", "raw-organizational", "all"],
        default="all",
        dest="output_type",
        help=(
            "Type of output to generate (default: all). "
            "'raw-organizational' requires --legislators-path."
        ),
    )
    parser.add_argument(
        "--legislators-path",
        type=Path,
        dest="legislators_path",
        help=(
            "Path to legislators.parquet for bioguide_id lookup. "
            "When provided, adds bioguide_id column to outputs."
        ),
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        dest="no_validate",
        help="Skip validation (not recommended)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=100,
        dest="sample_size",
        help="Sample size for aggregation validation (default: 100)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        dest="skip_existing",
        help="Skip files that already exist",
    )
    parser.add_argument(
        "--delay",
        type=int,
        default=0,
        help="Delay in seconds between cycles (helps with rate limiting)",
    )

    args = parser.parse_args(argv)

    # Determine cycles to process
    cycles: list[int] = []
    if args.cycle:
        cycles = [args.cycle]
    elif args.all:
        cycles = ALL_CYCLES
    elif args.start_cycle:
        if not args.end_cycle:
            print("ERROR: --start-cycle requires --end-cycle", file=sys.stderr)
            return 1
        cycles = [c for c in ALL_CYCLES if args.start_cycle <= c <= args.end_cycle]

    if not cycles:
        print("ERROR: No valid cycles specified", file=sys.stderr)
        return 1

    # Validate sample_size is positive
    if args.sample_size <= 0:
        print("ERROR: --sample-size must be a positive integer", file=sys.stderr)
        return 1

    # Validate cycles are in range
    for c in cycles:
        if c not in ALL_CYCLES:
            print(
                f"ERROR: Invalid cycle {c} (must be even year {MIN_CYCLE}-{MAX_CYCLE})",
                file=sys.stderr,
            )
            return 1

    # Validate raw-organizational requires legislators-path
    if args.output_type == "raw-organizational" and not args.legislators_path:
        print(
            "ERROR: --output-type raw-organizational requires --legislators-path",
            file=sys.stderr,
        )
        return 1

    # Validate legislators_path exists if provided
    if args.legislators_path and not args.legislators_path.exists():
        print(
            f"ERROR: Legislators file not found: {args.legislators_path}",
            file=sys.stderr,
        )
        return 1

    logger.info("[%s] Processing %d cycle(s)", datetime.now().isoformat(), len(cycles))
    logger.info("  Output directory: %s", args.output_dir)
    logger.info("  Output type: %s", args.output_type)
    if args.legislators_path:
        logger.info("  Legislators path: %s", args.legislators_path)
    logger.info("  Validation: %s", "disabled" if args.no_validate else "enabled")
    if args.skip_existing:
        logger.info("  Skip existing: enabled")
    if args.delay > 0:
        logger.info("  Delay between cycles: %ds", args.delay)

    success_count = 0
    error_count = 0
    skip_count = 0

    for i, cycle in enumerate(cycles):
        logger.info("")
        logger.info("=" * 60)
        logger.info("Cycle: %d", cycle)
        logger.info("=" * 60)

        try:
            cycle_did_work = False

            # Organizational contributions
            if args.output_type in ("organizational", "all"):
                output_path = (
                    args.output_dir / "organizational" / get_organizational_filename(cycle)
                )
                if args.skip_existing and output_path.exists():
                    logger.info("  Skipping (exists): %s", output_path)
                else:
                    result = extract_organizational_contributions(
                        output_path,
                        cycle,
                        legislators_path=args.legislators_path,
                        validate=not args.no_validate,
                    )
                    file_size = result.output_path.stat().st_size
                    logger.info("  Created: %s", result.output_path)
                    logger.info("  Size: %s", _format_size(file_size))
                    cycle_did_work = True

            # Recipient aggregates
            if args.output_type in ("aggregates", "all"):
                output_path = (
                    args.output_dir
                    / "recipient_aggregates"
                    / get_recipient_aggregates_filename(cycle)
                )
                if args.skip_existing and output_path.exists():
                    logger.info("  Skipping (exists): %s", output_path)
                else:
                    result = extract_recipient_aggregates(
                        output_path,
                        cycle,
                        legislators_path=args.legislators_path,
                        validate=not args.no_validate,
                        sample_size=args.sample_size,
                    )
                    file_size = result.output_path.stat().st_size
                    logger.info("  Created: %s", result.output_path)
                    logger.info("  Size: %s", _format_size(file_size))
                    cycle_did_work = True

            # Raw organizational contributions (requires legislators_path)
            if args.output_type in ("raw-organizational", "all") and args.legislators_path:
                output_path = (
                    args.output_dir / "raw_organizational" / get_raw_organizational_filename(cycle)
                )
                if args.skip_existing and output_path.exists():
                    logger.info("  Skipping (exists): %s", output_path)
                else:
                    result = extract_raw_organizational_contributions(
                        output_path,
                        cycle,
                        args.legislators_path,
                        validate=not args.no_validate,
                    )
                    file_size = result.output_path.stat().st_size
                    logger.info("  Created: %s", result.output_path)
                    logger.info("  Size: %s", _format_size(file_size))
                    cycle_did_work = True

            if cycle_did_work:
                success_count += 1
                # Apply delay between cycles (not after the last one)
                if args.delay > 0 and i < len(cycles) - 1:
                    import time

                    logger.info("  Waiting %ds before next cycle...", args.delay)
                    time.sleep(args.delay)
            else:
                skip_count += 1

        except ContributionFilterError as e:
            logger.error("ERROR: %s", e)
            error_count += 1

    logger.info("")
    logger.info("[%s] Complete", datetime.now().isoformat())
    logger.info("  Success: %d/%d", success_count, len(cycles))
    if skip_count > 0:
        logger.info("  Skipped: %d", skip_count)
    if error_count > 0:
        logger.info("  Errors: %d", error_count)
        return 1

    return 0


def _format_size(size_bytes: int) -> str:
    """Format file size in human-readable format."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


if __name__ == "__main__":
    sys.exit(main())
