"""Command-line interface for contribution filters."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from .exceptions import ContributionFilterError
from .extractor import extract_non_individual_contributions, extract_recipient_aggregates
from .schema import (
    ALL_CYCLES,
    MAX_CYCLE,
    MIN_CYCLE,
    get_non_individual_filename,
    get_recipient_aggregates_filename,
)


def main(argv: list[str] | None = None) -> int:
    """Main entry point for the CLI."""
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
        choices=["non_individual", "aggregates", "all"],
        default="all",
        dest="output_type",
        help="Type of output to generate (default: all)",
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

    # Validate cycles are in range
    for c in cycles:
        if c not in ALL_CYCLES:
            print(
                f"ERROR: Invalid cycle {c} (must be even year {MIN_CYCLE}-{MAX_CYCLE})",
                file=sys.stderr,
            )
            return 1

    print(f"[{datetime.now().isoformat()}] Processing {len(cycles)} cycle(s)")
    print(f"  Output directory: {args.output_dir}")
    print(f"  Output type: {args.output_type}")
    print(f"  Validation: {'disabled' if args.no_validate else 'enabled'}")
    if args.skip_existing:
        print("  Skip existing: enabled")
    if args.delay > 0:
        print(f"  Delay between cycles: {args.delay}s")

    success_count = 0
    error_count = 0
    skip_count = 0

    for i, cycle in enumerate(cycles):
        print(f"\n{'=' * 60}")
        print(f"Cycle: {cycle}")
        print(f"{'=' * 60}")

        try:
            cycle_did_work = False

            # Non-individual contributions
            if args.output_type in ("non_individual", "all"):
                output_path = (
                    args.output_dir / "non_individual" / get_non_individual_filename(cycle)
                )
                if args.skip_existing and output_path.exists():
                    print(f"  Skipping (exists): {output_path}")
                else:
                    result = extract_non_individual_contributions(
                        output_path,
                        cycle,
                        validate=not args.no_validate,
                    )
                    file_size = result.output_path.stat().st_size
                    print(f"  Created: {result.output_path}")
                    print(f"  Size: {_format_size(file_size)}")
                    cycle_did_work = True

            # Recipient aggregates
            if args.output_type in ("aggregates", "all"):
                output_path = (
                    args.output_dir
                    / "recipient_aggregates"
                    / get_recipient_aggregates_filename(cycle)
                )
                if args.skip_existing and output_path.exists():
                    print(f"  Skipping (exists): {output_path}")
                else:
                    result = extract_recipient_aggregates(
                        output_path,
                        cycle,
                        validate=not args.no_validate,
                        sample_size=args.sample_size,
                    )
                    file_size = result.output_path.stat().st_size
                    print(f"  Created: {result.output_path}")
                    print(f"  Size: {_format_size(file_size)}")
                    cycle_did_work = True

            if cycle_did_work:
                success_count += 1
                # Apply delay between cycles (not after the last one)
                if args.delay > 0 and i < len(cycles) - 1:
                    import time

                    print(f"  Waiting {args.delay}s before next cycle...")
                    time.sleep(args.delay)
            else:
                skip_count += 1

        except ContributionFilterError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            error_count += 1

    print(f"\n[{datetime.now().isoformat()}] Complete")
    print(f"  Success: {success_count}/{len(cycles)}")
    if skip_count > 0:
        print(f"  Skipped: {skip_count}")
    if error_count > 0:
        print(f"  Errors: {error_count}")
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
