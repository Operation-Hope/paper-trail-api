"""Command-line interface for distinct legislators extractor."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from .exceptions import DistinctLegislatorsError
from .extractor import extract_distinct_legislators
from .schema import MIN_CONGRESS, VOTEVIEW_MEMBERS_URL


def main() -> int:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Extract distinct legislators from Voteview data",
        epilog="""
Examples:
  %(prog)s legislators.parquet
  %(prog)s legislators.parquet --min-congress 100
  %(prog)s legislators.parquet --no-validate
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "output",
        type=Path,
        help="Output Parquet file path",
    )
    parser.add_argument(
        "--source-url",
        type=str,
        default=VOTEVIEW_MEMBERS_URL,
        help="Source parquet URL (default: HuggingFace Voteview)",
    )
    parser.add_argument(
        "--min-congress",
        type=int,
        default=MIN_CONGRESS,
        help=f"Minimum congress number (default: {MIN_CONGRESS} = 1979)",
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip validation (not recommended)",
    )
    parser.add_argument(
        "--aggregation-sample",
        type=int,
        default=100,
        help="Sample size for aggregation validation (default: 100)",
    )
    parser.add_argument(
        "--deep-sample",
        type=int,
        default=50,
        help="Sample size for deep validation (default: 50)",
    )

    args = parser.parse_args()

    print(f"[{datetime.now().isoformat()}] Extracting distinct legislators")
    print(f"  Min congress: {args.min_congress}")

    try:
        result = extract_distinct_legislators(
            args.output,
            source_url=args.source_url,
            min_congress=args.min_congress,
            validate=not args.no_validate,
            aggregation_sample_size=args.aggregation_sample,
            deep_sample_size=args.deep_sample,
        )

        print(f"\n[{datetime.now().isoformat()}] SUCCESS")
        print(f"  Output: {result.output_path}")
        print(f"  Size: {result.output_path.stat().st_size / 1024:.1f} KB")
        print(f"  Legislators: {result.output_count:,}")

        if args.no_validate:
            print("  Validation: SKIPPED")
        else:
            print("  Validation: ALL PASSED")

        return 0

    except DistinctLegislatorsError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
