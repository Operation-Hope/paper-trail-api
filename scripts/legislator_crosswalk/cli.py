"""Command-line interface for legislator crosswalk extractor."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from .exceptions import CrosswalkError
from .extractor import extract_crosswalk
from .schema import DIME_RECIPIENTS_URL


def main() -> int:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Extract legislator-recipient crosswalk from DIME Recipients data",
        epilog="""
Examples:
  %(prog)s crosswalk.parquet
  %(prog)s crosswalk.parquet --no-validate
  %(prog)s crosswalk.parquet --sample-size 200
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
        default=DIME_RECIPIENTS_URL,
        help="Source parquet URL (default: HuggingFace DIME Recipients)",
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip validation (not recommended)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=100,
        help="Sample size for validation (default: 100)",
    )

    args = parser.parse_args()

    print(f"[{datetime.now().isoformat()}] Extracting legislator-recipient crosswalk")

    try:
        result = extract_crosswalk(
            args.output,
            source_url=args.source_url,
            validate=not args.no_validate,
            sample_size=args.sample_size,
        )

        print(f"\n[{datetime.now().isoformat()}] SUCCESS")
        print(f"  Output: {result.output_path}")
        print(f"  Size: {result.output_path.stat().st_size / 1024:.1f} KB")
        print(f"  Crosswalk rows: {result.output_count:,}")
        print(f"  Unique legislators (ICPSR): {result.unique_icpsr_count:,}")
        print(f"  Unique recipients (bonica_rid): {result.unique_bonica_rid_count:,}")
        if result.unique_icpsr_count > 0:
            avg = result.unique_bonica_rid_count / result.unique_icpsr_count
            print(f"  Avg recipients per legislator: {avg:.1f}")

        if args.no_validate:
            print("  Validation: SKIPPED")
        else:
            print("  Validation: ALL PASSED")

        return 0

    except CrosswalkError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
