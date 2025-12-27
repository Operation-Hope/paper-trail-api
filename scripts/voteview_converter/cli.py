"""Command-line interface for Voteview CSV to Parquet converter."""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from .converter import convert_voteview_file
from .exceptions import VoteviewConversionError
from .schema import FileType


def detect_file_type(filename: str) -> FileType:
    """
    Auto-detect file type from filename.

    Recognizes patterns like:
    - HSall_members.csv -> MEMBERS
    - HSall_rollcalls.csv -> ROLLCALLS
    - HSall_votes.csv -> VOTES
    """
    name_lower = filename.lower()
    if "member" in name_lower:
        return FileType.MEMBERS
    if "rollcall" in name_lower:
        return FileType.ROLLCALLS
    if "vote" in name_lower:
        return FileType.VOTES
    raise ValueError(f"Cannot auto-detect file type from: {filename}")


def main() -> int:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Convert Voteview CSV files to Parquet format with validation",
        epilog="""
Examples:
  %(prog)s HSall_members.csv members.parquet
  %(prog)s HSall_rollcalls.csv rollcalls.parquet
  %(prog)s HSall_votes.csv votes.parquet --batch-size 200000
  %(prog)s input.csv output.parquet -t votes
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "source",
        type=Path,
        help="Source CSV file path",
    )
    parser.add_argument(
        "output",
        type=Path,
        help="Output Parquet file path",
    )
    parser.add_argument(
        "-t",
        "--file-type",
        choices=["members", "rollcalls", "votes"],
        default=None,
        help="Type of Voteview file (auto-detected from filename if not specified)",
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip validation (not recommended)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="Number of rows to sample for validation (uses type default if not specified)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100_000,
        help="Rows per batch for streaming conversion (default: 100000)",
    )

    args = parser.parse_args()

    # Validate source exists
    if not args.source.exists():
        print(f"ERROR: Source file not found: {args.source}", file=sys.stderr)
        return 1

    # Determine file type
    if args.file_type:
        file_type = FileType(args.file_type)
    else:
        try:
            file_type = detect_file_type(args.source.name)
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            print("Use -t/--file-type to specify explicitly", file=sys.stderr)
            return 1

    print(f"[{datetime.now().isoformat()}] Converting: {args.source.name}")
    print(f"  File type: {file_type.value}")

    try:
        result = convert_voteview_file(
            args.source,
            args.output,
            file_type,
            validate=not args.no_validate,
            sample_size=args.sample_size,
            batch_size=args.batch_size,
        )

        print(f"[{datetime.now().isoformat()}] SUCCESS: {result.row_count:,} rows")
        print(f"  Output: {result.output_path}")

        if args.no_validate:
            print("  Validation: SKIPPED")
        else:
            print("  Validation: ALL PASSED")

        return 0

    except VoteviewConversionError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
