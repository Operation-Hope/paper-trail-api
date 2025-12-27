"""Command-line interface for DIME converter."""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from .converter import convert_dime_file
from .exceptions import DIMEConversionError
from .schema import FileType


def detect_file_type(filename: str) -> FileType:
    """Auto-detect file type from filename."""
    name_lower = filename.lower()
    if "recipient" in name_lower:
        return FileType.RECIPIENTS
    if "contributor" in name_lower:
        return FileType.CONTRIBUTORS
    return FileType.CONTRIBUTIONS


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert DIME campaign finance CSV.gz files to Parquet format"
    )
    parser.add_argument(
        "source",
        type=Path,
        help="Source CSV.gz file path",
    )
    parser.add_argument(
        "output",
        type=Path,
        help="Output Parquet file path",
    )
    parser.add_argument(
        "-t",
        "--file-type",
        choices=["contributions", "recipients", "contributors"],
        default=None,
        help="Type of DIME file (auto-detected from filename if not specified)",
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip validation (not recommended)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=1000,
        help="Number of rows to sample for validation (default: 1000)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100_000,
        help="Rows per batch for streaming conversion (default: 100000)",
    )

    args = parser.parse_args()

    # Determine file type
    if args.file_type:
        file_type = FileType(args.file_type)
    else:
        file_type = detect_file_type(args.source.name)

    print(f"[{datetime.now().isoformat()}] Converting: {args.source.name}")
    print(f"  File type: {file_type.value}")

    try:
        result = convert_dime_file(
            args.source,
            args.output,
            file_type,
            validate=not args.no_validate,
            sample_size=args.sample_size,
            batch_size=args.batch_size,
        )
        print(f"[{datetime.now().isoformat()}] SUCCESS: {result.row_count:,} rows")
        return 0

    except DIMEConversionError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


def __main__():
    sys.exit(main())


if __name__ == "__main__":
    sys.exit(main())
