"""Command-line interface for Congress Legislators CSV to Parquet converter."""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from .converter import convert_legislators_file
from .downloader import download_all, download_file
from .exceptions import CongressLegislatorsConversionError, DownloadError
from .schema import FileType


def detect_file_type(filename: str) -> FileType:
    """
    Auto-detect file type from filename.

    Recognizes patterns like:
    - legislators-current.csv -> CURRENT
    - legislators-historical.csv -> HISTORICAL
    """
    name_lower = filename.lower()
    if "current" in name_lower:
        return FileType.CURRENT
    if "historical" in name_lower:
        return FileType.HISTORICAL
    raise ValueError(f"Cannot auto-detect file type from: {filename}")


def cmd_download(args: argparse.Namespace) -> int:
    """Handle the download subcommand."""
    output_dir = Path(args.output_dir)
    print(f"[{datetime.now().isoformat()}] Downloading legislators data...")

    try:
        if args.file_type:
            file_type = FileType(args.file_type)
            path = download_file(file_type, output_dir)
            print(f"  Downloaded: {path}")
        else:
            paths = download_all(output_dir)
            for ft, path in paths.items():
                print(f"  Downloaded {ft.value}: {path}")

        print(f"[{datetime.now().isoformat()}] Download complete")
        return 0

    except DownloadError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


def cmd_convert(args: argparse.Namespace) -> int:
    """Handle the convert subcommand."""
    source_path = Path(args.source)
    output_path = Path(args.output)

    # Validate source exists
    if not source_path.exists():
        print(f"ERROR: Source file not found: {source_path}", file=sys.stderr)
        return 1

    # Determine file type
    if args.file_type:
        file_type = FileType(args.file_type)
    else:
        try:
            file_type = detect_file_type(source_path.name)
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            print("Use -t/--file-type to specify explicitly", file=sys.stderr)
            return 1

    print(f"[{datetime.now().isoformat()}] Converting: {source_path.name}")
    print(f"  File type: {file_type.value}")

    try:
        result = convert_legislators_file(
            source_path,
            output_path,
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

    except CongressLegislatorsConversionError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


def cmd_all(args: argparse.Namespace) -> int:
    """Handle the all subcommand (download + convert all files)."""
    output_dir = Path(args.output_dir)
    print(f"[{datetime.now().isoformat()}] Processing all legislators data...")

    try:
        # Step 1: Download all files
        print("\n=== Downloading ===")
        csv_paths = download_all(output_dir)

        # Step 2: Convert all files
        print("\n=== Converting ===")
        for file_type, csv_path in csv_paths.items():
            parquet_path = output_dir / f"legislators-{file_type.value}.parquet"

            print(f"\n[{datetime.now().isoformat()}] Converting: {csv_path.name}")
            print(f"  File type: {file_type.value}")

            result = convert_legislators_file(
                csv_path,
                parquet_path,
                file_type,
                validate=not args.no_validate,
                sample_size=args.sample_size,
                batch_size=args.batch_size,
            )

            print(f"  SUCCESS: {result.row_count:,} rows -> {parquet_path.name}")

        print(f"\n[{datetime.now().isoformat()}] All conversions complete")
        return 0

    except (DownloadError, CongressLegislatorsConversionError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


def main() -> int:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Download and convert Congress Legislators data to Parquet",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Download subcommand
    download_parser = subparsers.add_parser(
        "download",
        help="Download CSV files from unitedstates.github.io",
    )
    download_parser.add_argument(
        "--output-dir",
        "-o",
        type=str,
        default=".",
        help="Directory to save downloaded files (default: current directory)",
    )
    download_parser.add_argument(
        "-t",
        "--file-type",
        choices=["current", "historical"],
        default=None,
        help="Specific file type to download (downloads all if not specified)",
    )
    download_parser.set_defaults(func=cmd_download)

    # Convert subcommand
    convert_parser = subparsers.add_parser(
        "convert",
        help="Convert a CSV file to Parquet format",
    )
    convert_parser.add_argument(
        "source",
        type=str,
        help="Source CSV file path",
    )
    convert_parser.add_argument(
        "output",
        type=str,
        help="Output Parquet file path",
    )
    convert_parser.add_argument(
        "-t",
        "--file-type",
        choices=["current", "historical"],
        default=None,
        help="Type of file (auto-detected from filename if not specified)",
    )
    convert_parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip validation (not recommended)",
    )
    convert_parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="Number of rows to sample for validation",
    )
    convert_parser.add_argument(
        "--batch-size",
        type=int,
        default=100_000,
        help="Rows per batch for streaming conversion (default: 100000)",
    )
    convert_parser.set_defaults(func=cmd_convert)

    # All subcommand (download + convert)
    all_parser = subparsers.add_parser(
        "all",
        help="Download and convert all files",
    )
    all_parser.add_argument(
        "--output-dir",
        "-o",
        type=str,
        default=".",
        help="Directory for downloads and conversions (default: current directory)",
    )
    all_parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip validation (not recommended)",
    )
    all_parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="Number of rows to sample for validation",
    )
    all_parser.add_argument(
        "--batch-size",
        type=int,
        default=100_000,
        help="Rows per batch for streaming conversion (default: 100000)",
    )
    all_parser.set_defaults(func=cmd_all)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
