"""Core DIME CSV to Parquet conversion logic."""

import csv
import gzip
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq

from .exceptions import CSVParseError, SchemaValidationError
from .schema import (
    DIME_SCHEMA,
    EXPECTED_COLUMNS,
    NULL_VALUES,
    FileType,
    FileTypeConfig,
    get_config,
)
from .validators import (
    ValidationResult,
    validate_checksums,
    validate_row_count,
    validate_sample_rows,
)


@dataclass
class StreamingStats:
    """Statistics accumulated during streaming conversion."""

    row_count: int = 0
    sum_column_value: float = 0.0  # Sum of configurable column (amount, cfscore, etc.)
    non_null_counts: dict[str, int] = field(default_factory=dict)
    schema: pa.Schema | None = None


@dataclass
class ConversionResult:
    """Result of a successful conversion."""

    source_path: Path
    output_path: Path
    row_count: int
    validation: ValidationResult


def convert_dime_file(
    source_path: Path | str,
    output_path: Path | str,
    file_type: FileType = FileType.CONTRIBUTIONS,
    *,
    validate: bool = True,
    sample_size: int = 1000,
    batch_size: int = 100_000,
) -> ConversionResult:
    """
    Convert a single DIME CSV.gz file to Parquet format using streaming.

    Uses memory-efficient streaming: reads CSV in batches and writes to
    Parquet incrementally, never loading the entire file into memory.

    Args:
        source_path: Path to input CSV.gz file
        output_path: Path for output .parquet file
        file_type: Type of DIME file (contributions, recipients, contributors)
        validate: Whether to run validation suite after conversion
        sample_size: Number of random rows to compare in sample validation
        batch_size: Number of rows to process per batch (default: 100,000)

    Returns:
        ConversionResult with conversion details and validation results

    Raises:
        CSVParseError: If CSV parsing fails
        RowCountMismatchError: If output row count doesn't match input
        ChecksumMismatchError: If column checksums don't match
        SampleMismatchError: If sample rows don't match
        SchemaValidationError: If column schema doesn't match expected
    """
    source_path = Path(source_path)
    output_path = Path(output_path)

    # Get configuration for this file type
    config = get_config(file_type)

    # Step 1: Pre-flight row count (fast, using csv module)
    print(f"  Counting rows in {source_path.name}...")
    expected_row_count = _count_csv_rows(source_path)
    print(f"  Found {expected_row_count:,} rows")

    # Step 2: Stream CSV to Parquet (memory-efficient)
    print(f"  Streaming CSV to Parquet (batch size: {batch_size:,})...")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    stats = _stream_csv_to_parquet(source_path, output_path, batch_size, config)
    print(f"  Written {stats.row_count:,} rows to {output_path}")

    # Step 3: Validate schema
    _validate_schema(source_path, stats.schema, config)

    # Step 4: Validate output
    validation_result = ValidationResult()
    if validate:
        print(f"  Validating...")

        # Validation tier 1: Row count (fastest - uses parquet metadata)
        validation_result = validate_row_count(
            source_path, output_path, expected_row_count
        )
        print(f"    Row count: PASS ({validation_result.row_count_actual:,})")

        # Validation tier 2: Column checksums (uses streaming stats + column reads)
        validation_result = validate_checksums(
            source_path,
            output_path,
            stats,
            validation_result,
            sum_column=config.sum_column,
            key_columns=config.key_columns,
        )
        if config.sum_column:
            print(f"    Checksums: PASS ({config.sum_column} sum: {validation_result.sum_column_actual:,.2f})")
        else:
            print(f"    Checksums: PASS (non-null counts verified)")

        # Validation tier 3: Sample comparison
        validation_result = validate_sample_rows(
            source_path, output_path, sample_size, validation_result
        )
        print(f"    Sample comparison: PASS ({validation_result.sample_size} rows)")

    return ConversionResult(
        source_path=source_path,
        output_path=output_path,
        row_count=stats.row_count,
        validation=validation_result,
    )


def _count_csv_rows(path: Path) -> int:
    """
    Count data rows in CSV using Python csv module.

    This handles multi-line records (embedded newlines in quoted fields)
    correctly, unlike simple newline counting.
    """
    count = 0
    with gzip.open(path, "rt", encoding="latin1") as f:
        reader = csv.reader(f, doublequote=True)
        # Skip header
        next(reader)
        # Count data rows with progress
        for _ in reader:
            count += 1
            if count % 10_000_000 == 0:
                print(f"    Counted {count:,} rows...", flush=True)
    return count


def _stream_csv_to_parquet(
    source_path: Path,
    output_path: Path,
    batch_size: int,
    config: FileTypeConfig,
) -> StreamingStats:
    """
    Stream CSV to Parquet without loading entire file into memory.

    Reads CSV in batches using PyArrow's streaming reader, writes each
    batch to Parquet incrementally, and accumulates statistics for validation.

    Key settings:
    - encoding='latin1': Handle non-UTF-8 characters
    - double_quote=True: Standard CSV escaping (NOT escape_char!)
    - null_values=['\\N', '']: MySQL export format
    - column_types: Explicit types to prevent inference issues
    """
    invalid_rows: list[tuple[int, str]] = []

    def invalid_row_handler(row) -> str:
        """Collect invalid rows for error reporting."""
        invalid_rows.append((row.number, str(row.text)[:500]))
        return "error"  # Fail on any invalid row

    read_options = pa_csv.ReadOptions(
        encoding="latin1",
        block_size=batch_size * 1024,  # Approximate bytes per batch
    )
    parse_options = pa_csv.ParseOptions(
        double_quote=True,
        # NOTE: Do NOT set escape_char - it conflicts with double_quote
        invalid_row_handler=invalid_row_handler,
    )
    convert_options = pa_csv.ConvertOptions(
        null_values=NULL_VALUES,
        strings_can_be_null=True,
        column_types=config.schema,
    )

    # Initialize stats
    stats = StreamingStats()
    for col in config.key_columns:
        stats.non_null_counts[col] = 0

    writer: pq.ParquetWriter | None = None

    try:
        # Open CSV as streaming reader
        reader = pa_csv.open_csv(
            source_path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
        )

        # Process batches
        batch_num = 0
        for batch in reader:
            batch_num += 1

            # First batch: capture schema and create writer
            if writer is None:
                stats.schema = batch.schema
                writer = pq.ParquetWriter(
                    output_path,
                    schema=batch.schema,
                    compression="zstd",
                    compression_level=3,
                )

            # Write batch to parquet
            table = pa.Table.from_batches([batch])
            writer.write_table(table)

            # Accumulate stats
            stats.row_count += batch.num_rows

            # Sum of configurable column (for checksum validation)
            if config.sum_column and config.sum_column in batch.schema.names:
                sum_col = batch.column(config.sum_column)
                batch_sum = pc.sum(sum_col).as_py()
                if batch_sum is not None:
                    stats.sum_column_value += batch_sum

            # Non-null counts for key columns
            for col in config.key_columns:
                if col in batch.schema.names:
                    col_data = batch.column(col)
                    count = pc.count(col_data).as_py()
                    stats.non_null_counts[col] += count

            # Progress indicator for large files
            if batch_num % 10 == 0:
                print(f"    Processed {stats.row_count:,} rows...", flush=True)

    except pa.ArrowInvalid as e:
        error_msg = str(e)
        raise CSVParseError(
            source_path=source_path,
            message=f"PyArrow CSV parse error: {error_msg}",
            line_number=invalid_rows[0][0] if invalid_rows else None,
            problematic_value=invalid_rows[0][1] if invalid_rows else None,
        ) from e

    finally:
        if writer is not None:
            writer.close()

    return stats


def _validate_schema(path: Path, schema: pa.Schema, config: FileTypeConfig) -> None:
    """Validate that table schema matches expected columns."""
    actual_columns = schema.names
    if set(actual_columns) != set(config.expected_columns):
        raise SchemaValidationError(
            source_path=path,
            message="Column schema mismatch",
            expected_columns=config.expected_columns,
            actual_columns=actual_columns,
        )
