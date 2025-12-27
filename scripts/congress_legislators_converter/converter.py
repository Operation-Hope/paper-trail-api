"""Core Congress Legislators CSV to Parquet conversion logic with streaming."""

import csv
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq

from .exceptions import CSVParseError, SchemaValidationError
from .schema import (
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
    sum_column_value: float = 0.0
    non_null_counts: dict[str, int] = field(default_factory=dict)
    schema: pa.Schema | None = None


@dataclass
class ConversionResult:
    """Result of a successful conversion."""

    source_path: Path
    output_path: Path
    row_count: int
    validation: ValidationResult


def convert_legislators_file(
    source_path: Path | str,
    output_path: Path | str,
    file_type: FileType,
    *,
    validate: bool = True,
    sample_size: int | None = None,
    batch_size: int = 100_000,
) -> ConversionResult:
    """
    Convert a Congress Legislators CSV file to Parquet format using streaming.

    Args:
        source_path: Path to input CSV file
        output_path: Path for output .parquet file
        file_type: Type of legislators file (CURRENT or HISTORICAL)
        validate: Whether to run validation suite after conversion
        sample_size: Number of random rows to compare (uses config default if None)
        batch_size: Number of rows to process per batch

    Returns:
        ConversionResult with conversion details and validation results

    Raises:
        CongressLegislatorsConversionError: If conversion or validation fails
    """
    source_path = Path(source_path)
    output_path = Path(output_path)

    config = get_config(file_type)
    actual_sample_size = sample_size or config.sample_size

    # Step 1: Pre-flight row count
    print(f"  Counting rows in {source_path.name}...")
    expected_row_count = _count_csv_rows(source_path)
    print(f"  Found {expected_row_count:,} rows")

    # Step 2: Stream CSV to Parquet
    print(f"  Streaming CSV to Parquet (batch size: {batch_size:,})...")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    stats = _stream_csv_to_parquet(source_path, output_path, batch_size, config)
    print(f"  Written {stats.row_count:,} rows to {output_path}")

    # Step 3: Validate schema
    _validate_schema(source_path, stats.schema, config)

    # Step 4: Validate output
    validation_result = ValidationResult()
    if validate:
        print("  Validating...")

        validation_result = validate_row_count(source_path, output_path, expected_row_count)
        print(f"    Row count: PASS ({validation_result.row_count_actual:,})")

        validation_result = validate_checksums(
            source_path,
            output_path,
            stats,
            validation_result,
            sum_column=config.sum_column,
            key_columns=config.key_columns,
        )
        print("    Checksums: PASS (non-null counts verified)")

        validation_result = validate_sample_rows(
            source_path, output_path, actual_sample_size, validation_result
        )
        print(f"    Sample comparison: PASS ({validation_result.sample_size} rows)")

    return ConversionResult(
        source_path=source_path,
        output_path=output_path,
        row_count=stats.row_count,
        validation=validation_result,
    )


def _count_csv_rows(path: Path) -> int:
    """Count data rows in plain CSV file (excluding header)."""
    count = 0
    with path.open(encoding="utf-8") as f:
        reader = csv.reader(f, doublequote=True)
        next(reader)  # Skip header
        for _ in reader:
            count += 1
    return count


def _stream_csv_to_parquet(
    source_path: Path,
    output_path: Path,
    batch_size: int,
    config: FileTypeConfig,
) -> StreamingStats:
    """Stream CSV to Parquet without loading entire file into memory."""
    invalid_rows: list[tuple[int, str]] = []

    def invalid_row_handler(row: pa_csv.InvalidRow) -> str:
        invalid_rows.append((row.number, str(row.text)[:500]))
        return "error"

    read_options = pa_csv.ReadOptions(
        encoding="utf-8",
        block_size=batch_size * 1024,
    )
    parse_options = pa_csv.ParseOptions(
        double_quote=True,
        invalid_row_handler=invalid_row_handler,
    )
    convert_options = pa_csv.ConvertOptions(
        null_values=NULL_VALUES,
        strings_can_be_null=True,
        column_types=config.schema,
    )

    stats = StreamingStats()
    for col in config.key_columns:
        stats.non_null_counts[col] = 0

    writer: pq.ParquetWriter | None = None

    try:
        reader = pa_csv.open_csv(
            source_path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
        )

        batch_num = 0
        for batch in reader:
            batch_num += 1

            if writer is None:
                stats.schema = batch.schema
                writer = pq.ParquetWriter(
                    output_path,
                    schema=batch.schema,
                    compression="zstd",
                    compression_level=3,
                )

            table = pa.Table.from_batches([batch])
            writer.write_table(table)

            stats.row_count += batch.num_rows

            # Sum column for checksum (not applicable for string-only datasets)
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

            # Progress every 10 batches
            if batch_num % 10 == 0:
                print(f"    Processed {stats.row_count:,} rows...", flush=True)

    except pa.ArrowInvalid as e:
        raise CSVParseError(
            source_path=source_path,
            message=f"PyArrow CSV parse error: {e}",
            line_number=invalid_rows[0][0] if invalid_rows else None,
            problematic_value=invalid_rows[0][1] if invalid_rows else None,
        ) from e

    finally:
        if writer is not None:
            writer.close()

    return stats


def _validate_schema(path: Path, schema: pa.Schema | None, config: FileTypeConfig) -> None:
    """Validate that table schema matches expected columns."""
    if schema is None:
        raise SchemaValidationError(
            source_path=path,
            message="No schema captured - file may be empty",
            expected_columns=config.expected_columns,
            actual_columns=[],
        )

    actual_columns = schema.names
    if set(actual_columns) != set(config.expected_columns):
        raise SchemaValidationError(
            source_path=path,
            message="Column schema mismatch",
            expected_columns=config.expected_columns,
            actual_columns=actual_columns,
        )
