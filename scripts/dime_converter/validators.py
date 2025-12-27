"""Three-tier validation suite for DIME parquet conversions."""

from __future__ import annotations

import csv
import gzip
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.compute as pc
import pyarrow.parquet as pq

from .exceptions import (
    ChecksumMismatchError,
    RowCountMismatchError,
    SampleMismatchError,
)

if TYPE_CHECKING:
    from .converter import StreamingStats


@dataclass
class ValidationResult:
    """Results from validation suite."""

    row_count_valid: bool = False
    row_count_expected: int = 0
    row_count_actual: int = 0

    checksum_valid: bool = False
    sum_column_name: str | None = None  # Name of column used for sum validation
    sum_column_expected: float = 0.0
    sum_column_actual: float = 0.0
    non_null_counts: dict[str, tuple[int, int]] = field(default_factory=dict)

    sample_valid: bool = False
    sample_size: int = 0

    @property
    def all_valid(self) -> bool:
        return self.row_count_valid and self.checksum_valid and self.sample_valid


def validate_row_count(
    source_path: Path,
    output_path: Path,
    expected_count: int,
) -> ValidationResult:
    """
    Tier 1 validation: Verify row counts match.

    This is the fastest validation - just reads parquet metadata.
    """
    meta = pq.read_metadata(output_path)
    actual_count = meta.num_rows

    result = ValidationResult(
        row_count_expected=expected_count,
        row_count_actual=actual_count,
    )

    if actual_count != expected_count:
        raise RowCountMismatchError(
            source_path=source_path,
            message="Row count validation failed",
            expected_rows=expected_count,
            actual_rows=actual_count,
        )

    result.row_count_valid = True
    return result


def validate_checksums(
    source_path: Path,
    output_path: Path,
    source_stats: StreamingStats,
    result: ValidationResult,
    sum_column: str | None = "amount",
    key_columns: list[str] | None = None,
) -> ValidationResult:
    """
    Tier 2 validation: Verify column checksums.

    Compares streaming stats (accumulated during conversion) against
    parquet data using memory-efficient column-level reads.

    Compares:
    - Sum of configurable column (detects truncation/conversion errors)
    - Non-null counts for key columns (detects dropped data)

    Args:
        source_path: Path to source CSV file
        output_path: Path to output parquet file
        source_stats: Statistics accumulated during conversion
        result: ValidationResult to update
        sum_column: Column to sum for checksum (None to skip sum validation)
        key_columns: Columns to check non-null counts
    """
    if key_columns is None:
        key_columns = ["transaction.id", "bonica.cid", "contributor.name", "amount"]

    result.sum_column_name = sum_column

    # Checksum 1: Sum of configurable column
    if sum_column:
        source_sum = source_stats.sum_column_value

        # Read only the sum column from parquet (memory-efficient)
        sum_table = pq.read_table(output_path, columns=[sum_column])
        parquet_sum = pc.sum(sum_table.column(sum_column)).as_py() or 0.0

        result.sum_column_expected = source_sum
        result.sum_column_actual = parquet_sum

        # Allow tiny floating point tolerance
        if abs(source_sum - parquet_sum) > 0.01:
            raise ChecksumMismatchError(
                source_path=source_path,
                message=f"{sum_column} sum mismatch",
                column_name=sum_column,
                expected_value=source_sum,
                actual_value=parquet_sum,
            )

    # Checksum 2: Non-null counts for key columns (column-level reads)
    for col in key_columns:
        source_count = source_stats.non_null_counts.get(col, 0)

        # Read only this column from parquet
        col_table = pq.read_table(output_path, columns=[col])
        parquet_count = pc.count(col_table.column(col)).as_py()

        result.non_null_counts[col] = (source_count, parquet_count)

        if source_count != parquet_count:
            raise ChecksumMismatchError(
                source_path=source_path,
                message=f"Non-null count mismatch for {col}",
                column_name=col,
                expected_value=source_count,
                actual_value=parquet_count,
            )

    result.checksum_valid = True
    return result


def validate_sample_rows(
    source_path: Path,
    output_path: Path,
    sample_size: int,
    result: ValidationResult,
) -> ValidationResult:
    """
    Tier 3 validation: Compare random sample of rows.

    Uses memory-efficient batch reading: iterates through parquet in batches,
    capturing sample rows as encountered rather than loading entire file.
    """
    # Get total row count from parquet metadata
    meta = pq.read_metadata(output_path)
    total_rows = meta.num_rows

    # Select random row indices
    actual_sample_size = min(sample_size, total_rows)
    sample_indices = sorted(random.sample(range(total_rows), actual_sample_size))

    # Read source CSV rows at sample indices
    source_rows = _read_csv_rows_at_indices(source_path, sample_indices)

    # Read parquet in batches, capturing sample rows (memory-efficient)
    parquet_file = pq.ParquetFile(output_path)
    schema_names = parquet_file.schema_arrow.names

    # Map from sample index to its position in our results
    index_to_position = {idx: pos for pos, idx in enumerate(sample_indices)}
    parquet_sample_rows: list[dict | None] = [None] * len(sample_indices)

    current_row = 0
    for batch in parquet_file.iter_batches():
        batch_end = current_row + batch.num_rows

        # Check if any sample indices fall in this batch
        for sample_idx in sample_indices:
            if current_row <= sample_idx < batch_end:
                # Extract this row from the batch
                local_idx = sample_idx - current_row
                row_dict = {}
                for col_name in schema_names:
                    row_dict[col_name] = batch.column(col_name)[local_idx].as_py()
                parquet_sample_rows[index_to_position[sample_idx]] = row_dict

        current_row = batch_end

        # Early exit if we have all samples
        if all(r is not None for r in parquet_sample_rows):
            break

    # Compare
    result.sample_size = len(sample_indices)
    for i, row_idx in enumerate(sample_indices):
        source_row = source_rows[i]
        parquet_row = parquet_sample_rows[i]

        for col_name in schema_names:
            source_val = source_row.get(col_name)
            parquet_val = parquet_row.get(col_name) if parquet_row else None

            # Normalize for comparison
            source_normalized = _normalize_value(source_val)
            parquet_normalized = _normalize_value(parquet_val)

            if not _values_equal(source_normalized, parquet_normalized):
                raise SampleMismatchError(
                    source_path=source_path,
                    message="Sample row mismatch",
                    row_index=row_idx,
                    column_name=col_name,
                    expected_value=str(source_normalized),
                    actual_value=str(parquet_normalized),
                )

    result.sample_valid = True
    return result


def _read_csv_rows_at_indices(path: Path, indices: list[int]) -> list[dict]:
    """Read specific rows from CSV by index."""
    rows = []
    indices_set = set(indices)
    index_to_position = {idx: pos for pos, idx in enumerate(sorted(indices))}

    # Pre-allocate result list
    rows = [None] * len(indices)

    with gzip.open(path, "rt", encoding="latin1") as f:
        reader = csv.DictReader(f, doublequote=True)
        for i, row in enumerate(reader):
            if i in indices_set:
                rows[index_to_position[i]] = row
                if all(r is not None for r in rows):
                    break

    return rows


def _normalize_value(val):
    """Normalize values for comparison."""
    import math

    if val is None or val == "" or val == "\\N":
        return None
    if isinstance(val, float):
        # Treat NaN as None for comparison purposes
        if math.isnan(val):
            return None
        # Round to avoid floating point comparison issues
        return round(val, 6)
    if isinstance(val, str):
        stripped = val.strip()
        # Treat string 'nan' as None (case-insensitive)
        if stripped.lower() == "nan":
            return None
        return stripped
    return val


def _values_equal(a, b) -> bool:
    """Compare two normalized values."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False

    # Try numeric comparison if either value looks numeric
    try:
        a_float = float(a) if isinstance(a, str) else a
        b_float = float(b) if isinstance(b, str) else b
        if isinstance(a_float, (int, float)) and isinstance(b_float, (int, float)):
            return abs(float(a_float) - float(b_float)) < 0.000001
    except (ValueError, TypeError):
        pass

    # String comparison
    return str(a) == str(b)
