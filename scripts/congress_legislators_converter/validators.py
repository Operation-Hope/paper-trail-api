"""Three-tier validation suite for Congress Legislators parquet conversions."""

from __future__ import annotations

import csv
import math
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow.compute as pc
import pyarrow.parquet as pq

from .exceptions import (
    ChecksumMismatchError,
    RowCountMismatchError,
    SampleMismatchError,
    UnifiedValidationError,
)

if TYPE_CHECKING:
    import duckdb

    from .converter import StreamingStats


def _escape_sql_path(path: Path) -> str:
    """Escape a path for use in SQL string literals (double single quotes)."""
    return str(path).replace("'", "''")


@dataclass
class ValidationResult:
    """Results from validation suite."""

    row_count_valid: bool = False
    row_count_expected: int = 0
    row_count_actual: int = 0

    checksum_valid: bool = False
    sum_column_name: str | None = None
    sum_column_expected: float = 0.0
    sum_column_actual: float = 0.0
    non_null_counts: dict[str, tuple[int, int]] = field(default_factory=dict)

    sample_valid: bool = False
    sample_size: int = 0

    @property
    def all_valid(self) -> bool:
        """Check if all validation tiers passed."""
        return self.row_count_valid and self.checksum_valid and self.sample_valid


def validate_row_count(
    source_path: Path,
    output_path: Path,
    expected_count: int,
) -> ValidationResult:
    """
    Tier 1: Verify row counts match using parquet metadata.

    This is the fastest validation - reads only metadata, no actual data.
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
    sum_column: str | None,
    key_columns: list[str] | None = None,
) -> ValidationResult:
    """
    Tier 2: Verify column checksums.

    Validates:
    - Sum of configurable numeric column (not applicable for string-only datasets)
    - Non-null counts for key columns (detects dropped data)
    """
    if key_columns is None:
        key_columns = []

    result.sum_column_name = sum_column

    # Checksum 1: Sum validation (skipped for string-only datasets)
    if sum_column:
        source_sum = source_stats.sum_column_value
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

    # Checksum 2: Non-null count validation
    for col in key_columns:
        source_count = source_stats.non_null_counts.get(col, 0)
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
    Tier 3: Compare random sample of rows field-by-field.

    Most thorough validation - catches subtle conversion errors.
    Memory-efficient: reads parquet in batches, captures only sample rows.
    """
    meta = pq.read_metadata(output_path)
    total_rows = meta.num_rows

    # Adjust sample size if file is smaller
    actual_sample_size = min(sample_size, total_rows)
    sample_indices = sorted(random.sample(range(total_rows), actual_sample_size))

    # Read source CSV rows at sample indices
    source_rows = _read_csv_rows_at_indices(source_path, sample_indices)

    # Read parquet in batches, capturing sample rows
    parquet_file = pq.ParquetFile(output_path)
    schema_names = parquet_file.schema_arrow.names

    index_to_position = {idx: pos for pos, idx in enumerate(sample_indices)}
    parquet_sample_rows: list[dict[str, Any] | None] = [None] * len(sample_indices)

    current_row = 0
    for batch in parquet_file.iter_batches():
        batch_end = current_row + batch.num_rows

        for sample_idx in sample_indices:
            if current_row <= sample_idx < batch_end:
                local_idx = sample_idx - current_row
                row_dict: dict[str, Any] = {}
                for col_name in schema_names:
                    row_dict[col_name] = batch.column(col_name)[local_idx].as_py()
                parquet_sample_rows[index_to_position[sample_idx]] = row_dict

        current_row = batch_end

        # Early exit if we've captured all sample rows
        if all(r is not None for r in parquet_sample_rows):
            break

    # Compare rows field by field
    result.sample_size = len(sample_indices)
    for i, row_idx in enumerate(sample_indices):
        source_row = source_rows[i]
        parquet_row = parquet_sample_rows[i]

        for col_name in schema_names:
            source_val = source_row.get(col_name) if source_row else None
            parquet_val = parquet_row.get(col_name) if parquet_row else None

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


def _read_csv_rows_at_indices(path: Path, indices: list[int]) -> list[dict[str, str] | None]:
    """Read specific rows from plain CSV by index."""
    indices_set = set(indices)
    index_to_position = {idx: pos for pos, idx in enumerate(sorted(indices))}
    rows: list[dict[str, str] | None] = [None] * len(indices)

    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f, doublequote=True)
        for i, row in enumerate(reader):
            if i in indices_set:
                rows[index_to_position[i]] = row
                if all(r is not None for r in rows):
                    break

    return rows


def _normalize_value(val: Any) -> Any:
    """Normalize values for comparison."""
    if val is None or val == "":
        return None
    if isinstance(val, float):
        if math.isnan(val):
            return None
        return round(val, 6)
    if isinstance(val, str):
        stripped = val.strip()
        # Treat various null representations as None
        if stripped.lower() in ("nan", "n/a", "na", "null"):
            return None
        return stripped
    return val


def _values_equal(a: Any, b: Any) -> bool:
    """Compare two normalized values with tolerance for floats."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False

    # Try numeric comparison with tolerance
    try:
        a_float = float(a) if isinstance(a, str) else a
        b_float = float(b) if isinstance(b, str) else b
        if isinstance(a_float, (int, float)) and isinstance(b_float, (int, float)):
            return abs(float(a_float) - float(b_float)) < 0.000001
    except (ValueError, TypeError):
        pass

    # String comparison
    return str(a) == str(b)


# =============================================================================
# UNIFIED LEGISLATORS VALIDATION (3-Tier)
# =============================================================================


def validate_unified_completeness(
    output_path: Path,
    conn: duckdb.DuckDBPyConnection,
    *,
    min_expected: int = 10_000,
    max_expected: int = 15_000,
) -> None:
    """
    Tier 1: Verify completeness of unified legislators output.

    Checks:
    - All bioguide_ids are unique (no duplicates)
    - No null bioguide_ids
    - Expected count range (configurable, default ~12,000-13,000 legislators)

    Args:
        output_path: Path to the unified legislators parquet file.
        conn: DuckDB connection.
        min_expected: Minimum expected legislator count (default 10,000).
        max_expected: Maximum expected legislator count (default 15,000).
    """
    output_sql = _escape_sql_path(output_path)

    # Check for null bioguide_ids
    null_count = conn.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{output_sql}')
        WHERE bioguide_id IS NULL OR bioguide_id = ''
    """).fetchone()[0]

    if null_count > 0:
        raise UnifiedValidationError(
            source_path=output_path,
            message="Null bioguide_ids found",
            validation_tier="Tier 1 (Completeness)",
            details=f"{null_count:,} null or empty bioguide_ids",
        )

    # Check for duplicate bioguide_ids
    total_count = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{output_sql}')
    """).fetchone()[0]

    unique_count = conn.execute(f"""
        SELECT COUNT(DISTINCT bioguide_id) FROM read_parquet('{output_sql}')
    """).fetchone()[0]

    if total_count != unique_count:
        duplicates = total_count - unique_count
        details = (
            f"{duplicates:,} duplicate bioguide_ids "
            f"(total: {total_count:,}, unique: {unique_count:,})"
        )
        raise UnifiedValidationError(
            source_path=output_path,
            message="Duplicate bioguide_ids found",
            validation_tier="Tier 1 (Completeness)",
            details=details,
        )

    # Sanity check on expected count range
    if not (min_expected <= total_count <= max_expected):
        raise UnifiedValidationError(
            source_path=output_path,
            message="Unexpected legislator count",
            validation_tier="Tier 1 (Completeness)",
            details=f"Expected {min_expected:,}-{max_expected:,}, got {total_count:,}",
        )


def validate_unified_coverage(
    output_path: Path,
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """
    Tier 2: Verify coverage of key identifier fields.

    Checks (informational, not strict thresholds):
    - fec_ids populated for some legislators
    - icpsr populated for most legislators
    """
    output_sql = _escape_sql_path(output_path)

    total_count = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{output_sql}')
    """).fetchone()[0]

    if total_count == 0:
        raise UnifiedValidationError(
            source_path=output_path,
            message="Empty output file",
            validation_tier="Tier 2 (Coverage)",
            details="No legislators in output",
        )

    # FEC IDs coverage (expect ~10% based on data analysis)
    fec_populated = conn.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{output_sql}')
        WHERE fec_ids IS NOT NULL AND LEN(fec_ids) > 0
    """).fetchone()[0]

    # ICPSR coverage (expect ~96% based on data analysis)
    icpsr_populated = conn.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{output_sql}')
        WHERE icpsr IS NOT NULL
    """).fetchone()[0]

    # These are informational - we don't fail on specific thresholds
    # But we do fail if there's zero coverage (indicates extraction bug)
    if fec_populated == 0:
        raise UnifiedValidationError(
            source_path=output_path,
            message="No FEC IDs populated",
            validation_tier="Tier 2 (Coverage)",
            details="Expected some legislators to have FEC IDs",
        )

    if icpsr_populated == 0:
        raise UnifiedValidationError(
            source_path=output_path,
            message="No ICPSR values populated",
            validation_tier="Tier 2 (Coverage)",
            details="Expected most legislators to have ICPSR values",
        )


def validate_unified_sample(
    current_path: Path,
    historical_path: Path,
    output_path: Path,
    conn: duckdb.DuckDBPyConnection,
    sample_size: int = 100,
) -> None:
    """
    Tier 3: Verify random sample of rows match source data.

    Samples from output and verifies key fields match the source files.
    """
    output_sql = _escape_sql_path(output_path)
    current_sql = _escape_sql_path(current_path)
    historical_sql = _escape_sql_path(historical_path)

    # Get sample of bioguide_ids from output
    # Note: state/party not validated as they represent "most recent" and may differ
    # between current/historical sources for the same legislator
    sample_rows = conn.execute(f"""
        SELECT bioguide_id, last_name, first_name, is_current
        FROM read_parquet('{output_sql}')
        USING SAMPLE {sample_size}
    """).fetchall()

    if len(sample_rows) == 0:
        raise UnifiedValidationError(
            source_path=output_path,
            message="No sample rows available",
            validation_tier="Tier 3 (Sample)",
            details="Output file appears to be empty",
        )

    # Verify each sample row exists in appropriate source
    for row in sample_rows:
        bioguide_id, last_name, first_name, is_current = row

        # Determine source file based on is_current flag
        source_path = current_path if is_current else historical_path
        source_sql = current_sql if is_current else historical_sql

        # Verify bioguide_id exists in source with matching fields
        source_match = conn.execute(
            f"""
            SELECT last_name, first_name
            FROM read_parquet('{source_sql}')
            WHERE bioguide_id = ?
        """,
            [bioguide_id],
        ).fetchone()

        if source_match is None:
            raise UnifiedValidationError(
                source_path=output_path,
                message="Sample row not found in source",
                validation_tier="Tier 3 (Sample)",
                details=f"bioguide_id={bioguide_id} not found in {source_path.name}",
            )

        src_last, src_first = source_match

        # Compare key fields (allowing for null handling)
        if not _values_equal(last_name, src_last):
            raise UnifiedValidationError(
                source_path=output_path,
                message="Sample mismatch: last_name",
                validation_tier="Tier 3 (Sample)",
                details=f"bioguide_id={bioguide_id}: expected {src_last!r}, got {last_name!r}",
            )

        if not _values_equal(first_name, src_first):
            raise UnifiedValidationError(
                source_path=output_path,
                message="Sample mismatch: first_name",
                validation_tier="Tier 3 (Sample)",
                details=f"bioguide_id={bioguide_id}: expected {src_first!r}, got {first_name!r}",
            )
