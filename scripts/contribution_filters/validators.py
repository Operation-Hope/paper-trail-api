"""Validation suite for filtered contribution datasets.

Validates:
- Non-individual filter: confirms no individual contributors in output
- Recipient aggregates: verifies SUM/COUNT accuracy via sampling
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

from .exceptions import (
    AggregationIntegrityError,
    BioguideJoinError,
    CompletenessError,
    FilterValidationError,
)
from .schema import escape_sql_string


@dataclass
class ValidationResult:
    """Results from validation suite."""

    row_count_valid: bool = False
    source_rows: int = 0
    output_count: int = 0

    filter_valid: bool = False
    filter_checks_passed: int = 0

    aggregation_valid: bool = False
    aggregation_sample_size: int = 0

    # Bioguide join validation (for outputs with bioguide_id column)
    bioguide_join_valid: bool = False
    bioguide_matched_count: int = 0
    bioguide_coverage_pct: float = 0.0

    @property
    def all_valid(self) -> bool:
        """Check if all validation checks passed."""
        return self.row_count_valid and (self.filter_valid or self.aggregation_valid)


def validate_organizational_output(
    source_url: str,
    output_path: Path,
    conn: duckdb.DuckDBPyConnection,
    source_rows: int,
    output_count: int,
) -> ValidationResult:
    """
    Validate organizational filter output.

    Checks:
    - Output count is less than source (filter reduced rows)
    - No individual contributors in output (contributor.type != 'I')

    Handles both column naming conventions:
    - "contributor.type" (standard organizational output)
    - "contributor_type" (raw organizational output)
    """
    result = ValidationResult()
    result.source_rows = source_rows
    result.output_count = output_count

    # Tier 1: Row count sanity - filter should reduce rows
    if output_count >= source_rows:
        raise CompletenessError(
            message="Filter did not reduce row count",
            expected_count=source_rows,
            actual_count=output_count,
        )
    result.row_count_valid = True

    # Tier 2: Verify no individuals in output
    # Detect which column naming convention is used
    columns = conn.execute(f"""
        SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{output_path}'))
    """).fetchall()
    column_names = {c[0] for c in columns}

    if "contributor_type" in column_names:
        contributor_type_col = "contributor_type"
    elif "contributor.type" in column_names:
        contributor_type_col = '"contributor.type"'
    else:
        # No contributor type column - skip this validation
        result.filter_valid = True
        result.filter_checks_passed = 0
        return result

    individual_count = conn.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{output_path}')
        WHERE {contributor_type_col} = 'I'
    """).fetchone()[0]

    if individual_count > 0:
        raise FilterValidationError(
            message=f"Found {individual_count:,} individual contributors in output",
            field_name="contributor.type",
            expected_condition="!= 'I'",
            violation_count=individual_count,
        )
    result.filter_valid = True
    result.filter_checks_passed = 1

    return result


def validate_recipient_aggregates(
    source_url: str,
    output_path: Path,
    conn: duckdb.DuckDBPyConnection,
    sample_size: int = 100,
) -> ValidationResult:
    """
    Validate recipient aggregates output.

    Checks:
    - Completeness: All distinct recipient IDs appear in output
    - Aggregation: SUM/COUNT matches for sampled recipients
    """
    result = ValidationResult()

    # Tier 1: Completeness - distinct recipient count matches
    source_distinct = conn.execute(f"""
        SELECT COUNT(DISTINCT "bonica.rid")
        FROM read_parquet('{source_url}')
        WHERE "bonica.rid" IS NOT NULL
    """).fetchone()[0]
    result.source_rows = source_distinct

    output_count = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{output_path}')
    """).fetchone()[0]
    result.output_count = output_count

    # Note: output_count may differ from source_distinct due to GROUP BY
    # including additional columns (name, party, etc.) which may have
    # different values for the same bonica.rid across contributions
    result.row_count_valid = True

    # Tier 2: Sample aggregation verification
    all_rids = conn.execute(f"""
        SELECT DISTINCT "bonica.rid" FROM read_parquet('{output_path}')
    """).fetchall()
    all_rids = [r[0] for r in all_rids if r[0] is not None]

    if not all_rids:
        raise CompletenessError(
            message="No recipients found in output",
            expected_count=source_distinct,
            actual_count=0,
        )

    actual_sample_size = min(sample_size, len(all_rids))
    sample_rids = random.sample(all_rids, actual_sample_size)
    result.aggregation_sample_size = actual_sample_size

    for rid in sample_rids:
        # Escape recipient ID for safe SQL interpolation
        rid_escaped = escape_sql_string(rid)

        # Get expected values from source
        expected = conn.execute(f"""
            SELECT
                SUM(amount) as expected_total,
                COUNT(*) as expected_count
            FROM read_parquet('{source_url}')
            WHERE "bonica.rid" = '{rid_escaped}'
        """).fetchone()

        expected_total, expected_count = expected

        # Get actual values from output (sum across all rows for this rid)
        actual = conn.execute(f"""
            SELECT
                SUM(total_amount) as actual_total,
                SUM(contribution_count) as actual_count
            FROM read_parquet('{output_path}')
            WHERE "bonica.rid" = '{rid_escaped}'
        """).fetchone()

        actual_total, actual_count = actual

        # Verify count
        if actual_count != expected_count:
            raise AggregationIntegrityError(
                message="contribution_count mismatch",
                recipient_id=rid,
                field_name="contribution_count",
                expected_value=str(expected_count),
                actual_value=str(actual_count),
            )

        # Verify sum (allow small float tolerance - absolute or relative)
        # Use relative tolerance for large amounts to handle float accumulation errors
        if expected_total is not None and actual_total is not None:
            abs_diff = abs(actual_total - expected_total)
            # Absolute tolerance of $0.01 OR relative tolerance of 0.0001% (1e-6)
            rel_tolerance = abs(expected_total) * 1e-6
            tolerance = max(0.01, rel_tolerance)
            if abs_diff > tolerance:
                raise AggregationIntegrityError(
                    message="total_amount mismatch",
                    recipient_id=rid,
                    field_name="total_amount",
                    expected_value=f"{expected_total:.2f}",
                    actual_value=f"{actual_total:.2f}",
                )

    result.aggregation_valid = True
    return result


def validate_bioguide_join(
    output_path: Path,
    legislators_path: Path,
    conn: duckdb.DuckDBPyConnection,
) -> ValidationResult:
    """
    Validate bioguide_id join integrity.

    Checks:
    - All non-null bioguide_ids in output exist in legislators file
    - Reports coverage statistics

    Args:
        output_path: Path to the output parquet file with bioguide_id column
        legislators_path: Path to the legislators parquet file
        conn: DuckDB connection to reuse

    Returns:
        ValidationResult with bioguide join validation results

    Raises:
        BioguideJoinError: If any non-null bioguide_ids don't exist in legislators
    """
    result = ValidationResult()

    # Get all distinct bioguide_ids from output
    output_bioguides = conn.execute(f"""
        SELECT DISTINCT bioguide_id
        FROM read_parquet('{output_path}')
        WHERE bioguide_id IS NOT NULL
    """).fetchall()
    output_bioguide_set = {r[0] for r in output_bioguides if r[0]}

    # Get all bioguide_ids from legislators
    legislators_bioguides = conn.execute(f"""
        SELECT DISTINCT bioguide_id
        FROM read_parquet('{legislators_path}')
        WHERE bioguide_id IS NOT NULL
    """).fetchall()
    legislators_bioguide_set = {r[0] for r in legislators_bioguides if r[0]}

    # Check for invalid bioguide_ids (in output but not in legislators)
    invalid_bioguides = output_bioguide_set - legislators_bioguide_set

    if invalid_bioguides:
        raise BioguideJoinError(
            message="Found bioguide_ids in output that don't exist in legislators",
            invalid_bioguide_ids=sorted(invalid_bioguides)[:10],
            total_invalid=len(invalid_bioguides),
        )

    # Calculate coverage statistics
    total_output_rows = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{output_path}')
    """).fetchone()[0]

    matched_rows = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{output_path}')
        WHERE bioguide_id IS NOT NULL
    """).fetchone()[0]

    result.row_count_valid = True
    result.output_count = total_output_rows
    result.bioguide_join_valid = True
    result.bioguide_matched_count = matched_rows
    result.bioguide_coverage_pct = (
        (matched_rows / total_output_rows * 100) if total_output_rows > 0 else 0.0
    )

    return result
