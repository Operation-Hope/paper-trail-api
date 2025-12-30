"""Validation suite for legislator crosswalk extraction.

Validates:
- Tier 1: Basic counts and non-null requirements
- Tier 2: Uniqueness of (icpsr, bonica_rid) pairs
- Tier 3: Sample verification against source
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

from .exceptions import DuplicateKeyError, ValidationError


@dataclass
class ValidationResult:
    """Results from validation suite."""

    counts_valid: bool = False
    source_count: int = 0
    output_count: int = 0

    uniqueness_valid: bool = False
    unique_icpsr_count: int = 0
    unique_bonica_rid_count: int = 0

    sample_valid: bool = False
    sample_size: int = 0

    @property
    def all_valid(self) -> bool:
        """Check if all validation tiers passed."""
        return self.counts_valid and self.uniqueness_valid and self.sample_valid


def validate_counts(
    source_url: str,
    output_path: Path,
    conn: duckdb.DuckDBPyConnection,
) -> ValidationResult:
    """
    Tier 1: Verify basic counts and non-null requirements.

    Checks:
    - Output has rows
    - All icpsr values are non-null
    - All bonica_rid values are non-null
    """
    result = ValidationResult()

    # Count source rows matching our filter
    # Note: DIME stores ICPSR as "{icpsr}{year}", we extract just the ICPSR portion
    source_count = conn.execute(f"""
        SELECT COUNT(DISTINCT (
            SUBSTRING(CAST("ICPSR" AS VARCHAR), 1, LENGTH(CAST("ICPSR" AS VARCHAR))-4),
            "bonica.rid"
        ))
        FROM read_parquet('{source_url}')
        WHERE "ICPSR" IS NOT NULL
          AND "ICPSR" != ''
          AND LENGTH(CAST("ICPSR" AS VARCHAR)) > 4
          AND "bonica.rid" IS NOT NULL
          AND "bonica.rid" != ''
    """).fetchone()[0]
    result.source_count = source_count

    # Count output rows
    output_count = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{output_path}')
    """).fetchone()[0]
    result.output_count = output_count

    if output_count == 0:
        raise ValidationError(
            message="Output file has no rows",
            expected_count=source_count,
            actual_count=0,
        )

    # Check for null icpsr values
    null_icpsr = conn.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{output_path}')
        WHERE icpsr IS NULL OR icpsr = ''
    """).fetchone()[0]

    if null_icpsr > 0:
        raise ValidationError(
            message=f"Found {null_icpsr} rows with null/empty icpsr",
            expected_count=0,
            actual_count=null_icpsr,
        )

    # Check for null bonica_rid values
    null_bonica_rid = conn.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{output_path}')
        WHERE bonica_rid IS NULL OR bonica_rid = ''
    """).fetchone()[0]

    if null_bonica_rid > 0:
        raise ValidationError(
            message=f"Found {null_bonica_rid} rows with null/empty bonica_rid",
            expected_count=0,
            actual_count=null_bonica_rid,
        )

    # Verify counts match
    if output_count != source_count:
        raise ValidationError(
            message="Row count mismatch between source and output",
            expected_count=source_count,
            actual_count=output_count,
        )

    result.counts_valid = True
    return result


def validate_uniqueness(
    output_path: Path,
    conn: duckdb.DuckDBPyConnection,
    result: ValidationResult,
) -> ValidationResult:
    """
    Tier 2: Verify uniqueness of key pairs.

    Checks:
    - No duplicate (icpsr, bonica_rid) pairs exist
    """
    # Check for duplicate key pairs
    duplicates = conn.execute(f"""
        SELECT icpsr, bonica_rid, COUNT(*) as cnt
        FROM read_parquet('{output_path}')
        GROUP BY icpsr, bonica_rid
        HAVING cnt > 1
        LIMIT 10
    """).fetchall()

    if duplicates:
        sample_dups = [(r[0], r[1]) for r in duplicates]
        raise DuplicateKeyError(
            message="Found duplicate (icpsr, bonica_rid) pairs",
            duplicate_count=len(duplicates),
            sample_duplicates=sample_dups,
        )

    # Get unique counts for reporting
    result.unique_icpsr_count = conn.execute(f"""
        SELECT COUNT(DISTINCT icpsr) FROM read_parquet('{output_path}')
    """).fetchone()[0]

    result.unique_bonica_rid_count = conn.execute(f"""
        SELECT COUNT(DISTINCT bonica_rid) FROM read_parquet('{output_path}')
    """).fetchone()[0]

    result.uniqueness_valid = True
    return result


def validate_sample(
    source_url: str,
    output_path: Path,
    conn: duckdb.DuckDBPyConnection,
    result: ValidationResult,
    sample_size: int = 100,
) -> ValidationResult:
    """
    Tier 3: Sample verification against source.

    For randomly sampled rows, verifies:
    - The (icpsr, bonica_rid) mapping exists in source data

    Note: We don't compare metadata columns exactly because our extraction
    uses MAX() aggregation when multiple rows have the same (icpsr, bonica_rid).
    """
    # Get random sample of output rows
    sample = conn.execute(f"""
        SELECT icpsr, bonica_rid
        FROM read_parquet('{output_path}')
        USING SAMPLE {sample_size}
    """).fetchall()

    actual_sample_size = len(sample)
    result.sample_size = actual_sample_size

    verified = 0
    for icpsr, bonica_rid in sample:
        # Verify this mapping exists in source
        # Note: DIME stores ICPSR as "{icpsr}{year}", so we compare the extracted portion
        source_exists = conn.execute(f"""
            SELECT 1
            FROM read_parquet('{source_url}')
            WHERE SUBSTRING(CAST("ICPSR" AS VARCHAR), 1, LENGTH(CAST("ICPSR" AS VARCHAR))-4) = '{icpsr}'
              AND "bonica.rid" = '{bonica_rid}'
            LIMIT 1
        """).fetchone()

        if source_exists is None:
            raise ValidationError(
                message=f"Mapping (icpsr={icpsr}, bonica_rid={bonica_rid}) not found in source",
                expected_count=1,
                actual_count=0,
            )

        verified += 1

    result.sample_valid = True
    return result
