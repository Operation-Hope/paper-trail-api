"""Three-tier validation suite for distinct legislators extraction.

Unlike CSV→Parquet converters which validate lossless conversion,
this validates correct aggregation/transformation:

- Tier 1: Completeness - every source bioguide_id appears exactly once
- Tier 2: Aggregation Integrity - MIN/MAX/LIST operations are correct
- Tier 3: Sample Verification - deep validation of random legislators
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

from .exceptions import (
    AggregationError,
    CompletenessError,
    SampleValidationError,
)


@dataclass
class ValidationResult:
    """Results from validation suite."""

    completeness_valid: bool = False
    source_distinct_count: int = 0
    output_count: int = 0

    aggregation_valid: bool = False
    aggregation_checks_passed: int = 0

    sample_valid: bool = False
    sample_size: int = 0

    @property
    def all_valid(self) -> bool:
        """Check if all validation tiers passed."""
        return self.completeness_valid and self.aggregation_valid and self.sample_valid


def validate_completeness(
    source_url: str,
    output_path: Path,
    conn: duckdb.DuckDBPyConnection,
    min_congress: int,
) -> ValidationResult:
    """
    Tier 1: Verify all source bioguide_ids appear exactly once in output.

    Checks:
    - Output count matches distinct source count
    - No missing bioguide_ids
    - No extra bioguide_ids
    - No duplicates in output
    """
    result = ValidationResult()

    # Count distinct legislators in source
    source_count = conn.execute(f"""
        SELECT COUNT(DISTINCT bioguide_id)
        FROM read_parquet('{source_url}')
        WHERE congress >= {min_congress}
          AND bioguide_id IS NOT NULL
    """).fetchone()[0]
    result.source_distinct_count = source_count

    # Count rows in output
    output_count = conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{output_path}')
    """).fetchone()[0]
    result.output_count = output_count

    # Check for duplicates in output
    duplicate_count = conn.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT bioguide_id, COUNT(*) as cnt
            FROM read_parquet('{output_path}')
            GROUP BY bioguide_id
            HAVING cnt > 1
        )
    """).fetchone()[0]

    if duplicate_count > 0:
        raise CompletenessError(
            message=f"Found {duplicate_count} duplicate bioguide_ids in output",
            expected_count=source_count,
            actual_count=output_count,
        )

    # Check counts match
    if output_count != source_count:
        # Find missing or extra IDs
        missing = conn.execute(f"""
            SELECT bioguide_id FROM (
                SELECT DISTINCT bioguide_id
                FROM read_parquet('{source_url}')
                WHERE congress >= {min_congress}
                  AND bioguide_id IS NOT NULL
            ) source
            WHERE bioguide_id NOT IN (
                SELECT bioguide_id FROM read_parquet('{output_path}')
            )
            LIMIT 10
        """).fetchall()
        missing_ids = [r[0] for r in missing]

        extra = conn.execute(f"""
            SELECT bioguide_id FROM read_parquet('{output_path}')
            WHERE bioguide_id NOT IN (
                SELECT DISTINCT bioguide_id
                FROM read_parquet('{source_url}')
                WHERE congress >= {min_congress}
                  AND bioguide_id IS NOT NULL
            )
            LIMIT 10
        """).fetchall()
        extra_ids = [r[0] for r in extra]

        raise CompletenessError(
            message="Count mismatch between source and output",
            expected_count=source_count,
            actual_count=output_count,
            missing_ids=missing_ids if missing_ids else None,
            extra_ids=extra_ids if extra_ids else None,
        )

    result.completeness_valid = True
    return result


def validate_aggregation(
    source_url: str,
    output_path: Path,
    conn: duckdb.DuckDBPyConnection,
    result: ValidationResult,
    min_congress: int,
    sample_size: int = 100,
) -> ValidationResult:
    """
    Tier 2: Verify aggregation operations (MIN/MAX/LIST) are correct.

    Randomly samples legislators and verifies:
    - first_congress = MIN(congress) from source
    - last_congress = MAX(congress) from source
    - congresses_served array length matches source count
    """
    # Get random sample of bioguide_ids
    all_ids = conn.execute(f"""
        SELECT bioguide_id FROM read_parquet('{output_path}')
    """).fetchall()
    all_ids = [r[0] for r in all_ids]

    actual_sample_size = min(sample_size, len(all_ids))
    sample_ids = random.sample(all_ids, actual_sample_size)

    checks_passed = 0

    for bioguide_id in sample_ids:
        # Get source data for this legislator
        source_data = conn.execute(f"""
            SELECT
                MIN(congress) as expected_first,
                MAX(congress) as expected_last,
                COUNT(*) as expected_count
            FROM read_parquet('{source_url}')
            WHERE bioguide_id = '{bioguide_id}'
              AND congress >= {min_congress}
        """).fetchone()

        expected_first, expected_last, expected_count = source_data

        # Get output data
        output_data = conn.execute(f"""
            SELECT
                first_congress,
                last_congress,
                LENGTH(congresses_served) as actual_count
            FROM read_parquet('{output_path}')
            WHERE bioguide_id = '{bioguide_id}'
        """).fetchone()

        actual_first, actual_last, actual_count = output_data

        # Validate first_congress
        if actual_first != expected_first:
            raise AggregationError(
                message="first_congress mismatch",
                bioguide_id=bioguide_id,
                field_name="first_congress",
                expected_value=str(expected_first),
                actual_value=str(actual_first),
            )

        # Validate last_congress
        if actual_last != expected_last:
            raise AggregationError(
                message="last_congress mismatch",
                bioguide_id=bioguide_id,
                field_name="last_congress",
                expected_value=str(expected_last),
                actual_value=str(actual_last),
            )

        # Validate congress count
        if actual_count != expected_count:
            raise AggregationError(
                message="congresses_served count mismatch",
                bioguide_id=bioguide_id,
                field_name="congresses_served (length)",
                expected_value=str(expected_count),
                actual_value=str(actual_count),
            )

        checks_passed += 1

    result.aggregation_checks_passed = checks_passed
    result.aggregation_valid = True
    return result


def validate_sample(
    source_url: str,
    output_path: Path,
    conn: duckdb.DuckDBPyConnection,
    result: ValidationResult,
    min_congress: int,
    sample_size: int = 50,
) -> ValidationResult:
    """
    Tier 3: Deep validation of random legislators.

    For each sampled legislator, verifies:
    - congresses_served array contains exactly the right congress numbers
    - bioname matches the most recent congress entry
    - state_abbrev matches the most recent congress entry
    """
    # Get random sample
    all_ids = conn.execute(f"""
        SELECT bioguide_id FROM read_parquet('{output_path}')
    """).fetchall()
    all_ids = [r[0] for r in all_ids]

    actual_sample_size = min(sample_size, len(all_ids))
    sample_ids = random.sample(all_ids, actual_sample_size)
    result.sample_size = actual_sample_size

    for i, bioguide_id in enumerate(sample_ids):
        # Get expected congresses from source
        expected_congresses = conn.execute(f"""
            SELECT LIST(congress ORDER BY congress)
            FROM read_parquet('{source_url}')
            WHERE bioguide_id = '{bioguide_id}'
              AND congress >= {min_congress}
        """).fetchone()[0]

        # Get actual congresses from output
        actual_congresses = conn.execute(f"""
            SELECT congresses_served
            FROM read_parquet('{output_path}')
            WHERE bioguide_id = '{bioguide_id}'
        """).fetchone()[0]

        # Compare congress arrays
        if list(expected_congresses) != list(actual_congresses):
            raise SampleValidationError(
                message="congresses_served array mismatch",
                bioguide_id=bioguide_id,
                field_name="congresses_served",
                expected_value=str(expected_congresses),
                actual_value=str(actual_congresses),
                sample_index=i,
            )

        # Verify most recent values (bioname, state_abbrev)
        expected_latest = conn.execute(f"""
            SELECT bioname, state_abbrev
            FROM read_parquet('{source_url}')
            WHERE bioguide_id = '{bioguide_id}'
              AND congress >= {min_congress}
            ORDER BY congress DESC
            LIMIT 1
        """).fetchone()

        actual_latest = conn.execute(f"""
            SELECT bioname, state_abbrev
            FROM read_parquet('{output_path}')
            WHERE bioguide_id = '{bioguide_id}'
        """).fetchone()

        if expected_latest[0] != actual_latest[0]:
            raise SampleValidationError(
                message="bioname mismatch (should be from most recent congress)",
                bioguide_id=bioguide_id,
                field_name="bioname",
                expected_value=str(expected_latest[0]),
                actual_value=str(actual_latest[0]),
                sample_index=i,
            )

        if expected_latest[1] != actual_latest[1]:
            raise SampleValidationError(
                message="state_abbrev mismatch (should be from most recent congress)",
                bioguide_id=bioguide_id,
                field_name="state_abbrev",
                expected_value=str(expected_latest[1]),
                actual_value=str(actual_latest[1]),
                sample_index=i,
            )

    result.sample_valid = True
    return result
