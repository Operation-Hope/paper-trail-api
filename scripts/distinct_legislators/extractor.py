"""Core extraction logic for distinct legislators from Voteview data."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from .exceptions import OutputWriteError, SourceReadError
from .schema import (
    AGGREGATION_QUERY,
    MIN_CONGRESS,
    VOTEVIEW_MEMBERS_URL,
)
from .validators import (
    ValidationResult,
    validate_aggregation,
    validate_completeness,
    validate_sample,
)


@dataclass
class ExtractionResult:
    """Result of a successful extraction."""

    source_url: str
    output_path: Path
    source_rows: int
    output_count: int
    validation: ValidationResult


def extract_distinct_legislators(
    output_path: Path | str,
    *,
    source_url: str = VOTEVIEW_MEMBERS_URL,
    min_congress: int = MIN_CONGRESS,
    validate: bool = True,
    aggregation_sample_size: int = 100,
    deep_sample_size: int = 50,
) -> ExtractionResult:
    """
    Extract distinct legislators from Voteview HSall_members data.

    Reads from HuggingFace, filters by congress, aggregates by bioguide_id,
    and writes to a local Parquet file with ZSTD compression.

    Args:
        output_path: Path for output .parquet file
        source_url: URL to source parquet file (default: HF Voteview members)
        min_congress: Minimum congress number to include (default: 96 = 1979)
        validate: Whether to run three-tier validation after extraction
        aggregation_sample_size: Sample size for Tier 2 validation
        deep_sample_size: Sample size for Tier 3 validation

    Returns:
        ExtractionResult with extraction details and validation results

    Raises:
        SourceReadError: If source data cannot be read
        CompletenessError: If Tier 1 validation fails
        AggregationError: If Tier 2 validation fails
        SampleValidationError: If Tier 3 validation fails
        OutputWriteError: If output cannot be written
    """
    output_path = Path(output_path)
    conn = duckdb.connect()

    try:
        # Step 1: Count source rows
        print(f"Reading from: {source_url}")
        try:
            source_rows = conn.execute(f"""
                SELECT COUNT(*)
                FROM read_parquet('{source_url}')
                WHERE congress >= {min_congress}
                  AND bioguide_id IS NOT NULL
            """).fetchone()[0]
        except Exception as e:
            raise SourceReadError(
                message=str(e),
                source_url=source_url,
            ) from e

        print(f"  Source rows (congress >= {min_congress}): {source_rows:,}")

        # Step 2: Execute aggregation query
        print("Aggregating by bioguide_id...")
        query = AGGREGATION_QUERY.format(
            source_url=source_url,
            min_congress=min_congress,
        )

        # Step 3: Write to parquet
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            conn.execute(f"""
                COPY ({query})
                TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
        except Exception as e:
            raise OutputWriteError(
                message=str(e),
                output_path=output_path,
            ) from e

        # Step 4: Count output
        output_count = conn.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{output_path}')
        """).fetchone()[0]
        print(f"  Distinct legislators: {output_count:,}")

        # Step 5: Validate
        validation = ValidationResult()
        if validate:
            print("Validating...")

            # Tier 1: Completeness
            validation = validate_completeness(source_url, output_path, conn)
            print(f"  Tier 1 (Completeness): PASS " f"({validation.output_count:,} legislators)")

            # Tier 2: Aggregation Integrity
            validation = validate_aggregation(
                source_url,
                output_path,
                conn,
                validation,
                sample_size=aggregation_sample_size,
            )
            print(
                f"  Tier 2 (Aggregation): PASS " f"({validation.aggregation_checks_passed} checks)"
            )

            # Tier 3: Sample Verification
            validation = validate_sample(
                source_url,
                output_path,
                conn,
                validation,
                sample_size=deep_sample_size,
            )
            print(f"  Tier 3 (Sample): PASS " f"({validation.sample_size} legislators verified)")

        return ExtractionResult(
            source_url=source_url,
            output_path=output_path,
            source_rows=source_rows,
            output_count=output_count,
            validation=validation,
        )

    finally:
        conn.close()
