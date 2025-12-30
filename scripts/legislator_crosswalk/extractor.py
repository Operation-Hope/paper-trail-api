"""Core extraction logic for legislator-recipient crosswalk from DIME data."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from .exceptions import InvalidSourceURLError, OutputWriteError, SourceReadError
from .schema import (
    ALLOWED_SOURCE_DOMAINS,
    DIME_RECIPIENTS_URL,
    EXTRACTION_QUERY,
    validate_source_url,
)
from .validators import (
    ValidationResult,
    validate_counts,
    validate_sample,
    validate_uniqueness,
)


@dataclass
class ExtractionResult:
    """Result of a successful extraction."""

    source_url: str
    output_path: Path
    source_rows: int
    output_count: int
    unique_icpsr_count: int
    unique_bonica_rid_count: int
    validation: ValidationResult


def extract_crosswalk(
    output_path: Path | str,
    *,
    source_url: str = DIME_RECIPIENTS_URL,
    validate: bool = True,
    sample_size: int = 100,
) -> ExtractionResult:
    """
    Extract legislator-recipient crosswalk from DIME Recipients data.

    Reads from HuggingFace, filters for records with ICPSR identifiers,
    and writes distinct mappings to a local Parquet file with ZSTD compression.

    Args:
        output_path: Path for output .parquet file
        source_url: URL to source parquet file (default: HF DIME Recipients)
        validate: Whether to run validation after extraction
        sample_size: Sample size for validation

    Returns:
        ExtractionResult with extraction details and validation results

    Raises:
        InvalidSourceURLError: If source URL is not from an allowed domain
        SourceReadError: If source data cannot be read
        ValidationError: If validation fails
        DuplicateKeyError: If duplicate key pairs are found
        OutputWriteError: If output cannot be written
    """
    output_path = Path(output_path)

    # Validate source URL to prevent SQL injection
    if not validate_source_url(source_url):
        raise InvalidSourceURLError(
            message="Source URL must be from an allowed domain",
            source_url=source_url,
            allowed_domains=ALLOWED_SOURCE_DOMAINS,
        )

    conn = duckdb.connect()

    try:
        # Step 1: Count source rows
        print(f"Reading from: {source_url}")
        try:
            source_rows = conn.execute(f"""
                SELECT COUNT(*)
                FROM read_parquet('{source_url}')
                WHERE "ICPSR" IS NOT NULL
                  AND "ICPSR" != ''
                  AND "bonica.rid" IS NOT NULL
                  AND "bonica.rid" != ''
            """).fetchone()[0]
        except Exception as e:
            raise SourceReadError(
                message=str(e),
                source_url=source_url,
            ) from e

        print(f"  Source rows with ICPSR: {source_rows:,}")

        # Step 2: Execute extraction query
        print("Extracting distinct crosswalk mappings...")
        query = EXTRACTION_QUERY.format(source_url=source_url)

        # Step 3: Write to parquet
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            conn.execute(f"""
                COPY ({query})
                TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3)
            """)
        except Exception as e:
            raise OutputWriteError(
                message=str(e),
                output_path=output_path,
            ) from e

        # Step 4: Count output and get unique counts
        output_count = conn.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{output_path}')
        """).fetchone()[0]
        print(f"  Crosswalk rows: {output_count:,}")

        unique_icpsr = conn.execute(f"""
            SELECT COUNT(DISTINCT icpsr) FROM read_parquet('{output_path}')
        """).fetchone()[0]
        print(f"  Unique ICPSR values: {unique_icpsr:,}")

        unique_bonica_rid = conn.execute(f"""
            SELECT COUNT(DISTINCT bonica_rid) FROM read_parquet('{output_path}')
        """).fetchone()[0]
        print(f"  Unique bonica_rid values: {unique_bonica_rid:,}")

        # Step 5: Validate
        validation = ValidationResult()
        if validate:
            print("Validating...")

            # Tier 1: Counts
            validation = validate_counts(source_url, output_path, conn)
            print(f"  Tier 1 (Counts): PASS ({validation.output_count:,} rows)")

            # Tier 2: Uniqueness
            validation = validate_uniqueness(output_path, conn, validation)
            print(
                f"  Tier 2 (Uniqueness): PASS ({validation.unique_icpsr_count:,} ICPSR → "
                f"{validation.unique_bonica_rid_count:,} bonica_rid)"
            )

            # Tier 3: Sample
            validation = validate_sample(source_url, output_path, conn, validation, sample_size)
            print(f"  Tier 3 (Sample): PASS ({validation.sample_size} rows verified)")

        return ExtractionResult(
            source_url=source_url,
            output_path=output_path,
            source_rows=source_rows,
            output_count=output_count,
            unique_icpsr_count=unique_icpsr,
            unique_bonica_rid_count=unique_bonica_rid,
            validation=validation,
        )

    finally:
        conn.close()
