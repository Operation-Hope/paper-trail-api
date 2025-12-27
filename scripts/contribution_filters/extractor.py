"""Core extraction logic for filtered contribution datasets."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import duckdb

from .exceptions import (
    InvalidCycleError,
    InvalidSourceURLError,
    OutputWriteError,
    SourceReadError,
)
from .schema import (
    ALLOWED_SOURCE_DOMAINS,
    CONTRIBUTIONS_URL_TEMPLATE,
    MAX_CYCLE,
    MIN_CYCLE,
    ORGANIZATIONAL_QUERY,
    RECIPIENT_AGGREGATES_QUERY,
    validate_cycle,
    validate_source_url,
)
from .validators import (
    ValidationResult,
    validate_organizational_output,
    validate_recipient_aggregates,
)


class OutputType(Enum):
    """Output types for contribution filtering."""

    ORGANIZATIONAL = "organizational"
    RECIPIENT_AGGREGATES = "recipient_aggregates"


@dataclass
class ExtractionResult:
    """Result of a successful extraction."""

    source_url: str
    output_path: Path
    cycle: int
    output_type: OutputType
    source_rows: int
    output_count: int
    validation: ValidationResult


def extract_organizational_contributions(
    output_path: Path | str,
    cycle: int,
    *,
    source_url: str | None = None,
    validate: bool = True,
) -> ExtractionResult:
    """
    Extract organizational contributions for a given cycle.

    Filters out individual contributors (contributor.type = 'I'),
    keeping only PACs, corporations, committees, unions, and other organizations.

    Args:
        output_path: Path for output .parquet file
        cycle: Election cycle year (even year 1980-2024)
        source_url: Optional custom source URL (default: HuggingFace)
        validate: Whether to run validation after extraction

    Returns:
        ExtractionResult with extraction details and validation results

    Raises:
        InvalidCycleError: If cycle is not valid
        InvalidSourceURLError: If source URL is not from an allowed domain
        SourceReadError: If source data cannot be read
        FilterValidationError: If validation fails
        OutputWriteError: If output cannot be written
    """
    output_path = Path(output_path)

    # Validate cycle
    if not validate_cycle(cycle):
        raise InvalidCycleError(
            message=f"Invalid cycle: {cycle}",
            cycle=cycle,
            min_cycle=MIN_CYCLE,
            max_cycle=MAX_CYCLE,
        )

    source_url = source_url or CONTRIBUTIONS_URL_TEMPLATE.format(cycle=cycle)

    # Validate source URL
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
            """).fetchone()[0]
        except Exception as e:
            raise SourceReadError(
                message=str(e),
                source_url=source_url,
            ) from e

        print(f"  Source rows: {source_rows:,}")

        # Step 2: Execute filter query and write
        print("Filtering organizational contributions...")
        query = ORGANIZATIONAL_QUERY.format(source_url=source_url)

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

        # Step 3: Count output
        output_count = conn.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{output_path}')
        """).fetchone()[0]
        print(f"  Organizational contributions: {output_count:,}")
        print(f"  Filtered out: {source_rows - output_count:,} individual contributions")

        # Step 4: Validate
        validation = ValidationResult()
        if validate:
            print("Validating...")
            validation = validate_organizational_output(
                source_url, output_path, conn, source_rows, output_count
            )
            print("  Validation: PASS")

        return ExtractionResult(
            source_url=source_url,
            output_path=output_path,
            cycle=cycle,
            output_type=OutputType.ORGANIZATIONAL,
            source_rows=source_rows,
            output_count=output_count,
            validation=validation,
        )

    finally:
        conn.close()


def extract_recipient_aggregates(
    output_path: Path | str,
    cycle: int,
    *,
    source_url: str | None = None,
    validate: bool = True,
    sample_size: int = 100,
) -> ExtractionResult:
    """
    Extract recipient aggregates for a given cycle.

    Groups contributions by recipient and calculates:
    - total_amount: SUM of all contributions
    - avg_amount: AVG contribution size
    - contribution_count: Number of contributions

    Args:
        output_path: Path for output .parquet file
        cycle: Election cycle year (even year 1980-2024)
        source_url: Optional custom source URL (default: HuggingFace)
        validate: Whether to run validation after extraction
        sample_size: Sample size for aggregation validation

    Returns:
        ExtractionResult with extraction details and validation results

    Raises:
        InvalidCycleError: If cycle is not valid
        InvalidSourceURLError: If source URL is not from an allowed domain
        SourceReadError: If source data cannot be read
        AggregationIntegrityError: If validation fails
        OutputWriteError: If output cannot be written
    """
    output_path = Path(output_path)

    # Validate cycle
    if not validate_cycle(cycle):
        raise InvalidCycleError(
            message=f"Invalid cycle: {cycle}",
            cycle=cycle,
            min_cycle=MIN_CYCLE,
            max_cycle=MAX_CYCLE,
        )

    source_url = source_url or CONTRIBUTIONS_URL_TEMPLATE.format(cycle=cycle)

    # Validate source URL
    if not validate_source_url(source_url):
        raise InvalidSourceURLError(
            message="Source URL must be from an allowed domain",
            source_url=source_url,
            allowed_domains=ALLOWED_SOURCE_DOMAINS,
        )

    conn = duckdb.connect()

    try:
        # Step 1: Count source rows (with valid recipient ID)
        print(f"Reading from: {source_url}")
        try:
            source_rows = conn.execute(f"""
                SELECT COUNT(*)
                FROM read_parquet('{source_url}')
                WHERE "bonica.rid" IS NOT NULL
            """).fetchone()[0]
        except Exception as e:
            raise SourceReadError(
                message=str(e),
                source_url=source_url,
            ) from e

        print(f"  Source rows (with recipient ID): {source_rows:,}")

        # Step 2: Execute aggregation query and write
        print("Aggregating by recipient...")
        query = RECIPIENT_AGGREGATES_QUERY.format(source_url=source_url)

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

        # Step 3: Count output
        output_count = conn.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{output_path}')
        """).fetchone()[0]
        print(f"  Distinct recipient groups: {output_count:,}")

        # Step 4: Validate
        validation = ValidationResult()
        if validate:
            print("Validating...")
            validation = validate_recipient_aggregates(source_url, output_path, conn, sample_size)
            print(f"  Validation: PASS ({validation.aggregation_sample_size} recipients verified)")

        return ExtractionResult(
            source_url=source_url,
            output_path=output_path,
            cycle=cycle,
            output_type=OutputType.RECIPIENT_AGGREGATES,
            source_rows=source_rows,
            output_count=output_count,
            validation=validation,
        )

    finally:
        conn.close()
