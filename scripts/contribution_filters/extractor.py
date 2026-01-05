"""Core extraction logic for filtered contribution datasets."""

from __future__ import annotations

import logging
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
    ORGANIZATIONAL_QUERY_WITH_BIOGUIDE,
    RAW_ORGANIZATIONAL_CONTRIBUTIONS_QUERY,
    RECIPIENT_AGGREGATES_QUERY,
    RECIPIENT_AGGREGATES_QUERY_WITH_BIOGUIDE,
    RECIPIENTS_URL,
    escape_sql_string,
    validate_cycle,
    validate_source_url,
)
from .validators import (
    ValidationResult,
    validate_organizational_output,
    validate_recipient_aggregates,
)

logger = logging.getLogger(__name__)


class OutputType(Enum):
    """Output types for contribution filtering."""

    ORGANIZATIONAL = "organizational"
    RECIPIENT_AGGREGATES = "recipient_aggregates"
    RAW_ORGANIZATIONAL = "raw_organizational"  # New: detailed org records with bioguide_id


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
    legislators_path: Path | str | None = None,
    validate: bool = True,
) -> ExtractionResult:
    """
    Extract organizational contributions for a given cycle.

    Filters out individual contributors (contributor.type = 'I'),
    keeping only PACs, corporations, committees, unions, and other organizations.

    If legislators_path is provided, adds bioguide_id column via FEC ID join.
    The bioguide_id will be NULL for ~90% of records (non-FEC ICPSR formats).

    Args:
        output_path: Path for output .parquet file
        cycle: Election cycle year (even year 1980-2024)
        source_url: Optional custom source URL (default: HuggingFace)
        legislators_path: Optional path to legislators.parquet for bioguide_id join
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

    conn = None
    try:
        conn = duckdb.connect()

        # Step 1: Count source rows
        logger.info("Reading from: %s", source_url)
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

        logger.info("  Source rows: %s", f"{source_rows:,}")

        # Step 2: Execute filter query and write
        if legislators_path:
            logger.info("Filtering organizational contributions (with bioguide_id)...")
            legislators_path_str = escape_sql_string(str(legislators_path))
            query = ORGANIZATIONAL_QUERY_WITH_BIOGUIDE.format(
                source_url=source_url,
                legislators_path=legislators_path_str,
                recipients_url=RECIPIENTS_URL,
            )
        else:
            logger.info("Filtering organizational contributions...")
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
        logger.info("  Organizational contributions: %s", f"{output_count:,}")
        filtered_count = source_rows - output_count
        logger.info("  Filtered out: %s individual contributions", f"{filtered_count:,}")

        # Log bioguide_id coverage if legislators_path was provided
        if legislators_path:
            matched_count = conn.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{output_path}')
                WHERE bioguide_id IS NOT NULL
            """).fetchone()[0]
            coverage_pct = (matched_count / output_count * 100) if output_count > 0 else 0
            logger.info(
                "  Bioguide ID coverage: %s/%s (%.1f%%)",
                f"{matched_count:,}",
                f"{output_count:,}",
                coverage_pct,
            )

        # Step 4: Validate
        validation = ValidationResult()
        if validate:
            logger.info("Validating...")
            validation = validate_organizational_output(
                source_url, output_path, conn, source_rows, output_count
            )
            logger.info("  Validation: PASS")

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
        if conn is not None:
            conn.close()


def extract_recipient_aggregates(
    output_path: Path | str,
    cycle: int,
    *,
    source_url: str | None = None,
    legislators_path: Path | str | None = None,
    validate: bool = True,
    sample_size: int = 100,
) -> ExtractionResult:
    """
    Extract recipient aggregates for a given cycle.

    Groups contributions by recipient and calculates:
    - total_amount: SUM of all contributions
    - avg_amount: AVG contribution size
    - contribution_count: Number of contributions

    If legislators_path is provided, adds bioguide_id column via FEC ID join.
    The bioguide_id will be NULL for ~90% of records (non-FEC ICPSR formats).

    Args:
        output_path: Path for output .parquet file
        cycle: Election cycle year (even year 1980-2024)
        source_url: Optional custom source URL (default: HuggingFace)
        legislators_path: Optional path to legislators.parquet for bioguide_id join
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

    conn = None
    try:
        conn = duckdb.connect()

        # Step 1: Count source rows (with valid recipient ID)
        logger.info("Reading from: %s", source_url)
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

        logger.info("  Source rows (with recipient ID): %s", f"{source_rows:,}")

        # Step 2: Execute aggregation query and write
        if legislators_path:
            logger.info("Aggregating by recipient (with bioguide_id)...")
            legislators_path_str = escape_sql_string(str(legislators_path))
            query = RECIPIENT_AGGREGATES_QUERY_WITH_BIOGUIDE.format(
                source_url=source_url,
                legislators_path=legislators_path_str,
                recipients_url=RECIPIENTS_URL,
            )
        else:
            logger.info("Aggregating by recipient...")
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
        logger.info("  Distinct recipient groups: %s", f"{output_count:,}")

        # Log bioguide_id coverage if legislators_path was provided
        if legislators_path:
            matched_count = conn.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{output_path}')
                WHERE bioguide_id IS NOT NULL
            """).fetchone()[0]
            coverage_pct = (matched_count / output_count * 100) if output_count > 0 else 0
            logger.info(
                "  Bioguide ID coverage: %s/%s (%.1f%%)",
                f"{matched_count:,}",
                f"{output_count:,}",
                coverage_pct,
            )

        # Step 4: Validate
        validation = ValidationResult()
        if validate:
            logger.info("Validating...")
            validation = validate_recipient_aggregates(source_url, output_path, conn, sample_size)
            verified = validation.aggregation_sample_size
            logger.info("  Validation: PASS (%d recipients verified)", verified)

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
        if conn is not None:
            conn.close()


def extract_raw_organizational_contributions(
    output_path: Path | str,
    cycle: int,
    legislators_path: Path | str,
    *,
    source_url: str | None = None,
    validate: bool = True,
) -> ExtractionResult:
    """
    Extract raw organizational contributions with bioguide_id for a given cycle.

    Creates detailed contribution records filtered to organizational contributors only
    (contributor.type != 'I'), with bioguide_id joined via FEC ID lookup.

    This is a NEW output type that provides raw organizational contribution data
    with legislator linking for downstream analysis.

    Args:
        output_path: Path for output .parquet file
        cycle: Election cycle year (even year 1980-2024)
        legislators_path: Path to legislators.parquet for bioguide_id join (required)
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
    legislators_path = Path(legislators_path)

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

    conn = None
    try:
        conn = duckdb.connect()

        # Step 1: Count source rows
        logger.info("Reading from: %s", source_url)
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

        logger.info("  Source rows: %s", f"{source_rows:,}")

        # Step 2: Execute filter query with bioguide join and write
        logger.info("Extracting raw organizational contributions (with bioguide_id)...")
        legislators_path_str = escape_sql_string(str(legislators_path))
        query = RAW_ORGANIZATIONAL_CONTRIBUTIONS_QUERY.format(
            source_url=source_url,
            legislators_path=legislators_path_str,
            recipients_url=RECIPIENTS_URL,
        )

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

        # Step 3: Count output and coverage
        output_count = conn.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{output_path}')
        """).fetchone()[0]
        logger.info("  Raw organizational contributions: %s", f"{output_count:,}")
        filtered_count = source_rows - output_count
        logger.info("  Filtered out: %s individual contributions", f"{filtered_count:,}")

        # Log bioguide_id coverage
        matched_count = conn.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{output_path}')
            WHERE bioguide_id IS NOT NULL
        """).fetchone()[0]
        coverage_pct = (matched_count / output_count * 100) if output_count > 0 else 0
        logger.info(
            "  Bioguide ID coverage: %s/%s (%.1f%%)",
            f"{matched_count:,}",
            f"{output_count:,}",
            coverage_pct,
        )

        # Step 4: Validate
        validation = ValidationResult()
        if validate:
            logger.info("Validating...")
            validation = validate_organizational_output(
                source_url, output_path, conn, source_rows, output_count
            )
            logger.info("  Validation: PASS")

        return ExtractionResult(
            source_url=source_url,
            output_path=output_path,
            cycle=cycle,
            output_type=OutputType.RAW_ORGANIZATIONAL,
            source_rows=source_rows,
            output_count=output_count,
            validation=validation,
        )

    finally:
        if conn is not None:
            conn.close()
