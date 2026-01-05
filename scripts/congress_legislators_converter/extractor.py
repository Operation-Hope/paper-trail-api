"""Core extraction logic for unified legislators from current + historical data."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from .congress_utils import DEFAULT_MIN_CONGRESS, congress_start_year
from .downloader import download_json_file
from .exceptions import SourceNotFoundError
from .json_parser import extract_bioguide_max_congress, filter_bioguides_by_congress
from .schema import UNIFIED_EXTRACTION_QUERY, FileType


def _escape_sql_path(path: Path) -> str:
    """Escape a path for use in SQL string literals (double single quotes)."""
    return str(path).replace("'", "''")


@dataclass
class UnifiedExtractionResult:
    """Result of a successful unified legislators extraction."""

    current_path: Path
    historical_path: Path
    output_path: Path
    current_count: int
    historical_count: int
    output_count: int
    unique_bioguide_count: int
    fec_ids_populated_count: int
    icpsr_populated_count: int
    min_congress: int | None = None
    filtered_out_count: int = 0


def extract_unified_legislators(
    current_path: Path | str,
    historical_path: Path | str,
    output_path: Path | str,
    *,
    validate: bool = True,
    sample_size: int = 100,
    min_congress: int | None = DEFAULT_MIN_CONGRESS,
) -> UnifiedExtractionResult:
    """
    Extract unified legislators from current + historical parquet files.

    Merges current and historical legislators into a single table with:
    - bioguide_id as primary key (non-nullable)
    - fec_ids parsed from comma-separated string to array
    - icpsr cast from string to int64 (with TRY_CAST for malformed values)
    - is_current flag based on source file
    - Deduplication by bioguide_id (current takes precedence)
    - Optional filtering by minimum congress number

    Args:
        current_path: Path to legislators-current.parquet
        historical_path: Path to legislators-historical.parquet
        output_path: Path for output legislators.parquet
        validate: Whether to run validation after extraction
        sample_size: Sample size for validation
        min_congress: Minimum congress number to include (default: 96).
                      Set to None to include all legislators.

    Returns:
        UnifiedExtractionResult with extraction details

    Raises:
        SourceNotFoundError: If source parquet files don't exist
        CongressLegislatorsConversionError: If extraction fails
    """
    current_path = Path(current_path)
    historical_path = Path(historical_path)
    output_path = Path(output_path)

    # Validate source files exist
    if not current_path.exists():
        raise SourceNotFoundError(
            source_path=current_path,
            message="Run 'all' command first to generate source parquet files",
        )
    if not historical_path.exists():
        raise SourceNotFoundError(
            source_path=historical_path,
            message="Run 'all' command first to generate source parquet files",
        )

    # Get eligible bioguide_ids if filtering by congress
    eligible_bioguides: set[str] | None = None
    if min_congress is not None:
        eligible_bioguides = _get_eligible_bioguides(current_path.parent, min_congress)
        start_year = congress_start_year(min_congress)
        print(f"Filtering to Congress {min_congress}+ ({start_year} onwards)")
        print(f"  Eligible legislators: {len(eligible_bioguides):,}")

    # Escape paths for SQL interpolation
    current_sql = _escape_sql_path(current_path)
    historical_sql = _escape_sql_path(historical_path)
    output_sql = _escape_sql_path(output_path)

    with duckdb.connect() as conn:
        # Step 1: Count source rows
        print(f"Reading from: {current_path.name}")
        current_count = conn.execute(f"""
            SELECT COUNT(*)
            FROM read_parquet('{current_sql}')
            WHERE bioguide_id IS NOT NULL AND bioguide_id != ''
        """).fetchone()[0]
        print(f"  Current legislators: {current_count:,}")

        print(f"Reading from: {historical_path.name}")
        historical_count = conn.execute(f"""
            SELECT COUNT(*)
            FROM read_parquet('{historical_sql}')
            WHERE bioguide_id IS NOT NULL AND bioguide_id != ''
        """).fetchone()[0]
        print(f"  Historical legislators: {historical_count:,}")

        # Step 2: Execute extraction query
        print("Extracting unified legislators...")
        query = UNIFIED_EXTRACTION_QUERY.format(
            current_path=current_sql,
            historical_path=historical_sql,
        )

        # Step 3: Write to parquet (with optional filtering)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if eligible_bioguides is not None:
            # Create temp table with eligible bioguide_ids
            conn.execute("CREATE TEMP TABLE eligible_bioguides (bioguide_id VARCHAR)")
            conn.executemany(
                "INSERT INTO eligible_bioguides VALUES (?)",
                [(b,) for b in eligible_bioguides],
            )

            # Wrap query with filter
            filtered_query = f"""
                SELECT u.*
                FROM ({query}) u
                WHERE u.bioguide_id IN (SELECT bioguide_id FROM eligible_bioguides)
            """
            conn.execute(f"""
                COPY ({filtered_query})
                TO '{output_sql}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3)
            """)
        else:
            conn.execute(f"""
                COPY ({query})
                TO '{output_sql}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3)
            """)

        # Step 4: Get output statistics
        output_count = conn.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{output_sql}')
        """).fetchone()[0]
        print(f"  Unified legislators: {output_count:,}")

        unique_bioguide = conn.execute(f"""
            SELECT COUNT(DISTINCT bioguide_id) FROM read_parquet('{output_sql}')
        """).fetchone()[0]
        print(f"  Unique bioguide_ids: {unique_bioguide:,}")

        # Calculate filtered out count - count unique bioguides in sources
        unique_in_sources = conn.execute(f"""
            SELECT COUNT(DISTINCT bioguide_id) FROM (
                SELECT bioguide_id FROM read_parquet('{current_sql}')
                UNION
                SELECT bioguide_id FROM read_parquet('{historical_sql}')
            )
        """).fetchone()[0]
        filtered_out = unique_in_sources - output_count

        if min_congress is not None and filtered_out > 0:
            print(f"  Filtered out: {filtered_out:,} (pre-Congress {min_congress})")

        # Coverage statistics
        fec_populated = conn.execute(f"""
            SELECT COUNT(*)
            FROM read_parquet('{output_sql}')
            WHERE fec_ids IS NOT NULL AND LEN(fec_ids) > 0
        """).fetchone()[0]
        fec_pct = (fec_populated / output_count * 100) if output_count > 0 else 0
        print(f"  With FEC IDs: {fec_populated:,} ({fec_pct:.1f}%)")

        icpsr_populated = conn.execute(f"""
            SELECT COUNT(*)
            FROM read_parquet('{output_sql}')
            WHERE icpsr IS NOT NULL
        """).fetchone()[0]
        icpsr_pct = (icpsr_populated / output_count * 100) if output_count > 0 else 0
        print(f"  With ICPSR: {icpsr_populated:,} ({icpsr_pct:.1f}%)")

        # Step 5: Validate
        if validate:
            from .validators import (
                validate_unified_completeness,
                validate_unified_coverage,
                validate_unified_sample,
            )

            print("Validating...")

            # Tier 1: Completeness (bounds depend on filtering)
            if min_congress is not None:
                # Filtered: Congress 96+ yields ~2,400 legislators
                completeness_min, completeness_max = 500, 5000
            else:
                # Unfiltered: all legislators ~12,700
                completeness_min, completeness_max = 10_000, 15_000

            validate_unified_completeness(
                output_path, conn, min_expected=completeness_min, max_expected=completeness_max
            )
            print(f"  Tier 1 (Completeness): PASS ({unique_bioguide:,} unique bioguide_ids)")

            # Tier 2: Coverage
            validate_unified_coverage(output_path, conn)
            print(f"  Tier 2 (Coverage): PASS (FEC: {fec_pct:.1f}%, ICPSR: {icpsr_pct:.1f}%)")

            # Tier 3: Sample
            validate_unified_sample(current_path, historical_path, output_path, conn, sample_size)
            print(f"  Tier 3 (Sample): PASS ({sample_size} rows verified)")

        return UnifiedExtractionResult(
            current_path=current_path,
            historical_path=historical_path,
            output_path=output_path,
            current_count=current_count,
            historical_count=historical_count,
            output_count=output_count,
            unique_bioguide_count=unique_bioguide,
            fec_ids_populated_count=fec_populated,
            icpsr_populated_count=icpsr_populated,
            min_congress=min_congress,
            filtered_out_count=filtered_out,
        )


def _get_eligible_bioguides(data_dir: Path, min_congress: int) -> set[str]:
    """
    Get set of bioguide_ids eligible for inclusion based on congress filter.

    Downloads JSON files if needed, parses term data, and filters by congress.

    Args:
        data_dir: Directory containing (or to download) JSON files.
        min_congress: Minimum congress number to include.

    Returns:
        Set of bioguide_ids that served in min_congress or later.
    """
    current_json = data_dir / "legislators-current.json"
    historical_json = data_dir / "legislators-historical.json"

    # Download JSON files if they don't exist
    if not current_json.exists():
        print("Downloading legislators-current.json for congress filtering...")
        download_json_file(FileType.CURRENT, data_dir)

    if not historical_json.exists():
        print("Downloading legislators-historical.json for congress filtering...")
        download_json_file(FileType.HISTORICAL, data_dir)

    # Parse JSON files to get bioguide -> max_congress mapping
    bioguide_max_congress = extract_bioguide_max_congress(current_json, historical_json)

    # Filter to eligible bioguides
    return filter_bioguides_by_congress(bioguide_max_congress, min_congress)
