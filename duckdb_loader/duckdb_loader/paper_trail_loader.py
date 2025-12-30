"""PostgreSQL loader for processed paper-trail-data from Huggingface.

Loads three datasets from the Dustinhax/paper-trail-data repository:
1. distinct_legislators - Unique legislators from Voteview data (~2,303 rows)
2. organizational_contributions - DIME contributions filtered to organizational donors
3. recipient_aggregates - Pre-computed contribution aggregations by recipient

This is a curated subset of the raw DIME data (Dustinhax/tyt) with pre-filtered
and pre-aggregated tables optimized for the Paper Trail API.

Uses DuckDB's native Parquet reading for efficient streaming from Huggingface URLs,
then batch-inserts into PostgreSQL.

Data source: https://huggingface.co/datasets/Dustinhax/paper-trail-data
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, cast

if TYPE_CHECKING:
    from typing import LiteralString

import duckdb
import psycopg
from psycopg import errors as pg_errors
from psycopg import sql

from .filters import CycleFilter, Filter
from .schema import (
    AVAILABLE_CYCLES,
    CROSSWALK_COLUMNS,
    LEGISLATORS_COLUMNS,
    ORGANIZATIONAL_COLUMNS,
    RECIPIENT_AGGREGATES_COLUMNS,
)

# Base URL for paper-trail-data on Huggingface
PAPER_TRAIL_BASE_URL = "https://huggingface.co/datasets/Dustinhax/paper-trail-data/resolve/main"

# URL patterns for each dataset
LEGISLATORS_URL = f"{PAPER_TRAIL_BASE_URL}/distinct_legislators.parquet"
CROSSWALK_URL = f"{PAPER_TRAIL_BASE_URL}/legislator_crosswalk.parquet"
ORGANIZATIONAL_URL_PATTERN = f"{PAPER_TRAIL_BASE_URL}/dime/contributions/organizational/contribDB_{{cycle}}_organizational.parquet"
RECIPIENT_AGGREGATES_URL_PATTERN = f"{PAPER_TRAIL_BASE_URL}/dime/contributions/recipient_aggregates/recipient_aggregates_{{cycle}}.parquet"

DEFAULT_BATCH_SIZE = 100_000  # Large batches for COPY performance

# Dataset type literals
DatasetType = Literal["legislators", "crosswalk", "organizational", "recipient_aggregates", "all"]


@dataclass
class PaperTrailLoadResult:
    """Result of a paper-trail-data load operation."""

    legislators_loaded: int
    crosswalk_loaded: int
    organizational_loaded: int
    recipient_aggregates_loaded: int
    database_url: str
    tables_created: list[str] = field(default_factory=list)
    filters_applied: list[str] = field(default_factory=list)
    cycles_loaded: list[int] = field(default_factory=list)

    @property
    def total_rows_loaded(self) -> int:
        """Total rows loaded across all tables."""
        return (
            self.legislators_loaded
            + self.crosswalk_loaded
            + self.organizational_loaded
            + self.recipient_aggregates_loaded
        )


def _get_organizational_url(cycle: int) -> str:
    """Get the Huggingface URL for a specific cycle's organizational contributions."""
    return ORGANIZATIONAL_URL_PATTERN.format(cycle=cycle)


def _get_recipient_aggregates_url(cycle: int) -> str:
    """Get the Huggingface URL for a specific cycle's recipient aggregates."""
    return RECIPIENT_AGGREGATES_URL_PATTERN.format(cycle=cycle)


def _get_cycles_from_filters(filters: list[Filter]) -> list[int] | None:
    """Extract cycles from CycleFilter if present, otherwise return None."""
    for f in filters:
        if isinstance(f, CycleFilter):
            return f.cycles
    return None


def _sanitize_column_name(col: str) -> str:
    """Convert column names with dots to underscores for PostgreSQL compatibility."""
    return col.replace(".", "_")


def _get_postgres_column_type_legislators(col: str) -> str:
    """Map legislator column names to PostgreSQL types."""
    type_map = {
        "icpsr": "INTEGER",
        "party_code": "INTEGER",
        "first_congress": "INTEGER",
        "last_congress": "INTEGER",
        "nominate_dim1": "DOUBLE PRECISION",
        "nominate_dim2": "DOUBLE PRECISION",
        "congresses_served": "INTEGER[]",
    }
    return type_map.get(col, "TEXT")


def _get_postgres_column_type_crosswalk(_col: str) -> str:
    """Map crosswalk column names to PostgreSQL types.

    All columns are TEXT since ICPSR in DIME is stored as string.
    """
    return "TEXT"


def _get_postgres_column_type_organizational(col: str) -> str:
    """Map organizational contributions column names to PostgreSQL types.

    Note: The paper-trail-data organizational parquet files have different types
    than the raw DIME data (e.g., is.corp, latitude, longitude are VARCHAR).
    """
    type_map = {
        "cycle": "INTEGER",
        "excluded.from.scaling": "INTEGER",
        "amount": "DOUBLE PRECISION",
        "contributor.cfscore": "DOUBLE PRECISION",
        "candidate.cfscore": "DOUBLE PRECISION",
        "gis.confidence": "DOUBLE PRECISION",
        # Note: latitude, longitude, is.corp are VARCHAR in paper-trail-data
    }
    return type_map.get(col, "TEXT")


def _get_postgres_column_type_recipient_aggregates(col: str) -> str:
    """Map recipient aggregates column names to PostgreSQL types."""
    type_map = {
        "candidate.cfscore": "DOUBLE PRECISION",
        "total_amount": "DOUBLE PRECISION",
        "avg_amount": "DOUBLE PRECISION",
        "contribution_count": "DOUBLE PRECISION",  # Float in parquet
        "individual_total": "DOUBLE PRECISION",
        "individual_count": "DOUBLE PRECISION",  # Float in parquet
        "organizational_total": "DOUBLE PRECISION",
        "organizational_count": "DOUBLE PRECISION",  # Float in parquet
    }
    return type_map.get(col, "TEXT")


def _build_column_def(col: str, type_func: Callable[[str], str]) -> sql.Composed:
    """Build a column definition using proper sql composition.

    Args:
        col: Original column name (may contain dots)
        type_func: Function that returns PostgreSQL type for the column

    Returns:
        sql.Composed representing '"sanitized_name" TYPE'
    """
    sanitized = _sanitize_column_name(col)
    # Type comes from fixed internal map, safe to cast to LiteralString
    pg_type = cast("LiteralString", type_func(col))
    return sql.SQL("{} {}").format(sql.Identifier(sanitized), sql.SQL(pg_type))


def _create_legislators_schema(
    conn: psycopg.Connection, table_name: str = "distinct_legislators"
) -> None:
    """Create the distinct_legislators table in PostgreSQL."""
    col_defs = [
        _build_column_def(col, _get_postgres_column_type_legislators) for col in LEGISLATORS_COLUMNS
    ]
    create_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {} (id SERIAL PRIMARY KEY, {})").format(
        sql.Identifier(table_name), sql.SQL(", ").join(col_defs)
    )
    conn.execute(create_sql)
    conn.commit()


def _create_organizational_schema(
    conn: psycopg.Connection, table_name: str = "organizational_contributions"
) -> None:
    """Create the organizational_contributions table in PostgreSQL."""
    col_defs = [
        _build_column_def(col, _get_postgres_column_type_organizational)
        for col in ORGANIZATIONAL_COLUMNS
    ]
    create_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {} (id SERIAL PRIMARY KEY, {})").format(
        sql.Identifier(table_name), sql.SQL(", ").join(col_defs)
    )
    conn.execute(create_sql)
    conn.commit()


def _create_recipient_aggregates_schema(
    conn: psycopg.Connection, table_name: str = "recipient_aggregates"
) -> None:
    """Create the recipient_aggregates table in PostgreSQL."""
    col_defs = [
        _build_column_def(col, _get_postgres_column_type_recipient_aggregates)
        for col in RECIPIENT_AGGREGATES_COLUMNS
    ]
    # Add cycle column for tracking which cycle the aggregate is from
    create_sql = sql.SQL(
        "CREATE TABLE IF NOT EXISTS {} (id SERIAL PRIMARY KEY, cycle INTEGER, {})"
    ).format(sql.Identifier(table_name), sql.SQL(", ").join(col_defs))
    conn.execute(create_sql)
    conn.commit()


def _create_legislators_indexes(
    conn: psycopg.Connection, table_name: str = "distinct_legislators"
) -> None:
    """Create indexes on distinct_legislators table."""
    indexes = [
        ("idx_leg_bioguide_id", "bioguide_id"),
        ("idx_leg_icpsr", "icpsr"),
        ("idx_leg_state_abbrev", "state_abbrev"),
        ("idx_leg_party_code", "party_code"),
    ]
    for idx_name, col in indexes:
        try:
            create_idx_sql = sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                sql.Identifier(idx_name),
                sql.Identifier(table_name),
                sql.Identifier(col),
            )
            conn.execute(create_idx_sql)
        except (pg_errors.UndefinedColumn, pg_errors.UndefinedTable):
            # Column or table may not exist if user selected subset
            pass
    conn.commit()


def _create_organizational_indexes(
    conn: psycopg.Connection, table_name: str = "organizational_contributions"
) -> None:
    """Create indexes on organizational_contributions table."""
    indexes = [
        ("idx_org_cycle", "cycle"),
        ("idx_org_contributor_state", "contributor_state"),
        ("idx_org_recipient_state", "recipient_state"),
        ("idx_org_amount", "amount"),
        ("idx_org_bonica_cid", "bonica_cid"),
        ("idx_org_bonica_rid", "bonica_rid"),
    ]
    for idx_name, col in indexes:
        try:
            create_idx_sql = sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                sql.Identifier(idx_name),
                sql.Identifier(table_name),
                sql.Identifier(col),
            )
            conn.execute(create_idx_sql)
        except (pg_errors.UndefinedColumn, pg_errors.UndefinedTable):
            # Column or table may not exist if user selected subset
            pass
    conn.commit()


def _create_recipient_aggregates_indexes(
    conn: psycopg.Connection, table_name: str = "recipient_aggregates"
) -> None:
    """Create indexes on recipient_aggregates table."""
    indexes = [
        ("idx_ragg_cycle", "cycle"),
        ("idx_ragg_bonica_rid", "bonica_rid"),
        ("idx_ragg_recipient_state", "recipient_state"),
        ("idx_ragg_recipient_party", "recipient_party"),
    ]
    for idx_name, col in indexes:
        try:
            create_idx_sql = sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                sql.Identifier(idx_name),
                sql.Identifier(table_name),
                sql.Identifier(col),
            )
            conn.execute(create_idx_sql)
        except (pg_errors.UndefinedColumn, pg_errors.UndefinedTable):
            # Column or table may not exist if user selected subset
            pass
    conn.commit()


def _create_crosswalk_schema(
    conn: psycopg.Connection, table_name: str = "legislator_recipient_crosswalk"
) -> None:
    """Create the legislator_recipient_crosswalk table in PostgreSQL."""
    col_defs = [
        _build_column_def(col, _get_postgres_column_type_crosswalk) for col in CROSSWALK_COLUMNS
    ]
    create_sql = sql.SQL(
        "CREATE TABLE IF NOT EXISTS {} (id SERIAL PRIMARY KEY, {}, UNIQUE(icpsr, bonica_rid))"
    ).format(sql.Identifier(table_name), sql.SQL(", ").join(col_defs))
    conn.execute(create_sql)
    conn.commit()


def _create_crosswalk_indexes(
    conn: psycopg.Connection, table_name: str = "legislator_recipient_crosswalk"
) -> None:
    """Create indexes on legislator_recipient_crosswalk table."""
    indexes = [
        ("idx_xwalk_icpsr", "icpsr"),
        ("idx_xwalk_bonica_rid", "bonica_rid"),
    ]
    for idx_name, col in indexes:
        try:
            create_idx_sql = sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                sql.Identifier(idx_name),
                sql.Identifier(table_name),
                sql.Identifier(col),
            )
            conn.execute(create_idx_sql)
        except (pg_errors.UndefinedColumn, pg_errors.UndefinedTable):
            # Column or table may not exist if user selected subset
            pass
    conn.commit()


def _load_crosswalk(
    pg_conn: psycopg.Connection,
    duck_conn: duckdb.DuckDBPyConnection,
    table_name: str = "legislator_recipient_crosswalk",
    batch_size: int = DEFAULT_BATCH_SIZE,
    show_progress: bool = True,
) -> int:
    """Load legislator_recipient_crosswalk table (always loads full file)."""
    _create_crosswalk_schema(pg_conn, table_name)

    pg_cols = [_sanitize_column_name(col) for col in CROSSWALK_COLUMNS]
    insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(c) for c in pg_cols),
        sql.SQL(", ").join(sql.Placeholder() for _ in pg_cols),
    )

    col_select = ", ".join(f'"{col}"' for col in CROSSWALK_COLUMNS)
    query_sql = f'SELECT {col_select} FROM read_parquet("{CROSSWALK_URL}")'

    if show_progress:
        print(f"Loading legislator_recipient_crosswalk from {CROSSWALK_URL}")

    rows_loaded = 0
    result = duck_conn.execute(query_sql)
    batch = []

    while True:
        row = result.fetchone()
        if row is None:
            break

        batch.append(row)

        if len(batch) >= batch_size:
            with pg_conn.cursor() as cur:
                cur.executemany(insert_sql, batch)
            pg_conn.commit()
            rows_loaded += len(batch)
            batch = []

    if batch:
        with pg_conn.cursor() as cur:
            cur.executemany(insert_sql, batch)
        pg_conn.commit()
        rows_loaded += len(batch)

    return rows_loaded


def _load_legislators(
    pg_conn: psycopg.Connection,
    duck_conn: duckdb.DuckDBPyConnection,
    table_name: str = "distinct_legislators",
    batch_size: int = DEFAULT_BATCH_SIZE,
    show_progress: bool = True,
) -> int:
    """Load distinct_legislators table (always loads full file)."""
    _create_legislators_schema(pg_conn, table_name)

    pg_cols = [_sanitize_column_name(col) for col in LEGISLATORS_COLUMNS]
    insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(c) for c in pg_cols),
        sql.SQL(", ").join(sql.Placeholder() for _ in pg_cols),
    )

    col_select = ", ".join(f'"{col}"' for col in LEGISLATORS_COLUMNS)
    query_sql = f'SELECT {col_select} FROM read_parquet("{LEGISLATORS_URL}")'

    if show_progress:
        print(f"Loading distinct_legislators from {LEGISLATORS_URL}")

    rows_loaded = 0
    result = duck_conn.execute(query_sql)
    batch = []

    # Find the index of congresses_served dynamically
    congresses_served_idx = LEGISLATORS_COLUMNS.index("congresses_served")

    while True:
        row = result.fetchone()
        if row is None:
            break

        # Convert list type for congresses_served
        row_list = list(row)
        if row_list[congresses_served_idx] is not None:
            row_list[congresses_served_idx] = list(row_list[congresses_served_idx])
        batch.append(tuple(row_list))

        if len(batch) >= batch_size:
            with pg_conn.cursor() as cur:
                cur.executemany(insert_sql, batch)
            pg_conn.commit()
            rows_loaded += len(batch)
            batch = []

    if batch:
        with pg_conn.cursor() as cur:
            cur.executemany(insert_sql, batch)
        pg_conn.commit()
        rows_loaded += len(batch)

    return rows_loaded


def _load_organizational_contributions(
    pg_conn: psycopg.Connection,
    duck_conn: duckdb.DuckDBPyConnection,
    cycles: list[int],
    table_name: str = "organizational_contributions",
    batch_size: int = DEFAULT_BATCH_SIZE,
    limit: int | None = None,
    show_progress: bool = True,
) -> int:
    """Load organizational contributions for specified cycles.

    Uses PostgreSQL COPY protocol for 10-100x faster bulk inserts.
    """
    _create_organizational_schema(pg_conn, table_name)

    pg_cols = [_sanitize_column_name(col) for col in ORGANIZATIONAL_COLUMNS]
    col_select = ", ".join(f'"{col}"' for col in ORGANIZATIONAL_COLUMNS)

    rows_loaded = 0

    if show_progress:
        print(f"Loading organizational_contributions for {len(cycles)} cycle(s)")

    for cycle in cycles:
        url = _get_organizational_url(cycle)

        # Calculate how many rows we'll load for this cycle
        remaining = (limit - rows_loaded) if limit else None
        if remaining is not None and remaining <= 0:
            break

        query_sql = f'SELECT {col_select} FROM read_parquet("{url}")'
        if remaining:
            query_sql += f" LIMIT {remaining}"

        if show_progress:
            print(f"  Cycle {cycle}: loading{f' up to {remaining:,}' if remaining else ''}...")

        try:
            result = duck_conn.execute(query_sql)

            # Use COPY protocol for bulk insert (much faster than executemany)
            copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(sql.Identifier(c) for c in pg_cols),
            )

            cycle_rows = 0
            batch_count = 0
            with pg_conn.cursor() as cur, cur.copy(copy_sql) as copy:
                while True:
                    rows = result.fetchmany(batch_size)
                    if not rows:
                        break

                    for row in rows:
                        copy.write_row(row)
                        cycle_rows += 1

                    batch_count += 1
                    if show_progress and batch_count % 10 == 0:
                        print(f"    {cycle_rows:,} rows...", end="\r")

            pg_conn.commit()
            rows_loaded += cycle_rows

            if show_progress:
                print(f"  Cycle {cycle}: {cycle_rows:,} rows loaded")

        except duckdb.HTTPException as e:
            if show_progress:
                print(f"  Warning: Could not load cycle {cycle}: {e}")
            continue

        if limit and rows_loaded >= limit:
            break

    return rows_loaded


def _load_recipient_aggregates(
    pg_conn: psycopg.Connection,
    duck_conn: duckdb.DuckDBPyConnection,
    cycles: list[int],
    table_name: str = "recipient_aggregates",
    batch_size: int = DEFAULT_BATCH_SIZE,
    limit: int | None = None,
    show_progress: bool = True,
) -> int:
    """Load recipient aggregates for specified cycles.

    Uses PostgreSQL COPY protocol for faster bulk inserts.
    """
    _create_recipient_aggregates_schema(pg_conn, table_name)

    # Include cycle as first column after id
    pg_cols = ["cycle"] + [_sanitize_column_name(col) for col in RECIPIENT_AGGREGATES_COLUMNS]
    col_select = ", ".join(f'"{col}"' for col in RECIPIENT_AGGREGATES_COLUMNS)

    rows_loaded = 0

    if show_progress:
        print(f"Loading recipient_aggregates for {len(cycles)} cycle(s)")

    for cycle in cycles:
        url = _get_recipient_aggregates_url(cycle)
        query_sql = f'SELECT {col_select} FROM read_parquet("{url}")'

        if limit:
            remaining = limit - rows_loaded
            if remaining <= 0:
                break
            query_sql += f" LIMIT {remaining}"

        try:
            result = duck_conn.execute(query_sql)

            # Use COPY protocol for bulk insert
            copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(sql.Identifier(c) for c in pg_cols),
            )

            cycle_rows = 0
            with pg_conn.cursor() as cur, cur.copy(copy_sql) as copy:
                while True:
                    rows = result.fetchmany(batch_size)
                    if not rows:
                        break

                    for row in rows:
                        # Prepend cycle to row data
                        copy.write_row((cycle, *row))
                        cycle_rows += 1

                        if limit and rows_loaded + cycle_rows >= limit:
                            break

                    if limit and rows_loaded + cycle_rows >= limit:
                        break

            pg_conn.commit()
            rows_loaded += cycle_rows

            if show_progress:
                print(f"  Cycle {cycle}: {cycle_rows:,} rows loaded")

        except duckdb.HTTPException as e:
            if show_progress:
                print(f"  Warning: Could not load cycle {cycle}: {e}")
            continue

        if limit and rows_loaded >= limit:
            break

    return rows_loaded


def load_paper_trail_to_postgres(
    database_url: str,
    *,
    datasets: DatasetType | list[DatasetType] = "all",
    filters: list[Filter] | None = None,
    limit: int | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    show_progress: bool = True,
    create_indexes_after: bool = True,
    legislators_table: str = "distinct_legislators",
    crosswalk_table: str = "legislator_recipient_crosswalk",
    organizational_table: str = "organizational_contributions",
    recipient_aggregates_table: str = "recipient_aggregates",
) -> PaperTrailLoadResult:
    """Load paper-trail-data from Huggingface into PostgreSQL.

    Loads four processed datasets:
    - distinct_legislators: ~2,303 unique legislators from Voteview
    - legislator_recipient_crosswalk: Maps legislators (icpsr) to DIME recipients (bonica_rid)
    - organizational_contributions: DIME contributions from organizational donors
    - recipient_aggregates: Pre-computed contribution totals by recipient

    Args:
        database_url: PostgreSQL connection URL
        datasets: Which datasets to load ("legislators", "crosswalk", "organizational",
                  "recipient_aggregates", "all", or list of these)
        filters: List of filters to apply (CycleFilter for cycle selection)
        limit: Maximum rows per dataset (legislators/crosswalk always load full)
        batch_size: Rows per batch insert (default: 10,000)
        show_progress: Show progress bars
        create_indexes_after: Create indexes after loading
        legislators_table: Table name for legislators (default: distinct_legislators)
        crosswalk_table: Table name for crosswalk (default: legislator_recipient_crosswalk)
        organizational_table: Table name for organizational (default: organizational_contributions)
        recipient_aggregates_table: Table name for aggregates (default: recipient_aggregates)

    Returns:
        PaperTrailLoadResult with statistics about the load

    Example:
        >>> from duckdb_loader import load_paper_trail_to_postgres, CycleFilter
        >>> result = load_paper_trail_to_postgres(
        ...     "postgresql://localhost/papertrail",
        ...     datasets=["legislators", "crosswalk", "recipient_aggregates"],
        ...     filters=[CycleFilter([2022, 2024])],
        ... )
        >>> print(f"Loaded {result.total_rows_loaded:,} total rows")
    """
    filters = filters or []

    # Normalize datasets to list
    valid_datasets = {"legislators", "crosswalk", "organizational", "recipient_aggregates", "all"}
    dataset_list: list[str]
    if isinstance(datasets, str):
        if datasets == "all":
            dataset_list = ["legislators", "crosswalk", "organizational", "recipient_aggregates"]
        else:
            dataset_list = [datasets]
    else:
        dataset_list = list(datasets)

    # Validate dataset names
    invalid_datasets = set(dataset_list) - valid_datasets
    if invalid_datasets:
        raise ValueError(
            f"Invalid dataset(s): {invalid_datasets}. Valid options: {valid_datasets - {'all'}}"
        )

    # Determine cycles from filters
    cycles = _get_cycles_from_filters(filters)
    if cycles is None:
        cycles = AVAILABLE_CYCLES
    cycles = [c for c in cycles if c in AVAILABLE_CYCLES]

    if not cycles and ("organizational" in dataset_list or "recipient_aggregates" in dataset_list):
        raise ValueError(f"No valid cycles specified. Available: {AVAILABLE_CYCLES}")

    # Connect to databases
    pg_conn = psycopg.connect(database_url)
    duck_conn = duckdb.connect()
    duck_conn.execute("SET enable_progress_bar = false")

    legislators_loaded = 0
    crosswalk_loaded = 0
    organizational_loaded = 0
    recipient_aggregates_loaded = 0
    tables_created: list[str] = []

    try:
        # Load legislators (always full, no cycle filtering)
        if "legislators" in dataset_list:
            legislators_loaded = _load_legislators(
                pg_conn,
                duck_conn,
                table_name=legislators_table,
                batch_size=batch_size,
                show_progress=show_progress,
            )
            tables_created.append(legislators_table)
            if create_indexes_after and legislators_loaded > 0:
                if show_progress:
                    print("Creating legislators indexes...")
                _create_legislators_indexes(pg_conn, legislators_table)

        # Load crosswalk (always full, no cycle filtering)
        if "crosswalk" in dataset_list:
            crosswalk_loaded = _load_crosswalk(
                pg_conn,
                duck_conn,
                table_name=crosswalk_table,
                batch_size=batch_size,
                show_progress=show_progress,
            )
            tables_created.append(crosswalk_table)
            if create_indexes_after and crosswalk_loaded > 0:
                if show_progress:
                    print("Creating crosswalk indexes...")
                _create_crosswalk_indexes(pg_conn, crosswalk_table)

        # Load organizational contributions
        if "organizational" in dataset_list:
            organizational_loaded = _load_organizational_contributions(
                pg_conn,
                duck_conn,
                cycles=cycles,
                table_name=organizational_table,
                batch_size=batch_size,
                limit=limit,
                show_progress=show_progress,
            )
            tables_created.append(organizational_table)
            if create_indexes_after and organizational_loaded > 0:
                if show_progress:
                    print("Creating organizational indexes...")
                _create_organizational_indexes(pg_conn, organizational_table)

        # Load recipient aggregates
        if "recipient_aggregates" in dataset_list:
            recipient_aggregates_loaded = _load_recipient_aggregates(
                pg_conn,
                duck_conn,
                cycles=cycles,
                table_name=recipient_aggregates_table,
                batch_size=batch_size,
                limit=limit,
                show_progress=show_progress,
            )
            tables_created.append(recipient_aggregates_table)
            if create_indexes_after and recipient_aggregates_loaded > 0:
                if show_progress:
                    print("Creating recipient_aggregates indexes...")
                _create_recipient_aggregates_indexes(pg_conn, recipient_aggregates_table)

    finally:
        duck_conn.close()
        pg_conn.close()

    return PaperTrailLoadResult(
        legislators_loaded=legislators_loaded,
        crosswalk_loaded=crosswalk_loaded,
        organizational_loaded=organizational_loaded,
        recipient_aggregates_loaded=recipient_aggregates_loaded,
        database_url=database_url.split("@")[-1] if "@" in database_url else database_url,
        tables_created=tables_created,
        filters_applied=[f.describe() for f in filters],
        cycles_loaded=cycles,
    )
