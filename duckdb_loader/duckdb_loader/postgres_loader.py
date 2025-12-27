"""PostgreSQL loader for DIME data from Huggingface.

Uses DuckDB's native Parquet reading for efficient streaming from Huggingface URLs,
then batch-inserts into PostgreSQL.
"""

from dataclasses import dataclass, field

import duckdb
import psycopg
from psycopg import sql
from tqdm import tqdm

from .filters import CycleFilter, Filter
from .schema import CONTRIBUTIONS_COLUMNS

# Base URL for year-partitioned contribution files on Huggingface
HF_BASE_URL = "https://huggingface.co/datasets/Dustinhax/tyt/resolve/main"
PARQUET_URL_PATTERN = f"{HF_BASE_URL}/dime/contributions/by_year/contribDB_{{cycle}}.parquet"

# Available election cycles in the dataset (even years 1980-2024)
AVAILABLE_CYCLES = list(range(1980, 2026, 2))

DEFAULT_BATCH_SIZE = 10_000


@dataclass
class PostgresLoadResult:
    """Result of a PostgreSQL load operation."""

    rows_loaded: int
    database_url: str
    table_name: str
    filters_applied: list[str] = field(default_factory=list)
    columns_loaded: list[str] = field(default_factory=list)


def _get_parquet_url(cycle: int) -> str:
    """Get the Huggingface URL for a specific cycle's Parquet file."""
    return PARQUET_URL_PATTERN.format(cycle=cycle)


def _get_cycles_from_filters(filters: list[Filter]) -> list[int] | None:
    """Extract cycles from CycleFilter if present, otherwise return None."""
    for f in filters:
        if isinstance(f, CycleFilter):
            return f.cycles
    return None


def _build_where_clause(filters: list[Filter], exclude_cycle: bool = False) -> str:
    """Build SQL WHERE clause from filters."""
    sql_parts = []
    for f in filters:
        if exclude_cycle and isinstance(f, CycleFilter):
            continue
        sql_str = f.to_sql()
        if sql_str:
            sql_parts.append(f"({sql_str})")
    return " AND ".join(sql_parts)


def _get_postgres_column_type(col: str) -> str:
    """Map column names to PostgreSQL types."""
    type_map = {
        # Integer columns
        "cycle": "INTEGER",
        "excluded.from.scaling": "INTEGER",
        # Float columns
        "amount": "DOUBLE PRECISION",
        "contributor.cfscore": "DOUBLE PRECISION",
        "candidate.cfscore": "DOUBLE PRECISION",
        "gis.confidence": "DOUBLE PRECISION",
        "latitude": "DOUBLE PRECISION",
        "longitude": "DOUBLE PRECISION",
    }
    return type_map.get(col, "TEXT")


def _sanitize_column_name(col: str) -> str:
    """Convert column names with dots to underscores for PostgreSQL compatibility."""
    return col.replace(".", "_")


def _create_postgres_schema(
    conn: psycopg.Connection,
    table_name: str,
    columns: list[str],
) -> None:
    """Create the contributions table in PostgreSQL."""
    col_defs = []
    for col in columns:
        pg_name = _sanitize_column_name(col)
        pg_type = _get_postgres_column_type(col)
        col_defs.append(f'"{pg_name}" {pg_type}')

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            {", ".join(col_defs)}
        )
    """
    conn.execute(create_sql)
    conn.commit()


def _create_postgres_indexes(conn: psycopg.Connection, table_name: str) -> None:
    """Create indexes on common query columns."""
    indexes = [
        ("idx_pg_cycle", "cycle"),
        ("idx_pg_contributor_state", "contributor_state"),
        ("idx_pg_recipient_state", "recipient_state"),
        ("idx_pg_amount", "amount"),
        ("idx_pg_date", "date"),
        ("idx_pg_bonica_cid", "bonica_cid"),
        ("idx_pg_bonica_rid", "bonica_rid"),
    ]

    for idx_name, col in indexes:
        try:
            conn.execute(f'CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} ("{col}")')
        except Exception:
            # Column may not exist
            pass
    conn.commit()


def load_to_postgres(
    database_url: str,
    *,
    filters: list[Filter] | None = None,
    columns: list[str] | None = None,
    table_name: str = "contributions",
    limit: int | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    show_progress: bool = True,
    create_indexes_after: bool = True,
) -> PostgresLoadResult:
    """Load DIME data from Huggingface into PostgreSQL.

    Uses DuckDB's native Parquet reading for efficient streaming from
    Huggingface URLs, then batch-inserts into PostgreSQL.

    Args:
        database_url: PostgreSQL connection URL (e.g., postgresql://user:pass@host/db)
        filters: List of filters to apply (rows must pass all filters)
        columns: Columns to load (default: CONTRIBUTIONS_COLUMNS subset)
        table_name: Name for the table (default: contributions)
        limit: Maximum rows to load (default: no limit)
        batch_size: Rows per batch insert (default: 10,000)
        show_progress: Show progress bar
        create_indexes_after: Create indexes after loading

    Returns:
        PostgresLoadResult with statistics about the load

    Example:
        >>> from duckdb_loader import load_to_postgres, CycleFilter
        >>> result = load_to_postgres(
        ...     "postgresql://localhost/papertrail",
        ...     filters=[CycleFilter([2024])],
        ...     limit=100_000,
        ... )
        >>> print(f"Loaded {result.rows_loaded:,} rows")
    """
    cols = columns or CONTRIBUTIONS_COLUMNS
    filters = filters or []

    # Determine which cycles to load
    cycles = _get_cycles_from_filters(filters)
    if cycles is None:
        cycles = AVAILABLE_CYCLES

    # Validate cycles
    cycles = [c for c in cycles if c in AVAILABLE_CYCLES]
    if not cycles:
        raise ValueError(f"No valid cycles specified. Available: {AVAILABLE_CYCLES}")

    # Build WHERE clause (excluding cycle filter since we load specific files)
    where_clause = _build_where_clause(filters, exclude_cycle=True)

    # Connect to PostgreSQL and create schema
    pg_conn = psycopg.connect(database_url)
    _create_postgres_schema(pg_conn, table_name, cols)

    # Prepare column names for PostgreSQL (sanitized)
    pg_cols = [_sanitize_column_name(col) for col in cols]
    insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(c) for c in pg_cols),
        sql.SQL(", ").join(sql.Placeholder() for _ in pg_cols),
    )

    # Build column selection SQL for DuckDB
    col_select = ", ".join(f'"{col}"' for col in cols)

    rows_loaded = 0
    cycles_to_process = cycles

    if show_progress:
        print(f"Loading {len(cycles)} cycle(s): {cycles}")
        cycles_to_process = tqdm(cycles, desc="Loading cycles", unit=" cycle")

    # Use DuckDB to read from Huggingface
    duck_conn = duckdb.connect()

    try:
        for cycle in cycles_to_process:
            url = _get_parquet_url(cycle)

            # Build query for this cycle
            query_sql = f'SELECT {col_select} FROM read_parquet("{url}")'
            if where_clause:
                query_sql += f" WHERE {where_clause}"
            if limit:
                remaining = limit - rows_loaded
                if remaining <= 0:
                    break
                query_sql += f" LIMIT {remaining}"

            try:
                # Stream from DuckDB and batch insert to PostgreSQL
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

                        if limit and rows_loaded >= limit:
                            break

                # Insert remaining rows
                if batch:
                    with pg_conn.cursor() as cur:
                        cur.executemany(insert_sql, batch)
                    pg_conn.commit()
                    rows_loaded += len(batch)

            except duckdb.HTTPException as e:
                if show_progress:
                    print(f"  Warning: Could not load cycle {cycle}: {e}")
                continue

            if limit and rows_loaded >= limit:
                break

    finally:
        duck_conn.close()
        if show_progress and hasattr(cycles_to_process, "close"):
            cycles_to_process.close()

    # Create indexes
    if create_indexes_after and rows_loaded > 0:
        if show_progress:
            print("Creating indexes...")
        _create_postgres_indexes(pg_conn, table_name)

    pg_conn.close()

    return PostgresLoadResult(
        rows_loaded=rows_loaded,
        database_url=database_url.split("@")[-1] if "@" in database_url else database_url,
        table_name=table_name,
        filters_applied=[f.describe() for f in filters],
        columns_loaded=cols,
    )


def get_postgres_table_info(database_url: str, table_name: str = "contributions") -> dict:
    """Get information about a loaded PostgreSQL table.

    Args:
        database_url: PostgreSQL connection URL
        table_name: Table to inspect

    Returns:
        Dict with row_count and columns
    """
    conn = psycopg.connect(database_url)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cur.fetchone()[0]

            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """,
                (table_name,),
            )
            columns = cur.fetchall()

        return {
            "row_count": row_count,
            "columns": [(col[0], col[1]) for col in columns],
        }
    finally:
        conn.close()
