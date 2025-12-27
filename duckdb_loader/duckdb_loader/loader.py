"""Core loader for DIME data from Huggingface to DuckDB.

Uses DuckDB's native Parquet reading for efficient data loading directly from
Huggingface URLs. Supports year-partitioned files for fast cycle-based filtering.
"""

from dataclasses import dataclass, field
from pathlib import Path

import duckdb
from tqdm import tqdm

from .filters import CycleFilter, Filter
from .schema import CONTRIBUTIONS_COLUMNS, create_indexes, create_schema

# Base URL for year-partitioned contribution files on Huggingface
HF_BASE_URL = "https://huggingface.co/datasets/Dustinhax/tyt/resolve/main"
PARQUET_URL_PATTERN = f"{HF_BASE_URL}/dime/contributions/by_year/contribDB_{{cycle}}.parquet"

# Available election cycles in the dataset (even years 1980-2024)
AVAILABLE_CYCLES = list(range(1980, 2026, 2))

DEFAULT_BATCH_SIZE = 100_000


@dataclass
class LoadResult:
    """Result of a load operation."""

    rows_loaded: int
    database_path: str
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
    """Build SQL WHERE clause from filters.

    Args:
        filters: List of filters to convert to SQL
        exclude_cycle: If True, exclude CycleFilter (used when loading specific year files)

    Returns:
        SQL WHERE clause string (without 'WHERE' keyword), or empty string
    """
    sql_parts = []
    for f in filters:
        if exclude_cycle and isinstance(f, CycleFilter):
            continue
        sql = f.to_sql()
        if sql:
            sql_parts.append(f"({sql})")
    return " AND ".join(sql_parts)


def load_to_duckdb(
    database_path: str | Path,
    *,
    filters: list[Filter] | None = None,
    columns: list[str] | None = None,
    table_name: str = "contributions",
    limit: int | None = None,
    show_progress: bool = True,
    create_indexes_after: bool = True,
) -> LoadResult:
    """Load DIME data from Huggingface into a local DuckDB database.

    Uses DuckDB's native Parquet reading for efficient data loading directly
    from Huggingface URLs. When a CycleFilter is specified, only the relevant
    year files are loaded.

    Args:
        database_path: Path to the DuckDB database file (created if not exists)
        filters: List of filters to apply (rows must pass all filters)
        columns: Columns to load (default: CONTRIBUTIONS_COLUMNS subset)
        table_name: Name for the table (default: contributions)
        limit: Maximum rows to load (default: no limit)
        show_progress: Show progress bar
        create_indexes_after: Create indexes after loading

    Returns:
        LoadResult with statistics about the load

    Example:
        >>> from duckdb_loader import load_to_duckdb, CycleFilter, AmountFilter
        >>> result = load_to_duckdb(
        ...     "contributions.duckdb",
        ...     filters=[CycleFilter([2020, 2022]), AmountFilter(min_amount=1000)],
        ...     limit=100_000,
        ... )
        >>> print(f"Loaded {result.rows_loaded:,} rows")
    """
    database_path = Path(database_path)
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

    # Connect and create schema
    conn = duckdb.connect(str(database_path))
    create_schema(conn, table_name, cols)

    # Build column selection SQL
    col_select = ", ".join(f'"{col}"' for col in cols)

    rows_loaded = 0
    cycles_to_process = cycles if show_progress else cycles

    if show_progress:
        print(f"Loading {len(cycles)} cycle(s): {cycles}")
        cycles_to_process = tqdm(cycles, desc="Loading cycles", unit=" cycle")

    try:
        for cycle in cycles_to_process:
            url = _get_parquet_url(cycle)

            # Build query for this cycle
            sql = f'SELECT {col_select} FROM read_parquet("{url}")'
            if where_clause:
                sql += f" WHERE {where_clause}"
            if limit:
                remaining = limit - rows_loaded
                if remaining <= 0:
                    break
                sql += f" LIMIT {remaining}"

            # Insert into table and track count
            insert_sql = f"INSERT INTO {table_name} {sql}"

            try:
                # Get count before insert
                result_before = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                count_before = result_before[0] if result_before else 0

                conn.execute(insert_sql)

                # Get count after insert
                result_after = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                count_after = result_after[0] if result_after else 0

                cycle_rows = count_after - count_before
                rows_loaded += cycle_rows
            except duckdb.HTTPException as e:
                if show_progress:
                    print(f"  Warning: Could not load cycle {cycle}: {e}")
                continue

            if limit and rows_loaded >= limit:
                break

    finally:
        if show_progress and isinstance(cycles_to_process, tqdm):
            cycles_to_process.close()

    # Create indexes
    if create_indexes_after and rows_loaded > 0:
        if show_progress:
            print("Creating indexes...")
        create_indexes(conn, table_name)

    conn.close()

    return LoadResult(
        rows_loaded=rows_loaded,
        database_path=str(database_path),
        table_name=table_name,
        filters_applied=[f.describe() for f in filters],
        columns_loaded=cols,
    )


def query_database(database_path: str | Path, sql: str) -> list[tuple]:
    """Execute a query on an existing database.

    Args:
        database_path: Path to the DuckDB database
        sql: SQL query to execute

    Returns:
        List of result tuples

    Example:
        >>> results = query_database(
        ...     "contributions.duckdb",
        ...     "SELECT cycle, SUM(amount) FROM contributions GROUP BY cycle"
        ... )
    """
    conn = duckdb.connect(str(database_path), read_only=True)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def get_table_info(database_path: str | Path, table_name: str = "contributions") -> dict:
    """Get information about a loaded table.

    Args:
        database_path: Path to the DuckDB database
        table_name: Table to inspect

    Returns:
        Dict with row_count, columns, and size_mb
    """
    conn = duckdb.connect(str(database_path), read_only=True)
    try:
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = result[0] if result else 0
        columns = conn.execute(f"DESCRIBE {table_name}").fetchall()

        # Get file size
        db_path = Path(database_path)
        size_mb = db_path.stat().st_size / (1024 * 1024) if db_path.exists() else 0

        return {
            "row_count": row_count,
            "columns": [(col[0], col[1]) for col in columns],
            "size_mb": round(size_mb, 2),
        }
    finally:
        conn.close()


def query_parquet_direct(
    sql: str,
    cycles: list[int] | None = None,
) -> list[tuple]:
    """Query Huggingface Parquet files directly without creating a local database.

    Useful for quick exploration or one-off queries.

    Args:
        sql: SQL query using 'contributions' as table name
        cycles: Specific cycles to query (default: all)

    Returns:
        List of result tuples

    Example:
        >>> results = query_parquet_direct(
        ...     "SELECT cycle, COUNT(*) FROM contributions GROUP BY cycle",
        ...     cycles=[2024]
        ... )
    """
    cycles = cycles or AVAILABLE_CYCLES
    urls = [_get_parquet_url(c) for c in cycles if c in AVAILABLE_CYCLES]

    if not urls:
        raise ValueError("No valid cycles specified")

    # Create a view called 'contributions' pointing to the Parquet files
    conn = duckdb.connect()
    try:
        if len(urls) == 1:
            url_expr = f'"{urls[0]}"'
        else:
            url_list = ", ".join(f'"{u}"' for u in urls)
            url_expr = f"[{url_list}]"

        conn.execute(f"CREATE VIEW contributions AS SELECT * FROM read_parquet({url_expr})")
        return conn.execute(sql).fetchall()
    finally:
        conn.close()
