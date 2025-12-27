"""Load DIME data from Huggingface into DuckDB or PostgreSQL."""

from .loader import (
    load_to_duckdb,
    LoadResult,
    query_parquet_direct,
    AVAILABLE_CYCLES,
)
from .postgres_loader import (
    load_to_postgres,
    PostgresLoadResult,
)
from .schema import create_schema, CONTRIBUTIONS_COLUMNS
from .filters import (
    Filter,
    CycleFilter,
    StateFilter,
    AmountFilter,
    DateFilter,
    recent_cycles,
    large_donors,
)

__all__ = [
    # DuckDB
    "load_to_duckdb",
    "LoadResult",
    "query_parquet_direct",
    # PostgreSQL
    "load_to_postgres",
    "PostgresLoadResult",
    # Shared
    "AVAILABLE_CYCLES",
    "create_schema",
    "CONTRIBUTIONS_COLUMNS",
    "Filter",
    "CycleFilter",
    "StateFilter",
    "AmountFilter",
    "DateFilter",
    "recent_cycles",
    "large_donors",
]
