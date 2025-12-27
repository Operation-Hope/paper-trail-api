"""Load DIME data from Huggingface into DuckDB or PostgreSQL."""

from .filters import (
    AmountFilter,
    CycleFilter,
    DateFilter,
    Filter,
    StateFilter,
    large_donors,
    recent_cycles,
)
from .loader import (
    AVAILABLE_CYCLES,
    LoadResult,
    load_to_duckdb,
    query_parquet_direct,
)
from .postgres_loader import (
    PostgresLoadResult,
    load_to_postgres,
)
from .schema import CONTRIBUTIONS_COLUMNS, create_schema

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
