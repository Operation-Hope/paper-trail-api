"""Load DIME data from Huggingface into a local DuckDB database."""

from .loader import (
    load_to_duckdb,
    LoadResult,
    query_parquet_direct,
    AVAILABLE_CYCLES,
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
    "load_to_duckdb",
    "LoadResult",
    "query_parquet_direct",
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
