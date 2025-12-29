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
    LoadResult,
    load_to_duckdb,
    query_parquet_direct,
)
from .paper_trail_loader import (
    PAPER_TRAIL_BASE_URL,
    PaperTrailLoadResult,
    load_paper_trail_to_postgres,
)
from .postgres_loader import (
    PostgresLoadResult,
    load_to_postgres,
)
from .schema import (
    AVAILABLE_CYCLES,
    CONTRIBUTIONS_COLUMNS,
    LEGISLATORS_COLUMNS,
    ORGANIZATIONAL_COLUMNS,
    RECIPIENT_AGGREGATES_COLUMNS,
    create_schema,
)

__all__ = [
    # DuckDB
    "load_to_duckdb",
    "LoadResult",
    "query_parquet_direct",
    # PostgreSQL (raw DIME from Dustinhax/tyt)
    "load_to_postgres",
    "PostgresLoadResult",
    # PostgreSQL (paper-trail-data from Dustinhax/paper-trail-data)
    "load_paper_trail_to_postgres",
    "PaperTrailLoadResult",
    "PAPER_TRAIL_BASE_URL",
    # Shared
    "AVAILABLE_CYCLES",
    "create_schema",
    # Schema columns
    "CONTRIBUTIONS_COLUMNS",
    "LEGISLATORS_COLUMNS",
    "ORGANIZATIONAL_COLUMNS",
    "RECIPIENT_AGGREGATES_COLUMNS",
    # Filters
    "Filter",
    "CycleFilter",
    "StateFilter",
    "AmountFilter",
    "DateFilter",
    "recent_cycles",
    "large_donors",
]
