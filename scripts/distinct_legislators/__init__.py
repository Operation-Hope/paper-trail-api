"""Distinct legislators extractor from Voteview congressional data.

This module extracts a deduplicated list of legislators from Voteview's
HSall_members data, aggregating congress sessions served for each legislator.

Unlike CSV→Parquet converters which perform lossless conversion, this module
performs aggregation (GROUP BY bioguide_id) and validates correct transformation
through three-tier validation:

- Tier 1: Completeness - every source bioguide_id appears exactly once
- Tier 2: Aggregation Integrity - MIN/MAX/LIST operations are correct
- Tier 3: Sample Verification - deep validation of random legislators

Example usage:
    from distinct_legislators import extract_distinct_legislators

    result = extract_distinct_legislators("legislators.parquet")
    print(f"Extracted {result.output_count:,} legislators")
"""

from .exceptions import (
    AggregationError,
    CompletenessError,
    DistinctLegislatorsError,
    OutputWriteError,
    SampleValidationError,
    SourceReadError,
)
from .extractor import ExtractionResult, extract_distinct_legislators
from .schema import (
    DISTINCT_LEGISLATORS_COLUMNS,
    DISTINCT_LEGISLATORS_SCHEMA,
    MIN_CONGRESS,
    VOTEVIEW_MEMBERS_URL,
    congress_to_years,
)
from .validators import ValidationResult

__all__ = [
    # Core functions
    "extract_distinct_legislators",
    "congress_to_years",
    # Data classes
    "ExtractionResult",
    "ValidationResult",
    # Schema
    "DISTINCT_LEGISLATORS_SCHEMA",
    "DISTINCT_LEGISLATORS_COLUMNS",
    "MIN_CONGRESS",
    "VOTEVIEW_MEMBERS_URL",
    # Exceptions
    "DistinctLegislatorsError",
    "SourceReadError",
    "CompletenessError",
    "AggregationError",
    "SampleValidationError",
    "OutputWriteError",
]
