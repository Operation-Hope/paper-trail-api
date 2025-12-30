"""Legislator-recipient crosswalk extractor from DIME data.

This module extracts a crosswalk table mapping legislators (via ICPSR) to
their DIME recipient IDs (bonica_rid). This enables linking legislators
to campaign contribution data.

Key relationships:
- One legislator (ICPSR) may have multiple bonica_rids across election cycles
- This is a 1:many relationship

Example usage:
    from legislator_crosswalk import extract_crosswalk

    result = extract_crosswalk("crosswalk.parquet")
    print(f"Extracted {result.output_count:,} mappings")
    print(f"  {result.unique_icpsr_count:,} legislators")
    print(f"  {result.unique_bonica_rid_count:,} recipients")
"""

from .exceptions import (
    CrosswalkError,
    DuplicateKeyError,
    InvalidSourceURLError,
    OutputWriteError,
    SourceReadError,
    ValidationError,
)
from .extractor import ExtractionResult, extract_crosswalk
from .schema import (
    CROSSWALK_COLUMNS,
    CROSSWALK_SCHEMA,
    DIME_RECIPIENTS_URL,
)
from .validators import ValidationResult

__all__ = [
    # Core functions
    "extract_crosswalk",
    # Data classes
    "ExtractionResult",
    "ValidationResult",
    # Schema
    "CROSSWALK_SCHEMA",
    "CROSSWALK_COLUMNS",
    "DIME_RECIPIENTS_URL",
    # Exceptions
    "CrosswalkError",
    "InvalidSourceURLError",
    "SourceReadError",
    "ValidationError",
    "DuplicateKeyError",
    "OutputWriteError",
]
