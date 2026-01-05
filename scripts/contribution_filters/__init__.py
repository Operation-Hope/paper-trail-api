"""Contribution filters for DIME campaign finance data.

This module provides tools to create filtered contribution datasets:
- Organizational contributions (excludes individual donors)
- Recipient aggregates (total/average amounts per recipient)
- Raw organizational contributions with bioguide_id (legislator-linked records)
"""

from .exceptions import (
    AggregationIntegrityError,
    BioguideJoinError,
    CompletenessError,
    ContributionFilterError,
    FilterValidationError,
    InvalidCycleError,
    InvalidSourceURLError,
    OutputWriteError,
    SourceReadError,
)
from .extractor import (
    ExtractionResult,
    OutputType,
    extract_organizational_contributions,
    extract_raw_organizational_contributions,
    extract_recipient_aggregates,
)
from .schema import (
    ALL_CYCLES,
    CONTRIBUTIONS_URL_TEMPLATE,
    MAX_CYCLE,
    MIN_CYCLE,
    get_organizational_filename,
    get_raw_organizational_filename,
    get_recipient_aggregates_filename,
)
from .validators import ValidationResult, validate_bioguide_join

__all__ = [
    # Extraction functions
    "extract_organizational_contributions",
    "extract_recipient_aggregates",
    "extract_raw_organizational_contributions",
    # Result types
    "ExtractionResult",
    "OutputType",
    "ValidationResult",
    # Constants
    "ALL_CYCLES",
    "MIN_CYCLE",
    "MAX_CYCLE",
    "CONTRIBUTIONS_URL_TEMPLATE",
    # Helpers
    "get_organizational_filename",
    "get_recipient_aggregates_filename",
    "get_raw_organizational_filename",
    # Validators
    "validate_bioguide_join",
    # Exceptions
    "ContributionFilterError",
    "SourceReadError",
    "OutputWriteError",
    "InvalidSourceURLError",
    "InvalidCycleError",
    "FilterValidationError",
    "AggregationIntegrityError",
    "CompletenessError",
    "BioguideJoinError",
]
