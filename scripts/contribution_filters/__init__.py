"""Contribution filters for DIME campaign finance data.

This module provides tools to create filtered contribution datasets:
- Organizational contributions (excludes individual donors)
- Recipient aggregates (total/average amounts per recipient)
"""

from .exceptions import (
    AggregationIntegrityError,
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
    extract_recipient_aggregates,
)
from .schema import (
    ALL_CYCLES,
    CONTRIBUTIONS_URL_TEMPLATE,
    MAX_CYCLE,
    MIN_CYCLE,
    get_organizational_filename,
    get_recipient_aggregates_filename,
)
from .validators import ValidationResult

__all__ = [
    # Extraction functions
    "extract_organizational_contributions",
    "extract_recipient_aggregates",
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
    # Exceptions
    "ContributionFilterError",
    "SourceReadError",
    "OutputWriteError",
    "InvalidSourceURLError",
    "InvalidCycleError",
    "FilterValidationError",
    "AggregationIntegrityError",
    "CompletenessError",
]
