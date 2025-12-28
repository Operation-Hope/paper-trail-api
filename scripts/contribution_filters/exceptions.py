"""Exception hierarchy for contribution filters."""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class ContributionFilterError(Exception):
    """Base exception for contribution filter errors."""

    message: str

    def __str__(self) -> str:
        return self.message


@dataclass
class SourceReadError(ContributionFilterError):
    """Raised when source data cannot be read."""

    source_url: str

    def __str__(self) -> str:
        return f"Failed to read source: {self.source_url}\n{self.message}"


@dataclass
class OutputWriteError(ContributionFilterError):
    """Raised when output cannot be written."""

    output_path: Path

    def __str__(self) -> str:
        return f"Failed to write output: {self.output_path}\n{self.message}"


@dataclass
class InvalidSourceURLError(ContributionFilterError):
    """Raised when source URL is not from an allowed domain."""

    source_url: str
    allowed_domains: list[str]

    def __str__(self) -> str:
        domains = ", ".join(self.allowed_domains)
        return f"Invalid source URL: {self.source_url}\nAllowed domains: {domains}"


@dataclass
class InvalidCycleError(ContributionFilterError):
    """Raised when cycle is not valid."""

    cycle: int
    min_cycle: int
    max_cycle: int

    def __str__(self) -> str:
        return (
            f"Invalid cycle: {self.cycle}\n"
            f"Must be even year between {self.min_cycle} and {self.max_cycle}"
        )


@dataclass
class FilterValidationError(ContributionFilterError):
    """Raised when filter validation fails."""

    field_name: str
    expected_condition: str
    violation_count: int

    def __str__(self) -> str:
        return (
            f"Filter validation failed: {self.message}\n"
            f"Field: {self.field_name}\n"
            f"Expected: {self.expected_condition}\n"
            f"Violations found: {self.violation_count:,}"
        )


@dataclass
class AggregationIntegrityError(ContributionFilterError):
    """Raised when aggregation integrity check fails."""

    recipient_id: str
    field_name: str
    expected_value: str
    actual_value: str

    def __str__(self) -> str:
        return (
            f"Aggregation integrity failed for {self.recipient_id}: {self.message}\n"
            f"Field: {self.field_name}\n"
            f"Expected: {self.expected_value}\n"
            f"Actual: {self.actual_value}"
        )


@dataclass
class CompletenessError(ContributionFilterError):
    """Raised when completeness validation fails."""

    expected_count: int
    actual_count: int

    def __str__(self) -> str:
        return (
            f"Completeness failed: {self.message}\n"
            f"Expected {self.expected_count:,}, got {self.actual_count:,}"
        )
