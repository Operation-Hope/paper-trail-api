"""Exception hierarchy for distinct legislators extractor."""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class DistinctLegislatorsError(Exception):
    """Base exception for distinct legislators extraction errors."""

    message: str

    def __str__(self) -> str:
        return self.message


@dataclass
class SourceReadError(DistinctLegislatorsError):
    """Raised when source data cannot be read."""

    source_url: str

    def __str__(self) -> str:
        return f"Failed to read source: {self.source_url}\n{self.message}"


@dataclass
class CompletenessError(DistinctLegislatorsError):
    """Raised when completeness validation fails (Tier 1)."""

    expected_count: int
    actual_count: int
    missing_ids: list[str] | None = None
    extra_ids: list[str] | None = None

    def __str__(self) -> str:
        parts = [f"Completeness validation failed: {self.message}"]
        parts.append(f"Expected {self.expected_count:,} legislators, got {self.actual_count:,}")
        if self.missing_ids:
            parts.append(
                f"Missing: {self.missing_ids[:5]}{'...' if len(self.missing_ids) > 5 else ''}"
            )
        if self.extra_ids:
            parts.append(f"Extra: {self.extra_ids[:5]}{'...' if len(self.extra_ids) > 5 else ''}")
        return "\n".join(parts)


@dataclass
class AggregationError(DistinctLegislatorsError):
    """Raised when aggregation validation fails (Tier 2)."""

    bioguide_id: str
    field_name: str
    expected_value: str
    actual_value: str

    def __str__(self) -> str:
        return (
            f"Aggregation validation failed for {self.bioguide_id}: {self.message}\n"
            f"Field: {self.field_name}\n"
            f"Expected: {self.expected_value}\n"
            f"Actual: {self.actual_value}"
        )


@dataclass
class SampleValidationError(DistinctLegislatorsError):
    """Raised when sample validation fails (Tier 3)."""

    bioguide_id: str
    field_name: str
    expected_value: str
    actual_value: str
    sample_index: int

    def __str__(self) -> str:
        return (
            f"Sample validation failed at index {self.sample_index}: {self.message}\n"
            f"Legislator: {self.bioguide_id}\n"
            f"Field: {self.field_name}\n"
            f"Expected: {self.expected_value}\n"
            f"Actual: {self.actual_value}"
        )


@dataclass
class OutputWriteError(DistinctLegislatorsError):
    """Raised when output cannot be written."""

    output_path: Path

    def __str__(self) -> str:
        return f"Failed to write output: {self.output_path}\n{self.message}"


@dataclass
class InvalidSourceURLError(DistinctLegislatorsError):
    """Raised when source URL is not from an allowed domain."""

    source_url: str
    allowed_domains: list[str]

    def __str__(self) -> str:
        domains = ", ".join(self.allowed_domains)
        return f"Invalid source URL: {self.source_url}\nAllowed domains: {domains}"
