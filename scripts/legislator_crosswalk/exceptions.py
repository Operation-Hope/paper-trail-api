"""Exception hierarchy for legislator crosswalk extractor."""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class CrosswalkError(Exception):
    """Base exception for crosswalk extraction errors."""

    message: str

    def __str__(self) -> str:
        return self.message


@dataclass
class SourceReadError(CrosswalkError):
    """Raised when source data cannot be read."""

    source_url: str

    def __str__(self) -> str:
        return f"Failed to read source: {self.source_url}\n{self.message}"


@dataclass
class InvalidSourceURLError(CrosswalkError):
    """Raised when source URL is not from an allowed domain."""

    source_url: str
    allowed_domains: list[str]

    def __str__(self) -> str:
        domains = ", ".join(self.allowed_domains)
        return f"Invalid source URL: {self.source_url}\nAllowed domains: {domains}"


@dataclass
class OutputWriteError(CrosswalkError):
    """Raised when output cannot be written."""

    output_path: Path

    def __str__(self) -> str:
        return f"Failed to write output: {self.output_path}\n{self.message}"


@dataclass
class ValidationError(CrosswalkError):
    """Raised when validation fails."""

    expected_count: int
    actual_count: int

    def __str__(self) -> str:
        return (
            f"Validation failed: {self.message}\n"
            f"Expected: {self.expected_count:,}\n"
            f"Actual: {self.actual_count:,}"
        )


@dataclass
class DuplicateKeyError(CrosswalkError):
    """Raised when duplicate key pairs are found."""

    duplicate_count: int
    sample_duplicates: list[tuple[str, str]] | None = None

    def __str__(self) -> str:
        msg = f"Found {self.duplicate_count:,} duplicate (icpsr, bonica_rid) pairs"
        if self.sample_duplicates:
            samples = ", ".join(f"({i}, {b})" for i, b in self.sample_duplicates[:5])
            msg += f"\nExamples: {samples}"
        return msg
