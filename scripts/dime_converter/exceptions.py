"""Custom exception types for DIME conversion errors with detailed context."""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class DIMEConversionError(Exception):
    """Base exception for DIME conversion errors."""

    source_path: Path
    message: str

    def __str__(self) -> str:
        return f"[{self.source_path.name}] {self.message}"


@dataclass
class CSVParseError(DIMEConversionError):
    """Error parsing CSV file."""

    line_number: int | None = None
    column_name: str | None = None
    problematic_value: str | None = None

    def __str__(self) -> str:
        parts = [f"[{self.source_path.name}]"]
        if self.line_number:
            parts.append(f"line {self.line_number}")
        if self.column_name:
            parts.append(f"column '{self.column_name}'")
        if self.problematic_value:
            # Truncate long values
            val = self.problematic_value[:100]
            if len(self.problematic_value) > 100:
                val += "..."
            parts.append(f"value: {repr(val)}")
        parts.append(self.message)
        return " ".join(parts)


@dataclass
class RowCountMismatchError(DIMEConversionError):
    """Row count validation failed."""

    expected_rows: int = 0
    actual_rows: int = 0

    def __str__(self) -> str:
        diff = self.expected_rows - self.actual_rows
        return (
            f"[{self.source_path.name}] Row count mismatch: "
            f"expected {self.expected_rows:,}, got {self.actual_rows:,} "
            f"(difference: {diff:,})"
        )


@dataclass
class ChecksumMismatchError(DIMEConversionError):
    """Column checksum validation failed."""

    column_name: str = ""
    expected_value: float | int = 0
    actual_value: float | int = 0

    def __str__(self) -> str:
        return (
            f"[{self.source_path.name}] Checksum mismatch for '{self.column_name}': "
            f"expected {self.expected_value}, got {self.actual_value}"
        )


@dataclass
class SampleMismatchError(DIMEConversionError):
    """Sample row comparison failed."""

    row_index: int = 0
    column_name: str = ""
    expected_value: str = ""
    actual_value: str = ""

    def __str__(self) -> str:
        return (
            f"[{self.source_path.name}] Sample mismatch at row {self.row_index}, "
            f"column '{self.column_name}': expected {repr(self.expected_value)}, "
            f"got {repr(self.actual_value)}"
        )


@dataclass
class SchemaValidationError(DIMEConversionError):
    """Schema validation failed."""

    expected_columns: list[str] | None = None
    actual_columns: list[str] | None = None

    def __str__(self) -> str:
        expected = set(self.expected_columns or [])
        actual = set(self.actual_columns or [])
        missing = expected - actual
        extra = actual - expected
        parts = [f"[{self.source_path.name}] Schema mismatch:"]
        if missing:
            parts.append(f"missing columns: {sorted(missing)}")
        if extra:
            parts.append(f"extra columns: {sorted(extra)}")
        return " ".join(parts)
