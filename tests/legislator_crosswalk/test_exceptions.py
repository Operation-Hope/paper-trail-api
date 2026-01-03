"""Tests for legislator crosswalk exception hierarchy."""

from pathlib import Path

import pytest

from scripts.legislator_crosswalk.exceptions import (
    CrosswalkError,
    DuplicateKeyError,
    InvalidSourceURLError,
    OutputWriteError,
    SourceReadError,
    ValidationError,
)


class TestCrosswalkError:
    """Tests for base CrosswalkError exception."""

    def test_str_returns_message(self):
        """String representation returns the message."""
        error = CrosswalkError(message="Test error message")
        assert str(error) == "Test error message"

    def test_is_exception_subclass(self):
        """CrosswalkError is an Exception subclass."""
        assert issubclass(CrosswalkError, Exception)


class TestSourceReadError:
    """Tests for SourceReadError exception."""

    def test_str_includes_source_url(self):
        """String representation includes source URL."""
        error = SourceReadError(
            message="Connection timeout",
            source_url="https://example.com/data.parquet",
        )
        result = str(error)
        assert "https://example.com/data.parquet" in result
        assert "Connection timeout" in result

    def test_str_format(self):
        """String has expected format."""
        error = SourceReadError(
            message="Error details",
            source_url="https://test.com/file.parquet",
        )
        result = str(error)
        assert result.startswith("Failed to read source:")

    def test_is_crosswalk_error_subclass(self):
        """SourceReadError is a CrosswalkError subclass."""
        assert issubclass(SourceReadError, CrosswalkError)


class TestInvalidSourceURLError:
    """Tests for InvalidSourceURLError exception."""

    def test_str_includes_url_and_domains(self):
        """String representation includes URL and allowed domains."""
        error = InvalidSourceURLError(
            message="URL not allowed",
            source_url="https://evil.com/data.parquet",
            allowed_domains=["huggingface.co", "github.com"],
        )
        result = str(error)
        assert "https://evil.com/data.parquet" in result
        assert "huggingface.co" in result
        assert "github.com" in result

    def test_str_format(self):
        """String has expected format."""
        error = InvalidSourceURLError(
            message="Invalid",
            source_url="https://test.com",
            allowed_domains=["allowed.com"],
        )
        result = str(error)
        assert "Invalid source URL:" in result
        assert "Allowed domains:" in result

    def test_is_crosswalk_error_subclass(self):
        """InvalidSourceURLError is a CrosswalkError subclass."""
        assert issubclass(InvalidSourceURLError, CrosswalkError)


class TestOutputWriteError:
    """Tests for OutputWriteError exception."""

    def test_str_includes_output_path(self):
        """String representation includes output path."""
        error = OutputWriteError(
            message="Permission denied",
            output_path=Path("/tmp/output.parquet"),
        )
        result = str(error)
        assert "/tmp/output.parquet" in result
        assert "Permission denied" in result

    def test_str_format(self):
        """String has expected format."""
        error = OutputWriteError(
            message="Error",
            output_path=Path("/test/path.parquet"),
        )
        result = str(error)
        assert result.startswith("Failed to write output:")

    def test_is_crosswalk_error_subclass(self):
        """OutputWriteError is a CrosswalkError subclass."""
        assert issubclass(OutputWriteError, CrosswalkError)


class TestValidationError:
    """Tests for ValidationError exception."""

    def test_str_includes_counts(self):
        """String representation includes expected and actual counts."""
        error = ValidationError(
            message="Row count mismatch",
            expected_count=1000,
            actual_count=500,
        )
        result = str(error)
        assert "1,000" in result  # Formatted with commas
        assert "500" in result

    def test_str_format(self):
        """String has expected format."""
        error = ValidationError(
            message="Test failure",
            expected_count=100,
            actual_count=50,
        )
        result = str(error)
        assert "Validation failed:" in result
        assert "Expected:" in result
        assert "Actual:" in result

    def test_large_numbers_formatted(self):
        """Large numbers are formatted with thousands separators."""
        error = ValidationError(
            message="Big mismatch",
            expected_count=1234567,
            actual_count=7654321,
        )
        result = str(error)
        assert "1,234,567" in result
        assert "7,654,321" in result

    def test_is_crosswalk_error_subclass(self):
        """ValidationError is a CrosswalkError subclass."""
        assert issubclass(ValidationError, CrosswalkError)


class TestDuplicateKeyError:
    """Tests for DuplicateKeyError exception."""

    def test_str_includes_duplicate_count(self):
        """String representation includes duplicate count."""
        error = DuplicateKeyError(
            message="Duplicates found",
            duplicate_count=15,
        )
        result = str(error)
        assert "15" in result

    def test_str_includes_sample_duplicates(self):
        """String representation includes sample duplicates when provided."""
        error = DuplicateKeyError(
            message="Duplicates found",
            duplicate_count=3,
            sample_duplicates=[
                ("12345", "RID001"),
                ("67890", "RID002"),
                ("11111", "RID003"),
            ],
        )
        result = str(error)
        assert "12345" in result
        assert "RID001" in result
        assert "Examples:" in result

    def test_str_without_samples(self):
        """String representation works without sample duplicates."""
        error = DuplicateKeyError(
            message="Duplicates found",
            duplicate_count=10,
            sample_duplicates=None,
        )
        result = str(error)
        assert "10" in result
        assert "Examples:" not in result

    def test_samples_limited_to_five(self):
        """Only first 5 samples are shown."""
        samples = [(f"ICPSR{i}", f"RID{i}") for i in range(10)]
        error = DuplicateKeyError(
            message="Many duplicates",
            duplicate_count=10,
            sample_duplicates=samples,
        )
        result = str(error)
        # First 5 should be present
        assert "ICPSR0" in result
        assert "ICPSR4" in result
        # 6th and beyond should not
        assert "ICPSR5" not in result
        assert "ICPSR9" not in result

    def test_is_crosswalk_error_subclass(self):
        """DuplicateKeyError is a CrosswalkError subclass."""
        assert issubclass(DuplicateKeyError, CrosswalkError)


class TestExceptionHierarchy:
    """Tests for exception hierarchy and catching."""

    def test_all_exceptions_catchable_as_crosswalk_error(self):
        """All custom exceptions can be caught as CrosswalkError."""
        exceptions = [
            CrosswalkError(message="base"),
            SourceReadError(message="read", source_url="url"),
            InvalidSourceURLError(message="invalid", source_url="url", allowed_domains=[]),
            OutputWriteError(message="write", output_path=Path()),
            ValidationError(message="validate", expected_count=0, actual_count=0),
            DuplicateKeyError(message="dup", duplicate_count=0),
        ]

        for exc in exceptions:
            try:
                raise exc
            except CrosswalkError as caught:
                assert caught is exc
            except Exception:
                pytest.fail(f"{type(exc).__name__} not caught as CrosswalkError")
