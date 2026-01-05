"""Tests for exceptions module."""

from __future__ import annotations

from pathlib import Path

from contribution_filters.exceptions import (
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


class TestContributionFilterError:
    """Tests for base ContributionFilterError."""

    def test_str_representation(self) -> None:
        """Base error should return message."""
        error = ContributionFilterError(message="Test error")
        assert str(error) == "Test error"


class TestSourceReadError:
    """Tests for SourceReadError."""

    def test_str_representation(self) -> None:
        """Should include source URL and message."""
        error = SourceReadError(
            message="Connection failed",
            source_url="https://example.com/file.parquet",
        )
        result = str(error)
        assert "https://example.com/file.parquet" in result
        assert "Connection failed" in result


class TestOutputWriteError:
    """Tests for OutputWriteError."""

    def test_str_representation(self) -> None:
        """Should include output path and message."""
        error = OutputWriteError(
            message="Permission denied",
            output_path=Path("/tmp/output.parquet"),
        )
        result = str(error)
        assert "/tmp/output.parquet" in result
        assert "Permission denied" in result


class TestInvalidSourceURLError:
    """Tests for InvalidSourceURLError."""

    def test_str_representation(self) -> None:
        """Should include URL and allowed domains."""
        error = InvalidSourceURLError(
            message="Invalid domain",
            source_url="https://evil.com/file.parquet",
            allowed_domains=["huggingface.co"],
        )
        result = str(error)
        assert "evil.com" in result
        assert "huggingface.co" in result


class TestInvalidCycleError:
    """Tests for InvalidCycleError."""

    def test_str_representation(self) -> None:
        """Should include cycle and valid range."""
        error = InvalidCycleError(
            message="Invalid cycle",
            cycle=2025,
            min_cycle=1980,
            max_cycle=2024,
        )
        result = str(error)
        assert "2025" in result
        assert "1980" in result
        assert "2024" in result


class TestFilterValidationError:
    """Tests for FilterValidationError."""

    def test_str_representation(self) -> None:
        """Should include field, condition, and violation count."""
        error = FilterValidationError(
            message="Found individual contributors",
            field_name="contributor.type",
            expected_condition="!= 'I'",
            violation_count=100,
        )
        result = str(error)
        assert "contributor.type" in result
        assert "!= 'I'" in result
        assert "100" in result


class TestAggregationIntegrityError:
    """Tests for AggregationIntegrityError."""

    def test_str_representation(self) -> None:
        """Should include recipient ID, field, expected, and actual."""
        error = AggregationIntegrityError(
            message="Count mismatch",
            recipient_id="rid_123",
            field_name="contribution_count",
            expected_value="100",
            actual_value="95",
        )
        result = str(error)
        assert "rid_123" in result
        assert "contribution_count" in result
        assert "100" in result
        assert "95" in result


class TestCompletenessError:
    """Tests for CompletenessError."""

    def test_str_representation(self) -> None:
        """Should include expected and actual counts."""
        error = CompletenessError(
            message="Missing recipients",
            expected_count=1000,
            actual_count=950,
        )
        result = str(error)
        assert "1,000" in result
        assert "950" in result


class TestBioguideJoinError:
    """Tests for BioguideJoinError."""

    def test_str_representation(self) -> None:
        """Should include invalid bioguide_ids and total count."""
        error = BioguideJoinError(
            message="Invalid bioguide_ids found",
            invalid_bioguide_ids=["A000001", "B000002", "C000003"],
            total_invalid=3,
        )
        result = str(error)
        assert "Invalid bioguide_ids found" in result
        assert "A000001" in result
        assert "3" in result

    def test_str_representation_truncates_list(self) -> None:
        """Should truncate list when more than 5 invalid IDs."""
        error = BioguideJoinError(
            message="Many invalid bioguide_ids",
            invalid_bioguide_ids=[
                "A000001",
                "B000002",
                "C000003",
                "D000004",
                "E000005",
                "F000006",
            ],
            total_invalid=10,
        )
        result = str(error)
        assert "A000001" in result
        assert "E000005" in result
        # F000006 should not appear (truncated)
        assert "F000006" not in result
        assert "... and 5 more" in result
