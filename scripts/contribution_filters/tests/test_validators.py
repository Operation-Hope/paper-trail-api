"""Tests for validators module."""

from __future__ import annotations

from contribution_filters.validators import ValidationResult


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_default_values(self) -> None:
        """Default values should be False/0."""
        result = ValidationResult()
        assert result.row_count_valid is False
        assert result.source_rows == 0
        assert result.output_count == 0
        assert result.filter_valid is False
        assert result.filter_checks_passed == 0
        assert result.aggregation_valid is False
        assert result.aggregation_sample_size == 0

    def test_all_valid_for_filter(self) -> None:
        """all_valid should be True when row_count and filter are valid."""
        result = ValidationResult(
            row_count_valid=True,
            filter_valid=True,
        )
        assert result.all_valid is True

    def test_all_valid_for_aggregation(self) -> None:
        """all_valid should be True when row_count and aggregation are valid."""
        result = ValidationResult(
            row_count_valid=True,
            aggregation_valid=True,
        )
        assert result.all_valid is True

    def test_all_valid_false_when_incomplete(self) -> None:
        """all_valid should be False when validation incomplete."""
        # Only row_count valid
        result = ValidationResult(row_count_valid=True)
        assert result.all_valid is False

        # Only filter valid
        result = ValidationResult(filter_valid=True)
        assert result.all_valid is False

        # Neither valid
        result = ValidationResult()
        assert result.all_valid is False

    def test_bioguide_join_fields_default_values(self) -> None:
        """Bioguide join fields should have proper defaults."""
        result = ValidationResult()
        assert result.bioguide_join_valid is False
        assert result.bioguide_matched_count == 0
        assert result.bioguide_coverage_pct == 0.0

    def test_bioguide_join_fields_populated(self) -> None:
        """Bioguide join fields should be settable."""
        result = ValidationResult(
            row_count_valid=True,
            filter_valid=True,
            bioguide_join_valid=True,
            bioguide_matched_count=500,
            bioguide_coverage_pct=10.5,
        )
        assert result.bioguide_join_valid is True
        assert result.bioguide_matched_count == 500
        assert result.bioguide_coverage_pct == 10.5
        assert result.all_valid is True  # filter + row_count still determines this
