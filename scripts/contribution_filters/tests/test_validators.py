"""Tests for validators module."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from contribution_filters.exceptions import BioguideJoinError
from contribution_filters.validators import ValidationResult, validate_bioguide_join


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


class TestValidateBioguideJoin:
    """Tests for validate_bioguide_join function."""

    def test_valid_bioguide_ids(self, tmp_path: Path) -> None:
        """Validation should pass when all bioguide_ids exist in legislators."""
        conn = duckdb.connect()

        # Create legislators parquet with some bioguide_ids
        legislators_path = tmp_path / "legislators.parquet"
        conn.execute(f"""
            COPY (
                SELECT 'A000001' as bioguide_id UNION ALL
                SELECT 'B000002' UNION ALL
                SELECT 'C000003'
            ) TO '{legislators_path}' (FORMAT PARQUET)
        """)

        # Create output parquet with matching bioguide_ids
        output_path = tmp_path / "output.parquet"
        conn.execute(f"""
            COPY (
                SELECT 'A000001' as bioguide_id, 100.0 as amount UNION ALL
                SELECT 'B000002', 200.0 UNION ALL
                SELECT NULL, 300.0  -- NULL should be ignored
            ) TO '{output_path}' (FORMAT PARQUET)
        """)

        result = validate_bioguide_join(output_path, legislators_path, conn)

        assert result.bioguide_join_valid is True
        assert result.bioguide_matched_count == 2
        assert result.output_count == 3
        assert result.bioguide_coverage_pct == pytest.approx(66.67, rel=0.01)
        conn.close()

    def test_invalid_bioguide_ids_raises_error(self, tmp_path: Path) -> None:
        """Validation should fail when bioguide_ids don't exist in legislators."""
        conn = duckdb.connect()

        # Create legislators parquet with limited bioguide_ids
        legislators_path = tmp_path / "legislators.parquet"
        conn.execute(f"""
            COPY (
                SELECT 'A000001' as bioguide_id
            ) TO '{legislators_path}' (FORMAT PARQUET)
        """)

        # Create output parquet with bioguide_ids NOT in legislators
        output_path = tmp_path / "output.parquet"
        conn.execute(f"""
            COPY (
                SELECT 'A000001' as bioguide_id, 100.0 as amount UNION ALL
                SELECT 'X000099', 200.0 UNION ALL
                SELECT 'Y000098', 300.0
            ) TO '{output_path}' (FORMAT PARQUET)
        """)

        with pytest.raises(BioguideJoinError) as exc_info:
            validate_bioguide_join(output_path, legislators_path, conn)

        assert exc_info.value.total_invalid == 2
        assert "X000099" in exc_info.value.invalid_bioguide_ids
        assert "Y000098" in exc_info.value.invalid_bioguide_ids
        conn.close()

    def test_all_null_bioguide_ids(self, tmp_path: Path) -> None:
        """Validation should pass when all bioguide_ids are NULL."""
        conn = duckdb.connect()

        # Create legislators parquet
        legislators_path = tmp_path / "legislators.parquet"
        conn.execute(f"""
            COPY (
                SELECT 'A000001' as bioguide_id
            ) TO '{legislators_path}' (FORMAT PARQUET)
        """)

        # Create output parquet with all NULL bioguide_ids
        output_path = tmp_path / "output.parquet"
        conn.execute(f"""
            COPY (
                SELECT NULL::VARCHAR as bioguide_id, 100.0 as amount UNION ALL
                SELECT NULL, 200.0
            ) TO '{output_path}' (FORMAT PARQUET)
        """)

        result = validate_bioguide_join(output_path, legislators_path, conn)

        assert result.bioguide_join_valid is True
        assert result.bioguide_matched_count == 0
        assert result.output_count == 2
        assert result.bioguide_coverage_pct == 0.0
        conn.close()

    def test_coverage_calculation(self, tmp_path: Path) -> None:
        """Coverage percentage should be calculated correctly."""
        conn = duckdb.connect()

        # Create legislators parquet
        legislators_path = tmp_path / "legislators.parquet"
        conn.execute(f"""
            COPY (
                SELECT 'A000001' as bioguide_id UNION ALL
                SELECT 'B000002'
            ) TO '{legislators_path}' (FORMAT PARQUET)
        """)

        # Create output with 1 match out of 10 rows
        output_path = tmp_path / "output.parquet"
        conn.execute(f"""
            COPY (
                SELECT 'A000001' as bioguide_id, 100.0 as amount UNION ALL
                SELECT NULL, 100.0 UNION ALL
                SELECT NULL, 100.0 UNION ALL
                SELECT NULL, 100.0 UNION ALL
                SELECT NULL, 100.0 UNION ALL
                SELECT NULL, 100.0 UNION ALL
                SELECT NULL, 100.0 UNION ALL
                SELECT NULL, 100.0 UNION ALL
                SELECT NULL, 100.0 UNION ALL
                SELECT NULL, 100.0
            ) TO '{output_path}' (FORMAT PARQUET)
        """)

        result = validate_bioguide_join(output_path, legislators_path, conn)

        assert result.bioguide_join_valid is True
        assert result.bioguide_matched_count == 1
        assert result.output_count == 10
        assert result.bioguide_coverage_pct == pytest.approx(10.0, rel=0.01)
        conn.close()
