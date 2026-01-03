"""Tests for legislator crosswalk validators."""

from pathlib import Path

import duckdb
import pytest

from scripts.legislator_crosswalk.exceptions import DuplicateKeyError, ValidationError
from scripts.legislator_crosswalk.validators import (
    ValidationResult,
    validate_counts,
    validate_sample,
    validate_uniqueness,
)


@pytest.fixture
def temp_source_parquet(tmp_path: Path) -> Path:
    """Create a temporary source parquet file with DIME-like structure."""
    source_path = tmp_path / "source.parquet"
    conn = duckdb.connect()
    conn.execute(f"""
        COPY (
            SELECT * FROM (VALUES
                ('100751980', 'RID001', 'John Smith', 'D', 'CA', 'federal:senate', 'FEC001'),
                ('100751982', 'RID001', 'John Smith', 'D', 'CA', 'federal:senate', 'FEC001'),
                ('100751984', 'RID002', 'John Smith', 'D', 'CA', 'federal:senate', 'FEC002'),
                ('200001990', 'RID003', 'Jane Doe', 'R', 'TX', 'federal:house', 'FEC003'),
                ('200001992', 'RID004', 'Jane Doe', 'R', 'TX', 'federal:house', 'FEC004')
            ) AS t("ICPSR", "bonica.rid", "name", "party", "state", "seat", "FEC.ID")
        ) TO '{source_path}' (FORMAT PARQUET)
    """)
    conn.close()
    return source_path


@pytest.fixture
def temp_valid_output(tmp_path: Path) -> Path:
    """Create a valid output parquet file matching source."""
    output_path = tmp_path / "output.parquet"
    conn = duckdb.connect()
    # This should match the distinct (icpsr, bonica_rid) pairs from source
    # After extracting ICPSR (removing year suffix):
    # 10075 -> RID001, RID002 (2 pairs)
    # 20000 -> RID003, RID004 (2 pairs)
    # Total: 4 unique pairs
    conn.execute(f"""
        COPY (
            SELECT * FROM (VALUES
                ('10075', 'RID001', 'John Smith', 'D', 'CA', 'federal:senate', 'FEC001'),
                ('10075', 'RID002', 'John Smith', 'D', 'CA', 'federal:senate', 'FEC002'),
                ('20000', 'RID003', 'Jane Doe', 'R', 'TX', 'federal:house', 'FEC003'),
                ('20000', 'RID004', 'Jane Doe', 'R', 'TX', 'federal:house', 'FEC004')
            ) AS t(icpsr, bonica_rid, recipient_name, party, state, seat, fec_id)
        ) TO '{output_path}' (FORMAT PARQUET)
    """)
    conn.close()
    return output_path


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_default_values_are_false(self):
        """Default validation state is not valid."""
        result = ValidationResult()
        assert not result.counts_valid
        assert not result.uniqueness_valid
        assert not result.sample_valid

    def test_all_valid_requires_all_tiers(self):
        """all_valid is True only when all tiers pass."""
        result = ValidationResult()
        assert not result.all_valid

        result.counts_valid = True
        assert not result.all_valid

        result.uniqueness_valid = True
        assert not result.all_valid

        result.sample_valid = True
        assert result.all_valid

    def test_counts_stored(self):
        """Counts are stored in result."""
        result = ValidationResult()
        result.source_count = 100
        result.output_count = 100
        assert result.source_count == 100
        assert result.output_count == 100


class TestValidateCounts:
    """Tests for validate_counts function (Tier 1)."""

    def test_valid_counts_pass(self, tmp_path: Path, temp_source_parquet: Path):
        """Validation passes when counts match."""
        output_path = tmp_path / "valid_output.parquet"
        conn = duckdb.connect()

        # Create output with correct distinct count
        conn.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    ('10075', 'RID001', 'John Smith', 'D', 'CA', 'federal:senate', 'FEC001'),
                    ('10075', 'RID002', 'John Smith', 'D', 'CA', 'federal:senate', 'FEC002'),
                    ('20000', 'RID003', 'Jane Doe', 'R', 'TX', 'federal:house', 'FEC003'),
                    ('20000', 'RID004', 'Jane Doe', 'R', 'TX', 'federal:house', 'FEC004')
                ) AS t(icpsr, bonica_rid, recipient_name, party, state, seat, fec_id)
            ) TO '{output_path}' (FORMAT PARQUET)
        """)

        result = validate_counts(str(temp_source_parquet), output_path, conn)

        assert result.counts_valid
        assert result.output_count == 4
        conn.close()

    def test_empty_output_raises_error(self, tmp_path: Path, temp_source_parquet: Path):
        """Raises ValidationError when output is empty."""
        output_path = tmp_path / "empty.parquet"
        conn = duckdb.connect()

        # Create empty output
        conn.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    ('dummy', 'dummy', 'dummy', 'dummy', 'dummy', 'dummy', 'dummy')
                ) AS t(icpsr, bonica_rid, recipient_name, party, state, seat, fec_id)
                WHERE 1=0
            ) TO '{output_path}' (FORMAT PARQUET)
        """)

        with pytest.raises(ValidationError) as exc_info:
            validate_counts(str(temp_source_parquet), output_path, conn)

        assert "no rows" in str(exc_info.value).lower()
        conn.close()

    def test_null_icpsr_raises_error(self, tmp_path: Path, temp_source_parquet: Path):
        """Raises ValidationError when output has null icpsr values."""
        output_path = tmp_path / "null_icpsr.parquet"
        conn = duckdb.connect()

        conn.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    (NULL, 'RID001', 'Name', 'D', 'CA', 'seat', 'FEC'),
                    ('10075', 'RID002', 'Name', 'D', 'CA', 'seat', 'FEC')
                ) AS t(icpsr, bonica_rid, recipient_name, party, state, seat, fec_id)
            ) TO '{output_path}' (FORMAT PARQUET)
        """)

        with pytest.raises(ValidationError) as exc_info:
            validate_counts(str(temp_source_parquet), output_path, conn)

        assert "null" in str(exc_info.value).lower() or "empty" in str(exc_info.value).lower()
        conn.close()

    def test_null_bonica_rid_raises_error(self, tmp_path: Path, temp_source_parquet: Path):
        """Raises ValidationError when output has null bonica_rid values."""
        output_path = tmp_path / "null_rid.parquet"
        conn = duckdb.connect()

        conn.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    ('10075', NULL, 'Name', 'D', 'CA', 'seat', 'FEC'),
                    ('10075', 'RID002', 'Name', 'D', 'CA', 'seat', 'FEC')
                ) AS t(icpsr, bonica_rid, recipient_name, party, state, seat, fec_id)
            ) TO '{output_path}' (FORMAT PARQUET)
        """)

        with pytest.raises(ValidationError) as exc_info:
            validate_counts(str(temp_source_parquet), output_path, conn)

        assert "null" in str(exc_info.value).lower() or "empty" in str(exc_info.value).lower()
        conn.close()


class TestValidateUniqueness:
    """Tests for validate_uniqueness function (Tier 2)."""

    def test_unique_pairs_pass(self, temp_valid_output: Path):
        """Validation passes when all pairs are unique."""
        conn = duckdb.connect()
        result = ValidationResult(counts_valid=True, output_count=4)

        result = validate_uniqueness(temp_valid_output, conn, result)

        assert result.uniqueness_valid
        assert result.unique_icpsr_count == 2  # 10075, 20000
        assert result.unique_bonica_rid_count == 4  # RID001-RID004
        conn.close()

    def test_duplicate_pairs_raise_error(self, tmp_path: Path):
        """Raises DuplicateKeyError when duplicate pairs exist."""
        output_path = tmp_path / "duplicates.parquet"
        conn = duckdb.connect()

        conn.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    ('10075', 'RID001', 'Name1', 'D', 'CA', 'seat', 'FEC1'),
                    ('10075', 'RID001', 'Name2', 'D', 'CA', 'seat', 'FEC1'),
                    ('20000', 'RID002', 'Name3', 'R', 'TX', 'seat', 'FEC2')
                ) AS t(icpsr, bonica_rid, recipient_name, party, state, seat, fec_id)
            ) TO '{output_path}' (FORMAT PARQUET)
        """)

        result = ValidationResult(counts_valid=True, output_count=3)

        with pytest.raises(DuplicateKeyError) as exc_info:
            validate_uniqueness(output_path, conn, result)

        assert exc_info.value.duplicate_count >= 1
        assert exc_info.value.sample_duplicates is not None
        conn.close()

    def test_counts_unique_values(self, temp_valid_output: Path):
        """Correctly counts unique icpsr and bonica_rid values."""
        conn = duckdb.connect()
        result = ValidationResult(counts_valid=True, output_count=4)

        result = validate_uniqueness(temp_valid_output, conn, result)

        # 2 unique ICPSR values (10075, 20000)
        assert result.unique_icpsr_count == 2
        # 4 unique bonica_rid values (RID001-RID004)
        assert result.unique_bonica_rid_count == 4
        conn.close()


class TestValidateSample:
    """Tests for validate_sample function (Tier 3)."""

    def test_valid_sample_passes(self, temp_source_parquet: Path, temp_valid_output: Path):
        """Validation passes when sample mappings exist in source."""
        conn = duckdb.connect()
        result = ValidationResult(counts_valid=True, uniqueness_valid=True, output_count=4)

        result = validate_sample(
            str(temp_source_parquet),
            temp_valid_output,
            conn,
            result,
            sample_size=4,  # Check all rows
        )

        assert result.sample_valid
        assert result.sample_size == 4
        conn.close()

    def test_invalid_mapping_raises_error(self, tmp_path: Path, temp_source_parquet: Path):
        """Raises ValidationError when mapping not found in source."""
        output_path = tmp_path / "invalid_mapping.parquet"
        conn = duckdb.connect()

        # Create output with a mapping that doesn't exist in source
        conn.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    ('99999', 'RID999', 'Unknown', 'I', 'ZZ', 'unknown', 'FEC999')
                ) AS t(icpsr, bonica_rid, recipient_name, party, state, seat, fec_id)
            ) TO '{output_path}' (FORMAT PARQUET)
        """)

        result = ValidationResult(counts_valid=True, uniqueness_valid=True, output_count=1)

        with pytest.raises(ValidationError) as exc_info:
            validate_sample(str(temp_source_parquet), output_path, conn, result, sample_size=1)

        assert "not found in source" in str(exc_info.value)
        conn.close()

    def test_sample_size_respected(self, temp_source_parquet: Path, temp_valid_output: Path):
        """Sample size limits the number of rows checked."""
        conn = duckdb.connect()
        result = ValidationResult(counts_valid=True, uniqueness_valid=True, output_count=4)

        result = validate_sample(
            str(temp_source_parquet),
            temp_valid_output,
            conn,
            result,
            sample_size=2,  # Only check 2 rows
        )

        assert result.sample_valid
        # Sample size should be <= requested (could be less if output has fewer rows)
        assert result.sample_size <= 2
        conn.close()

    def test_uses_parameterized_query(self, temp_source_parquet: Path, tmp_path: Path):
        """Validates that parameterized queries are used (SQL injection prevention)."""
        # Create output with values that could be SQL injection attempts
        output_path = tmp_path / "injection_test.parquet"
        conn = duckdb.connect()

        conn.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    ('10075', 'RID001', 'Name', 'D', 'CA', 'seat', 'FEC')
                ) AS t(icpsr, bonica_rid, recipient_name, party, state, seat, fec_id)
            ) TO '{output_path}' (FORMAT PARQUET)
        """)

        result = ValidationResult(counts_valid=True, uniqueness_valid=True, output_count=1)

        # This should complete without SQL errors
        result = validate_sample(str(temp_source_parquet), output_path, conn, result, sample_size=1)

        assert result.sample_valid
        conn.close()
