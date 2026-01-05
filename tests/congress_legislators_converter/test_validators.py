"""Tests for unified legislators validation functions."""

from pathlib import Path

import duckdb
import pytest

from scripts.congress_legislators_converter.exceptions import UnifiedValidationError
from scripts.congress_legislators_converter.validators import (
    validate_unified_completeness,
    validate_unified_coverage,
    validate_unified_sample,
)


@pytest.fixture
def mock_unified_parquet(tmp_path: Path) -> Path:
    """Create a mock unified legislators parquet file."""
    unified_path = tmp_path / "legislators.parquet"
    conn = duckdb.connect()
    conn.execute(f"""
        COPY (
            SELECT * FROM (VALUES
                ('A000001', 'Adams', 'John', 'MA', 'D', 12345, ['H8MA01011', 'S8MA00012'], 'N00001', true),
                ('B000002', 'Brown', 'Sarah', 'CA', 'R', 23456, ['H4CA05022'], 'N00002', true),
                ('C000003', 'Clark', 'Michael', 'TX', 'R', NULL, [], 'N00003', true),
                ('D000004', 'Davis', 'Robert', 'NY', 'D', 34567, ['S2NY00033'], 'N00004', false),
                ('E000005', 'Edwards', 'Alice', 'OH', 'R', 45678, [], NULL, false)
            ) AS t(
                bioguide_id, last_name, first_name, state, party, icpsr, fec_ids, opensecrets_id, is_current
            )
        ) TO '{unified_path}' (FORMAT PARQUET)
    """)
    conn.close()
    return unified_path


@pytest.fixture
def mock_current_parquet(tmp_path: Path) -> Path:
    """Create a mock current legislators parquet file."""
    current_path = tmp_path / "legislators-current.parquet"
    conn = duckdb.connect()
    conn.execute(f"""
        COPY (
            SELECT * FROM (VALUES
                ('A000001', 'Adams', 'John', 'John Adams', '1950-01-01', 'M',
                 'sen', 'MA', 'D', '12345', 'H8MA01011,S8MA00012', 'N00001'),
                ('B000002', 'Brown', 'Sarah', 'Sarah Brown', '1965-03-15', 'F',
                 'rep', 'CA', 'R', '23456', 'H4CA05022', 'N00002'),
                ('C000003', 'Clark', 'Michael', 'Michael Clark', '1970-07-20', 'M',
                 'rep', 'TX', 'R', NULL, '', 'N00003')
            ) AS t(
                bioguide_id, last_name, first_name, full_name, birthday, gender,
                type, state, party, icpsr_id, fec_ids, opensecrets_id
            )
        ) TO '{current_path}' (FORMAT PARQUET)
    """)
    conn.close()
    return current_path


@pytest.fixture
def mock_historical_parquet(tmp_path: Path) -> Path:
    """Create a mock historical legislators parquet file."""
    historical_path = tmp_path / "legislators-historical.parquet"
    conn = duckdb.connect()
    conn.execute(f"""
        COPY (
            SELECT * FROM (VALUES
                ('D000004', 'Davis', 'Robert', 'Robert Davis', '1920-11-30', 'M',
                 'sen', 'NY', 'D', '34567', 'S2NY00033', 'N00004'),
                ('E000005', 'Edwards', 'Alice', 'Alice Edwards', '1930-05-25', 'F',
                 'rep', 'OH', 'R', '45678', '', NULL)
            ) AS t(
                bioguide_id, last_name, first_name, full_name, birthday, gender,
                type, state, party, icpsr_id, fec_ids, opensecrets_id
            )
        ) TO '{historical_path}' (FORMAT PARQUET)
    """)
    conn.close()
    return historical_path


class TestValidateUnifiedCompleteness:
    """Tests for validate_unified_completeness function."""

    def test_passes_for_valid_data(self, mock_unified_parquet: Path):
        """Passes validation for valid data within expected range."""
        with duckdb.connect() as conn:
            validate_unified_completeness(
                mock_unified_parquet, conn, min_expected=1, max_expected=100
            )

    def test_raises_for_null_bioguide_ids(self, tmp_path: Path):
        """Raises error when null bioguide_ids are present."""
        parquet_path = tmp_path / "test.parquet"
        conn = duckdb.connect()
        conn.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    ('A000001', 'Adams'),
                    (NULL, 'Unknown'),
                    ('', 'Empty')
                ) AS t(bioguide_id, last_name)
            ) TO '{parquet_path}' (FORMAT PARQUET)
        """)

        with pytest.raises(UnifiedValidationError) as exc_info:
            validate_unified_completeness(parquet_path, conn, min_expected=1, max_expected=100)

        assert "Null bioguide_ids found" in str(exc_info.value)
        conn.close()

    def test_raises_for_duplicate_bioguide_ids(self, tmp_path: Path):
        """Raises error when duplicate bioguide_ids are present."""
        parquet_path = tmp_path / "test.parquet"
        conn = duckdb.connect()
        conn.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    ('A000001', 'Adams'),
                    ('A000001', 'Adams Duplicate'),
                    ('B000002', 'Brown')
                ) AS t(bioguide_id, last_name)
            ) TO '{parquet_path}' (FORMAT PARQUET)
        """)

        with pytest.raises(UnifiedValidationError) as exc_info:
            validate_unified_completeness(parquet_path, conn, min_expected=1, max_expected=100)

        assert "Duplicate bioguide_ids found" in str(exc_info.value)
        conn.close()

    def test_raises_for_count_below_min(self, mock_unified_parquet: Path):
        """Raises error when count is below minimum."""
        with duckdb.connect() as conn:
            with pytest.raises(UnifiedValidationError) as exc_info:
                validate_unified_completeness(
                    mock_unified_parquet, conn, min_expected=100, max_expected=200
                )

            assert "Unexpected legislator count" in str(exc_info.value)

    def test_raises_for_count_above_max(self, mock_unified_parquet: Path):
        """Raises error when count is above maximum."""
        with duckdb.connect() as conn:
            with pytest.raises(UnifiedValidationError) as exc_info:
                validate_unified_completeness(
                    mock_unified_parquet, conn, min_expected=1, max_expected=2
                )

            assert "Unexpected legislator count" in str(exc_info.value)


class TestValidateUnifiedCoverage:
    """Tests for validate_unified_coverage function."""

    def test_passes_for_valid_coverage(self, mock_unified_parquet: Path):
        """Passes validation when FEC and ICPSR coverage is present."""
        with duckdb.connect() as conn:
            validate_unified_coverage(mock_unified_parquet, conn)

    def test_raises_for_empty_file(self, tmp_path: Path):
        """Raises error for empty parquet file."""
        parquet_path = tmp_path / "empty.parquet"
        conn = duckdb.connect()
        conn.execute(f"""
            COPY (
                SELECT 'dummy'::VARCHAR as bioguide_id WHERE false
            ) TO '{parquet_path}' (FORMAT PARQUET)
        """)

        with pytest.raises(UnifiedValidationError) as exc_info:
            validate_unified_coverage(parquet_path, conn)

        assert "Empty output file" in str(exc_info.value)
        conn.close()

    def test_raises_for_no_fec_ids(self, tmp_path: Path):
        """Raises error when no legislators have FEC IDs."""
        parquet_path = tmp_path / "no_fec.parquet"
        conn = duckdb.connect()
        conn.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    ('A000001', []::VARCHAR[], 12345),
                    ('B000002', []::VARCHAR[], 23456)
                ) AS t(bioguide_id, fec_ids, icpsr)
            ) TO '{parquet_path}' (FORMAT PARQUET)
        """)

        with pytest.raises(UnifiedValidationError) as exc_info:
            validate_unified_coverage(parquet_path, conn)

        assert "No FEC IDs populated" in str(exc_info.value)
        conn.close()

    def test_raises_for_no_icpsr(self, tmp_path: Path):
        """Raises error when no legislators have ICPSR."""
        parquet_path = tmp_path / "no_icpsr.parquet"
        conn = duckdb.connect()
        conn.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    ('A000001', ['H8MA01011']::VARCHAR[], NULL::BIGINT),
                    ('B000002', ['H4CA05022']::VARCHAR[], NULL::BIGINT)
                ) AS t(bioguide_id, fec_ids, icpsr)
            ) TO '{parquet_path}' (FORMAT PARQUET)
        """)

        with pytest.raises(UnifiedValidationError) as exc_info:
            validate_unified_coverage(parquet_path, conn)

        assert "No ICPSR values populated" in str(exc_info.value)
        conn.close()


class TestValidateUnifiedSample:
    """Tests for validate_unified_sample function."""

    def test_passes_for_matching_data(
        self,
        mock_unified_parquet: Path,
        mock_current_parquet: Path,
        mock_historical_parquet: Path,
    ):
        """Passes when sample rows match source data."""
        with duckdb.connect() as conn:
            validate_unified_sample(
                mock_current_parquet,
                mock_historical_parquet,
                mock_unified_parquet,
                conn,
                sample_size=5,
            )

    def test_raises_for_empty_output(
        self, tmp_path: Path, mock_current_parquet: Path, mock_historical_parquet: Path
    ):
        """Raises error when output is empty."""
        empty_path = tmp_path / "empty.parquet"
        conn = duckdb.connect()
        conn.execute(f"""
            COPY (
                SELECT 'dummy'::VARCHAR as bioguide_id,
                       'dummy'::VARCHAR as last_name,
                       'dummy'::VARCHAR as first_name,
                       true as is_current
                WHERE false
            ) TO '{empty_path}' (FORMAT PARQUET)
        """)

        with pytest.raises(UnifiedValidationError) as exc_info:
            validate_unified_sample(
                mock_current_parquet,
                mock_historical_parquet,
                empty_path,
                conn,
                sample_size=5,
            )

        assert "No sample rows available" in str(exc_info.value)
        conn.close()

    def test_raises_for_missing_bioguide_in_source(
        self, tmp_path: Path, mock_current_parquet: Path, mock_historical_parquet: Path
    ):
        """Raises error when bioguide_id not found in source."""
        bad_path = tmp_path / "bad.parquet"
        conn = duckdb.connect()
        conn.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    ('X999999', 'Unknown', 'Person', true)
                ) AS t(bioguide_id, last_name, first_name, is_current)
            ) TO '{bad_path}' (FORMAT PARQUET)
        """)

        with pytest.raises(UnifiedValidationError) as exc_info:
            validate_unified_sample(
                mock_current_parquet,
                mock_historical_parquet,
                bad_path,
                conn,
                sample_size=5,
            )

        assert "Sample row not found in source" in str(exc_info.value)
        conn.close()

    def test_raises_for_last_name_mismatch(
        self, tmp_path: Path, mock_current_parquet: Path, mock_historical_parquet: Path
    ):
        """Raises error when last_name doesn't match source."""
        bad_path = tmp_path / "bad.parquet"
        conn = duckdb.connect()
        conn.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    ('A000001', 'WrongName', 'John', true)
                ) AS t(bioguide_id, last_name, first_name, is_current)
            ) TO '{bad_path}' (FORMAT PARQUET)
        """)

        with pytest.raises(UnifiedValidationError) as exc_info:
            validate_unified_sample(
                mock_current_parquet,
                mock_historical_parquet,
                bad_path,
                conn,
                sample_size=5,
            )

        assert "Sample mismatch: last_name" in str(exc_info.value)
        conn.close()

    def test_raises_for_first_name_mismatch(
        self, tmp_path: Path, mock_current_parquet: Path, mock_historical_parquet: Path
    ):
        """Raises error when first_name doesn't match source."""
        bad_path = tmp_path / "bad.parquet"
        conn = duckdb.connect()
        conn.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    ('A000001', 'Adams', 'WrongFirst', true)
                ) AS t(bioguide_id, last_name, first_name, is_current)
            ) TO '{bad_path}' (FORMAT PARQUET)
        """)

        with pytest.raises(UnifiedValidationError) as exc_info:
            validate_unified_sample(
                mock_current_parquet,
                mock_historical_parquet,
                bad_path,
                conn,
                sample_size=5,
            )

        assert "Sample mismatch: first_name" in str(exc_info.value)
        conn.close()


class TestValuesEqual:
    """Tests for _values_equal helper function."""

    def test_equal_strings(self):
        """Returns True for equal strings."""
        from scripts.congress_legislators_converter.validators import _values_equal

        assert _values_equal("hello", "hello") is True

    def test_unequal_strings(self):
        """Returns False for unequal strings."""
        from scripts.congress_legislators_converter.validators import _values_equal

        assert _values_equal("hello", "world") is False

    def test_nan_string_as_name(self):
        """Handles 'Nan' as a person's name (not float NaN)."""
        from scripts.congress_legislators_converter.validators import _values_equal

        # "Nan" is Nan Hayworth's actual first name - should compare as strings
        assert _values_equal("Nan", "Nan") is True
        assert _values_equal("Nan", "John") is False

    def test_numeric_strings(self):
        """Compares numeric strings with tolerance."""
        from scripts.congress_legislators_converter.validators import _values_equal

        assert _values_equal("123", "123") is True
        assert _values_equal("123.0", "123") is True
        assert _values_equal("123", "456") is False

    def test_none_values(self):
        """Handles None values correctly."""
        from scripts.congress_legislators_converter.validators import _values_equal

        assert _values_equal(None, None) is True
        assert _values_equal(None, "value") is False
        assert _values_equal("value", None) is False
