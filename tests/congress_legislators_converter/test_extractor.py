"""Tests for unified legislators extractor."""

from pathlib import Path

import duckdb
import pytest

from scripts.congress_legislators_converter.exceptions import SourceNotFoundError
from scripts.congress_legislators_converter.extractor import (
    UnifiedExtractionResult,
    extract_unified_legislators,
)


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
                ('A000001', 'Adams', 'John', 'John Q. Adams', '1950-01-01', 'M',
                 'rep', 'MA', 'D', '12345', 'H8MA01011', 'N00001'),
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


class TestUnifiedExtractionResult:
    """Tests for UnifiedExtractionResult dataclass."""

    def test_stores_all_fields(self, tmp_path: Path):
        """UnifiedExtractionResult stores all extraction details."""
        result = UnifiedExtractionResult(
            current_path=tmp_path / "current.parquet",
            historical_path=tmp_path / "historical.parquet",
            output_path=tmp_path / "output.parquet",
            current_count=100,
            historical_count=1000,
            output_count=1050,
            unique_bioguide_count=1050,
            fec_ids_populated_count=500,
            icpsr_populated_count=900,
        )

        assert result.current_count == 100
        assert result.historical_count == 1000
        assert result.output_count == 1050
        assert result.unique_bioguide_count == 1050
        assert result.fec_ids_populated_count == 500
        assert result.icpsr_populated_count == 900


class TestExtractUnifiedLegislators:
    """Tests for extract_unified_legislators function."""

    def test_raises_error_when_current_missing(self, tmp_path: Path):
        """Raises SourceNotFoundError when current parquet is missing."""
        output_path = tmp_path / "legislators.parquet"
        current_path = tmp_path / "legislators-current.parquet"
        historical_path = tmp_path / "legislators-historical.parquet"

        # Create only historical
        conn = duckdb.connect()
        conn.execute(f"""
            COPY (SELECT 'A' as bioguide_id)
            TO '{historical_path}' (FORMAT PARQUET)
        """)
        conn.close()

        with pytest.raises(SourceNotFoundError) as exc_info:
            extract_unified_legislators(
                current_path=current_path,
                historical_path=historical_path,
                output_path=output_path,
            )

        assert "legislators-current.parquet" in str(exc_info.value)

    def test_raises_error_when_historical_missing(self, tmp_path: Path):
        """Raises SourceNotFoundError when historical parquet is missing."""
        output_path = tmp_path / "legislators.parquet"
        current_path = tmp_path / "legislators-current.parquet"
        historical_path = tmp_path / "legislators-historical.parquet"

        # Create only current
        conn = duckdb.connect()
        conn.execute(f"""
            COPY (SELECT 'A' as bioguide_id)
            TO '{current_path}' (FORMAT PARQUET)
        """)
        conn.close()

        with pytest.raises(SourceNotFoundError) as exc_info:
            extract_unified_legislators(
                current_path=current_path,
                historical_path=historical_path,
                output_path=output_path,
            )

        assert "legislators-historical.parquet" in str(exc_info.value)

    def test_extracts_unified_legislators(
        self, tmp_path: Path, mock_current_parquet: Path, mock_historical_parquet: Path
    ):
        """Successfully extracts unified legislators."""
        output_path = tmp_path / "legislators.parquet"

        result = extract_unified_legislators(
            current_path=mock_current_parquet,
            historical_path=mock_historical_parquet,
            output_path=output_path,
            validate=False,
            min_congress=None,  # Disable congress filtering for mock data
        )

        assert output_path.exists()
        assert result.current_count == 3
        assert result.historical_count == 3
        # A000001 appears in both, should be deduplicated (current takes precedence)
        # Total unique: A000001, B000002, C000003, D000004, E000005 = 5
        assert result.output_count == 5
        assert result.unique_bioguide_count == 5

    def test_current_takes_precedence_over_historical(
        self, tmp_path: Path, mock_current_parquet: Path, mock_historical_parquet: Path
    ):
        """Current legislators take precedence when bioguide_id appears in both."""
        output_path = tmp_path / "legislators.parquet"

        extract_unified_legislators(
            current_path=mock_current_parquet,
            historical_path=mock_historical_parquet,
            output_path=output_path,
            validate=False,
            min_congress=None,  # Disable congress filtering for mock data
        )

        conn = duckdb.connect()
        # A000001 is in both; should use current (full_name='John Adams', type='sen')
        row = conn.execute(f"""
            SELECT full_name, type, is_current
            FROM '{output_path}'
            WHERE bioguide_id = 'A000001'
        """).fetchone()
        conn.close()

        assert row[0] == "John Adams"  # Current's full_name
        assert row[1] == "sen"  # Current's type
        assert row[2] is True  # is_current flag

    def test_parses_fec_ids_to_array(
        self, tmp_path: Path, mock_current_parquet: Path, mock_historical_parquet: Path
    ):
        """FEC IDs are parsed from comma-separated string to array."""
        output_path = tmp_path / "legislators.parquet"

        extract_unified_legislators(
            current_path=mock_current_parquet,
            historical_path=mock_historical_parquet,
            output_path=output_path,
            validate=False,
            min_congress=None,  # Disable congress filtering for mock data
        )

        conn = duckdb.connect()
        # A000001 has two FEC IDs in current
        fec_ids = conn.execute(f"""
            SELECT fec_ids
            FROM '{output_path}'
            WHERE bioguide_id = 'A000001'
        """).fetchone()[0]
        conn.close()

        assert isinstance(fec_ids, list)
        assert len(fec_ids) == 2
        assert "H8MA01011" in fec_ids
        assert "S8MA00012" in fec_ids

    def test_handles_empty_fec_ids(
        self, tmp_path: Path, mock_current_parquet: Path, mock_historical_parquet: Path
    ):
        """Empty FEC IDs result in empty array."""
        output_path = tmp_path / "legislators.parquet"

        extract_unified_legislators(
            current_path=mock_current_parquet,
            historical_path=mock_historical_parquet,
            output_path=output_path,
            validate=False,
            min_congress=None,  # Disable congress filtering for mock data
        )

        conn = duckdb.connect()
        # C000003 has empty fec_ids
        fec_ids = conn.execute(f"""
            SELECT fec_ids
            FROM '{output_path}'
            WHERE bioguide_id = 'C000003'
        """).fetchone()[0]
        conn.close()

        assert isinstance(fec_ids, list)
        assert len(fec_ids) == 0

    def test_casts_icpsr_to_int64(
        self, tmp_path: Path, mock_current_parquet: Path, mock_historical_parquet: Path
    ):
        """ICPSR is cast from string to int64."""
        output_path = tmp_path / "legislators.parquet"

        extract_unified_legislators(
            current_path=mock_current_parquet,
            historical_path=mock_historical_parquet,
            output_path=output_path,
            validate=False,
            min_congress=None,  # Disable congress filtering for mock data
        )

        conn = duckdb.connect()
        icpsr = conn.execute(f"""
            SELECT icpsr
            FROM '{output_path}'
            WHERE bioguide_id = 'A000001'
        """).fetchone()[0]
        conn.close()

        assert icpsr == 12345
        assert isinstance(icpsr, int)

    def test_handles_null_icpsr(
        self, tmp_path: Path, mock_current_parquet: Path, mock_historical_parquet: Path
    ):
        """Null ICPSR values are preserved."""
        output_path = tmp_path / "legislators.parquet"

        extract_unified_legislators(
            current_path=mock_current_parquet,
            historical_path=mock_historical_parquet,
            output_path=output_path,
            validate=False,
            min_congress=None,  # Disable congress filtering for mock data
        )

        conn = duckdb.connect()
        # C000003 has null icpsr_id
        icpsr = conn.execute(f"""
            SELECT icpsr
            FROM '{output_path}'
            WHERE bioguide_id = 'C000003'
        """).fetchone()[0]
        conn.close()

        assert icpsr is None

    def test_sets_is_current_flag(
        self, tmp_path: Path, mock_current_parquet: Path, mock_historical_parquet: Path
    ):
        """is_current flag correctly identifies current legislators."""
        output_path = tmp_path / "legislators.parquet"

        extract_unified_legislators(
            current_path=mock_current_parquet,
            historical_path=mock_historical_parquet,
            output_path=output_path,
            validate=False,
            min_congress=None,  # Disable congress filtering for mock data
        )

        conn = duckdb.connect()
        results = conn.execute(f"""
            SELECT bioguide_id, is_current
            FROM '{output_path}'
            ORDER BY bioguide_id
        """).fetchall()
        conn.close()

        is_current_map = {row[0]: row[1] for row in results}
        # Current legislators
        assert is_current_map["A000001"] is True  # In both, uses current
        assert is_current_map["B000002"] is True
        assert is_current_map["C000003"] is True
        # Historical only
        assert is_current_map["D000004"] is False
        assert is_current_map["E000005"] is False

    def test_creates_output_directory(
        self, tmp_path: Path, mock_current_parquet: Path, mock_historical_parquet: Path
    ):
        """Creates output directory if it doesn't exist."""
        output_path = tmp_path / "nested" / "dir" / "legislators.parquet"

        extract_unified_legislators(
            current_path=mock_current_parquet,
            historical_path=mock_historical_parquet,
            output_path=output_path,
            validate=False,
            min_congress=None,  # Disable congress filtering for mock data
        )

        assert output_path.exists()
        assert output_path.parent.exists()

    def test_output_has_correct_schema(
        self, tmp_path: Path, mock_current_parquet: Path, mock_historical_parquet: Path
    ):
        """Output parquet has expected columns and types."""
        output_path = tmp_path / "legislators.parquet"

        extract_unified_legislators(
            current_path=mock_current_parquet,
            historical_path=mock_historical_parquet,
            output_path=output_path,
            validate=False,
            min_congress=None,  # Disable congress filtering for mock data
        )

        conn = duckdb.connect()
        schema = conn.execute(f"""
            DESCRIBE SELECT * FROM '{output_path}'
        """).fetchall()
        conn.close()

        schema_dict = {row[0]: row[1] for row in schema}

        assert schema_dict["bioguide_id"] == "VARCHAR"
        assert schema_dict["icpsr"] == "BIGINT"
        assert schema_dict["fec_ids"] == "VARCHAR[]"
        assert schema_dict["is_current"] == "BOOLEAN"

    def test_accepts_path_as_string(
        self, tmp_path: Path, mock_current_parquet: Path, mock_historical_parquet: Path
    ):
        """Accepts paths as strings."""
        output_path = str(tmp_path / "legislators.parquet")

        extract_unified_legislators(
            current_path=str(mock_current_parquet),
            historical_path=str(mock_historical_parquet),
            output_path=output_path,
            validate=False,
            min_congress=None,  # Disable congress filtering for mock data
        )

        assert Path(output_path).exists()
