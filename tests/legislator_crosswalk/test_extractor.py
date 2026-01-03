"""Tests for legislator crosswalk extractor."""

from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from scripts.legislator_crosswalk.exceptions import (
    InvalidSourceURLError,
    SourceReadError,
)
from scripts.legislator_crosswalk.extractor import ExtractionResult, extract_crosswalk


@pytest.fixture
def mock_source_parquet(tmp_path: Path) -> Path:
    """Create a mock source parquet file with DIME-like structure."""
    source_path = tmp_path / "mock_source.parquet"
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


class TestExtractionResult:
    """Tests for ExtractionResult dataclass."""

    def test_stores_all_fields(self, tmp_path: Path):
        """ExtractionResult stores all extraction details."""
        from scripts.legislator_crosswalk.validators import ValidationResult

        output_path = tmp_path / "output.parquet"
        result = ExtractionResult(
            source_url="https://example.com/data.parquet",
            output_path=output_path,
            source_rows=1000,
            output_count=500,
            unique_icpsr_count=100,
            unique_bonica_rid_count=400,
            validation=ValidationResult(counts_valid=True),
        )

        assert result.source_url == "https://example.com/data.parquet"
        assert result.output_path == output_path
        assert result.source_rows == 1000
        assert result.output_count == 500
        assert result.unique_icpsr_count == 100
        assert result.unique_bonica_rid_count == 400
        assert result.validation.counts_valid


class TestExtractCrosswalk:
    """Tests for extract_crosswalk function."""

    def test_rejects_invalid_source_url(self, tmp_path: Path):
        """Raises InvalidSourceURLError for non-allowed domains."""
        output_path = tmp_path / "output.parquet"

        with pytest.raises(InvalidSourceURLError) as exc_info:
            extract_crosswalk(
                output_path,
                source_url="https://evil-site.com/data.parquet",
            )

        assert "evil-site.com" in str(exc_info.value)

    def test_extracts_from_local_file_when_allowed(self, tmp_path: Path, mock_source_parquet: Path):
        """Successfully extracts from a local file when URL validation is patched."""
        output_path = tmp_path / "output.parquet"

        # Patch validate_source_url to allow local file
        with patch(
            "scripts.legislator_crosswalk.extractor.validate_source_url",
            return_value=True,
        ):
            result = extract_crosswalk(
                output_path,
                source_url=str(mock_source_parquet),
                validate=True,
            )

        assert result.output_path == output_path
        assert output_path.exists()
        # Should have 4 distinct (icpsr, bonica_rid) pairs
        # 10075 -> RID001, RID002
        # 20000 -> RID003, RID004
        assert result.output_count == 4
        assert result.unique_icpsr_count == 2
        assert result.unique_bonica_rid_count == 4

    def test_creates_output_directory(self, tmp_path: Path, mock_source_parquet: Path):
        """Creates output directory if it doesn't exist."""
        output_path = tmp_path / "nested" / "dir" / "output.parquet"

        with patch(
            "scripts.legislator_crosswalk.extractor.validate_source_url",
            return_value=True,
        ):
            extract_crosswalk(
                output_path,
                source_url=str(mock_source_parquet),
                validate=False,
            )

        assert output_path.exists()
        assert output_path.parent.exists()

    def test_skips_validation_when_disabled(self, tmp_path: Path, mock_source_parquet: Path):
        """Skips validation when validate=False."""
        output_path = tmp_path / "output.parquet"

        with patch(
            "scripts.legislator_crosswalk.extractor.validate_source_url",
            return_value=True,
        ):
            result = extract_crosswalk(
                output_path,
                source_url=str(mock_source_parquet),
                validate=False,
            )

        # Validation result should have default (False) values
        assert not result.validation.counts_valid
        assert not result.validation.uniqueness_valid
        assert not result.validation.sample_valid

    def test_runs_full_validation_when_enabled(self, tmp_path: Path, mock_source_parquet: Path):
        """Runs all validation tiers when validate=True."""
        output_path = tmp_path / "output.parquet"

        with patch(
            "scripts.legislator_crosswalk.extractor.validate_source_url",
            return_value=True,
        ):
            result = extract_crosswalk(
                output_path,
                source_url=str(mock_source_parquet),
                validate=True,
                sample_size=10,
            )

        assert result.validation.counts_valid
        assert result.validation.uniqueness_valid
        assert result.validation.sample_valid
        assert result.validation.all_valid

    def test_handles_source_read_error(self, tmp_path: Path):
        """Raises SourceReadError when source cannot be read."""
        output_path = tmp_path / "output.parquet"
        nonexistent_source = tmp_path / "nonexistent.parquet"

        with (
            patch(
                "scripts.legislator_crosswalk.extractor.validate_source_url",
                return_value=True,
            ),
            pytest.raises(SourceReadError),
        ):
            extract_crosswalk(
                output_path,
                source_url=str(nonexistent_source),
            )

    def test_output_has_correct_schema(self, tmp_path: Path, mock_source_parquet: Path):
        """Output parquet has expected columns."""
        output_path = tmp_path / "output.parquet"

        with patch(
            "scripts.legislator_crosswalk.extractor.validate_source_url",
            return_value=True,
        ):
            extract_crosswalk(
                output_path,
                source_url=str(mock_source_parquet),
                validate=False,
            )

        # Read output and check columns
        conn = duckdb.connect()
        df = conn.execute(f"SELECT * FROM '{output_path}' LIMIT 1").fetchdf()
        conn.close()

        expected_cols = [
            "icpsr",
            "bonica_rid",
            "recipient_name",
            "party",
            "state",
            "seat",
            "fec_id",
        ]
        for col in expected_cols:
            assert col in df.columns

    def test_extracts_icpsr_without_year_suffix(self, tmp_path: Path, mock_source_parquet: Path):
        """ICPSR values have year suffix removed."""
        output_path = tmp_path / "output.parquet"

        with patch(
            "scripts.legislator_crosswalk.extractor.validate_source_url",
            return_value=True,
        ):
            extract_crosswalk(
                output_path,
                source_url=str(mock_source_parquet),
                validate=False,
            )

        conn = duckdb.connect()
        icpsr_values = conn.execute(f"""
            SELECT DISTINCT icpsr FROM '{output_path}'
        """).fetchall()
        conn.close()

        icpsr_list = [row[0] for row in icpsr_values]
        # Should have 10075 and 20000 (not 100751980, etc.)
        assert "10075" in icpsr_list
        assert "20000" in icpsr_list
        # Should NOT have year-suffixed values
        assert "100751980" not in icpsr_list
        assert "200001990" not in icpsr_list

    def test_deduplicates_icpsr_bonica_rid_pairs(self, tmp_path: Path, mock_source_parquet: Path):
        """Output has unique (icpsr, bonica_rid) pairs."""
        output_path = tmp_path / "output.parquet"

        with patch(
            "scripts.legislator_crosswalk.extractor.validate_source_url",
            return_value=True,
        ):
            extract_crosswalk(
                output_path,
                source_url=str(mock_source_parquet),
                validate=False,
            )

        conn = duckdb.connect()
        # Check no duplicates
        dup_count = conn.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT icpsr, bonica_rid, COUNT(*) as cnt
                FROM '{output_path}'
                GROUP BY icpsr, bonica_rid
                HAVING cnt > 1
            )
        """).fetchone()[0]
        conn.close()

        assert dup_count == 0

    def test_accepts_path_as_string(self, tmp_path: Path, mock_source_parquet: Path):
        """Accepts output_path as string."""
        output_path = str(tmp_path / "output.parquet")

        with patch(
            "scripts.legislator_crosswalk.extractor.validate_source_url",
            return_value=True,
        ):
            extract_crosswalk(
                output_path,  # String, not Path
                source_url=str(mock_source_parquet),
                validate=False,
            )

        assert Path(output_path).exists()
