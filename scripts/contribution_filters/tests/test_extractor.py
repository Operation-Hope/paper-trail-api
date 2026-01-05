"""Tests for extractor module."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from contribution_filters.exceptions import InvalidCycleError, InvalidSourceURLError
from contribution_filters.extractor import (
    ExtractionResult,
    OutputType,
    extract_raw_organizational_contributions,
)


class TestExtractRawOrganizationalContributions:
    """Tests for extract_raw_organizational_contributions function."""

    @pytest.fixture
    def mock_contributions(self, tmp_path: Path) -> Path:
        """Create a mock contributions parquet file."""
        conn = duckdb.connect()
        contributions_path = tmp_path / "contributions.parquet"

        # Create mock contributions data with organizational and individual contributors
        conn.execute(f"""
            COPY (
                SELECT
                    2020 as cycle,
                    'rid001' as "bonica.rid",
                    'Recipient One' as "recipient.name",
                    'PAC Corp' as "contributor.name",
                    'C' as "contributor.type",
                    'cid001' as "bonica.cid",
                    1000.0 as amount,
                    '2020-01-15' as date,
                    'DC' as "contributor.state"
                UNION ALL
                SELECT 2020, 'rid002', 'Recipient Two', 'Union ABC', 'L',
                       'cid002', 500.0, '2020-02-20', 'VA'
                UNION ALL
                SELECT 2020, 'rid001', 'Recipient One', 'John Doe', 'I',
                       'cid003', 100.0, '2020-03-10', 'MD'
                UNION ALL
                SELECT 2020, 'rid003', 'Recipient Three', 'Corp XYZ', 'C',
                       'cid004', 2000.0, '2020-04-05', 'NY'
            ) TO '{contributions_path}' (FORMAT PARQUET)
        """)
        conn.close()
        return contributions_path

    @pytest.fixture
    def mock_legislators(self, tmp_path: Path) -> Path:
        """Create a mock legislators parquet file."""
        conn = duckdb.connect()
        legislators_path = tmp_path / "legislators.parquet"

        # Create mock legislators with FEC IDs
        conn.execute(f"""
            COPY (
                SELECT
                    'A000001' as bioguide_id,
                    ['H0DC00001'] as fec_ids,
                    'Smith' as last_name,
                    'John' as first_name
                UNION ALL
                SELECT 'B000002', ['S0VA00002'], 'Jones', 'Jane'
            ) TO '{legislators_path}' (FORMAT PARQUET)
        """)
        conn.close()
        return legislators_path

    @pytest.fixture
    def mock_recipients(self, tmp_path: Path) -> Path:
        """Create a mock recipients parquet file."""
        conn = duckdb.connect()
        recipients_path = tmp_path / "recipients.parquet"

        # Create mock recipients with ICPSR codes matching the contributions
        conn.execute(f"""
            COPY (
                SELECT
                    'rid001' as "bonica.rid",
                    'Recipient One' as "recipient.name",
                    'H0DC000012020' as "ICPSR"
                UNION ALL
                SELECT 'rid002', 'Recipient Two', 'S0VA000022020'
                UNION ALL
                SELECT 'rid003', 'Recipient Three', 'cand12345'
            ) TO '{recipients_path}' (FORMAT PARQUET)
        """)
        conn.close()
        return recipients_path

    def test_invalid_cycle_raises_error(self, tmp_path: Path, mock_legislators: Path) -> None:
        """Invalid cycle should raise InvalidCycleError."""
        output_path = tmp_path / "output.parquet"

        with pytest.raises(InvalidCycleError) as exc_info:
            extract_raw_organizational_contributions(
                output_path=output_path,
                cycle=2025,  # Invalid: future year
                legislators_path=mock_legislators,
            )

        assert exc_info.value.cycle == 2025

    def test_invalid_source_url_raises_error(self, tmp_path: Path, mock_legislators: Path) -> None:
        """Invalid source URL domain should raise InvalidSourceURLError."""
        output_path = tmp_path / "output.parquet"

        with pytest.raises(InvalidSourceURLError) as exc_info:
            extract_raw_organizational_contributions(
                output_path=output_path,
                cycle=2020,
                legislators_path=mock_legislators,
                source_url="https://evil.com/data.parquet",
            )

        assert "evil.com" in exc_info.value.source_url

    def test_extraction_result_type(
        self,
        tmp_path: Path,
        mock_contributions: Path,
        mock_legislators: Path,
        mock_recipients: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Extraction should return correct result type."""
        # Patch the RECIPIENTS_URL and ALLOWED_LOCAL_DIRECTORIES to use our mocks
        import contribution_filters.extractor as extractor_module
        import contribution_filters.schema as schema_module

        monkeypatch.setattr(schema_module, "RECIPIENTS_URL", str(mock_recipients))
        monkeypatch.setattr(extractor_module, "RECIPIENTS_URL", str(mock_recipients))
        monkeypatch.setattr(
            schema_module, "ALLOWED_LOCAL_DIRECTORIES", ["/tmp/", str(tmp_path) + "/"]
        )

        output_path = tmp_path / "output.parquet"

        result = extract_raw_organizational_contributions(
            output_path=output_path,
            cycle=2020,
            legislators_path=mock_legislators,
            source_url=str(mock_contributions),
            validate=False,  # Skip validation for this test
        )

        assert isinstance(result, ExtractionResult)
        assert result.output_type == OutputType.RAW_ORGANIZATIONAL
        assert result.cycle == 2020
        assert result.output_path == output_path

    def test_filters_individual_contributors(
        self,
        tmp_path: Path,
        mock_contributions: Path,
        mock_legislators: Path,
        mock_recipients: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Extraction should filter out individual contributors."""
        import contribution_filters.extractor as extractor_module
        import contribution_filters.schema as schema_module

        monkeypatch.setattr(schema_module, "RECIPIENTS_URL", str(mock_recipients))
        monkeypatch.setattr(extractor_module, "RECIPIENTS_URL", str(mock_recipients))
        monkeypatch.setattr(
            schema_module, "ALLOWED_LOCAL_DIRECTORIES", ["/tmp/", str(tmp_path) + "/"]
        )

        output_path = tmp_path / "output.parquet"

        result = extract_raw_organizational_contributions(
            output_path=output_path,
            cycle=2020,
            legislators_path=mock_legislators,
            source_url=str(mock_contributions),
            validate=False,
        )

        # Source has 4 rows, 1 is individual (type='I'), so output should have 3
        assert result.source_rows == 4
        assert result.output_count == 3

        # Verify no individuals in output
        conn = duckdb.connect()
        individual_count = conn.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{output_path}')
            WHERE contributor_type = 'I'
        """).fetchone()[0]
        conn.close()

        assert individual_count == 0

    def test_includes_bioguide_id(
        self,
        tmp_path: Path,
        mock_contributions: Path,
        mock_legislators: Path,
        mock_recipients: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Output should include bioguide_id column."""
        import contribution_filters.extractor as extractor_module
        import contribution_filters.schema as schema_module

        monkeypatch.setattr(schema_module, "RECIPIENTS_URL", str(mock_recipients))
        monkeypatch.setattr(extractor_module, "RECIPIENTS_URL", str(mock_recipients))
        monkeypatch.setattr(
            schema_module, "ALLOWED_LOCAL_DIRECTORIES", ["/tmp/", str(tmp_path) + "/"]
        )

        output_path = tmp_path / "output.parquet"

        extract_raw_organizational_contributions(
            output_path=output_path,
            cycle=2020,
            legislators_path=mock_legislators,
            source_url=str(mock_contributions),
            validate=False,
        )

        # Verify bioguide_id column exists
        conn = duckdb.connect()
        columns = conn.execute(f"""
            SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{output_path}'))
        """).fetchall()
        conn.close()

        column_names = [c[0] for c in columns]
        assert "bioguide_id" in column_names

    def test_bioguide_id_matches_legislators(
        self,
        tmp_path: Path,
        mock_contributions: Path,
        mock_legislators: Path,
        mock_recipients: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """bioguide_id values should match legislators via FEC ID join."""
        import contribution_filters.extractor as extractor_module
        import contribution_filters.schema as schema_module

        monkeypatch.setattr(schema_module, "RECIPIENTS_URL", str(mock_recipients))
        monkeypatch.setattr(extractor_module, "RECIPIENTS_URL", str(mock_recipients))
        monkeypatch.setattr(
            schema_module, "ALLOWED_LOCAL_DIRECTORIES", ["/tmp/", str(tmp_path) + "/"]
        )

        output_path = tmp_path / "output.parquet"

        extract_raw_organizational_contributions(
            output_path=output_path,
            cycle=2020,
            legislators_path=mock_legislators,
            source_url=str(mock_contributions),
            validate=False,
        )

        # Check that matched records have correct bioguide_ids
        conn = duckdb.connect()
        matched = conn.execute(f"""
            SELECT "bonica.rid", bioguide_id
            FROM read_parquet('{output_path}')
            WHERE bioguide_id IS NOT NULL
            ORDER BY "bonica.rid"
        """).fetchall()
        conn.close()

        # rid001 should match A000001 (via H0DC000012020 ICPSR)
        # rid002 should match B000002 (via S0VA000022020 ICPSR)
        assert len(matched) == 2
        rid_to_bioguide = {r[0]: r[1] for r in matched}
        assert rid_to_bioguide.get("rid001") == "A000001"
        assert rid_to_bioguide.get("rid002") == "B000002"
