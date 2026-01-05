"""Tests for CLI commands, particularly cmd_unified."""

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from scripts.congress_legislators_converter.cli import cmd_unified, main
from scripts.congress_legislators_converter.congress_utils import DEFAULT_MIN_CONGRESS


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
                 'rep', 'CA', 'R', '23456', 'H4CA05022', 'N00002')
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
                ('C000003', 'Clark', 'Michael', 'Michael Clark', '1920-11-30', 'M',
                 'sen', 'NY', 'D', '34567', 'S2NY00033', 'N00003')
            ) AS t(
                bioguide_id, last_name, first_name, full_name, birthday, gender,
                type, state, party, icpsr_id, fec_ids, opensecrets_id
            )
        ) TO '{historical_path}' (FORMAT PARQUET)
    """)
    conn.close()
    return historical_path


@pytest.fixture
def setup_parquet_files(
    tmp_path: Path, mock_current_parquet: Path, mock_historical_parquet: Path
) -> Path:
    """Set up parquet files in the expected locations."""
    return tmp_path


class TestCmdUnified:
    """Tests for cmd_unified function."""

    def test_returns_success_with_valid_files(self, setup_parquet_files: Path, capsys):
        """Returns 0 for successful execution."""
        args = argparse.Namespace(
            output_dir=str(setup_parquet_files),
            min_congress=None,
            all_congresses=True,
            no_validate=True,
            sample_size=10,
        )

        result = cmd_unified(args)

        assert result == 0
        output_path = setup_parquet_files / "legislators.parquet"
        assert output_path.exists()

    def test_returns_error_for_missing_current(self, tmp_path: Path, capsys):
        """Returns 1 when current parquet is missing."""
        # Only create historical
        historical_path = tmp_path / "legislators-historical.parquet"
        conn = duckdb.connect()
        conn.execute(f"""
            COPY (SELECT 'A' as bioguide_id)
            TO '{historical_path}' (FORMAT PARQUET)
        """)
        conn.close()

        args = argparse.Namespace(
            output_dir=str(tmp_path),
            min_congress=None,
            all_congresses=True,
            no_validate=True,
            sample_size=10,
        )

        result = cmd_unified(args)

        assert result == 1
        captured = capsys.readouterr()
        # Error message goes to stderr
        assert "not found" in captured.err.lower() or "ERROR" in captured.err

    def test_returns_error_for_missing_historical(self, tmp_path: Path, capsys):
        """Returns 1 when historical parquet is missing."""
        # Only create current
        current_path = tmp_path / "legislators-current.parquet"
        conn = duckdb.connect()
        conn.execute(f"""
            COPY (SELECT 'A' as bioguide_id)
            TO '{current_path}' (FORMAT PARQUET)
        """)
        conn.close()

        args = argparse.Namespace(
            output_dir=str(tmp_path),
            min_congress=None,
            all_congresses=True,
            no_validate=True,
            sample_size=10,
        )

        result = cmd_unified(args)

        assert result == 1

    def test_uses_default_min_congress(self, setup_parquet_files: Path):
        """Uses DEFAULT_MIN_CONGRESS when not overridden."""
        args = argparse.Namespace(
            output_dir=str(setup_parquet_files),
            min_congress=DEFAULT_MIN_CONGRESS,
            all_congresses=False,
            no_validate=True,
            sample_size=10,
        )

        # This will try to filter by congress 96+ which requires JSON files
        # Since we don't have them, it will fail - but we're testing the arg passing
        with patch(
            "scripts.congress_legislators_converter.cli.extract_unified_legislators"
        ) as mock_extract:
            mock_extract.return_value = MagicMock(
                output_path=setup_parquet_files / "legislators.parquet",
                output_count=100,
                current_count=50,
                fec_ids_populated_count=20,
                icpsr_populated_count=90,
                min_congress=96,
                filtered_out_count=10,
            )

            cmd_unified(args)

            mock_extract.assert_called_once()
            call_kwargs = mock_extract.call_args[1]
            assert call_kwargs["min_congress"] == DEFAULT_MIN_CONGRESS

    def test_all_congresses_sets_min_congress_none(self, setup_parquet_files: Path):
        """--all-congresses flag sets min_congress to None."""
        args = argparse.Namespace(
            output_dir=str(setup_parquet_files),
            min_congress=DEFAULT_MIN_CONGRESS,
            all_congresses=True,
            no_validate=True,
            sample_size=10,
        )

        with patch(
            "scripts.congress_legislators_converter.cli.extract_unified_legislators"
        ) as mock_extract:
            mock_extract.return_value = MagicMock(
                output_path=setup_parquet_files / "legislators.parquet",
                output_count=100,
                current_count=50,
                fec_ids_populated_count=20,
                icpsr_populated_count=90,
                min_congress=None,
                filtered_out_count=0,
            )

            cmd_unified(args)

            call_kwargs = mock_extract.call_args[1]
            assert call_kwargs["min_congress"] is None


class TestMainArgumentParsing:
    """Tests for main() argument parsing."""

    def test_unified_command_parses_min_congress(self):
        """--min-congress argument is parsed correctly."""
        with (
            patch("sys.argv", ["prog", "unified", "-o", "/tmp", "--min-congress", "100"]),
            patch("scripts.congress_legislators_converter.cli.cmd_unified") as mock_cmd,
        ):
            mock_cmd.return_value = 0
            main()

            args = mock_cmd.call_args[0][0]
            assert args.min_congress == 100

    def test_unified_command_parses_all_congresses(self):
        """--all-congresses flag is parsed correctly."""
        with (
            patch("sys.argv", ["prog", "unified", "-o", "/tmp", "--all-congresses"]),
            patch("scripts.congress_legislators_converter.cli.cmd_unified") as mock_cmd,
        ):
            mock_cmd.return_value = 0
            main()

            args = mock_cmd.call_args[0][0]
            assert args.all_congresses is True

    def test_unified_command_default_min_congress(self):
        """Default min_congress is DEFAULT_MIN_CONGRESS."""
        with (
            patch("sys.argv", ["prog", "unified", "-o", "/tmp"]),
            patch("scripts.congress_legislators_converter.cli.cmd_unified") as mock_cmd,
        ):
            mock_cmd.return_value = 0
            main()

            args = mock_cmd.call_args[0][0]
            assert args.min_congress == DEFAULT_MIN_CONGRESS
            assert args.all_congresses is False
