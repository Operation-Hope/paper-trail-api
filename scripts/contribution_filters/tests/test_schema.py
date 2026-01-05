"""Tests for schema module."""

from __future__ import annotations

from contribution_filters.schema import (
    ALL_CYCLES,
    MAX_CYCLE,
    MIN_CYCLE,
    escape_sql_string,
    get_organizational_filename,
    get_raw_organizational_filename,
    get_recipient_aggregates_filename,
    validate_cycle,
    validate_path_string,
    validate_source_url,
)


class TestValidateCycle:
    """Tests for validate_cycle function."""

    def test_valid_cycles(self) -> None:
        """Valid even years within range should pass."""
        assert validate_cycle(1980) is True
        assert validate_cycle(2000) is True
        assert validate_cycle(2024) is True

    def test_invalid_odd_years(self) -> None:
        """Odd years should fail."""
        assert validate_cycle(1981) is False
        assert validate_cycle(2001) is False
        assert validate_cycle(2023) is False

    def test_out_of_range(self) -> None:
        """Years outside the valid range should fail."""
        assert validate_cycle(1978) is False
        assert validate_cycle(1979) is False
        assert validate_cycle(2025) is False
        assert validate_cycle(2026) is False

    def test_all_cycles_list(self) -> None:
        """ALL_CYCLES should contain expected values."""
        assert MIN_CYCLE == 1980
        assert MAX_CYCLE == 2024
        assert len(ALL_CYCLES) == 23
        assert all(c % 2 == 0 for c in ALL_CYCLES)


class TestEscapeSqlString:
    """Tests for escape_sql_string function."""

    def test_no_escaping_needed(self) -> None:
        """Plain strings should pass through unchanged."""
        assert escape_sql_string("simple_string") == "simple_string"
        assert escape_sql_string("recipient123") == "recipient123"

    def test_single_quotes_escaped(self) -> None:
        """Single quotes should be doubled."""
        assert escape_sql_string("O'Brien") == "O''Brien"
        assert escape_sql_string("'quoted'") == "''quoted''"

    def test_backslashes_escaped(self) -> None:
        """Backslashes should be doubled."""
        assert escape_sql_string("back\\slash") == "back\\\\slash"

    def test_combined_escaping(self) -> None:
        """Both quotes and backslashes should be escaped."""
        assert escape_sql_string("O'Brien\\test") == "O''Brien\\\\test"


class TestValidatePathString:
    """Tests for validate_path_string function."""

    def test_normal_paths(self) -> None:
        """Normal file paths should pass."""
        assert validate_path_string("/path/to/file.parquet") is True
        assert validate_path_string("./relative/path.parquet") is True
        assert validate_path_string("https://example.com/file.parquet") is True

    def test_sql_injection_patterns(self) -> None:
        """SQL injection patterns should be rejected."""
        assert validate_path_string("; DROP TABLE users;--") is False
        assert validate_path_string("file.parquet; DELETE FROM data") is False
        assert validate_path_string("UNION SELECT * FROM secrets") is False
        assert validate_path_string("' OR '1'='1") is False


class TestValidateSourceUrl:
    """Tests for validate_source_url function."""

    def test_valid_huggingface_url(self) -> None:
        """HuggingFace URLs should be allowed."""
        url = "https://huggingface.co/datasets/test/file.parquet"
        assert validate_source_url(url) is True

    def test_invalid_domain(self) -> None:
        """Non-allowed domains should be rejected."""
        assert validate_source_url("https://evil.com/file.parquet") is False
        assert validate_source_url("https://example.org/file.parquet") is False

    def test_sql_injection_in_url(self) -> None:
        """URLs with SQL injection should be rejected."""
        url = "https://huggingface.co/datasets/test; DROP TABLE users;--"
        assert validate_source_url(url) is False


class TestFilenameGeneration:
    """Tests for filename generation functions."""

    def test_organizational_filename(self) -> None:
        """Organizational filenames should follow pattern."""
        assert get_organizational_filename(2020) == "contribDB_2020_organizational.parquet"
        assert get_organizational_filename(1980) == "contribDB_1980_organizational.parquet"

    def test_recipient_aggregates_filename(self) -> None:
        """Recipient aggregates filenames should follow pattern."""
        assert get_recipient_aggregates_filename(2020) == "recipient_aggregates_2020.parquet"
        assert get_recipient_aggregates_filename(1980) == "recipient_aggregates_1980.parquet"

    def test_raw_organizational_filename(self) -> None:
        """Raw organizational filenames should follow pattern."""
        assert get_raw_organizational_filename(2020) == "organizational_contributions_2020.parquet"
        assert get_raw_organizational_filename(1980) == "organizational_contributions_1980.parquet"
