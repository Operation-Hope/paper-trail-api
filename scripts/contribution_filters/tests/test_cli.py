"""Tests for CLI module."""

from __future__ import annotations

import pytest

from contribution_filters.cli import _format_size, main


class TestFormatSize:
    """Tests for _format_size helper function."""

    def test_bytes(self) -> None:
        """Sizes under 1KB should show bytes."""
        assert _format_size(0) == "0 B"
        assert _format_size(512) == "512 B"
        assert _format_size(1023) == "1023 B"

    def test_kilobytes(self) -> None:
        """Sizes under 1MB should show KB."""
        assert _format_size(1024) == "1.0 KB"
        assert _format_size(10240) == "10.0 KB"

    def test_megabytes(self) -> None:
        """Sizes under 1GB should show MB."""
        assert _format_size(1024 * 1024) == "1.0 MB"
        assert _format_size(100 * 1024 * 1024) == "100.0 MB"

    def test_gigabytes(self) -> None:
        """Sizes 1GB and above should show GB."""
        assert _format_size(1024 * 1024 * 1024) == "1.00 GB"
        assert _format_size(2 * 1024 * 1024 * 1024) == "2.00 GB"


class TestCLIArguments:
    """Tests for CLI argument parsing."""

    def test_missing_output_dir(self) -> None:
        """Missing output_dir should exit with error."""
        with pytest.raises(SystemExit) as exc_info:
            main([])
        assert exc_info.value.code != 0

    def test_missing_cycle_args(self) -> None:
        """Missing cycle specification should exit with error."""
        with pytest.raises(SystemExit) as exc_info:
            main(["output/"])
        assert exc_info.value.code != 0

    def test_invalid_cycle(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Invalid cycle should exit with error."""
        result = main(["output/", "--cycle", "2025"])
        assert result == 1
        captured = capsys.readouterr()
        assert "Invalid cycle" in captured.err or "2025" in captured.err

    def test_odd_year_cycle(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Odd year cycle should exit with error."""
        result = main(["output/", "--cycle", "2021"])
        assert result == 1

    def test_negative_sample_size(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Negative sample-size should exit with error."""
        result = main(["output/", "--cycle", "2020", "--sample-size", "-1"])
        assert result == 1
        captured = capsys.readouterr()
        assert "sample-size" in captured.err

    def test_zero_sample_size(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Zero sample-size should exit with error."""
        result = main(["output/", "--cycle", "2020", "--sample-size", "0"])
        assert result == 1
        captured = capsys.readouterr()
        assert "sample-size" in captured.err

    def test_start_cycle_without_end(self, capsys: pytest.CaptureFixture[str]) -> None:
        """--start-cycle without --end-cycle should exit with error."""
        result = main(["output/", "--start-cycle", "2000"])
        assert result == 1
        captured = capsys.readouterr()
        assert "end-cycle" in captured.err
