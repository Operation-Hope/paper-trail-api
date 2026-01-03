"""Tests for legislator crosswalk CLI."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.legislator_crosswalk.cli import main
from scripts.legislator_crosswalk.exceptions import CrosswalkError
from scripts.legislator_crosswalk.extractor import ExtractionResult
from scripts.legislator_crosswalk.validators import ValidationResult


class TestCLIArgumentParsing:
    """Tests for CLI argument parsing."""

    def test_requires_output_argument(self):
        """CLI requires output path argument."""
        with patch.object(sys, "argv", ["cli"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code != 0

    def test_accepts_output_path(self, tmp_path: Path):
        """CLI accepts output path as positional argument."""
        output_path = tmp_path / "output.parquet"

        mock_result = ExtractionResult(
            source_url="https://huggingface.co/test.parquet",
            output_path=output_path,
            source_rows=100,
            output_count=50,
            unique_icpsr_count=10,
            unique_bonica_rid_count=40,
            validation=ValidationResult(
                counts_valid=True,
                uniqueness_valid=True,
                sample_valid=True,
            ),
        )

        # Create a dummy file so stat() works
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"dummy")

        with (
            patch.object(sys, "argv", ["cli", str(output_path)]),
            patch(
                "scripts.legislator_crosswalk.cli.extract_crosswalk",
                return_value=mock_result,
            ),
        ):
            result = main()

        assert result == 0

    def test_no_validate_flag(self, tmp_path: Path):
        """CLI accepts --no-validate flag."""
        output_path = tmp_path / "output.parquet"
        output_path.write_bytes(b"dummy")

        mock_result = ExtractionResult(
            source_url="https://huggingface.co/test.parquet",
            output_path=output_path,
            source_rows=100,
            output_count=50,
            unique_icpsr_count=10,
            unique_bonica_rid_count=40,
            validation=ValidationResult(),  # Not validated
        )

        with (
            patch.object(sys, "argv", ["cli", str(output_path), "--no-validate"]),
            patch(
                "scripts.legislator_crosswalk.cli.extract_crosswalk",
                return_value=mock_result,
            ) as mock_extract,
        ):
            main()

        # Verify validate=False was passed
        mock_extract.assert_called_once()
        call_kwargs = mock_extract.call_args[1]
        assert call_kwargs["validate"] is False

    def test_sample_size_argument(self, tmp_path: Path):
        """CLI accepts --sample-size argument."""
        output_path = tmp_path / "output.parquet"
        output_path.write_bytes(b"dummy")

        mock_result = ExtractionResult(
            source_url="https://huggingface.co/test.parquet",
            output_path=output_path,
            source_rows=100,
            output_count=50,
            unique_icpsr_count=10,
            unique_bonica_rid_count=40,
            validation=ValidationResult(
                counts_valid=True,
                uniqueness_valid=True,
                sample_valid=True,
            ),
        )

        with (
            patch.object(sys, "argv", ["cli", str(output_path), "--sample-size", "200"]),
            patch(
                "scripts.legislator_crosswalk.cli.extract_crosswalk",
                return_value=mock_result,
            ) as mock_extract,
        ):
            main()

        mock_extract.assert_called_once()
        call_kwargs = mock_extract.call_args[1]
        assert call_kwargs["sample_size"] == 200

    def test_source_url_argument(self, tmp_path: Path):
        """CLI accepts --source-url argument."""
        output_path = tmp_path / "output.parquet"
        output_path.write_bytes(b"dummy")
        custom_url = "https://huggingface.co/custom/data.parquet"

        mock_result = ExtractionResult(
            source_url=custom_url,
            output_path=output_path,
            source_rows=100,
            output_count=50,
            unique_icpsr_count=10,
            unique_bonica_rid_count=40,
            validation=ValidationResult(
                counts_valid=True,
                uniqueness_valid=True,
                sample_valid=True,
            ),
        )

        with (
            patch.object(sys, "argv", ["cli", str(output_path), "--source-url", custom_url]),
            patch(
                "scripts.legislator_crosswalk.cli.extract_crosswalk",
                return_value=mock_result,
            ) as mock_extract,
        ):
            main()

        mock_extract.assert_called_once()
        call_kwargs = mock_extract.call_args[1]
        assert call_kwargs["source_url"] == custom_url


class TestCLIOutput:
    """Tests for CLI output formatting."""

    def test_prints_success_message(self, tmp_path: Path, capsys):
        """CLI prints success message on completion."""
        output_path = tmp_path / "output.parquet"
        output_path.write_bytes(b"dummy" * 1000)  # 5KB

        mock_result = ExtractionResult(
            source_url="https://huggingface.co/test.parquet",
            output_path=output_path,
            source_rows=100,
            output_count=50,
            unique_icpsr_count=10,
            unique_bonica_rid_count=40,
            validation=ValidationResult(
                counts_valid=True,
                uniqueness_valid=True,
                sample_valid=True,
            ),
        )

        with (
            patch.object(sys, "argv", ["cli", str(output_path)]),
            patch(
                "scripts.legislator_crosswalk.cli.extract_crosswalk",
                return_value=mock_result,
            ),
        ):
            main()

        captured = capsys.readouterr()
        assert "SUCCESS" in captured.out

    def test_prints_validation_skipped(self, tmp_path: Path, capsys):
        """CLI indicates when validation is skipped."""
        output_path = tmp_path / "output.parquet"
        output_path.write_bytes(b"dummy")

        mock_result = ExtractionResult(
            source_url="https://huggingface.co/test.parquet",
            output_path=output_path,
            source_rows=100,
            output_count=50,
            unique_icpsr_count=10,
            unique_bonica_rid_count=40,
            validation=ValidationResult(),
        )

        with (
            patch.object(sys, "argv", ["cli", str(output_path), "--no-validate"]),
            patch(
                "scripts.legislator_crosswalk.cli.extract_crosswalk",
                return_value=mock_result,
            ),
        ):
            main()

        captured = capsys.readouterr()
        assert "SKIPPED" in captured.out

    def test_prints_validation_passed(self, tmp_path: Path, capsys):
        """CLI indicates when validation passes."""
        output_path = tmp_path / "output.parquet"
        output_path.write_bytes(b"dummy")

        mock_result = ExtractionResult(
            source_url="https://huggingface.co/test.parquet",
            output_path=output_path,
            source_rows=100,
            output_count=50,
            unique_icpsr_count=10,
            unique_bonica_rid_count=40,
            validation=ValidationResult(
                counts_valid=True,
                uniqueness_valid=True,
                sample_valid=True,
            ),
        )

        with (
            patch.object(sys, "argv", ["cli", str(output_path)]),
            patch(
                "scripts.legislator_crosswalk.cli.extract_crosswalk",
                return_value=mock_result,
            ),
        ):
            main()

        captured = capsys.readouterr()
        assert "ALL PASSED" in captured.out


class TestCLIErrorHandling:
    """Tests for CLI error handling."""

    def test_returns_nonzero_on_error(self, tmp_path: Path):
        """CLI returns non-zero exit code on error."""
        output_path = tmp_path / "output.parquet"

        with (
            patch.object(sys, "argv", ["cli", str(output_path)]),
            patch(
                "scripts.legislator_crosswalk.cli.extract_crosswalk",
                side_effect=CrosswalkError(message="Test error"),
            ),
        ):
            result = main()

        assert result == 1

    def test_prints_error_to_stderr(self, tmp_path: Path, capsys):
        """CLI prints errors to stderr."""
        output_path = tmp_path / "output.parquet"

        with (
            patch.object(sys, "argv", ["cli", str(output_path)]),
            patch(
                "scripts.legislator_crosswalk.cli.extract_crosswalk",
                side_effect=CrosswalkError(message="Test error message"),
            ),
        ):
            main()

        captured = capsys.readouterr()
        assert "ERROR" in captured.err
        assert "Test error message" in captured.err


class TestCLIDivisionByZero:
    """Tests for division by zero protection in CLI output."""

    def test_handles_zero_icpsr_count(self, tmp_path: Path, capsys):
        """CLI handles zero unique_icpsr_count without crashing."""
        output_path = tmp_path / "output.parquet"
        output_path.write_bytes(b"dummy")

        # Edge case: somehow no unique ICPSR values
        mock_result = ExtractionResult(
            source_url="https://huggingface.co/test.parquet",
            output_path=output_path,
            source_rows=0,
            output_count=0,
            unique_icpsr_count=0,  # This would cause division by zero
            unique_bonica_rid_count=0,
            validation=ValidationResult(),
        )

        with (
            patch.object(sys, "argv", ["cli", str(output_path), "--no-validate"]),
            patch(
                "scripts.legislator_crosswalk.cli.extract_crosswalk",
                return_value=mock_result,
            ),
        ):
            # Should not raise ZeroDivisionError
            result = main()

        assert result == 0
        captured = capsys.readouterr()
        # Should NOT contain "Avg recipients per legislator" when count is 0
        assert "Avg recipients per legislator" not in captured.out

    def test_shows_avg_when_icpsr_count_positive(self, tmp_path: Path, capsys):
        """CLI shows average when unique_icpsr_count is positive."""
        output_path = tmp_path / "output.parquet"
        output_path.write_bytes(b"dummy")

        mock_result = ExtractionResult(
            source_url="https://huggingface.co/test.parquet",
            output_path=output_path,
            source_rows=100,
            output_count=50,
            unique_icpsr_count=10,
            unique_bonica_rid_count=40,
            validation=ValidationResult(),
        )

        with (
            patch.object(sys, "argv", ["cli", str(output_path), "--no-validate"]),
            patch(
                "scripts.legislator_crosswalk.cli.extract_crosswalk",
                return_value=mock_result,
            ),
        ):
            main()

        captured = capsys.readouterr()
        assert "Avg recipients per legislator: 4.0" in captured.out
