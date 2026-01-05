"""Tests for JSON parser utilities."""

import json
from pathlib import Path

import pytest

from scripts.congress_legislators_converter.json_parser import (
    extract_bioguide_max_congress,
    filter_bioguides_by_congress,
    get_congress_stats,
    parse_legislators_json,
)


@pytest.fixture
def sample_current_json(tmp_path: Path) -> Path:
    """Create a sample current legislators JSON file."""
    data = [
        {
            "id": {"bioguide": "A000001"},
            "name": {"first": "John", "last": "Adams"},
            "terms": [
                {"type": "rep", "start": "2021-01-03", "end": "2023-01-03"},
                {"type": "sen", "start": "2023-01-03", "end": "2025-01-03"},
            ],
        },
        {
            "id": {"bioguide": "B000002"},
            "name": {"first": "Jane", "last": "Brown"},
            "terms": [
                {"type": "rep", "start": "2019-01-03", "end": "2021-01-03"},
            ],
        },
    ]
    json_path = tmp_path / "legislators-current.json"
    json_path.write_text(json.dumps(data))
    return json_path


@pytest.fixture
def sample_historical_json(tmp_path: Path) -> Path:
    """Create a sample historical legislators JSON file."""
    data = [
        {
            "id": {"bioguide": "C000003"},
            "name": {"first": "Robert", "last": "Clark"},
            "terms": [
                {"type": "sen", "start": "1979-01-03", "end": "1985-01-03"},
            ],
        },
        {
            "id": {"bioguide": "D000004"},
            "name": {"first": "Mary", "last": "Davis"},
            "terms": [
                {"type": "rep", "start": "1970-01-03", "end": "1975-01-03"},
            ],
        },
        {
            # A000001 also in historical with earlier term
            "id": {"bioguide": "A000001"},
            "name": {"first": "John", "last": "Adams"},
            "terms": [
                {"type": "rep", "start": "2015-01-03", "end": "2017-01-03"},
            ],
        },
    ]
    json_path = tmp_path / "legislators-historical.json"
    json_path.write_text(json.dumps(data))
    return json_path


class TestParseLegislatorsJson:
    """Tests for parse_legislators_json function."""

    def test_parses_json_file(self, sample_current_json: Path):
        """Parses a JSON file and returns list of legislators."""
        result = parse_legislators_json(sample_current_json)
        assert len(result) == 2
        assert result[0]["id"]["bioguide"] == "A000001"
        assert result[1]["id"]["bioguide"] == "B000002"

    def test_returns_empty_list_for_empty_file(self, tmp_path: Path):
        """Returns empty list for empty JSON array."""
        empty_json = tmp_path / "empty.json"
        empty_json.write_text("[]")
        result = parse_legislators_json(empty_json)
        assert result == []


class TestExtractBioguideMaxCongress:
    """Tests for extract_bioguide_max_congress function."""

    def test_extracts_max_congress_from_current(self, sample_current_json: Path, tmp_path: Path):
        """Extracts max congress from current file only."""
        empty_historical = tmp_path / "legislators-historical.json"
        empty_historical.write_text("[]")

        result = extract_bioguide_max_congress(sample_current_json, empty_historical)

        # A000001: term ends 2025-01-03 -> Congress 119 (Jan 3 is new congress start)
        # B000002: term ends 2021-01-03 -> Congress 117
        assert result["A000001"] == 119
        assert result["B000002"] == 117

    def test_extracts_max_congress_from_historical(
        self, sample_historical_json: Path, tmp_path: Path
    ):
        """Extracts max congress from historical file only."""
        empty_current = tmp_path / "legislators-current.json"
        empty_current.write_text("[]")

        result = extract_bioguide_max_congress(empty_current, sample_historical_json)

        # C000003: term ends 1985-01-03 -> Congress 99 (Jan 3 is new congress start)
        # D000004: term ends 1975-01-03 -> Congress 94
        # A000001: term ends 2017-01-03 -> Congress 115
        assert result["C000003"] == 99
        assert result["D000004"] == 94
        assert result["A000001"] == 115

    def test_merges_and_keeps_max(self, sample_current_json: Path, sample_historical_json: Path):
        """When legislator appears in both files, keeps the max congress."""
        result = extract_bioguide_max_congress(sample_current_json, sample_historical_json)

        # A000001: current has 119, historical has 115 -> should be 119
        assert result["A000001"] == 119
        assert result["B000002"] == 117
        assert result["C000003"] == 99
        assert result["D000004"] == 94

    def test_handles_missing_file(self, sample_current_json: Path, tmp_path: Path):
        """Handles non-existent historical file gracefully."""
        missing = tmp_path / "nonexistent.json"

        result = extract_bioguide_max_congress(sample_current_json, missing)

        assert "A000001" in result
        assert "B000002" in result

    def test_skips_legislators_without_bioguide(self, tmp_path: Path):
        """Skips entries without bioguide ID."""
        data = [
            {"id": {}, "terms": [{"start": "2021-01-03", "end": "2023-01-03"}]},
            {"id": {"bioguide": "A000001"}, "terms": [{"start": "2021-01-03"}]},
        ]
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(data))
        empty = tmp_path / "empty.json"
        empty.write_text("[]")

        result = extract_bioguide_max_congress(json_path, empty)

        assert len(result) == 1
        assert "A000001" in result

    def test_skips_legislators_without_terms(self, tmp_path: Path):
        """Skips entries without terms."""
        data = [
            {"id": {"bioguide": "A000001"}},
            {"id": {"bioguide": "B000002"}, "terms": []},
        ]
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(data))
        empty = tmp_path / "empty.json"
        empty.write_text("[]")

        result = extract_bioguide_max_congress(json_path, empty)

        assert len(result) == 0

    def test_handles_invalid_dates(self, tmp_path: Path):
        """Skips terms with invalid dates."""
        data = [
            {
                "id": {"bioguide": "A000001"},
                "terms": [
                    {"start": "invalid-date"},
                    {"start": "2021-01-03", "end": "2023-01-03"},
                ],
            },
        ]
        json_path = tmp_path / "test.json"
        json_path.write_text(json.dumps(data))
        empty = tmp_path / "empty.json"
        empty.write_text("[]")

        result = extract_bioguide_max_congress(json_path, empty)

        # 2023-01-03 is the start of Congress 118
        assert result["A000001"] == 118


class TestFilterBioguidesByCongress:
    """Tests for filter_bioguides_by_congress function."""

    def test_filters_by_min_congress(self):
        """Filters to legislators who served in min_congress or later."""
        bioguide_max = {
            "A000001": 118,
            "B000002": 96,
            "C000003": 95,
            "D000004": 100,
        }

        result = filter_bioguides_by_congress(bioguide_max, min_congress=96)

        assert result == {"A000001", "B000002", "D000004"}
        assert "C000003" not in result

    def test_includes_exact_min(self):
        """Includes legislators who served exactly at min_congress."""
        bioguide_max = {"A000001": 96}

        result = filter_bioguides_by_congress(bioguide_max, min_congress=96)

        assert "A000001" in result

    def test_excludes_below_min(self):
        """Excludes legislators who served before min_congress."""
        bioguide_max = {"A000001": 95}

        result = filter_bioguides_by_congress(bioguide_max, min_congress=96)

        assert "A000001" not in result

    def test_returns_empty_for_all_below(self):
        """Returns empty set if all legislators are below min."""
        bioguide_max = {"A000001": 90, "B000002": 80}

        result = filter_bioguides_by_congress(bioguide_max, min_congress=96)

        assert result == set()


class TestGetCongressStats:
    """Tests for get_congress_stats function."""

    def test_returns_stats_for_non_empty(self):
        """Returns correct stats for non-empty mapping."""
        bioguide_max = {
            "A000001": 118,
            "B000002": 96,
            "C000003": 100,
        }

        result = get_congress_stats(bioguide_max)

        assert result["count"] == 3
        assert result["min_congress"] == 96
        assert result["max_congress"] == 118

    def test_returns_empty_stats_for_empty(self):
        """Returns null stats for empty mapping."""
        result = get_congress_stats({})

        assert result["count"] == 0
        assert result["min_congress"] is None
        assert result["max_congress"] is None
