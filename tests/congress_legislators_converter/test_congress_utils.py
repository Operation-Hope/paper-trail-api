"""Tests for congress number calculation utilities."""

from datetime import date

from scripts.congress_legislators_converter.congress_utils import (
    DEFAULT_MIN_CONGRESS,
    congress_end_year,
    congress_from_date,
    congress_start_year,
)


class TestCongressFromDate:
    """Tests for congress_from_date function."""

    def test_congress_96_start(self):
        """Congress 96 started January 3, 1979."""
        assert congress_from_date(date(1979, 1, 3)) == 96

    def test_congress_96_mid(self):
        """Mid-1979 is still Congress 96."""
        assert congress_from_date(date(1979, 6, 15)) == 96

    def test_congress_96_end(self):
        """December 1980 is still Congress 96."""
        assert congress_from_date(date(1980, 12, 31)) == 96

    def test_congress_97_start(self):
        """Congress 97 started January 3, 1981."""
        assert congress_from_date(date(1981, 1, 3)) == 97

    def test_congress_118_2023(self):
        """Congress 118 covers 2023-2025."""
        assert congress_from_date(date(2023, 1, 3)) == 118
        assert congress_from_date(date(2024, 12, 31)) == 118

    def test_january_1_odd_year_is_previous_congress(self):
        """January 1-2 of odd years are still previous Congress."""
        # January 2, 2023 is still Congress 117
        assert congress_from_date(date(2023, 1, 2)) == 117
        # January 3, 2023 is Congress 118
        assert congress_from_date(date(2023, 1, 3)) == 118


class TestCongressStartYear:
    """Tests for congress_start_year function."""

    def test_congress_96(self):
        """Congress 96 started in 1979."""
        assert congress_start_year(96) == 1979

    def test_congress_118(self):
        """Congress 118 started in 2023."""
        assert congress_start_year(118) == 2023

    def test_congress_74(self):
        """Congress 74 started in 1935 (20th Amendment)."""
        assert congress_start_year(74) == 1935


class TestCongressEndYear:
    """Tests for congress_end_year function."""

    def test_congress_96(self):
        """Congress 96 ended in 1981."""
        assert congress_end_year(96) == 1981

    def test_congress_118(self):
        """Congress 118 ends in 2025."""
        assert congress_end_year(118) == 2025


class TestDefaultMinCongress:
    """Tests for DEFAULT_MIN_CONGRESS constant."""

    def test_default_is_96(self):
        """Default minimum congress is 96 (1979)."""
        assert DEFAULT_MIN_CONGRESS == 96
