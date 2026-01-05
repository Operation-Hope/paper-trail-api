"""Utilities for calculating Congress numbers from dates."""

from __future__ import annotations

from datetime import date


def congress_from_date(d: date) -> int:
    """
    Calculate the Congress number for a given date.

    Congress sessions are numbered starting from the 1st Congress (1789-1791).
    Each Congress lasts approximately 2 years, starting on January 3rd of odd years
    (since the 20th Amendment took effect in 1935).

    Before 1935, Congresses started on March 4th. This function uses a simplified
    calculation that's accurate for modern Congresses (post-1935).

    Args:
        d: A date to calculate the Congress number for.

    Returns:
        The Congress number (e.g., 96 for 1979-1981, 118 for 2023-2025).

    Examples:
        >>> congress_from_date(date(1979, 1, 3))
        96
        >>> congress_from_date(date(2023, 1, 3))
        118
        >>> congress_from_date(date(2024, 12, 31))
        118
    """
    year = d.year

    # Congress N covers odd year Y to even year Y+1
    # January 1-2 of odd years are still previous Congress
    if d.month == 1 and d.day < 3 and year % 2 == 1:
        year -= 1

    # For years after 1935 (20th Amendment):
    # Congress 74 started January 3, 1935
    if year >= 1935:
        # Adjust for odd/even years (Congress starts in odd years)
        base_year = year if year % 2 == 1 else year - 1
        return ((base_year - 1935) // 2) + 74

    # For years 1789-1934 (historical, less precise):
    # Congress 1 started March 4, 1789
    # This is a simplified calculation
    base_year = year if year % 2 == 1 else year - 1
    return ((base_year - 1789) // 2) + 1


def congress_start_year(congress: int) -> int:
    """
    Get the starting year for a given Congress number.

    Args:
        congress: The Congress number (e.g., 96).

    Returns:
        The year the Congress started (e.g., 1979 for Congress 96).

    Examples:
        >>> congress_start_year(96)
        1979
        >>> congress_start_year(118)
        2023
    """
    if congress >= 74:
        # Post-20th Amendment
        return 1935 + (congress - 74) * 2
    # Pre-20th Amendment
    return 1789 + (congress - 1) * 2


def congress_end_year(congress: int) -> int:
    """
    Get the ending year for a given Congress number.

    Args:
        congress: The Congress number (e.g., 96).

    Returns:
        The year the Congress ended (e.g., 1981 for Congress 96).

    Examples:
        >>> congress_end_year(96)
        1981
        >>> congress_end_year(118)
        2025
    """
    return congress_start_year(congress) + 2


# Default minimum congress for filtering (1979-1981)
# This aligns with DIME contribution data coverage
DEFAULT_MIN_CONGRESS = 96
