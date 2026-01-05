"""Parse Congress Legislators JSON files to extract congress numbers from term data."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from .congress_utils import congress_from_date


def parse_legislators_json(json_path: Path) -> list[dict]:
    """
    Parse a legislators JSON file.

    Args:
        json_path: Path to the JSON file.

    Returns:
        List of legislator dictionaries with id, name, bio, and terms.
    """
    with json_path.open(encoding="utf-8") as f:
        return json.load(f)


def extract_bioguide_max_congress(
    current_json_path: Path,
    historical_json_path: Path,
) -> dict[str, int]:
    """
    Extract mapping of bioguide_id to maximum congress number served.

    Parses both current and historical JSON files to calculate the max
    congress number for each legislator based on their term dates.

    Args:
        current_json_path: Path to legislators-current.json
        historical_json_path: Path to legislators-historical.json

    Returns:
        Dictionary mapping bioguide_id to max congress number.
    """
    bioguide_to_max_congress: dict[str, int] = {}

    for json_path in [current_json_path, historical_json_path]:
        if not json_path.exists():
            continue

        legislators = parse_legislators_json(json_path)

        for leg in legislators:
            bioguide_id = leg.get("id", {}).get("bioguide")
            if not bioguide_id:
                continue

            terms = leg.get("terms", [])
            if not terms:
                continue

            # Calculate congress number for each term
            max_congress = 0
            for term in terms:
                # Use end date if available, otherwise start date
                term_date_str = term.get("end") or term.get("start")
                if not term_date_str:
                    continue

                try:
                    term_date = date.fromisoformat(term_date_str)
                    congress = congress_from_date(term_date)
                    max_congress = max(max_congress, congress)
                except ValueError:
                    # Skip invalid dates
                    continue

            if max_congress > 0:
                # Keep the higher value if legislator appears in both files
                existing = bioguide_to_max_congress.get(bioguide_id, 0)
                bioguide_to_max_congress[bioguide_id] = max(existing, max_congress)

    return bioguide_to_max_congress


def filter_bioguides_by_congress(
    bioguide_max_congress: dict[str, int],
    min_congress: int,
) -> set[str]:
    """
    Get set of bioguide_ids that served in min_congress or later.

    Args:
        bioguide_max_congress: Mapping of bioguide_id to max congress.
        min_congress: Minimum congress number (inclusive).

    Returns:
        Set of bioguide_ids that meet the criteria.
    """
    return {
        bioguide_id
        for bioguide_id, max_congress in bioguide_max_congress.items()
        if max_congress >= min_congress
    }


def get_congress_stats(bioguide_max_congress: dict[str, int]) -> dict:
    """
    Get statistics about congress distribution.

    Utility function for debugging and data inspection.

    Args:
        bioguide_max_congress: Mapping of bioguide_id to max congress.

    Returns:
        Dictionary with min, max, and distribution info.
    """
    if not bioguide_max_congress:
        return {"count": 0, "min_congress": None, "max_congress": None}

    congresses = list(bioguide_max_congress.values())
    return {
        "count": len(congresses),
        "min_congress": min(congresses),
        "max_congress": max(congresses),
    }
