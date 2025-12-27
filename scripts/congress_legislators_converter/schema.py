"""Congress Legislators schema definitions with explicit PyArrow types."""

from dataclasses import dataclass
from enum import Enum

import pyarrow as pa


class FileType(Enum):
    """Supported Congress Legislators file types."""

    CURRENT = "current"
    HISTORICAL = "historical"


# =============================================================================
# LEGISLATORS SCHEMA (36 columns)
# =============================================================================
# All columns stored as strings for lossless conversion.
# IDs kept as strings to preserve any leading zeros or special formats.
# Dates kept as strings since some historical dates may be incomplete.
LEGISLATORS_SCHEMA: dict[str, pa.DataType] = {
    # Name fields
    "last_name": pa.string(),
    "first_name": pa.string(),
    "middle_name": pa.string(),
    "suffix": pa.string(),
    "nickname": pa.string(),
    "full_name": pa.string(),
    # Demographics
    "birthday": pa.string(),  # YYYY-MM-DD format, keep as string for incomplete dates
    "gender": pa.string(),  # M/F
    # Position information
    "type": pa.string(),  # sen/rep
    "state": pa.string(),  # 2-letter code
    "district": pa.string(),  # Keep as string (nullable)
    "senate_class": pa.string(),  # 1/2/3 or empty
    "party": pa.string(),  # Various values including historical parties
    # Contact information
    "url": pa.string(),
    "address": pa.string(),
    "phone": pa.string(),
    "contact_form": pa.string(),
    "rss_url": pa.string(),
    # Social media
    "twitter": pa.string(),
    "twitter_id": pa.string(),
    "facebook": pa.string(),
    "youtube": pa.string(),
    "youtube_id": pa.string(),
    "mastodon": pa.string(),
    # Identifier fields - all kept as strings
    "bioguide_id": pa.string(),  # Primary identifier (e.g., C000127)
    "thomas_id": pa.string(),
    "opensecrets_id": pa.string(),
    "lis_id": pa.string(),
    "fec_ids": pa.string(),  # May contain multiple comma-separated IDs
    "cspan_id": pa.string(),
    "govtrack_id": pa.string(),
    "votesmart_id": pa.string(),
    "ballotpedia_id": pa.string(),
    "washington_post_id": pa.string(),
    "icpsr_id": pa.string(),  # Cross-reference with Voteview
    "wikipedia_id": pa.string(),
}

LEGISLATORS_COLUMNS = [
    "last_name",
    "first_name",
    "middle_name",
    "suffix",
    "nickname",
    "full_name",
    "birthday",
    "gender",
    "type",
    "state",
    "district",
    "senate_class",
    "party",
    "url",
    "address",
    "phone",
    "contact_form",
    "rss_url",
    "twitter",
    "twitter_id",
    "facebook",
    "youtube",
    "youtube_id",
    "mastodon",
    "bioguide_id",
    "thomas_id",
    "opensecrets_id",
    "lis_id",
    "fec_ids",
    "cspan_id",
    "govtrack_id",
    "votesmart_id",
    "ballotpedia_id",
    "washington_post_id",
    "icpsr_id",
    "wikipedia_id",
]


# =============================================================================
# FILE TYPE CONFIGURATION
# =============================================================================
@dataclass
class FileTypeConfig:
    """Configuration for a specific Congress Legislators file type."""

    schema: dict[str, pa.DataType]
    expected_columns: list[str]
    sum_column: str | None  # No numeric columns to sum for this dataset
    key_columns: list[str]  # Columns to track non-null counts
    sample_size: int = 1000  # Default sample size for validation


FILE_TYPE_CONFIGS: dict[FileType, FileTypeConfig] = {
    FileType.CURRENT: FileTypeConfig(
        schema=LEGISLATORS_SCHEMA,
        expected_columns=LEGISLATORS_COLUMNS,
        sum_column=None,  # All string columns, no numeric sum
        key_columns=["bioguide_id", "icpsr_id", "state", "type"],
        sample_size=500,  # Current legislators is smaller (~540 rows)
    ),
    FileType.HISTORICAL: FileTypeConfig(
        schema=LEGISLATORS_SCHEMA,
        expected_columns=LEGISLATORS_COLUMNS,
        sum_column=None,  # All string columns, no numeric sum
        key_columns=["bioguide_id", "icpsr_id", "state", "type"],
        sample_size=1000,  # Historical has ~12,500 rows
    ),
}


def get_config(file_type: FileType) -> FileTypeConfig:
    """Get configuration for a file type."""
    return FILE_TYPE_CONFIGS[file_type]


# Null markers in Congress Legislators CSVs
NULL_VALUES = [""]

# Download URLs
BASE_URL = "https://unitedstates.github.io/congress-legislators"
FILE_URLS = {
    FileType.CURRENT: f"{BASE_URL}/legislators-current.csv",
    FileType.HISTORICAL: f"{BASE_URL}/legislators-historical.csv",
}
