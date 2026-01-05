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

# JSON file URLs (for term-level data with congress calculation)
JSON_FILE_URLS = {
    FileType.CURRENT: f"{BASE_URL}/legislators-current.json",
    FileType.HISTORICAL: f"{BASE_URL}/legislators-historical.json",
}


# =============================================================================
# UNIFIED LEGISLATORS SCHEMA (for merged current + historical output)
# =============================================================================
# This schema is used for the unified legislators.parquet file that merges
# current and historical legislators with bioguide_id as primary key.
# Key differences from source schema:
# - icpsr is int64 (TRY_CAST from string to handle malformed values)
# - fec_ids is list<string> (parsed from comma-separated string)
# - is_current is bool (derived from source file)
# - Subset of columns (most useful for downstream joins)
UNIFIED_LEGISLATORS_SCHEMA = pa.schema(
    [
        pa.field("bioguide_id", pa.string(), nullable=False),  # Primary key
        pa.field("last_name", pa.string()),
        pa.field("first_name", pa.string()),
        pa.field("full_name", pa.string()),
        pa.field("birthday", pa.string()),
        pa.field("gender", pa.string()),
        pa.field("type", pa.string()),  # sen/rep (most recent)
        pa.field("state", pa.string()),  # 2-letter (most recent)
        pa.field("party", pa.string()),  # Most recent
        pa.field("icpsr", pa.int64()),  # Nullable - use TRY_CAST for malformed values
        pa.field("fec_ids", pa.list_(pa.string())),  # Array of FEC candidate IDs
        pa.field("opensecrets_id", pa.string()),
        pa.field("is_current", pa.bool_()),
    ]
)

UNIFIED_LEGISLATORS_COLUMNS = [
    "bioguide_id",
    "last_name",
    "first_name",
    "full_name",
    "birthday",
    "gender",
    "type",
    "state",
    "party",
    "icpsr",
    "fec_ids",
    "opensecrets_id",
    "is_current",
]


# =============================================================================
# UNIFIED LEGISLATORS EXTRACTION QUERY
# =============================================================================
# DuckDB SQL query to merge current + historical legislators.
# Uses UNION ALL with is_current flag, then deduplicates by bioguide_id
# preferring current over historical records.
UNIFIED_EXTRACTION_QUERY = """
WITH combined AS (
    -- Current legislators
    SELECT
        bioguide_id,
        last_name,
        first_name,
        full_name,
        birthday,
        gender,
        type,
        state,
        party,
        TRY_CAST(icpsr_id AS BIGINT) as icpsr,
        CASE
            WHEN fec_ids IS NULL OR fec_ids = '' THEN []
            ELSE STRING_SPLIT(fec_ids, ',')
        END as fec_ids,
        opensecrets_id,
        TRUE as is_current,
        1 as source_priority  -- Current takes precedence
    FROM read_parquet('{current_path}')
    WHERE bioguide_id IS NOT NULL AND bioguide_id != ''

    UNION ALL

    -- Historical legislators
    SELECT
        bioguide_id,
        last_name,
        first_name,
        full_name,
        birthday,
        gender,
        type,
        state,
        party,
        TRY_CAST(icpsr_id AS BIGINT) as icpsr,
        CASE
            WHEN fec_ids IS NULL OR fec_ids = '' THEN []
            ELSE STRING_SPLIT(fec_ids, ',')
        END as fec_ids,
        opensecrets_id,
        FALSE as is_current,
        2 as source_priority  -- Historical is fallback
    FROM read_parquet('{historical_path}')
    WHERE bioguide_id IS NOT NULL AND bioguide_id != ''
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY bioguide_id
            ORDER BY source_priority ASC
        ) as rn
    FROM combined
)
SELECT
    bioguide_id,
    last_name,
    first_name,
    full_name,
    birthday,
    gender,
    type,
    state,
    party,
    icpsr,
    fec_ids,
    opensecrets_id,
    is_current
FROM ranked
WHERE rn = 1
ORDER BY bioguide_id
"""
