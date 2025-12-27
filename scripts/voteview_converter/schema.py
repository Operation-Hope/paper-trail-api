"""Voteview schema definitions with explicit PyArrow types for all file types."""

from dataclasses import dataclass
from enum import Enum

import pyarrow as pa


class FileType(Enum):
    """Supported Voteview file types."""

    MEMBERS = "members"
    ROLLCALLS = "rollcalls"
    VOTES = "votes"


# =============================================================================
# MEMBERS SCHEMA (22 columns)
# =============================================================================
# ICPSR IDs are integers (no leading zeros), so int32 is appropriate
# Many columns have float notation in CSV (e.g., "0.0") so use float64
VOTEVIEW_MEMBERS_SCHEMA: dict[str, pa.DataType] = {
    # Integer columns (never have float notation)
    "congress": pa.int16(),
    "icpsr": pa.int32(),
    # Columns with potential float notation in CSV (e.g., "0.0", "4.0")
    "state_icpsr": pa.float64(),
    "district_code": pa.float64(),
    "party_code": pa.float64(),
    # Nullable integer columns stored as float64
    "occupancy": pa.float64(),  # 0/1 but nullable
    "last_means": pa.float64(),  # nullable
    "nominate_number_of_votes": pa.float64(),  # integer counts but nullable
    "nominate_number_of_errors": pa.float64(),  # integer counts but nullable
    # Float columns - NOMINATE scores and years
    "born": pa.float64(),  # year as float (1754.0)
    "died": pa.float64(),  # year as float
    "nominate_dim1": pa.float64(),
    "nominate_dim2": pa.float64(),
    "nominate_log_likelihood": pa.float64(),
    "nominate_geo_mean_probability": pa.float64(),
    "conditional": pa.float64(),  # nullable
    "nokken_poole_dim1": pa.float64(),
    "nokken_poole_dim2": pa.float64(),
    # String columns
    "chamber": pa.string(),  # House/Senate/President
    "state_abbrev": pa.string(),
    "bioname": pa.string(),  # Contains commas: "WASHINGTON, George"
    "bioguide_id": pa.string(),  # Alphanumeric ID: B000084
}

VOTEVIEW_MEMBERS_COLUMNS = [
    "congress",
    "chamber",
    "icpsr",
    "state_icpsr",
    "district_code",
    "state_abbrev",
    "party_code",
    "occupancy",
    "last_means",
    "bioname",
    "bioguide_id",
    "born",
    "died",
    "nominate_dim1",
    "nominate_dim2",
    "nominate_log_likelihood",
    "nominate_geo_mean_probability",
    "nominate_number_of_votes",
    "nominate_number_of_errors",
    "conditional",
    "nokken_poole_dim1",
    "nokken_poole_dim2",
]


# =============================================================================
# ROLLCALLS SCHEMA (18 columns)
# =============================================================================
VOTEVIEW_ROLLCALLS_SCHEMA: dict[str, pa.DataType] = {
    # Integer columns
    "congress": pa.int16(),
    "rollnumber": pa.int32(),
    "yea_count": pa.int32(),
    "nay_count": pa.int32(),
    # Nullable integer columns stored as float64
    "session": pa.float64(),  # nullable
    "clerk_rollnumber": pa.float64(),  # nullable
    # Float columns - NOMINATE parameters
    "nominate_mid_1": pa.float64(),
    "nominate_mid_2": pa.float64(),
    "nominate_spread_1": pa.float64(),
    "nominate_spread_2": pa.float64(),
    "nominate_log_likelihood": pa.float64(),
    # String columns
    "chamber": pa.string(),
    "date": pa.string(),  # ISO format: YYYY-MM-DD
    "bill_number": pa.string(),  # HR2, S17, etc.
    "vote_result": pa.string(),  # nullable
    "vote_desc": pa.string(),  # nullable
    "vote_question": pa.string(),  # nullable
    "dtl_desc": pa.string(),  # long descriptions
}

VOTEVIEW_ROLLCALLS_COLUMNS = [
    "congress",
    "chamber",
    "rollnumber",
    "date",
    "session",
    "clerk_rollnumber",
    "yea_count",
    "nay_count",
    "nominate_mid_1",
    "nominate_mid_2",
    "nominate_spread_1",
    "nominate_spread_2",
    "nominate_log_likelihood",
    "bill_number",
    "vote_result",
    "vote_desc",
    "vote_question",
    "dtl_desc",
]


# =============================================================================
# VOTES SCHEMA (6 columns)
# =============================================================================
# This is the large file (~26M rows) - keep schema minimal
# Some columns have float notation in CSV (e.g., "10713.0")
VOTEVIEW_VOTES_SCHEMA: dict[str, pa.DataType] = {
    # Integer columns (never have float notation)
    "congress": pa.int16(),
    # Columns with potential float notation in CSV
    "rollnumber": pa.float64(),
    "icpsr": pa.float64(),
    "cast_code": pa.float64(),  # 1-9 values but may have ".0" suffix
    # Float columns
    "prob": pa.float64(),  # nullable probability
    # String columns
    "chamber": pa.string(),
}

VOTEVIEW_VOTES_COLUMNS = [
    "congress",
    "chamber",
    "rollnumber",
    "icpsr",
    "cast_code",
    "prob",
]


# =============================================================================
# FILE TYPE CONFIGURATION
# =============================================================================
@dataclass
class FileTypeConfig:
    """Configuration for a specific Voteview file type."""

    schema: dict[str, pa.DataType]
    expected_columns: list[str]
    sum_column: str | None  # Column to sum for checksum validation
    key_columns: list[str]  # Columns to track non-null counts
    sample_size: int = 1000  # Default sample size for validation


FILE_TYPE_CONFIGS: dict[FileType, FileTypeConfig] = {
    FileType.MEMBERS: FileTypeConfig(
        schema=VOTEVIEW_MEMBERS_SCHEMA,
        expected_columns=VOTEVIEW_MEMBERS_COLUMNS,
        sum_column="nominate_number_of_votes",  # Integer sum
        key_columns=["icpsr", "congress", "chamber", "bioname"],
        sample_size=1000,
    ),
    FileType.ROLLCALLS: FileTypeConfig(
        schema=VOTEVIEW_ROLLCALLS_SCHEMA,
        expected_columns=VOTEVIEW_ROLLCALLS_COLUMNS,
        sum_column="yea_count",  # Integer sum
        key_columns=["congress", "chamber", "rollnumber", "date"],
        sample_size=1000,
    ),
    FileType.VOTES: FileTypeConfig(
        schema=VOTEVIEW_VOTES_SCHEMA,
        expected_columns=VOTEVIEW_VOTES_COLUMNS,
        sum_column="cast_code",  # Integer sum (1-9 values)
        key_columns=["congress", "chamber", "rollnumber", "icpsr", "cast_code"],
        sample_size=2000,  # Larger sample for 26M row file
    ),
}


def get_config(file_type: FileType) -> FileTypeConfig:
    """Get configuration for a file type."""
    return FILE_TYPE_CONFIGS[file_type]


# Null markers in Voteview CSVs
NULL_VALUES = ["", "N/A"]
