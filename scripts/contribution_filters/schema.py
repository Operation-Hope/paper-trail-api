"""Schema definitions for contribution filters."""

from __future__ import annotations

import re

import pyarrow as pa

# =============================================================================
# SOURCE CONFIGURATION
# =============================================================================

HF_BASE_URL = "https://huggingface.co/datasets/Dustinhax/tyt/resolve/main"
CONTRIBUTIONS_URL_TEMPLATE = f"{HF_BASE_URL}/dime/contributions/by_year/contribDB_{{cycle}}.parquet"
RECIPIENTS_URL = f"{HF_BASE_URL}/dime/recipients/dime_recipients_all_1979_2024.parquet"

# Allowed domains for source URLs (SQL injection mitigation)
ALLOWED_SOURCE_DOMAINS = [
    "huggingface.co",
]

# Allowed local directories for source files (path traversal mitigation)
ALLOWED_LOCAL_DIRECTORIES = [
    "/Users/d/projects/tyt/paper-trail-api/",
    "/tmp/",
]


def escape_sql_string(value: str) -> str:
    """Escape a string value for safe SQL interpolation.

    Escapes single quotes and backslashes to prevent SQL injection.

    Args:
        value: String value to escape

    Returns:
        Escaped string safe for SQL interpolation
    """
    # Escape backslashes first, then single quotes
    return value.replace("\\", "\\\\").replace("'", "''")


def validate_path_string(path: str) -> bool:
    """Validate a file path string for safe SQL use.

    Rejects paths containing SQL injection patterns.

    Args:
        path: File path to validate

    Returns:
        True if path is safe for SQL interpolation
    """
    # Reject obvious SQL injection patterns
    dangerous_patterns = [
        r";\s*--",  # SQL comment after semicolon
        r";\s*DROP",  # DROP statement
        r";\s*DELETE",  # DELETE statement
        r";\s*INSERT",  # INSERT statement
        r";\s*UPDATE",  # UPDATE statement
        r"UNION\s+SELECT",  # UNION injection
        r"'\s*OR\s+'",  # OR injection
    ]
    path_upper = path.upper()
    return all(not re.search(pattern, path_upper, re.IGNORECASE) for pattern in dangerous_patterns)


def validate_source_url(url: str) -> bool:
    """Check if source URL is from an allowed domain or is a valid local file path.

    Args:
        url: The URL or local file path to validate

    Returns:
        True if URL is from an allowed domain or is a valid local file path
    """
    from pathlib import Path
    from urllib.parse import urlparse

    # Validate path string for SQL safety
    if not validate_path_string(url):
        return False

    # Allow local file paths within allowed directories
    if url.startswith("/") or url.startswith("./"):
        path = Path(url).resolve()
        if not path.exists():
            return False
        # Check if path is within an allowed directory
        return any(str(path).startswith(allowed_dir) for allowed_dir in ALLOWED_LOCAL_DIRECTORIES)

    parsed = urlparse(url)
    return any(parsed.netloc.endswith(domain) for domain in ALLOWED_SOURCE_DOMAINS)


# =============================================================================
# CYCLE CONFIGURATION
# =============================================================================

# Election cycles (1980-2024, even years)
MIN_CYCLE = 1980
MAX_CYCLE = 2024
ALL_CYCLES = list(range(MIN_CYCLE, MAX_CYCLE + 1, 2))  # 23 cycles


def validate_cycle(cycle: int) -> bool:
    """Check if cycle is valid (even year between MIN_CYCLE and MAX_CYCLE)."""
    return cycle in ALL_CYCLES


# =============================================================================
# FEC ID JOIN PATTERN (for bioguide_id lookup)
# =============================================================================
# Year suffix length to strip from DIME ICPSR (e.g., "S4VT000332020" → "S4VT00033")
YEAR_SUFFIX_LENGTH = 4

# FEC lookup CTE - flattens fec_ids array for joining to DIME contributions
# Uses UNNEST to expand array of FEC IDs into separate rows for matching
FEC_LOOKUP_CTE = """
WITH fec_lookup AS (
    SELECT bioguide_id, UNNEST(fec_ids) as fec_id
    FROM read_parquet('{legislators_path}')
    WHERE fec_ids IS NOT NULL AND LEN(fec_ids) > 0
)
"""

# FEC join condition pattern
# Matches FEC-style ICPSR only (H/S prefix for House/Senate, strips year suffix)
# DIME stores "{fec_id}{year}" format (e.g., "S4VT000332020" = FEC ID + 2020)
# This join only matches ~10% of DIME records (those with FEC-style ICPSR)
FEC_JOIN_CONDITION = """SUBSTRING(CAST({alias}."ICPSR" AS VARCHAR), 1,
              LENGTH(CAST({alias}."ICPSR" AS VARCHAR)) - 4) = fec_lookup.fec_id
    AND (CAST({alias}."ICPSR" AS VARCHAR) LIKE 'H%'
         OR CAST({alias}."ICPSR" AS VARCHAR) LIKE 'S%')"""


# =============================================================================
# SQL QUERIES
# =============================================================================

# Organizational contributions filter
# Filters out individual contributors (contributor.type = 'I')
# Keeps PACs, corporations, committees, unions, and other organizations
ORGANIZATIONAL_QUERY = """
SELECT *
FROM read_parquet('{source_url}')
WHERE "contributor.type" != 'I'
  AND "contributor.type" IS NOT NULL
"""

# Organizational contributions filter WITH bioguide_id (requires legislators lookup)
# JOIN path: contributions → recipients (on bonica.rid) → legislators (on FEC ID)
# LEFT JOINs keep all organizational records even when bioguide_id is NULL
ORGANIZATIONAL_QUERY_WITH_BIOGUIDE = """
WITH fec_lookup AS (
    SELECT bioguide_id, UNNEST(fec_ids) as fec_id
    FROM read_parquet('{legislators_path}')
    WHERE fec_ids IS NOT NULL AND LEN(fec_ids) > 0
),
recipients_with_bioguide AS (
    SELECT DISTINCT
        r."bonica.rid",
        f.bioguide_id
    FROM read_parquet('{recipients_url}') r
    LEFT JOIN fec_lookup f ON
        SUBSTRING(r."ICPSR", 1, LENGTH(r."ICPSR") - 4) = f.fec_id
        AND (r."ICPSR" LIKE 'H%' OR r."ICPSR" LIKE 'S%')
    WHERE r."ICPSR" IS NOT NULL AND LENGTH(r."ICPSR") > 4
)
SELECT DISTINCT
    rb.bioguide_id,
    c.*
FROM read_parquet('{source_url}') c
LEFT JOIN recipients_with_bioguide rb ON c."bonica.rid" = rb."bonica.rid"
WHERE c."contributor.type" != 'I'
  AND c."contributor.type" IS NOT NULL
"""

# Recipient aggregates
# Groups by recipient and calculates total/average contribution amounts
# Includes breakdowns by contributor type (individual vs non-individual)
RECIPIENT_AGGREGATES_QUERY = """
SELECT
    "bonica.rid",
    "recipient.name",
    "recipient.party",
    "recipient.type",
    "recipient.state",
    "candidate.cfscore",
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    COUNT(*) as contribution_count,
    -- Individual contributor breakdown (contributor.type = 'I')
    SUM(CASE WHEN "contributor.type" = 'I' THEN amount ELSE 0 END) as individual_total,
    SUM(CASE WHEN "contributor.type" = 'I' THEN 1 ELSE 0 END) as individual_count,
    -- Organizational contributor breakdown (PACs, corporations, committees, etc.)
    SUM(CASE WHEN "contributor.type" != 'I' AND "contributor.type" IS NOT NULL
        THEN amount ELSE 0 END) as organizational_total,
    SUM(CASE WHEN "contributor.type" != 'I' AND "contributor.type" IS NOT NULL
        THEN 1 ELSE 0 END) as organizational_count
FROM read_parquet('{source_url}')
-- Defensive: all DIME records have bonica.rid, but guard against future edge cases
WHERE "bonica.rid" IS NOT NULL
GROUP BY
    "bonica.rid",
    "recipient.name",
    "recipient.party",
    "recipient.type",
    "recipient.state",
    "candidate.cfscore"
ORDER BY total_amount DESC
"""

# Recipient aggregates WITH bioguide_id (requires legislators lookup)
# JOIN path: contributions → recipients (on bonica.rid) → legislators (on FEC ID)
# bioguide_id will be NULL for ~90% of records (non-FEC ICPSR formats)
RECIPIENT_AGGREGATES_QUERY_WITH_BIOGUIDE = """
WITH fec_lookup AS (
    SELECT bioguide_id, UNNEST(fec_ids) as fec_id
    FROM read_parquet('{legislators_path}')
    WHERE fec_ids IS NOT NULL AND LEN(fec_ids) > 0
),
recipients_with_bioguide AS (
    SELECT DISTINCT
        r."bonica.rid",
        f.bioguide_id
    FROM read_parquet('{recipients_url}') r
    LEFT JOIN fec_lookup f ON
        SUBSTRING(r."ICPSR", 1, LENGTH(r."ICPSR") - 4) = f.fec_id
        AND (r."ICPSR" LIKE 'H%' OR r."ICPSR" LIKE 'S%')
    WHERE r."ICPSR" IS NOT NULL AND LENGTH(r."ICPSR") > 4
),
aggregates AS (
    SELECT
        "bonica.rid",
        "recipient.name",
        "recipient.party",
        "recipient.type",
        "recipient.state",
        "candidate.cfscore",
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount,
        COUNT(*) as contribution_count,
        SUM(CASE WHEN "contributor.type" = 'I' THEN amount ELSE 0 END) as individual_total,
        SUM(CASE WHEN "contributor.type" = 'I' THEN 1 ELSE 0 END) as individual_count,
        SUM(CASE WHEN "contributor.type" != 'I' AND "contributor.type" IS NOT NULL
            THEN amount ELSE 0 END) as organizational_total,
        SUM(CASE WHEN "contributor.type" != 'I' AND "contributor.type" IS NOT NULL
            THEN 1 ELSE 0 END) as organizational_count
    FROM read_parquet('{source_url}')
    WHERE "bonica.rid" IS NOT NULL
    GROUP BY
        "bonica.rid",
        "recipient.name",
        "recipient.party",
        "recipient.type",
        "recipient.state",
        "candidate.cfscore"
)
SELECT DISTINCT
    rb.bioguide_id,
    agg."bonica.rid",
    agg."recipient.name",
    agg."recipient.party",
    agg."recipient.type",
    agg."recipient.state",
    agg."candidate.cfscore",
    agg.total_amount,
    agg.avg_amount,
    agg.contribution_count,
    agg.individual_total,
    agg.individual_count,
    agg.organizational_total,
    agg.organizational_count
FROM aggregates agg
LEFT JOIN recipients_with_bioguide rb ON agg."bonica.rid" = rb."bonica.rid"
ORDER BY agg.total_amount DESC
"""

# Raw organizational contributions (NEW: detailed records with bioguide_id)
# JOIN path: contributions → recipients (on bonica.rid) → legislators (on FEC ID)
# contributor.type != 'I' filters to organizational contributors only
# LEFT JOIN keeps records even when bioguide_id cannot be determined
RAW_ORGANIZATIONAL_CONTRIBUTIONS_QUERY = """
WITH fec_lookup AS (
    SELECT bioguide_id, UNNEST(fec_ids) as fec_id
    FROM read_parquet('{legislators_path}')
    WHERE fec_ids IS NOT NULL AND LEN(fec_ids) > 0
),
recipients_with_bioguide AS (
    SELECT DISTINCT
        r."bonica.rid",
        f.bioguide_id
    FROM read_parquet('{recipients_url}') r
    LEFT JOIN fec_lookup f ON
        SUBSTRING(r."ICPSR", 1, LENGTH(r."ICPSR") - 4) = f.fec_id
        AND (r."ICPSR" LIKE 'H%' OR r."ICPSR" LIKE 'S%')
    WHERE r."ICPSR" IS NOT NULL AND LENGTH(r."ICPSR") > 4
)
SELECT DISTINCT
    rb.bioguide_id,
    c.cycle,
    c."bonica.rid",
    c."recipient.name",
    c."contributor.name" as contributor_name,
    c."contributor.type" as contributor_type,
    c."bonica.cid" as contributor_id,
    c.amount,
    c.date,
    c."contributor.state" as contributor_state
FROM read_parquet('{source_url}') c
LEFT JOIN recipients_with_bioguide rb ON c."bonica.rid" = rb."bonica.rid"
WHERE c."contributor.type" != 'I'
  AND c."contributor.type" IS NOT NULL
"""

# =============================================================================
# OUTPUT SCHEMAS
# =============================================================================

RECIPIENT_AGGREGATES_SCHEMA = pa.schema(
    [
        pa.field("bonica.rid", pa.string(), nullable=False),
        pa.field("recipient.name", pa.string()),
        pa.field("recipient.party", pa.string()),
        pa.field("recipient.type", pa.string()),
        pa.field("recipient.state", pa.string()),
        pa.field("candidate.cfscore", pa.float64()),
        pa.field("total_amount", pa.float64()),
        pa.field("avg_amount", pa.float64()),
        pa.field("contribution_count", pa.int64()),
        pa.field("individual_total", pa.float64()),
        pa.field("individual_count", pa.int64()),
        pa.field("organizational_total", pa.float64()),
        pa.field("organizational_count", pa.int64()),
    ]
)

RECIPIENT_AGGREGATES_COLUMNS = [
    "bonica.rid",
    "recipient.name",
    "recipient.party",
    "recipient.type",
    "recipient.state",
    "candidate.cfscore",
    "total_amount",
    "avg_amount",
    "contribution_count",
    "individual_total",
    "individual_count",
    "organizational_total",
    "organizational_count",
]

# Recipient aggregates schema WITH bioguide_id column
RECIPIENT_AGGREGATES_WITH_BIOGUIDE_SCHEMA = pa.schema(
    [
        pa.field("bioguide_id", pa.string()),  # Nullable - NULL for non-FEC records
        pa.field("bonica.rid", pa.string(), nullable=False),
        pa.field("recipient.name", pa.string()),
        pa.field("recipient.party", pa.string()),
        pa.field("recipient.type", pa.string()),
        pa.field("recipient.state", pa.string()),
        pa.field("candidate.cfscore", pa.float64()),
        pa.field("total_amount", pa.float64()),
        pa.field("avg_amount", pa.float64()),
        pa.field("contribution_count", pa.int64()),
        pa.field("individual_total", pa.float64()),
        pa.field("individual_count", pa.int64()),
        pa.field("organizational_total", pa.float64()),
        pa.field("organizational_count", pa.int64()),
    ]
)

RECIPIENT_AGGREGATES_WITH_BIOGUIDE_COLUMNS = [
    "bioguide_id",
    "bonica.rid",
    "recipient.name",
    "recipient.party",
    "recipient.type",
    "recipient.state",
    "candidate.cfscore",
    "total_amount",
    "avg_amount",
    "contribution_count",
    "individual_total",
    "individual_count",
    "organizational_total",
    "organizational_count",
]

# Raw organizational contributions schema (NEW)
# Detailed contribution records filtered to organizational contributors only
RAW_ORGANIZATIONAL_CONTRIBUTIONS_SCHEMA = pa.schema(
    [
        pa.field("bioguide_id", pa.string()),  # Nullable - NULL for non-FEC records
        pa.field("cycle", pa.int64()),
        pa.field("bonica.rid", pa.string()),
        pa.field("recipient.name", pa.string()),
        pa.field("contributor_name", pa.string()),
        pa.field("contributor_type", pa.string()),
        pa.field("contributor_id", pa.string()),
        pa.field("amount", pa.float64()),
        pa.field("date", pa.string()),
        pa.field("contributor_state", pa.string()),
    ]
)

RAW_ORGANIZATIONAL_CONTRIBUTIONS_COLUMNS = [
    "bioguide_id",
    "cycle",
    "bonica.rid",
    "recipient.name",
    "contributor_name",
    "contributor_type",
    "contributor_id",
    "amount",
    "date",
    "contributor_state",
]

# =============================================================================
# OUTPUT FILE NAMING
# =============================================================================


def get_organizational_filename(cycle: int) -> str:
    """Get output filename for organizational contributions."""
    return f"contribDB_{cycle}_organizational.parquet"


def get_recipient_aggregates_filename(cycle: int) -> str:
    """Get output filename for recipient aggregates."""
    return f"recipient_aggregates_{cycle}.parquet"


def get_raw_organizational_filename(cycle: int) -> str:
    """Get output filename for raw organizational contributions with bioguide_id."""
    return f"organizational_contributions_{cycle}.parquet"
