"""Schema definitions for contribution filters."""

from __future__ import annotations

import re

import pyarrow as pa

# =============================================================================
# SOURCE CONFIGURATION
# =============================================================================

HF_BASE_URL = "https://huggingface.co/datasets/Dustinhax/tyt/resolve/main"
CONTRIBUTIONS_URL_TEMPLATE = f"{HF_BASE_URL}/dime/contributions/by_year/contribDB_{{cycle}}.parquet"

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

# =============================================================================
# OUTPUT FILE NAMING
# =============================================================================


def get_organizational_filename(cycle: int) -> str:
    """Get output filename for organizational contributions."""
    return f"contribDB_{cycle}_organizational.parquet"


def get_recipient_aggregates_filename(cycle: int) -> str:
    """Get output filename for recipient aggregates."""
    return f"recipient_aggregates_{cycle}.parquet"
