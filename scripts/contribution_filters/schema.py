"""Schema definitions for contribution filters."""

from __future__ import annotations

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


def validate_source_url(url: str) -> bool:
    """Check if source URL is from an allowed domain or is a local file path.

    Args:
        url: The URL or local file path to validate

    Returns:
        True if URL is from an allowed domain or is a valid local file path
    """
    from pathlib import Path
    from urllib.parse import urlparse

    # Allow local file paths
    if url.startswith("/") or url.startswith("./"):
        return Path(url).exists()

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
