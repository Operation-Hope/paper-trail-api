"""Schema definitions for legislator-recipient crosswalk extraction."""

from typing import Final

import pyarrow as pa

# =============================================================================
# SOURCE CONFIGURATION
# =============================================================================

# HuggingFace dataset URL for DIME Recipients
HF_BASE_URL = "https://huggingface.co/datasets/Dustinhax/tyt/resolve/main"
DIME_RECIPIENTS_URL = f"{HF_BASE_URL}/dime/recipients/dime_recipients_all_1979_2024.parquet"

# Allowed domains for source URLs (SQL injection mitigation)
ALLOWED_SOURCE_DOMAINS: Final[list[str]] = [
    "huggingface.co",
]


def validate_source_url(url: str) -> bool:
    """Check if source URL is from an allowed domain.

    Args:
        url: The URL to validate

    Returns:
        True if URL is from an allowed domain
    """
    from urllib.parse import urlparse

    parsed = urlparse(url)
    return any(parsed.netloc.endswith(domain) for domain in ALLOWED_SOURCE_DOMAINS)


# =============================================================================
# OUTPUT SCHEMA
# =============================================================================

CROSSWALK_SCHEMA = pa.schema(
    [
        # ICPSR identifier (links to distinct_legislators)
        pa.field("icpsr", pa.string(), nullable=False),
        # DIME recipient identifier (links to contributions)
        pa.field("bonica_rid", pa.string(), nullable=False),
        # Recipient metadata
        pa.field("recipient_name", pa.string()),
        pa.field("party", pa.string()),
        pa.field("state", pa.string()),
        pa.field("seat", pa.string()),
        pa.field("fec_id", pa.string()),
    ]
)

CROSSWALK_COLUMNS: Final[list[str]] = [
    "icpsr",
    "bonica_rid",
    "recipient_name",
    "party",
    "state",
    "seat",
    "fec_id",
]

# Columns used for validation checksums
KEY_COLUMNS: Final[list[str]] = ["icpsr", "bonica_rid"]

# =============================================================================
# EXTRACTION SQL
# =============================================================================

# Query extracts unique mappings from ICPSR to bonica_rid
# Each row represents one DIME recipient record for a legislator
#
# NOTE: DIME stores ICPSR as "{icpsr}{year}" (e.g., "100751980" = ICPSR 10075 + year 1980)
# We extract just the ICPSR portion by removing the last 4 characters (the year)
#
# Uses GROUP BY on (icpsr, bonica_rid) to get one row per unique pair,
# taking arbitrary values for metadata columns (FIRST_VALUE or ANY_VALUE would also work)
EXTRACTION_QUERY = """
WITH extracted AS (
    SELECT
        SUBSTRING(CAST("ICPSR" AS VARCHAR), 1, LENGTH(CAST("ICPSR" AS VARCHAR))-4) as icpsr,
        "bonica.rid" as bonica_rid,
        "name" as recipient_name,
        "party",
        "state",
        "seat",
        "FEC.ID" as fec_id
    FROM read_parquet('{source_url}')
    WHERE "ICPSR" IS NOT NULL
      AND "ICPSR" != ''
      AND LENGTH(CAST("ICPSR" AS VARCHAR)) > 4
      AND "bonica.rid" IS NOT NULL
      AND "bonica.rid" != ''
)
SELECT
    icpsr,
    bonica_rid,
    MAX(recipient_name) as recipient_name,
    MAX(party) as party,
    MAX(state) as state,
    MAX(seat) as seat,
    MAX(fec_id) as fec_id
FROM extracted
GROUP BY icpsr, bonica_rid
ORDER BY icpsr, bonica_rid
"""

# =============================================================================
# DATA INTERPRETATION NOTES
# =============================================================================

DATA_INTERPRETATION = """
## Data Interpretation Decisions

### Source Data
- **Source:** DIME Recipients (dime_recipients_all_1979_2024.parquet)
- **Original structure:** One row per recipient entity with ICPSR and bonica_rid
- **Output structure:** Distinct mappings between ICPSR and bonica_rid

### ICPSR Format Transformation
- DIME stores ICPSR as "{icpsr}{year}" (e.g., "100751980" = ICPSR 10075 + year 1980)
- Voteview stores ICPSR as integers (e.g., 10075)
- We extract just the ICPSR portion by removing the last 4 characters (the year)
- This allows proper joining between distinct_legislators.icpsr and crosswalk.icpsr

### Filtering
- **ICPSR filter:** WHERE ICPSR IS NOT NULL AND ICPSR != '' AND LENGTH > 4
  - Only includes recipients who have a valid ICPSR (legislators)
- **bonica_rid filter:** WHERE bonica.rid IS NOT NULL AND bonica.rid != ''
  - Ensures valid DIME recipient IDs

### Key Relationships
- A single legislator (one ICPSR) may have multiple bonica_rids
  - Different election cycles create new recipient records
  - Example: A Senator running for re-election gets new bonica_rids each cycle
- This is a 1:many relationship (one ICPSR to many bonica_rids)

### Usage
Join chain to link legislators with contributions:
1. distinct_legislators.icpsr → crosswalk.icpsr (cast types to match)
2. crosswalk.bonica_rid → contributions.bonica_rid (1:many)

Example SQL:
```sql
SELECT l.bioname, x.bonica_rid, oc.amount
FROM distinct_legislators l
JOIN legislator_recipient_crosswalk x ON CAST(l.icpsr AS VARCHAR) = x.icpsr
JOIN organizational_contributions oc ON x.bonica_rid = oc."bonica.rid"
WHERE l.bioguide_id = 'C000127';
```

### Column Sources
| Output Column | Source Column | Transformation |
|--------------|---------------|----------------|
| icpsr | ICPSR | Extract first N-4 chars (removes year suffix) |
| bonica_rid | bonica.rid | Direct copy |
| recipient_name | name | Direct copy |
| party | party | Direct copy |
| state | state | Direct copy |
| seat | seat | Direct copy |
| fec_id | FEC.ID | Direct copy |
"""
