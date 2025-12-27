"""Schema definitions for distinct legislators output."""

import pyarrow as pa

# =============================================================================
# SOURCE CONFIGURATION
# =============================================================================

# HuggingFace dataset URL
HF_BASE_URL = "https://huggingface.co/datasets/Dustinhax/tyt/resolve/main"
VOTEVIEW_MEMBERS_URL = f"{HF_BASE_URL}/voteview/HSall_members.parquet"

# Allowed domains for source URLs (SQL injection mitigation)
ALLOWED_SOURCE_DOMAINS = [
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


# Congress filter: 96th congress (1979-1980) and later
# Congress 96 started January 3, 1979
MIN_CONGRESS = 96

# Congress number to year mapping (approximate - congress starts in odd years)
# Congress N covers years (1787 + 2*N) to (1788 + 2*N)
# Example: Congress 96 = 1979-1980, Congress 119 = 2025-2026


def congress_to_years(congress: int) -> tuple[int, int]:
    """Convert congress number to start/end years."""
    start_year = 1787 + 2 * congress
    end_year = start_year + 1
    return (start_year, end_year)


# =============================================================================
# OUTPUT SCHEMA
# =============================================================================

DISTINCT_LEGISLATORS_SCHEMA = pa.schema(
    [
        # Primary identifier
        pa.field("bioguide_id", pa.string(), nullable=False),
        # Biographical info (most recent values)
        pa.field("bioname", pa.string()),
        pa.field("state_abbrev", pa.string()),
        pa.field("party_code", pa.float64()),  # 100=Democrat, 200=Republican
        # Congress sessions served
        pa.field("congresses_served", pa.list_(pa.int16())),
        pa.field("first_congress", pa.int16()),
        pa.field("last_congress", pa.int16()),
        # Ideology scores (most recent values)
        pa.field("nominate_dim1", pa.float64()),  # Economic left-right
        pa.field("nominate_dim2", pa.float64()),  # Social conservatism
    ]
)

DISTINCT_LEGISLATORS_COLUMNS = [
    "bioguide_id",
    "bioname",
    "state_abbrev",
    "party_code",
    "congresses_served",
    "first_congress",
    "last_congress",
    "nominate_dim1",
    "nominate_dim2",
]

# Columns used for validation checksums
KEY_COLUMNS = ["bioguide_id", "first_congress", "last_congress"]

# =============================================================================
# AGGREGATION SQL
# =============================================================================

AGGREGATION_QUERY = """
SELECT
    bioguide_id,
    LAST(bioname ORDER BY congress) as bioname,
    LAST(state_abbrev ORDER BY congress) as state_abbrev,
    LAST(party_code ORDER BY congress) as party_code,
    LIST(congress ORDER BY congress) as congresses_served,
    MIN(congress)::SMALLINT as first_congress,
    MAX(congress)::SMALLINT as last_congress,
    LAST(nominate_dim1 ORDER BY congress) as nominate_dim1,
    LAST(nominate_dim2 ORDER BY congress) as nominate_dim2
FROM read_parquet('{source_url}')
WHERE congress >= {min_congress}
  AND bioguide_id IS NOT NULL
GROUP BY bioguide_id
ORDER BY bioguide_id
"""

# =============================================================================
# DATA INTERPRETATION NOTES
# =============================================================================

DATA_INTERPRETATION = """
## Data Interpretation Decisions

### Source Data
- **Source:** Voteview HSall_members.parquet
- **Original structure:** One row per legislator per congress session
- **Output structure:** One row per legislator (aggregated)

### Filtering
- **Congress filter:** >= 96 (1979-1980 onward)
- **Null filter:** bioguide_id IS NOT NULL (excludes ~17 records without bioguide)
  - These are typically Presidents or historical members without bioguide IDs

### Aggregation Rules
| Field | Aggregation | Rationale |
|-------|-------------|-----------|
| bioguide_id | GROUP BY | Primary key, unique per legislator |
| bioname | LAST by congress | Name format may change; use most recent |
| state_abbrev | LAST by congress | Legislators may change states (rare) |
| party_code | LAST by congress | Party affiliation may change over career |
| congresses_served | LIST ordered | Complete history of all sessions served |
| first_congress | MIN | Earliest congress served (career start) |
| last_congress | MAX | Latest congress served (current or end) |
| nominate_dim1 | LAST by congress | Ideology score from most recent session |
| nominate_dim2 | LAST by congress | Ideology score from most recent session |

### Party Codes
| Code | Party |
|------|-------|
| 100 | Democrat |
| 200 | Republican |
| 328 | Independent |
| Other | Historical parties (Whig, Federalist, etc.) |

### NOMINATE Scores
- **dim1:** Economic liberalism/conservatism (-1 to +1, negative=liberal)
- **dim2:** Social issues/civil rights (-1 to +1, interpretation varies by era)
- Scores are session-specific; we keep the most recent for simplicity

### Known Edge Cases
1. **Party switchers:** Uses most recent party (e.g., Arlen Specter shows Democrat)
2. **State changers:** Uses most recent state (rare, but possible)
3. **Gaps in service:** congresses_served array handles non-consecutive terms
4. **Presidents:** Excluded (no bioguide_id in Voteview data)
"""
