# Distinct Legislators Extractor

Extracts a deduplicated list of legislators from Voteview's HSall_members data, aggregating congress sessions served for each legislator (96th congress onward = 1979+).

## Source Data

- **Source:** Voteview HSall_members.parquet
- **URL:** HuggingFace dataset `Dustinhax/tyt`
- **Original structure:** One row per legislator per congress session
- **Output structure:** One row per legislator (aggregated)

### Filtering

- **Congress filter:** >= 96 (1979-1980 onward)
- **Null filter:** `bioguide_id IS NOT NULL` (excludes ~17 records without bioguide)
  - These are typically Presidents or historical members without bioguide IDs

## Installation

Requires Python 3.13+ and the following dependencies:

```bash
uv pip install duckdb pyarrow
```

## Usage

### Command Line

```bash
# Basic usage
python -m distinct_legislators legislators.parquet

# Custom congress filter
python -m distinct_legislators legislators.parquet --min-congress 100

# Skip validation (not recommended)
python -m distinct_legislators legislators.parquet --no-validate

# Custom sample sizes for validation
python -m distinct_legislators legislators.parquet --aggregation-sample 200 --deep-sample 100
```

### Python API

```python
from distinct_legislators import extract_distinct_legislators

# Extract with default settings
result = extract_distinct_legislators("legislators.parquet")
print(f"Extracted {result.output_count:,} legislators")
print(f"Validation passed: {result.validation.all_valid}")

# Custom options
result = extract_distinct_legislators(
    "legislators.parquet",
    min_congress=100,  # 100th congress (1987) onward
    aggregation_sample_size=200,
    deep_sample_size=100,
)
```

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| bioguide_id | VARCHAR | Primary key (unique per legislator) |
| bioname | VARCHAR | Full name from Voteview (most recent) |
| state_abbrev | VARCHAR | Most recent state represented |
| party_code | DOUBLE | Most recent party (100=Dem, 200=Rep) |
| congresses_served | INT16[] | Array of all congress numbers served |
| first_congress | INT16 | Earliest congress (MIN) for range queries |
| last_congress | INT16 | Latest congress (MAX) for range queries |
| nominate_dim1 | DOUBLE | Economic liberalism/conservatism score |
| nominate_dim2 | DOUBLE | Social issues score |

### Congress Numbers to Years

Congress N covers years `(1787 + 2*N)` to `(1788 + 2*N)`:
- Congress 96 = 1979-1980
- Congress 100 = 1987-1988
- Congress 119 = 2025-2026

## Data Interpretation Decisions

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
3. **Gaps in service:** `congresses_served` array handles non-consecutive terms
4. **Presidents:** Excluded (no bioguide_id in Voteview data)

## Validation

Three-tier validation ensures correct aggregation:

### Tier 1: Completeness

Verifies every source bioguide_id appears exactly once in output:
- Output count matches distinct source count
- No missing bioguide_ids
- No extra bioguide_ids
- No duplicates in output

### Tier 2: Aggregation Integrity

Randomly samples legislators and verifies:
- `first_congress` = MIN(congress) from source
- `last_congress` = MAX(congress) from source
- `congresses_served` array length matches source row count

### Tier 3: Sample Verification

Deep validation of random legislators:
- `congresses_served` array contains exactly the right congress numbers
- `bioname` matches the most recent congress entry
- `state_abbrev` matches the most recent congress entry

## Technical Details

### Compression

- Algorithm: ZSTD (Zstandard)
- Typical output size: ~53 KB for ~2,300 legislators

### Query Engine

- Uses DuckDB for efficient remote Parquet reading
- Direct URL access to HuggingFace dataset

## Module Structure

```
scripts/distinct_legislators/
├── __init__.py      # Public API exports
├── __main__.py      # Module entry point
├── cli.py           # Command-line interface
├── extractor.py     # Core extraction logic
├── exceptions.py    # Exception hierarchy
├── schema.py        # Schema and aggregation SQL
├── validators.py    # Three-tier validation
└── README.md        # This file
```

## Error Handling

All errors inherit from `DistinctLegislatorsError` with detailed context:

- `SourceReadError`: Source URL and error details
- `CompletenessError`: Expected/actual counts, missing/extra IDs
- `AggregationError`: bioguide_id, field name, expected/actual values
- `SampleValidationError`: Same as above plus sample index
- `OutputWriteError`: Output path and error details

## HuggingFace Upload

After extraction, upload validated files to the processed data repository:

```bash
# Target location (processed data, not raw)
https://huggingface.co/datasets/Dustinhax/paper-trail-data

# Using HuggingFace CLI
hf upload Dustinhax/paper-trail-data distinct_legislators.parquet distinct_legislators.parquet --repo-type dataset
```

Note: Raw source data lives in `Dustinhax/tyt`. Processed/transformed data goes to `Dustinhax/paper-trail-data`.

## Citation

When using this data, cite the original Voteview source:

```
Lewis, Jeffrey B., Keith Poole, Howard Rosenthal, Adam Boche,
Aaron Rudkin, and Luke Sonnet. 2024. Voteview: Congressional
Roll-Call Votes Database. https://voteview.com/
```
