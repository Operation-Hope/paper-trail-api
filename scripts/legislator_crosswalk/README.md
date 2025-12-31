# Legislator-Recipient Crosswalk Extractor

Extracts a crosswalk table mapping legislators (via ICPSR) to DIME recipient IDs (bonica_rid), enabling joins between Voteview legislator data and DIME campaign contribution data.

## Purpose

Links two different identifier systems:
- **Voteview/distinct_legislators:** Uses `icpsr` (integer) to identify legislators
- **DIME contributions:** Uses `bonica_rid` (string) to identify recipients

**Join chain:**
```
distinct_legislators.icpsr → crosswalk.icpsr → crosswalk.bonica_rid → contributions.bonica_rid
```

## Source Data

- **Source:** DIME Recipients (dime_recipients_all_1979_2024.parquet)
- **URL:** HuggingFace dataset `Dustinhax/tyt`
- **Original structure:** One row per recipient entity with ICPSR and bonica_rid
- **Output structure:** Distinct mappings between ICPSR and bonica_rid

### ICPSR Format

DIME stores ICPSR as `{icpsr}{year}` (e.g., "100751980" = ICPSR 10075 + year 1980). We extract just the ICPSR portion by removing the last 4 characters (the year suffix), enabling proper joins with Voteview's integer ICPSR values.

### Filtering

- **ICPSR filter:** `WHERE ICPSR IS NOT NULL AND ICPSR != '' AND LENGTH > 4`
  - Only includes recipients who have a valid ICPSR (legislators)
- **bonica_rid filter:** `WHERE bonica.rid IS NOT NULL AND bonica.rid != ''`
  - Ensures valid DIME recipient IDs

## Installation

Requires Python 3.13+ and the following dependencies:

```bash
uv pip install duckdb pyarrow
```

## Usage

### Command Line

```bash
# Basic usage
python -m legislator_crosswalk crosswalk.parquet

# Skip validation (not recommended)
python -m legislator_crosswalk crosswalk.parquet --no-validate

# Custom sample size for validation
python -m legislator_crosswalk crosswalk.parquet --sample-size 200

# Custom source URL
python -m legislator_crosswalk crosswalk.parquet --source-url https://example.com/data.parquet
```

### Python API

```python
from legislator_crosswalk import extract_crosswalk

# Extract with default settings
result = extract_crosswalk("crosswalk.parquet")
print(f"Extracted {result.output_count:,} crosswalk rows")
print(f"Unique legislators: {result.unique_icpsr_count:,}")
print(f"Unique recipients: {result.unique_bonica_rid_count:,}")

# Custom options
result = extract_crosswalk(
    "crosswalk.parquet",
    validate=True,
    sample_size=200,
)
```

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| icpsr | VARCHAR | ICPSR identifier (links to distinct_legislators) |
| bonica_rid | VARCHAR | DIME recipient ID (links to contributions) |
| recipient_name | VARCHAR | Recipient name from DIME |
| party | VARCHAR | Party affiliation |
| state | VARCHAR | State code |
| seat | VARCHAR | Office sought |
| fec_id | VARCHAR | FEC committee ID |

### Key Relationships

- A single legislator (one ICPSR) may have multiple bonica_rids
  - Different election cycles create new recipient records
  - Example: A Senator running for re-election gets new bonica_rids each cycle
- This is a 1:many relationship (one ICPSR to many bonica_rids)

## Data Interpretation Decisions

### Column Transformations

| Output Column | Source Column | Transformation |
|--------------|---------------|----------------|
| icpsr | ICPSR | Extract first N-4 chars (removes year suffix) |
| bonica_rid | bonica.rid | Direct copy |
| recipient_name | name | MAX() aggregation for duplicates |
| party | party | MAX() aggregation for duplicates |
| state | state | MAX() aggregation for duplicates |
| seat | seat | MAX() aggregation for duplicates |
| fec_id | FEC.ID | MAX() aggregation for duplicates |

### Deduplication

The extraction uses `GROUP BY icpsr, bonica_rid` to ensure unique key pairs. When multiple source rows have the same (icpsr, bonica_rid), metadata columns use `MAX()` aggregation.

## Validation

Three-tier validation ensures correct extraction:

### Tier 1: Counts

Verifies:
- Output has rows
- All icpsr values are non-null and non-empty
- All bonica_rid values are non-null and non-empty
- Row count matches expected distinct pairs from source

### Tier 2: Uniqueness

Verifies:
- No duplicate (icpsr, bonica_rid) pairs exist
- Reports unique counts for both columns

### Tier 3: Sample Verification

Randomly samples output rows and verifies:
- Each (icpsr, bonica_rid) mapping exists in source data
- Uses parameterized queries to prevent SQL injection from malicious parquet data

## Technical Details

### Compression

- Algorithm: ZSTD (Zstandard) level 3
- Typical output size: ~3.7 MB for ~426K crosswalk rows

### Query Engine

- Uses DuckDB for efficient remote Parquet reading
- Direct URL access to HuggingFace dataset
- Context manager pattern for connection handling

### Security

- Source URL validation against allowed domains (SQL injection mitigation)
- Parameterized queries for sample validation (prevents injection from malicious data)

## Module Structure

```
scripts/legislator_crosswalk/
├── __init__.py      # Public API exports
├── __main__.py      # Module entry point
├── cli.py           # Command-line interface
├── extractor.py     # Core extraction logic
├── exceptions.py    # Exception hierarchy
├── schema.py        # Schema, constants, and extraction SQL
├── validators.py    # Three-tier validation
└── README.md        # This file
```

## Error Handling

All errors inherit from `CrosswalkError` with detailed context:

- `InvalidSourceURLError`: URL and allowed domains list
- `SourceReadError`: Source URL and error details
- `ValidationError`: Expected/actual counts
- `DuplicateKeyError`: Duplicate count and sample duplicate pairs
- `OutputWriteError`: Output path and error details

## Example Queries

### Get contributions to a specific legislator

```sql
SELECT l.bioname, oc.contributor_name, oc.amount, oc.cycle
FROM distinct_legislators l
JOIN legislator_recipient_crosswalk x ON CAST(l.icpsr AS VARCHAR) = x.icpsr
JOIN organizational_contributions oc ON x.bonica_rid = oc.bonica_rid
WHERE l.bioguide_id = 'C000127';
```

### Aggregate contributions per legislator

```sql
SELECT l.bioguide_id, l.bioname, SUM(oc.amount) as total
FROM distinct_legislators l
LEFT JOIN legislator_recipient_crosswalk x ON CAST(l.icpsr AS VARCHAR) = x.icpsr
LEFT JOIN organizational_contributions oc ON x.bonica_rid = oc.bonica_rid
GROUP BY l.bioguide_id, l.bioname
ORDER BY total DESC NULLS LAST;
```

## HuggingFace Upload

After extraction, upload validated files to the processed data repository:

```bash
# Target location (processed data, not raw)
https://huggingface.co/datasets/Dustinhax/paper-trail-data

# Using HuggingFace CLI
hf upload Dustinhax/paper-trail-data legislator_crosswalk.parquet legislator_crosswalk.parquet --repo-type dataset
```

Note: Raw source data lives in `Dustinhax/tyt`. Processed/transformed data goes to `Dustinhax/paper-trail-data`.

## Citation

When using this data, cite the original DIME source:

```
Bonica, Adam. 2024. Database on Ideology, Money in Politics,
and Elections: Public version 4.0. Stanford, CA: Stanford
University Libraries. https://data.stanford.edu/dime
```
