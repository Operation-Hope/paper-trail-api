# Contribution Filters

This document describes the extraction of filtered contribution datasets from DIME campaign finance data.

## Overview

The contribution filters module extracts three specialized datasets from the DIME contributions data:

1. **Organizational Contributions**: Filters out individual contributors, keeping only committees, corporations, PACs, and other organizational entities
2. **Recipient Aggregates**: Groups contributions by recipient ID with summary statistics
3. **Raw Organizational Contributions**: Detailed organizational contribution records with legislator linking via `bioguide_id`

All outputs span 23 election cycles (1980-2024) and are validated for correctness before publishing.

### Legislator Linking (Optional)

All output types can optionally include a `bioguide_id` column that links recipients to Congress legislators. This enables:
- Filtering contributions to specific legislators
- Joining with congressional voting records
- Building legislator-centric campaign finance analyses

The join path: **contributions → recipients (on `bonica.rid`) → legislators (via FEC ID from ICPSR)**

**Expected Coverage**: ~10% of records match (only recipients with FEC-style ICPSR codes).

## Source Data

- **Source**: DIME contributions Parquet files on HuggingFace
- **Dataset**: `Dustinhax/tyt` (original DIME conversion)
- **Format**: Apache Parquet with ZSTD compression
- **Path pattern**: `dime/contributions/by_year/contribDB_{cycle}.parquet`
- **Total source size**: ~58 GB across 23 cycles

## Extraction Methodology

### DuckDB Streaming Architecture

The extraction uses DuckDB's `read_parquet()` for remote streaming queries, avoiding the need to download source files:

```sql
-- Organizational filter
COPY (
    SELECT *
    FROM read_parquet('https://huggingface.co/.../contribDB_2024.parquet')
    WHERE "contributor.type" != 'I'
      AND "contributor.type" IS NOT NULL
) TO 'output.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3)
```

This approach:
- Streams data directly from remote source to local output
- Never loads entire datasets into memory
- Uses SQL-based filtering for correctness

### Compression

- **Algorithm**: ZSTD (Zstandard)
- **Level**: 3 (balanced speed/ratio)

## Output Types

### Organizational Contributions

Filters contributions to exclude individual contributors (`contributor.type = 'I'`).

**Filter Logic**:
```sql
WHERE "contributor.type" != 'I'
  AND "contributor.type" IS NOT NULL
```

**Contributor Types Included**:
- `C` - Committee
- `L` - Corporation
- `O` - Organization
- `U` - Union
- `P` - PAC
- Other non-individual types

**Output Schema**: Same as source contributions (45 columns)

**Typical Reduction**: 15-25% of source rows retained (organizational contributions are a minority)

### Recipient Aggregates

Groups contributions by recipient with computed summary statistics.

**Aggregation Logic**:
```sql
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
    -- Individual contributor breakdown
    SUM(CASE WHEN "contributor.type" = 'I' THEN amount ELSE 0 END) as individual_total,
    SUM(CASE WHEN "contributor.type" = 'I' THEN 1 ELSE 0 END) as individual_count,
    -- Organizational contributor breakdown
    SUM(CASE WHEN "contributor.type" != 'I' THEN amount ELSE 0 END) as organizational_total,
    SUM(CASE WHEN "contributor.type" != 'I' THEN 1 ELSE 0 END) as organizational_count
FROM source
-- Defensive: all DIME records have bonica.rid, but guard against future edge cases
WHERE "bonica.rid" IS NOT NULL
GROUP BY "bonica.rid", "recipient.name", "recipient.party",
         "recipient.type", "recipient.state", "candidate.cfscore"
ORDER BY total_amount DESC
```

**Output Schema** (14 columns with `--legislators-path`):

| Column | Type | Description |
|--------|------|-------------|
| `bioguide_id` | string | Congress legislator ID (nullable, ~10% coverage) |
| `bonica.rid` | string | Recipient ID (not nullable) |
| `recipient.name` | string | Recipient name |
| `recipient.party` | string | Party affiliation |
| `recipient.type` | string | Recipient type code |
| `recipient.state` | string | State code |
| `candidate.cfscore` | float64 | CFscore ideology measure |
| `total_amount` | float64 | Sum of all contributions |
| `avg_amount` | float64 | Average contribution size |
| `contribution_count` | int64 | Number of contributions |
| `individual_total` | float64 | Sum from individual contributors |
| `individual_count` | int64 | Count from individual contributors |
| `organizational_total` | float64 | Sum from PACs, corps, committees |
| `organizational_count` | int64 | Count from PACs, corps, committees |

### Raw Organizational Contributions

Detailed organizational contribution records with legislator linking. This output type **requires** `--legislators-path`.

**Filter Logic**:
```sql
WHERE "contributor.type" != 'I'
  AND "contributor.type" IS NOT NULL
```

**Output Schema** (10 columns):

| Column | Type | Description |
|--------|------|-------------|
| `bioguide_id` | string | Congress legislator ID (nullable, ~10% coverage) |
| `cycle` | int64 | Election cycle year |
| `bonica.rid` | string | Recipient ID |
| `recipient.name` | string | Recipient name |
| `contributor_name` | string | Organization name |
| `contributor_type` | string | Contributor type code (C, L, O, U, P, etc.) |
| `contributor_id` | string | DIME contributor ID |
| `amount` | float64 | Contribution amount |
| `date` | string | Contribution date |
| `contributor_state` | string | Contributor state |

**Use Cases**:
- Detailed analysis of PAC/corporate giving patterns
- Linking specific contributions to legislators
- Building contributor-to-legislator networks

## Validation Suite

Every extracted file passes a **two-tier validation** suite before being accepted:

### Organizational Filter Validation

#### Tier 1: Completeness Check

Verifies the filter reduced the row count (sanity check that filtering worked):

```
Row count: PASS (output < source)
```

#### Tier 2: Filter Integrity

Scans the entire output file to confirm no individual contributors remain:

```sql
SELECT COUNT(*)
FROM output.parquet
WHERE "contributor.type" = 'I'
-- Must equal 0
```

```
Filter validation: PASS (0 individual contributors found)
```

### Recipient Aggregates Validation

#### Tier 1: Completeness Check

Confirms output contains recipient records:

```
Distinct recipients: PASS (output_count > 0)
```

#### Tier 2: Aggregation Integrity (Sample-Based)

Randomly samples 100 recipient IDs and verifies:

1. **Count accuracy**: `contribution_count` matches actual count in source
2. **Sum accuracy**: `total_amount` matches sum in source (tolerance: $0.01)

```
Aggregation validation: PASS (100 recipients verified)
```

## Output Dataset

The filtered datasets are available on HuggingFace:

**https://huggingface.co/datasets/Dustinhax/paper-trail-data**

### Directory Structure

```
contributions/
├── organizational/
│   ├── contribDB_1980_organizational.parquet
│   ├── contribDB_1982_organizational.parquet
│   ├── ...
│   └── contribDB_2024_organizational.parquet
└── recipient_aggregates/
    ├── recipient_aggregates_1980.parquet
    ├── recipient_aggregates_1982.parquet
    ├── ...
    └── recipient_aggregates_2024.parquet
```

**23 files per output type, 46 files total**

## Usage

### CLI

```bash
# Single cycle
contribution-filters output/ --cycle 2020

# All cycles
contribution-filters output/ --all

# Cycle range
contribution-filters output/ --start-cycle 2000 --end-cycle 2020

# Specific output type
contribution-filters output/ --cycle 2020 --output-type aggregates

# With legislator linking (adds bioguide_id column)
contribution-filters output/ --cycle 2020 --legislators-path /path/to/legislators.parquet

# Raw organizational contributions (requires --legislators-path)
contribution-filters output/ --cycle 2020 --output-type raw-organizational \
    --legislators-path /path/to/legislators.parquet

# Resume interrupted run
contribution-filters output/ --all --skip-existing

# Rate limit mitigation
contribution-filters output/ --all --delay 30
```

### CLI Options

| Option | Description |
|--------|-------------|
| `output_dir` | Output directory for parquet files |
| `--cycle` | Single election cycle (even year 1980-2024) |
| `--all` | Process all cycles (1980-2024) |
| `--start-cycle` | Start of cycle range |
| `--end-cycle` | End of cycle range |
| `--output-type` | `organizational`, `aggregates`, `raw-organizational`, or `all` (default) |
| `--legislators-path` | Path to legislators.parquet for bioguide_id lookup |
| `--no-validate` | Skip validation (not recommended) |
| `--sample-size` | Sample size for aggregation validation (default: 100) |
| `--skip-existing` | Skip files that already exist |
| `--delay` | Delay in seconds between cycles (helps with rate limiting) |

**Note**: `raw-organizational` output type requires `--legislators-path` to be specified.

### Programmatic

```python
from contribution_filters import (
    extract_organizational_contributions,
    extract_recipient_aggregates,
    extract_raw_organizational_contributions,
)

# Organizational contributions
result = extract_organizational_contributions(
    "output/organizational/contribDB_2020_organizational.parquet",
    cycle=2020,
    validate=True,
)
print(f"Extracted {result.output_count:,} organizational contributions")
print(f"Validation: {'PASS' if result.validation.all_valid else 'FAIL'}")

# Organizational contributions with legislator linking
result = extract_organizational_contributions(
    "output/organizational/contribDB_2020_organizational.parquet",
    cycle=2020,
    legislators_path="/path/to/legislators.parquet",
    validate=True,
)
print(f"Bioguide ID coverage: {result.validation.bioguide_coverage_pct:.1f}%")

# Recipient aggregates
result = extract_recipient_aggregates(
    "output/recipient_aggregates/recipient_aggregates_2020.parquet",
    cycle=2020,
    validate=True,
    sample_size=100,
)
print(f"Extracted {result.output_count:,} recipient aggregate records")
print(f"Validation: {'PASS' if result.validation.all_valid else 'FAIL'}")

# Raw organizational contributions (requires legislators_path)
result = extract_raw_organizational_contributions(
    "output/raw_organizational/organizational_contributions_2020.parquet",
    cycle=2020,
    legislators_path="/path/to/legislators.parquet",
    validate=True,
)
print(f"Extracted {result.output_count:,} raw organizational contributions")
```

### Custom Source URL

For local files or alternative sources:

```python
result = extract_organizational_contributions(
    "output.parquet",
    cycle=2020,
    source_url="/path/to/local/contribDB_2020.parquet",
)
```

## Data Integrity Guarantees

1. **No data mutation**: Filter queries use `SELECT *` with `WHERE` clauses only
2. **Schema preservation**: Output retains all source columns for organizational filter
3. **Aggregation accuracy**: Sample-based verification confirms SUM/COUNT correctness
4. **Type safety**: Explicit PyArrow schema for recipient aggregates output
5. **SQL injection prevention**: Source URLs validated against allowlist of domains

## Files

The extraction scripts are located in `scripts/contribution_filters/`:

| File | Description |
|------|-------------|
| `extractor.py` | Core DuckDB extraction logic |
| `validators.py` | Two-tier validation suite |
| `schema.py` | SQL queries, schemas, and constants |
| `exceptions.py` | Custom exception hierarchy |
| `cli.py` | Command-line interface |
| `__main__.py` | Module entry point for `python -m` invocation |
| `__init__.py` | Package exports |
| `pyproject.toml` | Package metadata and dependencies |

## Technical Notes

### Memory Efficiency

The DuckDB `COPY (query) TO` pattern streams directly from source to output:

- No intermediate DataFrame or table materialization
- Constant memory usage regardless of source file size
- Handles 14GB source files (2020 cycle) on modest hardware

### Source URL Validation

For security, source URLs are validated against an allowlist:

```python
ALLOWED_SOURCE_DOMAINS = ["huggingface.co"]
```

Local file paths are permitted from `/tmp/` by default. Additional directories can be allowed via the `DIME_ALLOWED_DIRS` environment variable (colon-separated):

```bash
export DIME_ALLOWED_DIRS="/path/to/data:/another/path"
```

### FEC ID Format and Legislator Linking

The `bioguide_id` column links DIME recipients to Congress legislators via FEC candidate IDs. The join works as follows:

1. **DIME ICPSR Format**: Recipients store ICPSR codes like `H0DC000012020` or `S4VT000332020`
   - Prefix: `H` (House) or `S` (Senate)
   - FEC ID: The core identifier (e.g., `0DC00001`, `4VT00033`)
   - Suffix: 4-digit year

2. **Legislators FEC IDs**: The `legislators.parquet` file contains `fec_ids` arrays
   - Format: `H0DC00001` or `S4VT00033` (without year suffix)

3. **Join Logic**: Strips the year suffix from ICPSR and matches against FEC IDs
   ```sql
   SUBSTRING(ICPSR, 1, LENGTH(ICPSR) - 4) = fec_id
   AND (ICPSR LIKE 'H%' OR ICPSR LIKE 'S%')
   ```

**Why ~10% Coverage?**
- DIME uses multiple ICPSR format conventions
- Only records with FEC-style prefixes (H/S) can be matched
- Historical records before FEC ID adoption won't match

### Error Handling

Custom exceptions provide detailed error context:

- `InvalidCycleError`: Cycle not in 1980-2024 even years
- `InvalidSourceURLError`: URL not from allowed domain
- `SourceReadError`: Failed to read from source
- `OutputWriteError`: Failed to write output file
- `FilterValidationError`: Individual contributors found in output
- `AggregationIntegrityError`: SUM/COUNT mismatch detected
- `CompletenessError`: Missing or invalid output data
- `BioguideJoinError`: Invalid bioguide_ids found in output (not in legislators file)

### Rate Limiting

When streaming from HuggingFace, HTTP 429 errors may occur. Mitigations:

1. Use `--delay` flag to add pauses between cycles
2. Use `--skip-existing` to resume interrupted runs
3. Download files locally with `huggingface_hub`, then process with local paths
