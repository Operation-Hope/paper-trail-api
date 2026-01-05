# Congress Legislators CSV to Parquet Converter

Converts Congress Legislators data from [unitedstates/congress-legislators](https://github.com/unitedstates/congress-legislators) CSV format to Parquet with lossless validation.

## Data Source

- **URL**: https://unitedstates.github.io/congress-legislators/
- **Files**:
  - `legislators-current.csv` - Currently serving legislators (~540 rows)
  - `legislators-historical.csv` - All historical legislators (~12,500 rows)

## Installation

```bash
cd scripts/congress_legislators_converter
uv pip install pyarrow
```

## Usage

### Download and Convert All Files

```bash
uv run -m congress_legislators_converter all --output-dir ./data
```

### Create Unified Legislators File

After running `all`, create a merged file with enhanced schema:

```bash
uv run -m congress_legislators_converter unified --output-dir ./data
```

This creates `legislators.parquet` with:
- Legislators from Congress 96 onward (1979+) by default
- Deduplicated by `bioguide_id` (current takes precedence)
- `fec_ids` parsed from comma-separated string to array
- `icpsr` cast from string to int64
- `is_current` flag to identify current legislators

#### Congress Filtering Options

By default, the `unified` command filters to Congress 96+ (1979 onward) to match DIME contribution data coverage:

```bash
# Default: Congress 96+ (~2,400 legislators)
uv run -m congress_legislators_converter unified --output-dir ./data

# Custom minimum congress
uv run -m congress_legislators_converter unified --output-dir ./data --min-congress 100

# Include all legislators (no filtering, ~12,700 legislators)
uv run -m congress_legislators_converter unified --output-dir ./data --all-congresses
```

### Download Only

```bash
# Download all files
uv run -m congress_legislators_converter download --output-dir ./data

# Download specific file
uv run -m congress_legislators_converter download --output-dir ./data -t current
```

### Convert Only

```bash
uv run -m congress_legislators_converter convert legislators-current.csv legislators-current.parquet
```

### Options

**General:**
- `--no-validate`: Skip validation (not recommended)
- `--sample-size N`: Number of rows to sample for validation
- `--batch-size N`: Rows per batch for streaming (default: 100,000)

**Unified command:**
- `--min-congress N`: Minimum congress to include (default: 96, i.e. 1979+)
- `--all-congresses`: Include all legislators (no congress filtering)

## Schema

All 36 columns are stored as strings for lossless conversion:

| Column | Description |
|--------|-------------|
| `last_name` | Legislator's last name |
| `first_name` | First name |
| `middle_name` | Middle name |
| `suffix` | Name suffix (Jr., Sr., etc.) |
| `nickname` | Common nickname |
| `full_name` | Complete display name |
| `birthday` | Birth date (YYYY-MM-DD) |
| `gender` | M/F |
| `type` | sen (Senator) or rep (Representative) |
| `state` | 2-letter state code |
| `district` | Congressional district (representatives only) |
| `senate_class` | Senate class 1/2/3 (senators only) |
| `party` | Political party |
| `url` | Official website URL |
| `address` | Office address |
| `phone` | Office phone number |
| `contact_form` | Contact form URL |
| `rss_url` | RSS feed URL |
| `twitter` | Twitter handle |
| `twitter_id` | Twitter numeric ID |
| `facebook` | Facebook handle |
| `youtube` | YouTube channel |
| `youtube_id` | YouTube channel ID |
| `mastodon` | Mastodon handle |
| `bioguide_id` | Biographical Directory ID (primary key) |
| `thomas_id` | THOMAS system ID |
| `opensecrets_id` | OpenSecrets CRP ID |
| `lis_id` | Legislative Information System ID |
| `fec_ids` | FEC candidate IDs (comma-separated) |
| `cspan_id` | C-SPAN ID |
| `govtrack_id` | GovTrack ID |
| `votesmart_id` | VoteSmart ID |
| `ballotpedia_id` | Ballotpedia ID |
| `washington_post_id` | Washington Post ID |
| `icpsr_id` | ICPSR ID (for Voteview cross-reference) |
| `wikipedia_id` | Wikipedia article title |

## Validation

Three-tier validation ensures lossless conversion:

1. **Row Count**: Verifies Parquet row count matches source CSV
2. **Checksums**: Verifies non-null counts for key columns (bioguide_id, icpsr_id, state, type)
3. **Sample Comparison**: Field-by-field comparison of random rows

## Output Format

- **Compression**: zstd level 3
- **All columns**: String type (pa.string())
- **Null handling**: Empty strings converted to null

## Unified Legislators Schema

The `unified` command creates `legislators.parquet` with an enhanced schema optimized for downstream joins:

| Column | Type | Description |
|--------|------|-------------|
| `bioguide_id` | STRING | Primary key (non-nullable) |
| `last_name` | STRING | Legislator's last name |
| `first_name` | STRING | First name |
| `full_name` | STRING | Complete display name |
| `birthday` | STRING | Birth date (YYYY-MM-DD) |
| `gender` | STRING | M/F |
| `type` | STRING | sen/rep (most recent) |
| `state` | STRING | 2-letter code (most recent) |
| `party` | STRING | Political party (most recent) |
| `icpsr` | INT64 | ICPSR ID for Voteview joins (nullable) |
| `fec_ids` | LIST\<STRING\> | Array of FEC candidate IDs |
| `opensecrets_id` | STRING | OpenSecrets CRP ID |
| `is_current` | BOOLEAN | True for currently serving legislators |

### Coverage Statistics

Typical coverage for unified output with default Congress 96+ filter (~2,400 legislators):
- **FEC IDs**: ~50-60% (legislators with campaign finance records)
- **ICPSR**: ~95%+ (nearly all modern legislators)

Without filtering (~12,700 legislators from 1789):
- **FEC IDs**: ~12% (FEC data starts 1979)
- **ICPSR**: ~96%

## Cross-references

The `icpsr` column (unified) or `icpsr_id` column (source) can be used to join with Voteview data:
- Voteview Members: `icpsr` column
- Voteview Votes: `icpsr` column

The `fec_ids` array can be used to join with DIME contribution data:
- DIME Recipients: `ICPSR` column (contains FEC IDs with year suffix)
