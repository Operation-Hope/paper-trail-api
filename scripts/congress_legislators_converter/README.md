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

- `--no-validate`: Skip validation (not recommended)
- `--sample-size N`: Number of rows to sample for validation
- `--batch-size N`: Rows per batch for streaming (default: 100,000)

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

## Cross-references

The `icpsr_id` column can be used to join with Voteview data:
- Voteview Members: `icpsr` column
- Voteview Votes: `icpsr` column
