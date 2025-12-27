# Voteview CSV to Parquet Converter

Converts Voteview congressional voting data from CSV to Parquet format with lossless, non-mutated conversion guaranteed through three-tier validation.

## Source Data

Voteview provides comprehensive congressional roll-call voting data from 1789 to present.

- **Provider:** Lewis, Poole, Rosenthal, Boche, Rudkin, Sonnet
- **URL:** https://voteview.com/
- **License:** Public domain

### Supported File Types

| File Type | Description | Typical Size | Records |
|-----------|-------------|--------------|---------|
| `members` | Congressional member info with NOMINATE scores | ~6MB | ~51K |
| `rollcalls` | Roll call vote metadata | ~29MB | ~112K |
| `votes` | Individual vote records | ~677MB | ~26M |

## Installation

Requires Python 3.13+ and the following dependencies:

```bash
uv pip install pyarrow
```

## Usage

### Command Line

```bash
# Auto-detect file type from filename
python -m voteview_converter HSall_members.csv members.parquet
python -m voteview_converter HSall_rollcalls.csv rollcalls.parquet
python -m voteview_converter HSall_votes.csv votes.parquet

# Explicit file type
python -m voteview_converter input.csv output.parquet -t votes

# Larger batch size for big files
python -m voteview_converter HSall_votes.csv votes.parquet --batch-size 200000

# Skip validation (not recommended)
python -m voteview_converter input.csv output.parquet --no-validate

# Custom sample size for validation
python -m voteview_converter input.csv output.parquet --sample-size 5000
```

### Python API

```python
from voteview_converter import convert_voteview_file, FileType

# Convert with default settings
result = convert_voteview_file(
    "HSall_members.csv",
    "members.parquet",
    FileType.MEMBERS,
)
print(f"Converted {result.row_count:,} rows")
print(f"Validation passed: {result.validation.all_valid}")

# Custom options
result = convert_voteview_file(
    "HSall_votes.csv",
    "votes.parquet",
    FileType.VOTES,
    batch_size=200_000,
    sample_size=5000,
)
```

## Output Schema

All schemas use explicit PyArrow types to prevent inference errors.

### Members Schema (22 columns)

| Column | Type | Description |
|--------|------|-------------|
| congress | int16 | Congress number (1-118) |
| chamber | string | House/Senate/President |
| icpsr | int32 | ICPSR member identifier |
| state_icpsr | int16 | State ICPSR code |
| district_code | int16 | Congressional district |
| state_abbrev | string | Two-letter state code |
| party_code | int16 | Numeric party code |
| bioname | string | Member name (LAST, First) |
| bioguide_id | string | Bioguide identifier |
| nominate_dim1 | float64 | First dimension NOMINATE score |
| nominate_dim2 | float64 | Second dimension NOMINATE score |
| ... | ... | Additional NOMINATE metrics |

### Rollcalls Schema (18 columns)

| Column | Type | Description |
|--------|------|-------------|
| congress | int16 | Congress number |
| chamber | string | House/Senate |
| rollnumber | int32 | Roll call number |
| date | string | Vote date (YYYY-MM-DD) |
| yea_count | int32 | Number of yea votes |
| nay_count | int32 | Number of nay votes |
| bill_number | string | Bill identifier (HR2, S17) |
| dtl_desc | string | Vote description |
| ... | ... | Additional NOMINATE parameters |

### Votes Schema (6 columns)

| Column | Type | Description |
|--------|------|-------------|
| congress | int16 | Congress number |
| chamber | string | House/Senate |
| rollnumber | int32 | Roll call number |
| icpsr | int32 | Member ICPSR ID |
| cast_code | int8 | Vote code (1-9) |
| prob | float64 | Classification probability |

**Vote Codes:**
- 1: Yea
- 2: Paired Yea
- 3: Announced Yea
- 4: Announced Nay
- 5: Paired Nay
- 6: Nay
- 7: Present (not voting)
- 8: Present (Abstain)
- 9: Not in legislature

## Validation

Three-tier validation ensures lossless conversion:

### Tier 1: Row Count
- Fastest check using Parquet metadata
- Verifies no rows were dropped during conversion

### Tier 2: Checksums
- Sum of key numeric column (detects truncation)
- Non-null counts for key columns (detects dropped data)

| File Type | Sum Column | Key Columns |
|-----------|------------|-------------|
| members | nominate_number_of_votes | icpsr, congress, chamber, bioname |
| rollcalls | yea_count | congress, chamber, rollnumber, date |
| votes | cast_code | congress, chamber, rollnumber, icpsr |

### Tier 3: Random Sample Comparison
- Field-by-field comparison of random rows
- Default: 1000 rows (2000 for votes)
- Catches subtle conversion errors

## Technical Details

### Streaming Architecture
- Memory-efficient batch processing (default 100K rows)
- Never loads entire file into memory
- Handles 26M+ row files on modest hardware

### Compression
- Algorithm: ZSTD (Zstandard)
- Level: 3 (balanced speed/compression)
- Typical reduction: 40-50%

### Encoding
- Input: UTF-8 CSV
- Output: Parquet with ZSTD compression

### Null Handling
- Empty strings (`""`) treated as null
- Nullable integers stored as float64

## HuggingFace Upload

After conversion, manually upload validated files to HuggingFace:

```bash
# Target location
https://huggingface.co/datasets/Dustinhax/tyt/tree/main/tyt/voteview/

# Using HuggingFace CLI
huggingface-cli upload Dustinhax/tyt members.parquet tyt/voteview/members.parquet
huggingface-cli upload Dustinhax/tyt rollcalls.parquet tyt/voteview/rollcalls.parquet
huggingface-cli upload Dustinhax/tyt votes.parquet tyt/voteview/votes.parquet
```

## Module Structure

```
scripts/voteview_converter/
├── __init__.py      # Public API exports
├── __main__.py      # Module entry point
├── cli.py           # Command-line interface
├── converter.py     # Core streaming conversion
├── exceptions.py    # Exception hierarchy
├── schema.py        # PyArrow schemas and configs
├── validators.py    # Three-tier validation
└── README.md        # This file
```

## Error Handling

All errors inherit from `VoteviewConversionError` with detailed context:

- `CSVParseError`: Line number, column, problematic value
- `RowCountMismatchError`: Expected vs actual counts
- `ChecksumMismatchError`: Column name, expected/actual values
- `SampleMismatchError`: Row index, column, expected/actual
- `SchemaValidationError`: Missing/extra columns

## Citation

When using this data, cite:

```
Lewis, Jeffrey B., Keith Poole, Howard Rosenthal, Adam Boche,
Aaron Rudkin, and Luke Sonnet. 2024. Voteview: Congressional
Roll-Call Votes Database. https://voteview.com/
```
