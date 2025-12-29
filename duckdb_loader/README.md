# Local Development: Load DIME Data

This guide shows how to load the [DIME campaign finance dataset](https://huggingface.co/datasets/Dustinhax/tyt) from Huggingface into a local database for development and analysis.

## Dataset Overview

- **915 million** contribution records (1979-2024)
- **58.5 GB** in Parquet format
- **43 columns** including contributor/recipient info, amounts, dates, ideology scores

## Installation

```bash
cd duckdb_loader
uv pip install -e .
```

## Quick Start

Choose your target database:

| Target | Best For | Setup |
|--------|----------|-------|
| **DuckDB** | Quick analysis, exploration, no database setup | Just run the command |
| **PostgreSQL** | Full-stack development, testing API integration | Requires running PostgreSQL |
| **Paper Trail** | Pre-processed datasets (legislators, aggregates) | Requires running PostgreSQL |

### Option 1: DuckDB (Recommended for Analysis)

```bash
# Load a sample (100k rows) - fastest way to get started
duckdb-loader load sample.duckdb --limit 100000

# Load specific election cycles
duckdb-loader load cycles.duckdb -c 2020 -c 2022 -c 2024

# Load last 4 cycles, California only, large contributions
duckdb-loader load ca_large.duckdb --recent 4 -s CA --min-amount 1000

# Check what you loaded
duckdb-loader info ca_large.duckdb

# Run a query
duckdb-loader query ca_large.duckdb "SELECT COUNT(*) FROM contributions"
```

### Option 2: PostgreSQL (For Full-Stack Development)

```bash
# Set your database URL (or export DATABASE_URL)
export DATABASE_URL="postgresql://user:password@localhost:5432/papertrail"

# Load a sample (100k rows)
duckdb-loader load-postgres $DATABASE_URL --limit 100000

# Load 2024 cycle only
duckdb-loader load-postgres $DATABASE_URL -c 2024

# Load last 4 cycles, California only
duckdb-loader load-postgres $DATABASE_URL --recent 4 -s CA
```

**Note:** Column names with dots (e.g., `contributor.name`) are converted to underscores (e.g., `contributor_name`) in PostgreSQL for compatibility.

### Option 3: Paper Trail Data (Processed Datasets)

Load pre-processed datasets from [Dustinhax/paper-trail-data](https://huggingface.co/datasets/Dustinhax/paper-trail-data):

| Dataset | Description | Size |
|---------|-------------|------|
| `legislators` | Unique legislators from Voteview | ~2,300 rows |
| `organizational` | Organizational donor contributions only | ~15M rows/cycle |
| `recipient_aggregates` | Pre-computed totals by recipient | ~20k rows/cycle |

```bash
# Load all datasets for 2024
duckdb-loader load-paper-trail $DATABASE_URL -c 2024

# Load only legislators (always loads fully)
duckdb-loader load-paper-trail $DATABASE_URL -d legislators

# Load organizational contributions for last 4 cycles
duckdb-loader load-paper-trail $DATABASE_URL -d organizational --recent 4

# Load specific datasets
duckdb-loader load-paper-trail $DATABASE_URL -d legislators -d recipient_aggregates -c 2024
```

### Python API

```python
# DuckDB
from duckdb_loader import load_to_duckdb, CycleFilter, StateFilter, AmountFilter

result = load_to_duckdb(
    "contributions.duckdb",
    filters=[
        CycleFilter([2020, 2022, 2024]),
        StateFilter(["CA", "NY", "TX"]),
        AmountFilter(min_amount=100),
    ],
)
print(f"Loaded {result.rows_loaded:,} rows")

# PostgreSQL (raw DIME data)
from duckdb_loader import load_to_postgres

result = load_to_postgres(
    "postgresql://localhost/papertrail",
    filters=[CycleFilter([2024])],
    limit=100_000,
)
print(f"Loaded {result.rows_loaded:,} rows")

# PostgreSQL (paper-trail processed datasets)
from duckdb_loader import load_paper_trail_to_postgres

result = load_paper_trail_to_postgres(
    "postgresql://localhost/papertrail",
    datasets=["legislators", "organizational", "recipient_aggregates"],
    filters=[CycleFilter([2024])],
)
print(f"Loaded {result.total_rows_loaded:,} total rows")
print(f"  Legislators: {result.legislators_loaded:,}")
print(f"  Organizational: {result.organizational_loaded:,}")
print(f"  Recipient Aggregates: {result.recipient_aggregates_loaded:,}")
```

## Filter Options

### By Election Cycle

```python
from duckdb_loader import CycleFilter, recent_cycles

# Specific cycles
CycleFilter([2020, 2022])

# Last N cycles (preset)
recent_cycles(4)  # 2018, 2020, 2022, 2024
```

### By State

```python
from duckdb_loader import StateFilter

# Contributor state
StateFilter(["CA", "NY"])

# Recipient state
StateFilter(["TX"], field="recipient.state")
```

### By Amount

```python
from duckdb_loader import AmountFilter

AmountFilter(min_amount=1000)  # $1,000+
AmountFilter(max_amount=100)   # Up to $100
AmountFilter(min_amount=100, max_amount=5000)  # Range
```

### Combining Filters

All filters are combined with AND logic:

```python
result = load_to_duckdb(
    "my_subset.duckdb",
    filters=[
        CycleFilter([2024]),
        StateFilter(["CA"]),
        AmountFilter(min_amount=1000),
    ],
)
# Loads: 2024 California contributions over $1,000
```

## Querying the Database

### Using the CLI

```bash
duckdb-loader query contributions.duckdb \
    "SELECT cycle, SUM(amount) FROM contributions GROUP BY cycle"
```

### Using Python

```python
from duckdb_loader.loader import query_database

results = query_database(
    "contributions.duckdb",
    """
    SELECT
        "recipient.name",
        "recipient.party",
        SUM(amount) as total
    FROM contributions
    GROUP BY "recipient.name", "recipient.party"
    ORDER BY total DESC
    LIMIT 10
    """
)

for name, party, total in results:
    print(f"{name} ({party}): ${total:,.2f}")
```

### Using DuckDB Directly

```python
import duckdb

conn = duckdb.connect("contributions.duckdb", read_only=True)

# Use SQL
df = conn.execute("""
    SELECT cycle, COUNT(*) as n, SUM(amount) as total
    FROM contributions
    GROUP BY cycle
    ORDER BY cycle
""").fetchdf()

print(df)
```

## Available Columns

The default load includes these columns (subset of 43 available):

| Column | Type | Description |
|--------|------|-------------|
| `cycle` | INTEGER | Election cycle year |
| `transaction.id` | VARCHAR | Unique transaction ID |
| `amount` | DOUBLE | Contribution amount |
| `date` | VARCHAR | Transaction date |
| `bonica.cid` | VARCHAR | Contributor ID |
| `contributor.name` | VARCHAR | Contributor name |
| `contributor.state` | VARCHAR | Contributor state |
| `contributor.occupation` | VARCHAR | Contributor occupation |
| `contributor.employer` | VARCHAR | Contributor employer |
| `contributor.cfscore` | DOUBLE | Contributor ideology score |
| `bonica.rid` | VARCHAR | Recipient ID |
| `recipient.name` | VARCHAR | Recipient name |
| `recipient.party` | VARCHAR | Recipient party |
| `recipient.state` | VARCHAR | Recipient state |
| `candidate.cfscore` | DOUBLE | Candidate ideology score |
| `seat` | VARCHAR | Office sought |

### Loading All Columns

```python
from duckdb_loader.schema import ALL_COLUMNS

result = load_to_duckdb(
    "full.duckdb",
    columns=ALL_COLUMNS,
    limit=10000,
)
```

## Size Estimates

| Subset | Rows | Size |
|--------|------|------|
| Sample (100k rows) | 100,000 | ~10 MB |
| Single cycle (2024) | ~40M | ~4 GB |
| Last 4 cycles | ~160M | ~16 GB |
| Single state (CA) | ~100M | ~10 GB |
| Full dataset | 915M | ~60 GB |

## Example Queries

### Top Recipients

```sql
SELECT
    "recipient.name",
    "recipient.party",
    COUNT(*) as contributions,
    SUM(amount) as total
FROM contributions
GROUP BY "recipient.name", "recipient.party"
ORDER BY total DESC
LIMIT 20;
```

### Contributions by State

```sql
SELECT
    "contributor.state" as state,
    COUNT(*) as n,
    SUM(amount) as total,
    AVG(amount) as avg
FROM contributions
WHERE "contributor.state" IS NOT NULL
GROUP BY "contributor.state"
ORDER BY total DESC;
```

### Party Totals by Cycle

```sql
SELECT
    cycle,
    "recipient.party" as party,
    SUM(amount) as total
FROM contributions
WHERE "recipient.party" IN ('D', 'R')
GROUP BY cycle, "recipient.party"
ORDER BY cycle, party;
```

### Large Individual Donors

```sql
SELECT
    "contributor.name",
    "contributor.employer",
    COUNT(*) as contributions,
    SUM(amount) as total
FROM contributions
WHERE amount >= 10000
GROUP BY "contributor.name", "contributor.employer"
ORDER BY total DESC
LIMIT 50;
```

## Troubleshooting

### Memory Issues

For large loads, the data streams in batches (default 100k rows). If you still run out of memory:

```python
# Reduce batch size
result = load_to_duckdb(
    "large.duckdb",
    batch_size=50_000,  # Smaller batches
)
```

### Slow Loading

Loading streams directly from Huggingface. Ensure you have a stable internet connection. Consider:
- Starting with a smaller subset using `--limit`
- Using filters to reduce data volume
- Loading during off-peak hours

### Column Names with Dots

DuckDB requires quoting column names with dots:

```sql
-- Correct
SELECT "contributor.name" FROM contributions

-- Wrong (will error)
SELECT contributor.name FROM contributions
```
