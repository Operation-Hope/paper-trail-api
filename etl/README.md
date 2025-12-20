# Paper Trail ETL Pipeline

ETL pipeline for downloading, transforming, and loading campaign finance and congressional voting data into the Paper Trail database.

## Overview

This ETL pipeline processes data from multiple sources:
- **DIME** (Database on Ideology, Money in Politics, and Elections) - Campaign contributions
- **Voteview** - Federal legislators and voting records
- **Congressional Bills Project** - Bill topics and classifications
- **Congress.gov** - Legislative data

The pipeline has three phases:
1. **Download** - Fetch raw data from S3 and public sources
2. **Transform** - Clean, normalize, and prepare data
3. **Load** - Bulk insert into PostgreSQL database

## Prerequisites

### System Requirements
- Python 3.13+
- ~50GB disk space for full data load
- PostgreSQL 15+ database (Supabase recommended)

### Software Dependencies

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install project dependencies
cd etl
uv sync
```

### AWS Access

Campaign contribution data is stored in a private S3 bucket. You need AWS credentials with read access.

**To request access**: Contact repository maintainers for credentials to `s3://paper-trail-dime/`.

Configure AWS CLI:
```bash
# Install AWS CLI
brew install awscli  # macOS
# or
apt-get install awscli  # Linux

# Configure credentials
aws configure
# AWS Access Key ID: [your-access-key]
# AWS Secret Access Key: [your-secret-key]
# Default region: us-east-1
# Default output format: json

# Verify access
aws s3 ls s3://paper-trail-dime/filtered-parquet/
```

### Database Setup

Set the `DATABASE_URL` environment variable:

```bash
# Supabase (recommended)
export DATABASE_URL="postgresql://postgres:[password]@db.[project-ref].supabase.co:5432/postgres"

# Local PostgreSQL
export DATABASE_URL="postgresql://localhost/paper_trail"
```

## Quick Start

The easiest way to run the full ETL pipeline:

```bash
# Download, transform, and load all data (1980-2024)
uv run download_and_load.py --transform --load

# Download and load data for specific year
uv run download_and_load.py --year 2020 --transform --load

# Download specific year range
uv run download_and_load.py --start-year 2016 --end-year 2024 --transform --load
```

## Usage

### Setup Directory Structure

```bash
./setup.sh
```

Creates required directories:
- `../data/raw/contributions/` - Downloaded contribution files
- `../data/raw/voteview/` - Voteview member data
- `../data/raw/recipients/` - DIME recipients
- `../data/raw/contributors/` - DIME contributors
- `../data/transformed/` - Cleaned data ready for loading

### Download Data

#### Option 1: Unified Download Script (Recommended)

```bash
# Download all data (1980-2024)
uv run download_and_load.py

# Download specific year
uv run download_and_load.py --year 2020

# Download year range
uv run download_and_load.py --start-year 2016 --end-year 2024

# Dry run (show what would be downloaded)
uv run download_and_load.py --year 2020 --dry-run

# Force re-download
uv run download_and_load.py --force
```

#### Option 2: Individual Download Scripts

```bash
# Download contributions from DIME S3 bucket
./download_contributions.sh

# Download Voteview data
./download_voteview.sh

# Download Congressional Bills Project data
./download_cbp.sh

# Download Congress.gov data
./download_congressgov.sh
```

### Transform Data

Transform raw data into normalized format:

```bash
# Transform politicians
uv run transform_politicians.py

# Transform donors
uv run transform_donors.py

# Transform contributions
uv run transform_contributions.py

# Transform voting records
uv run transform_rollcalls.py
uv run transform_votes.py

# Transform bill topics
uv run transform_bill_topics.py
```

Or use the unified script:

```bash
# Transform after download
uv run download_and_load.py --skip-download --transform
```

### Load Into Database

Load transformed data into PostgreSQL:

```bash
# Load politicians
uv run load_politicians.py

# Load donors
uv run load_donors.py

# Load contributions
uv run load_contributions_optimized.py

# Load voting records
uv run load_votes.py

# Load bill topics
uv run load_bill_topics.py
```

Or use the unified script:

```bash
# Load after transform
uv run download_and_load.py --skip-download --transform --load
```

### Create Indexes and Views

```bash
# Create database indexes for performance
psql $DATABASE_URL -f create_indexes.sql

# Create materialized views
psql $DATABASE_URL -f create_donation_summary_view.sql
```

### Validate Data

```bash
# Validate database integrity
uv run validate_database.py

# Validate indexes
uv run validate_indexes.py

# Smoke tests
uv run smoke_test_politicians.py
uv run smoke_test_donors.py
uv run smoke_test_contributions.py
uv run smoke_test_rollcalls_votes.py
```

## Pipeline Options

### Year Filtering

Contribution data is available for election years (1980-2024, biennial):

```bash
# Single year
uv run download_and_load.py --year 2020 --transform --load

# Year range
uv run download_and_load.py --start-year 2016 --end-year 2024 --transform --load

# All years (default)
uv run download_and_load.py --transform --load
```

### Pipeline Control

```bash
# Skip download, use existing files
uv run download_and_load.py --skip-download --transform --load

# Transform only (no download or load)
uv run download_and_load.py --skip-download --transform

# Verbose output
uv run download_and_load.py --transform --load --verbose
```

## File Structure

```
etl/
├── README.md                          # This file
├── pyproject.toml                     # Python dependencies
├── setup.sh                           # Create directory structure
│
├── download_and_load.py               # Unified download/transform/load script
├── download_*.sh                      # Individual download scripts
│
├── transform_*.py                     # Data transformation scripts
├── load_*.py                          # Database loading scripts
│
├── create_indexes.sql                 # Database indexes
├── create_donation_summary_view.sql   # Materialized views
├── schema.sql                         # ETL staging schema
│
├── validate_*.py                      # Data validation scripts
├── smoke_test_*.py                    # Quick validation tests
│
└── utils/
    └── normalization.py               # Data normalization utilities
```

## Data Sources

### DIME (Database on Ideology, Money in Politics, and Elections)
- **Source**: Private S3 bucket (`s3://paper-trail-dime/`)
- **Contains**: Campaign contributions, recipients, contributors
- **Coverage**: 1980-2024 (election years)
- **Format**: Parquet files

### Voteview
- **Source**: https://voteview.com/static/data/out
- **Contains**: Federal legislators, NOMINATE scores
- **Format**: CSV

### Congressional Bills Project
- **Source**: http://www.congressionalbills.org/
- **Contains**: Bill topics and classifications
- **Format**: CSV/XML

### Congress.gov
- **Source**: https://www.congress.gov/
- **Contains**: Bill status, sponsors, cosponsors
- **Format**: XML

## Troubleshooting

### AWS Access Issues

```bash
# Verify AWS credentials
aws sts get-caller-identity

# Test S3 bucket access
aws s3 ls s3://paper-trail-dime/filtered-parquet/

# Check environment variables
echo $AWS_ACCESS_KEY_ID
echo $AWS_SECRET_ACCESS_KEY
```

### Database Connection Issues

```bash
# Test database connection
psql $DATABASE_URL -c "SELECT version();"

# Check environment variable
echo $DATABASE_URL
```

### Disk Space Issues

```bash
# Check available space
df -h .

# Estimated space needed:
# - Full load (1980-2024): ~45 GB raw + transformed
# - Single year: ~3-4 GB
# - Database: ~25-30 GB for full data
```

### Memory Issues

For large data loads, you may need to adjust batch sizes in load scripts:

```python
# In load_contributions_optimized.py
BATCH_SIZE = 10000  # Reduce if running out of memory
```

## Performance

Typical execution times (varies by system and network):

- **Download** (all years): 30-60 minutes
- **Transform** (all years): 60-90 minutes
- **Load** (all years): 120-180 minutes
- **Total**: 4-6 hours for full pipeline

Single year loads are significantly faster (~30-60 minutes total).

## Support

For issues or questions:
- Check the main project documentation in `../docs/`
- Review `../docs/ETL_REBUILD_GUIDE.md` for detailed instructions
- Contact repository maintainers for AWS access or technical support

## License

See main project LICENSE file.
