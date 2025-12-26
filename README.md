# Paper Trail ETL Pipeline 🚧

<p align="center">
  <strong>Campaign Finance Data Pipeline</strong>
  <br />
  Download, transform, and load political contribution data into PostgreSQL
  <br />
  💰 Campaign Contributions • 🏛️ Politicians • 👥 Donors
</p>

> 🚧 **Work in Progress** - This project is under active development
>
> **Note**: This repository temporarily houses both the ETL pipeline and API for ease of iteration. Once each component reaches a more stable state, they will be separated into independent repositories.

---

## Overview

This ETL pipeline processes campaign finance data from DIME (Database on Ideology, Money in Politics, and Elections) and loads it into a PostgreSQL database.

### Data Sources

**DIME** (Database on Ideology, Money in Politics, and Elections)
- Campaign contributions (1980-2024) - ~290 million transaction records
- Politicians/Recipients - Federal candidates who received contributions
- Donors/Contributors - Individuals and organizations making contributions
- Source: Private S3 bucket (AWS credentials required)

#### Coming Soon

Additional data sources in development:
- **Voteview** - Roll call voting records (1789-present), NOMINATE scores, individual legislator votes on bills
- **Congressional Bills Project** - Manual bill topic coding using Policy Agendas taxonomy (1980-2002, 21 categories)
- **Congress.gov** - Library of Congress bill metadata and subject indexing (2015-2024, 966+ subject terms)

#### Under Exploration

Data sources explored but not yet integrated:
- **Open States** - State legislators, governors, and municipal leaders (50 states + territories)
- **FEC Candidate Master Files** - Direct Federal Election Commission candidate data
- **FEC Schedule A/B** - Individual and other receipts/disbursements detail records
- **FEC Committees & PACs** - Committee master files, PAC acronyms, principal campaign committees
- **OpenSecrets** - Industry categorization codes (catcodes) for enhanced donor classification
- **IRS 990 Forms** - Tax-exempt organization filings and Exempt Organization Business Master File
- **IRS Political Organization Filings** - Form 8871 (notifications) and 8872 (periodic reports)
- **DOL Union Filings** - LM-2/LM-3/LM-4 labor union financial disclosures
- **LDA LD-203** - Lobbying Disclosure Act contribution reports
- **SEC CIK/Ticker** - Corporate identifier reference data
- **GLEIF LEI** - Legal Entity Identifier global database

---

## Data Sources & Licensing

### Public Domain
- **FEC** - Candidate files, Schedule A/B, Committees/PACs
- **IRS** - 990 forms, Exempt Org BMF, Form 8871/8872
- **Congress.gov** - Bill metadata and subject indexing
- **DOL** - LM-2/3/4 union filings
- **LDA LD-203** - Lobbying contribution reports
- **SEC** - CIK/Ticker reference data
- **GLEIF LEI** - Legal Entity Identifier (CC0)

### Open License (Non-Commercial)
- **Voteview** - CC BY-NC 4.0 (attribution required, non-commercial only)
  - Citation: Lewis, Jeffrey B., Keith Poole, Howard Rosenthal, Adam Boche, Aaron Rudkin, and Luke Sonnet. 2024. Voteview: Congressional Roll-Call Votes Database. https://voteview.com/

### Open License (Check Terms)
- **Open States** - Open Data Commons

### Academic Use (Transformed/Derived)
- **DIME** - [Stanford DIME Project](https://data.stanford.edu/dime)
  - Citation: Bonica, Adam. 2024. Database on Ideology, Money in Politics, and Elections: Public version 4.0. Stanford, CA: Stanford University Libraries.
  - Used for non-commercial research and public transparency
  - NIMSP/CRP state-level records excluded (CC BY-NC-SA, requires separate access)
- **Congressional Bills Project** - Academic dataset (1980-2002)

### Not Included
- OpenSecrets bulk data (requires agreement)
- NIMSP/CRP itemized state contributions

### Disclaimer
This project provides tools for processing publicly available political finance data. The data is provided "as is" without warranty of any kind. Users are responsible for verifying data accuracy and complying with all applicable data source licenses. This project is intended for research, journalism, and public transparency purposes.

---

### What It Does

1. **Download** - Fetches raw data from S3 (DIME)
2. **Transform** - Cleans, normalizes, and prepares data for database insertion
3. **Load** - Bulk inserts data into PostgreSQL with optimized batch processing

---

## Quick Start

### Prerequisites

- **[Python 3.13+](https://www.python.org/downloads/)**
- **[uv](https://docs.astral.sh/uv/getting-started/installation/)** package manager
- **[PostgreSQL 15+](https://www.postgresql.org/download/)** database
- **[AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)** with credentials for S3 access (contact maintainers)

### Installation

```bash
# Clone repository
git clone https://github.com/Operation-Hope/paper-trail-api.git
cd paper-trail-api/etl

# Install dependencies
uv sync
```

### Configuration

Set your database connection:

```bash
export DATABASE_URL="postgresql://user:password@host:5432/database"
```

Configure AWS credentials (required for DIME data):

```bash
aws configure
# AWS Access Key ID: [your-access-key]
# AWS Secret Access Key: [your-secret-key]
# Default region: us-east-1
```

**Note:** Campaign contribution data is in a private S3 bucket. Contact repository maintainers for AWS credentials.

### Run the Pipeline

```bash
cd etl

# Download, transform, and load all data (1980-2024)
uv run download_and_load.py --transform --load

# Load specific year
uv run download_and_load.py --year 2020 --transform --load

# Load year range
uv run download_and_load.py --start-year 2016 --end-year 2024 --transform --load
```

---

## Usage

### Full Pipeline

The `download_and_load.py` script handles all three phases:

```bash
# Complete pipeline for all years
uv run download_and_load.py --transform --load

# Dry run (see what would be downloaded)
uv run download_and_load.py --year 2020 --dry-run

# Skip download, use existing files
uv run download_and_load.py --skip-download --transform --load

# Verbose output
uv run download_and_load.py --transform --load --verbose
```

### Individual Phases

You can also run each phase separately:

**Transform Data**
```bash
uv run transform_politicians.py
uv run transform_donors.py
uv run transform_contributions.py
```

**Load Into Database**
```bash
uv run load_politicians.py
uv run load_donors.py
uv run load_contributions_optimized.py
```

**Create Indexes**
```bash
psql $DATABASE_URL -f create_indexes.sql
psql $DATABASE_URL -f create_donation_summary_view.sql
```

---

## Database Schema

The pipeline creates these core tables:

- **`politicians`** - Federal legislators with biographical and ideological data
- **`donors`** - Individual and organizational contributors
- **`contributions`** - Campaign finance transactions

And this materialized view:

- **`canonical_politician_industry_summary`** - Pre-aggregated donation totals by politician and industry

---

## Data Coverage

### Campaign Contributions
- **Years**: 1980-2024 (election years, biennial)
- **Records**: ~290 million contributions
- **Source**: DIME

### Politicians
- **Coverage**: Federal candidates who received contributions
- **Includes**: NOMINATE scores, party, state, district
- **Source**: DIME recipients data

### Donors
- **Coverage**: Individual and organizational contributors
- **Includes**: Donor type (individual/PAC), employer, occupation, industry classification
- **Source**: DIME contributors data

---

## Year Filtering

Contribution data is available for election years only:

```bash
# Valid years: 1980, 1982, 1984, ..., 2022, 2024

# Single year
uv run download_and_load.py --year 2020 --transform --load

# Year range
uv run download_and_load.py --start-year 2016 --end-year 2024 --transform --load

# All available years (default)
uv run download_and_load.py --transform --load
```

---

## Performance

Typical execution times (varies by system and network):

| Phase | Time (Single Year) | Time (All Years) |
|-------|-------------------|------------------|
| Download | 5-10 minutes | 30-60 minutes |
| Transform | 10-20 minutes | 60-90 minutes |
| Load | 15-30 minutes | 120-180 minutes |
| **Total** | **30-60 minutes** | **4-6 hours** |

### Resource Requirements

- **Disk Space**: ~3-4 GB per year, ~50 GB for full dataset
- **Database Size**: ~25-30 GB for full data (1980-2024)
- **Memory**: 8 GB RAM recommended for large loads

---

## Troubleshooting

### AWS Access Issues

```bash
# Verify credentials
aws sts get-caller-identity

# Test S3 bucket access
aws s3 ls s3://paper-trail-dime/filtered-parquet/

# Reconfigure if needed
aws configure
```

### Database Connection Issues

```bash
# Test connection
psql $DATABASE_URL -c "SELECT version();"

# Verify environment variable
echo $DATABASE_URL
```

### Disk Space Issues

```bash
# Check available space
df -h .

# Clear transformed data after loading
rm -rf ../data/transformed/*.parquet
```

### Memory Issues

For large loads, reduce batch sizes in load scripts:

```python
# In load_contributions_optimized.py
BATCH_SIZE = 10000  # Reduce from default if needed
```

---

## Project Structure

```
.gitignore                             # Exclude data directories

database/
└── schema.sql                         # Database schema

etl/
├── README.md                          # ETL documentation
├── pyproject.toml                     # Python dependencies
│
├── download_and_load.py               # Unified pipeline script
├── download_contributions.sh          # Download DIME data
├── download_voteview.sh               # Download Voteview data
│
├── transform_politicians.py           # Transform legislators
├── transform_donors.py                # Transform contributors
├── transform_contributions.py         # Transform contributions
│
├── load_politicians.py                # Load politicians table
├── load_donors.py                     # Load donors table
├── load_contributions_optimized.py    # Load contributions table
│
├── create_indexes.sql                 # Database indexes
├── create_donation_summary_view.sql   # Materialized view
│
└── utils/
    └── normalization.py               # Data normalization utilities
```

---

## Getting AWS Access

#### 🔜 *TBD*

---

## Contributing

We welcome contributions! Here's how:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/improvement`)
3. Make your changes
4. Test thoroughly with a subset of data
5. Commit your changes (`git commit -m 'Add improvement'`)
6. Push to branch (`git push origin feature/improvement`)
7. Open a Pull Request

### Development Guidelines

#### 🔜 *TBD*

---

## Acknowledgments

#### 🔜 *TBD*

---

## Support

#### 🔜 *TBD*

---

**Built for transparency in political finance**
