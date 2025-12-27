# Paper Trail 🚧

<p align="center">
  <strong>Campaign Finance Data for Research & Analysis</strong>
  <br />
  915 million political contributions (1979-2024) available on Huggingface
  <br />
  💰 Campaign Contributions • 🏛️ Politicians • 👥 Donors
</p>

> 🚧 **Work in Progress** - This project is under active development

---

## Quick Start: Use the Data

The DIME campaign finance dataset is available on Huggingface:

**[📊 Dustinhax/tyt on Huggingface](https://huggingface.co/datasets/Dustinhax/tyt)**

- **915 million** contribution records (1979-2024)
- **58.5 GB** in Parquet format
- **43 columns** including contributor/recipient info, amounts, ideology scores

### Load into DuckDB or PostgreSQL

```bash
# Install the loader
cd duckdb_loader
uv pip install -e .

# Load into DuckDB (quick analysis)
duckdb-loader load contributions.duckdb --recent 4 -s CA --min-amount 1000

# Or load into PostgreSQL (full-stack development)
duckdb-loader load-postgres $DATABASE_URL --limit 100000
```

Or use Python:

```python
from duckdb_loader import load_to_duckdb, CycleFilter, StateFilter

result = load_to_duckdb(
    "contributions.duckdb",
    filters=[
        CycleFilter([2020, 2022, 2024]),
        StateFilter(["CA", "NY"]),
    ],
    limit=100_000,  # Start small
)
print(f"Loaded {result.rows_loaded:,} rows")
```

See **[docs/LOCAL_DEVELOPMENT.md](docs/LOCAL_DEVELOPMENT.md)** for full documentation.

---

## Overview

This repository provides:

1. **Huggingface Dataset** - Raw DIME contribution data in Parquet format
2. **Data Loader** - Tools to load subsets into DuckDB or PostgreSQL for local development

### Data Sources

**DIME** (Database on Ideology, Money in Politics, and Elections)
- Campaign contributions (1979-2024) - 915 million transaction records
- Politicians/Recipients - Federal candidates who received contributions
- Donors/Contributors - Individuals and organizations making contributions
- Available on [Huggingface](https://huggingface.co/datasets/Dustinhax/tyt)

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

## Data Coverage

### Huggingface Dataset

| Field | Coverage |
|-------|----------|
| **Years** | 1979-2024 |
| **Records** | 915 million contributions |
| **Size** | 58.5 GB (Parquet) |
| **Columns** | 43 fields |

Key columns include:
- Transaction info: `cycle`, `amount`, `date`, `transaction.id`
- Contributor: `contributor.name`, `contributor.state`, `contributor.employer`, `contributor.cfscore`
- Recipient: `recipient.name`, `recipient.party`, `recipient.state`, `candidate.cfscore`

See [docs/LOCAL_DEVELOPMENT.md](docs/LOCAL_DEVELOPMENT.md) for the full column list.

---

## Project Structure

```
duckdb_loader/                 # Load HF data into DuckDB or PostgreSQL
├── duckdb_loader/
│   ├── loader.py              # DuckDB loading logic
│   ├── postgres_loader.py     # PostgreSQL loading logic
│   ├── filters.py             # Filter presets
│   ├── schema.py              # Schema definitions
│   └── cli.py                 # Command-line interface
└── pyproject.toml

docs/
├── RAW_DATA_CATALOG.md        # Data source documentation
└── LOCAL_DEVELOPMENT.md       # Local development guide

scripts/
└── dime_converter/            # CSV to Parquet converter (one-time use)

examples/                      # Usage examples
```

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
