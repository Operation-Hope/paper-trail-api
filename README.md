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

**[📊 Dustinhax/tyt on Huggingface](https://huggingface.co/datasets/Dustinhax/tyt)** (raw data)

- **915 million** contribution records (1979-2024)
- **58.5 GB** in Parquet format
- **43 columns** including contributor/recipient info, amounts, ideology scores

**[📊 Dustinhax/paper-trail-data on Huggingface](https://huggingface.co/datasets/Dustinhax/paper-trail-data)** (processed data)

- Transformed/filtered datasets ready for database population
- `dime/contributions/organizational/` - 57.8M PAC/committee contributions (1980-2024)
- `dime/contributions/recipient_aggregates/` - 752K recipient summary records (1980-2024)
- `distinct_legislators.parquet` - 2,303 legislators (1979-present)

#### Organizational Contributions Schema

Contributions from PACs, corporations, committees, unions (excludes individual donors). Same 45-column schema as source DIME contributions.

#### Recipient Aggregates Schema

| Column | Type | Description |
|--------|------|-------------|
| `bonica.rid` | string | Recipient ID |
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

See **[duckdb_loader/README.md](duckdb_loader/README.md)** for full documentation.

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
- Available on [Huggingface](https://huggingface.co/datasets/Dustinhax/tyt/tree/main/dime)

**Voteview** (Congressional Roll-Call Voting)
- Roll call voting records (1789-present) - 26M individual vote records
- Congressional member info with NOMINATE ideology scores - 51K members
- Roll call metadata with vote descriptions - 113K roll calls
- Available on [Huggingface](https://huggingface.co/datasets/Dustinhax/tyt/tree/main/voteview)

**United States Congress Legislators**
- Currently serving legislators - 540 members
- Historical legislators (all time) - 12,222 members
- Cross-reference IDs: bioguide_id, icpsr_id (links to Voteview)
- Available on [Huggingface](https://huggingface.co/datasets/Dustinhax/tyt/tree/main/unitedstates_congress_github)

#### Coming Soon

Additional data sources in development:
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
- **United States Congress Legislators** - [github.com/unitedstates/congress-legislators](https://github.com/unitedstates/congress-legislators)

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

See [duckdb_loader/README.md](duckdb_loader/README.md) for the full column list.

---

## Project Structure

```
paper-trail-api/
├── pyproject.toml             # Root config (dev tools, workspace)
├── CONTRIBUTING.md            # Contributor guidelines
├── .github/workflows/         # CI pipeline
│
├── duckdb_loader/             # Main package - HF data loader
│   ├── README.md              # Usage documentation
│   └── duckdb_loader/
│       ├── loader.py          # DuckDB loading logic
│       ├── postgres_loader.py # PostgreSQL loading logic
│       ├── filters.py         # Filter presets
│       ├── schema.py          # Schema definitions
│       └── cli.py             # Command-line interface
│
├── tests/                     # Test suite (pytest)
├── docs/                      # Data source documentation
├── scripts/                   # Utility scripts
└── examples/                  # Usage examples
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

### Development Setup

```bash
# Clone and install
git clone https://github.com/Operation-Hope/paper-trail-api.git
cd paper-trail-api
uv sync --all-extras

# Set up pre-commit hooks
uv run pre-commit install

# Run checks
uv run pytest tests/ -v      # Tests
uv run ruff check .          # Linting
uv run ruff format .         # Formatting
```

See **[CONTRIBUTING.md](CONTRIBUTING.md)** for full guidelines.

---

## Acknowledgments

#### 🔜 *TBD*

---

## Support

#### 🔜 *TBD*

---

**Built for transparency in political finance**
