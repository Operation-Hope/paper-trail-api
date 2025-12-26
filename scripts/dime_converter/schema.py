"""DIME schema definitions with explicit PyArrow types for all file types."""

from dataclasses import dataclass
from enum import Enum

import pyarrow as pa


class FileType(Enum):
    """Supported DIME file types."""

    CONTRIBUTIONS = "contributions"
    RECIPIENTS = "recipients"
    CONTRIBUTORS = "contributors"


# =============================================================================
# CONTRIBUTIONS SCHEMA (45 columns)
# =============================================================================
# Critical: IDs and codes as strings to preserve leading zeros and prevent float conversion
DIME_CONTRIBUTIONS_SCHEMA: dict[str, pa.DataType] = {
    # Integer columns
    "cycle": pa.int32(),
    "excluded.from.scaling": pa.int32(),  # 0/1 flag

    # Float columns
    "amount": pa.float64(),
    "gis.confidence": pa.float64(),
    "contributor.cfscore": pa.float64(),
    "candidate.cfscore": pa.float64(),

    # String columns (all IDs, codes, and text fields)
    "transaction.id": pa.string(),
    "transaction.type": pa.string(),
    "date": pa.string(),  # Keep as string - YYYY-MM-DD format
    "bonica.cid": pa.string(),  # ID - MUST be string
    "contributor.name": pa.string(),
    "contributor.lname": pa.string(),
    "contributor.fname": pa.string(),
    "contributor.mname": pa.string(),
    "contributor.suffix": pa.string(),
    "contributor.title": pa.string(),
    "contributor.ffname": pa.string(),
    "contributor.type": pa.string(),
    "contributor.gender": pa.string(),
    "contributor.address": pa.string(),
    "contributor.city": pa.string(),
    "contributor.state": pa.string(),
    "contributor.zipcode": pa.string(),  # MUST be string - leading zeros
    "contributor.occupation": pa.string(),
    "contributor.employer": pa.string(),
    "occ.standardized": pa.string(),
    "is.corp": pa.string(),
    "recipient.name": pa.string(),
    "bonica.rid": pa.string(),  # ID - MUST be string
    "recipient.party": pa.string(),
    "recipient.type": pa.string(),
    "recipient.state": pa.string(),
    "seat": pa.string(),
    "election.type": pa.string(),
    "latitude": pa.string(),  # Stored as string per source format
    "longitude": pa.string(),  # Stored as string per source format
    "contributor.district": pa.string(),
    "censustract": pa.string(),  # Census tract codes - string is safer
    "efec.memo": pa.string(),
    "efec.memo2": pa.string(),
    "efec.transaction.id.orig": pa.string(),
    "bk.ref.transaction.id": pa.string(),
    "efec.org.orig": pa.string(),
    "efec.comid.orig": pa.string(),
    "efec.form.type": pa.string(),
}

DIME_CONTRIBUTIONS_COLUMNS = list(DIME_CONTRIBUTIONS_SCHEMA.keys())

# Backwards compatibility alias
DIME_SCHEMA = DIME_CONTRIBUTIONS_SCHEMA
EXPECTED_COLUMNS = DIME_CONTRIBUTIONS_COLUMNS


# =============================================================================
# RECIPIENTS SCHEMA (66 columns)
# =============================================================================
DIME_RECIPIENTS_SCHEMA: dict[str, pa.DataType] = {
    # String columns - IDs and codes
    "election": pa.string(),
    "bonica.rid": pa.string(),  # Primary ID - MUST be string
    "bonica.cid": pa.string(),  # ID - MUST be string
    "name": pa.string(),
    "lname": pa.string(),
    "ffname": pa.string(),
    "fname": pa.string(),
    "mname": pa.string(),
    "title": pa.string(),
    "suffix": pa.string(),
    "party": pa.string(),
    "state": pa.string(),
    "seat": pa.string(),
    "district": pa.string(),
    "distcyc": pa.string(),
    "ico.status": pa.string(),
    "cand.gender": pa.string(),
    "pwinner": pa.string(),
    "gwinner": pa.string(),
    "s.elec.stat": pa.string(),
    "r.elec.stat": pa.string(),
    "fec.cand.status": pa.string(),
    "recipient.type": pa.string(),
    "igcat": pa.string(),
    "comtype": pa.string(),
    "ICPSR": pa.string(),
    "ICPSR2": pa.string(),
    "Cand.ID": pa.string(),
    "FEC.ID": pa.string(),
    "NID": pa.string(),
    "before.switch.ICPSR": pa.string(),
    "after.switch.ICPSR": pa.string(),
    "party.orig": pa.string(),
    "nimsp.party": pa.string(),
    "nimsp.candidate.ICO.code": pa.string(),
    "nimsp.district": pa.string(),
    "nimsp.office": pa.string(),
    "nimsp.candidate.status": pa.string(),
    "included_in_scaling": pa.string(),
    # Integer columns (use float64 to handle nulls gracefully)
    "cycle": pa.float64(),
    "fecyear": pa.float64(),
    "num.givers": pa.float64(),
    "num.givers.total": pa.float64(),
    # Float columns - scores
    "recipient.cfscore": pa.float64(),
    "recipient.cfscore.dyn": pa.float64(),
    "contributor.cfscore": pa.float64(),
    "dwdime": pa.float64(),
    "dwnom1": pa.float64(),
    "dwnom2": pa.float64(),
    "ps.dwnom1": pa.float64(),
    "ps.dwnom2": pa.float64(),
    "irt.cfscore": pa.float64(),
    "composite.score": pa.float64(),
    # Float columns - financial
    "total.receipts": pa.float64(),
    "total.disbursements": pa.float64(),
    "total.indiv.contribs": pa.float64(),
    "total.unitemized": pa.float64(),
    "total.pac.contribs": pa.float64(),
    "total.party.contribs": pa.float64(),
    "total.contribs.from.candidate": pa.float64(),
    "ind.exp.support": pa.float64(),
    "ind.exp.oppose": pa.float64(),
    # Float columns - vote percentages
    "prim.vote.pct": pa.float64(),
    "gen.vote.pct": pa.float64(),
    "district.pres.vs": pa.float64(),
}

DIME_RECIPIENTS_COLUMNS = list(DIME_RECIPIENTS_SCHEMA.keys())


# =============================================================================
# CONTRIBUTORS SCHEMA (43 columns)
# =============================================================================
# Generate amount columns for each election cycle (1980-2024, even years)
_AMOUNT_YEARS = list(range(1980, 2025, 2))  # 1980, 1982, ..., 2024

DIME_CONTRIBUTORS_SCHEMA: dict[str, pa.DataType] = {
    # String columns - IDs
    "bonica.cid": pa.string(),  # Primary ID - MUST be string
    "contributor.type": pa.string(),
    "most.recent.contributor.name": pa.string(),
    "most.recent.contributor.address": pa.string(),
    "most.recent.contributor.city": pa.string(),
    "most.recent.contributor.zipcode": pa.string(),  # MUST be string - leading zeros
    "most.recent.contributor.state": pa.string(),
    "most.recent.contributor.occupation": pa.string(),
    "most.recent.contributor.employer": pa.string(),
    "most.recent.transaction.id": pa.string(),
    "most.recent.transaction.date": pa.string(),  # Keep as string - YYYY-MM-DD format
    "contributor.gender": pa.string(),
    "is.corp": pa.string(),
    "is.projected": pa.string(),
    # Float columns (use float64 to handle nulls)
    "num.distinct": pa.float64(),
    "most.recent.contributor.latitude": pa.float64(),
    "most.recent.contributor.longitude": pa.float64(),
    "contributor.cfscore": pa.float64(),
    "first_cycle_active": pa.float64(),
    "last_cycle_active": pa.float64(),
    # Amount columns for each election cycle
    **{f"amount.{year}": pa.float64() for year in _AMOUNT_YEARS},
}

DIME_CONTRIBUTORS_COLUMNS = list(DIME_CONTRIBUTORS_SCHEMA.keys())


# =============================================================================
# FILE TYPE CONFIGURATION
# =============================================================================
@dataclass
class FileTypeConfig:
    """Configuration for a specific DIME file type."""

    schema: dict[str, pa.DataType]
    expected_columns: list[str]
    sum_column: str | None  # Column to sum for checksum validation
    key_columns: list[str]  # Columns to track non-null counts


FILE_TYPE_CONFIGS: dict[FileType, FileTypeConfig] = {
    FileType.CONTRIBUTIONS: FileTypeConfig(
        schema=DIME_CONTRIBUTIONS_SCHEMA,
        expected_columns=DIME_CONTRIBUTIONS_COLUMNS,
        sum_column="amount",
        key_columns=["transaction.id", "bonica.cid", "contributor.name", "amount"],
    ),
    FileType.RECIPIENTS: FileTypeConfig(
        schema=DIME_RECIPIENTS_SCHEMA,
        expected_columns=DIME_RECIPIENTS_COLUMNS,
        sum_column="recipient.cfscore",
        key_columns=["bonica.rid", "bonica.cid", "name"],
    ),
    FileType.CONTRIBUTORS: FileTypeConfig(
        schema=DIME_CONTRIBUTORS_SCHEMA,
        expected_columns=DIME_CONTRIBUTORS_COLUMNS,
        sum_column="contributor.cfscore",
        key_columns=["bonica.cid", "most.recent.contributor.name"],
    ),
}


def get_config(file_type: FileType) -> FileTypeConfig:
    """Get configuration for a file type."""
    return FILE_TYPE_CONFIGS[file_type]


# MySQL export null marker and empty string
NULL_VALUES = ["\\N", ""]
