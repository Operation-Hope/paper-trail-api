"""DuckDB schema definitions for DIME contributions data."""

from typing import Final

# Core columns to load (subset of the 43 available columns for efficiency)
# Users can customize this list when loading
CONTRIBUTIONS_COLUMNS: Final[list[str]] = [
    # Transaction info
    "cycle",
    "transaction.id",
    "transaction.type",
    "amount",
    "date",
    # Contributor info
    "bonica.cid",
    "contributor.name",
    "contributor.type",
    "contributor.state",
    "contributor.occupation",
    "contributor.employer",
    "contributor.cfscore",
    # Recipient info
    "bonica.rid",
    "recipient.name",
    "recipient.party",
    "recipient.type",
    "recipient.state",
    "candidate.cfscore",
    # Classification
    "seat",
    "election.type",
    "occ.standardized",
]

# All available columns in the HF dataset
ALL_COLUMNS: Final[list[str]] = [
    "cycle",
    "transaction.id",
    "transaction.type",
    "amount",
    "date",
    "bonica.cid",
    "contributor.name",
    "contributor.fname",
    "contributor.lname",
    "contributor.mname",
    "contributor.suffix",
    "contributor.title",
    "contributor.ffname",
    "contributor.type",
    "contributor.gender",
    "contributor.address",
    "contributor.city",
    "contributor.state",
    "contributor.zipcode",
    "contributor.occupation",
    "contributor.employer",
    "contributor.district",
    "contributor.cfscore",
    "recipient.name",
    "bonica.rid",
    "recipient.party",
    "recipient.type",
    "recipient.state",
    "candidate.cfscore",
    "latitude",
    "longitude",
    "gis.confidence",
    "censustract",
    "seat",
    "election.type",
    "occ.standardized",
    "is.corp",
    "excluded.from.scaling",
    "efec.memo",
    "efec.memo2",
    "efec.transaction.id.orig",
    "bk.ref.transaction.id",
    "efec.org.orig",
    "efec.comid.orig",
    "efec.form.type",
]


def create_schema(
    conn, table_name: str = "contributions", columns: list[str] | None = None
) -> None:
    """Create the contributions table schema in DuckDB.

    Args:
        conn: DuckDB connection
        table_name: Name for the table (default: contributions)
        columns: Columns to include (default: CONTRIBUTIONS_COLUMNS)
    """
    cols = columns or CONTRIBUTIONS_COLUMNS

    # Build column definitions with appropriate types
    col_defs = []
    for col in cols:
        sql_type = _get_column_type(col)
        # DuckDB uses double quotes for identifiers with dots
        col_defs.append(f'"{col}" {sql_type}')

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {", ".join(col_defs)}
        )
    """
    conn.execute(create_sql)


def create_indexes(conn, table_name: str = "contributions") -> None:
    """Create indexes for common query patterns.

    Args:
        conn: DuckDB connection
        table_name: Table to index
    """
    indexes = [
        ("idx_cycle", "cycle"),
        ("idx_contributor_state", '"contributor.state"'),
        ("idx_recipient_state", '"recipient.state"'),
        ("idx_amount", "amount"),
        ("idx_date", "date"),
        ("idx_bonica_cid", '"bonica.cid"'),
        ("idx_bonica_rid", '"bonica.rid"'),
    ]

    for idx_name, col in indexes:
        try:
            conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} ({col})")
        except Exception:
            # Column may not exist if user selected subset
            pass


def _get_column_type(col: str) -> str:
    """Map column names to DuckDB types."""
    type_map = {
        # Integer columns
        "cycle": "INTEGER",
        "excluded.from.scaling": "INTEGER",
        # Float columns
        "amount": "DOUBLE",
        "contributor.cfscore": "DOUBLE",
        "candidate.cfscore": "DOUBLE",
        "gis.confidence": "DOUBLE",
        "latitude": "DOUBLE",
        "longitude": "DOUBLE",
    }
    return type_map.get(col, "VARCHAR")
