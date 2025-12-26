"""Command-line interface for loading DIME data into DuckDB or PostgreSQL."""

import click

from .filters import AmountFilter, CycleFilter, StateFilter, recent_cycles
from .loader import get_table_info, load_to_duckdb, query_database
from .postgres_loader import get_postgres_table_info, load_to_postgres


@click.group()
def main():
    """Load DIME campaign finance data from Huggingface into DuckDB."""


@main.command()
@click.argument("database", type=click.Path())
@click.option(
    "--cycles",
    "-c",
    multiple=True,
    type=int,
    help="Election cycle(s) to include (e.g., -c 2020 -c 2022)",
)
@click.option(
    "--recent",
    "-r",
    type=int,
    default=None,
    help="Include last N election cycles (e.g., --recent 4 for 2018-2024)",
)
@click.option(
    "--state",
    "-s",
    multiple=True,
    help="State(s) to include (e.g., -s CA -s NY)",
)
@click.option(
    "--min-amount",
    type=float,
    default=None,
    help="Minimum contribution amount",
)
@click.option(
    "--max-amount",
    type=float,
    default=None,
    help="Maximum contribution amount",
)
@click.option(
    "--limit",
    "-l",
    type=int,
    default=None,
    help="Maximum rows to load",
)
@click.option(
    "--table",
    "-t",
    default="contributions",
    help="Table name (default: contributions)",
)
@click.option(
    "--no-indexes",
    is_flag=True,
    help="Skip creating indexes after load",
)
def load(
    database: str,
    cycles: tuple[int, ...],
    recent: int | None,
    state: tuple[str, ...],
    min_amount: float | None,
    max_amount: float | None,
    limit: int | None,
    table: str,
    no_indexes: bool,
):
    """Load DIME data into a DuckDB database.

    DATABASE is the path to the DuckDB file (created if not exists).

    Examples:

        # Load 2020 and 2022 cycles
        duckdb-loader load contributions.duckdb -c 2020 -c 2022

        # Load last 4 cycles, CA only, contributions over $1000
        duckdb-loader load ca_large.duckdb --recent 4 -s CA --min-amount 1000

        # Load a sample of 100k rows
        duckdb-loader load sample.duckdb --limit 100000
    """
    filters = []

    # Cycle filters
    if cycles:
        filters.append(CycleFilter(cycles=list(cycles)))
    elif recent:
        filters.append(recent_cycles(recent))

    # State filter
    if state:
        filters.append(StateFilter(states=[s.upper() for s in state]))

    # Amount filter
    if min_amount is not None or max_amount is not None:
        filters.append(AmountFilter(min_amount=min_amount, max_amount=max_amount))

    # Show what we're doing
    click.echo(f"Loading DIME data into {database}")
    if filters:
        click.echo("Filters:")
        for f in filters:
            click.echo(f"  - {f.describe()}")
    if limit:
        click.echo(f"Limit: {limit:,} rows")
    click.echo()

    # Load
    result = load_to_duckdb(
        database,
        filters=filters if filters else None,
        table_name=table,
        limit=limit,
        create_indexes_after=not no_indexes,
    )

    click.echo()
    click.echo(f"Loaded {result.rows_loaded:,} rows into {result.table_name}")
    click.echo(f"Database: {result.database_path}")


@main.command("load-postgres")
@click.argument("database_url", envvar="DATABASE_URL")
@click.option(
    "--cycles",
    "-c",
    multiple=True,
    type=int,
    help="Election cycle(s) to include (e.g., -c 2020 -c 2022)",
)
@click.option(
    "--recent",
    "-r",
    type=int,
    default=None,
    help="Include last N election cycles (e.g., --recent 4 for 2018-2024)",
)
@click.option(
    "--state",
    "-s",
    multiple=True,
    help="State(s) to include (e.g., -s CA -s NY)",
)
@click.option(
    "--min-amount",
    type=float,
    default=None,
    help="Minimum contribution amount",
)
@click.option(
    "--max-amount",
    type=float,
    default=None,
    help="Maximum contribution amount",
)
@click.option(
    "--limit",
    "-l",
    type=int,
    default=None,
    help="Maximum rows to load",
)
@click.option(
    "--table",
    "-t",
    default="contributions",
    help="Table name (default: contributions)",
)
@click.option(
    "--batch-size",
    "-b",
    type=int,
    default=10_000,
    help="Batch size for inserts (default: 10000)",
)
@click.option(
    "--no-indexes",
    is_flag=True,
    help="Skip creating indexes after load",
)
def load_postgres(
    database_url: str,
    cycles: tuple[int, ...],
    recent: int | None,
    state: tuple[str, ...],
    min_amount: float | None,
    max_amount: float | None,
    limit: int | None,
    table: str,
    batch_size: int,
    no_indexes: bool,
):
    """Load DIME data into a PostgreSQL database.

    DATABASE_URL is the PostgreSQL connection string. Can also be set via
    the DATABASE_URL environment variable.

    Examples:

        # Load 100k sample rows
        duckdb-loader load-postgres postgresql://localhost/papertrail --limit 100000

        # Load 2024 cycle only
        duckdb-loader load-postgres $DATABASE_URL -c 2024

        # Load last 4 cycles, CA only
        duckdb-loader load-postgres $DATABASE_URL --recent 4 -s CA
    """
    filters = []

    # Cycle filters
    if cycles:
        filters.append(CycleFilter(cycles=list(cycles)))
    elif recent:
        filters.append(recent_cycles(recent))

    # State filter
    if state:
        filters.append(StateFilter(states=[s.upper() for s in state]))

    # Amount filter
    if min_amount is not None or max_amount is not None:
        filters.append(AmountFilter(min_amount=min_amount, max_amount=max_amount))

    # Show what we're doing
    click.echo("Loading DIME data into PostgreSQL")
    if filters:
        click.echo("Filters:")
        for f in filters:
            click.echo(f"  - {f.describe()}")
    if limit:
        click.echo(f"Limit: {limit:,} rows")
    click.echo()

    # Load
    result = load_to_postgres(
        database_url,
        filters=filters if filters else None,
        table_name=table,
        limit=limit,
        batch_size=batch_size,
        create_indexes_after=not no_indexes,
    )

    click.echo()
    click.echo(f"Loaded {result.rows_loaded:,} rows into {result.table_name}")
    click.echo(f"Database: {result.database_url}")


@main.command()
@click.argument("database", type=click.Path(exists=True))
@click.option("--table", "-t", default="contributions", help="Table name")
def info(database: str, table: str):
    """Show information about a loaded database.

    Example:
        duckdb-loader info contributions.duckdb
    """
    try:
        info = get_table_info(database, table)
        click.echo(f"Database: {database}")
        click.echo(f"Table: {table}")
        click.echo(f"Rows: {info['row_count']:,}")
        click.echo(f"Size: {info['size_mb']:.2f} MB")
        click.echo(f"Columns ({len(info['columns'])}):")
        for name, dtype in info["columns"]:
            click.echo(f"  {name}: {dtype}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1)


@main.command()
@click.argument("database", type=click.Path(exists=True))
@click.argument("sql")
def query(database: str, sql: str):
    """Run a SQL query on the database.

    Example:
        duckdb-loader query contributions.duckdb "SELECT cycle, COUNT(*) FROM contributions GROUP BY cycle"
    """
    try:
        results = query_database(database, sql)
        for row in results:
            click.echo("\t".join(str(v) for v in row))
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
