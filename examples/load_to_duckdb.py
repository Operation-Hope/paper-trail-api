#!/usr/bin/env python3
"""Example: Load DIME data from Huggingface into a local DuckDB database.

This script demonstrates how to:
1. Load a subset of the DIME campaign finance data
2. Apply filters (by cycle, state, amount)
3. Query the resulting DuckDB database

Prerequisites:
    cd duckdb_loader
    uv pip install -e .

Usage:
    python examples/load_to_duckdb.py
"""

from pathlib import Path

# Import from the duckdb_loader package
from duckdb_loader import (
    AmountFilter,
    CycleFilter,
    StateFilter,
    load_to_duckdb,
    recent_cycles,
)
from duckdb_loader.loader import get_table_info, query_database


def example_load_recent_cycles():
    """Load the last 4 election cycles (2018-2024)."""
    print("=" * 60)
    print("Example 1: Load recent election cycles")
    print("=" * 60)

    result = load_to_duckdb(
        "recent_cycles.duckdb",
        filters=[recent_cycles(4)],  # 2018, 2020, 2022, 2024
        limit=50_000,  # Limit for demo purposes
    )

    print(f"\nResult: {result.rows_loaded:,} rows loaded")
    print(f"Filters: {result.filters_applied}")

    # Show some stats
    info = get_table_info("recent_cycles.duckdb")
    print(f"Database size: {info['size_mb']:.2f} MB")


def example_load_single_state():
    """Load contributions for a single state."""
    print("\n" + "=" * 60)
    print("Example 2: Load California contributions over $1,000")
    print("=" * 60)

    result = load_to_duckdb(
        "california_large.duckdb",
        filters=[
            StateFilter(states=["CA"]),
            AmountFilter(min_amount=1000),
            CycleFilter(cycles=[2020, 2022, 2024]),
        ],
        limit=50_000,  # Limit for demo purposes
    )

    print(f"\nResult: {result.rows_loaded:,} rows loaded")
    print(f"Filters: {result.filters_applied}")


def example_query_database():
    """Query an existing database."""
    print("\n" + "=" * 60)
    print("Example 3: Query the loaded data")
    print("=" * 60)

    db_path = "recent_cycles.duckdb"
    if not Path(db_path).exists():
        print(f"Database {db_path} not found. Run example 1 first.")
        return

    # Top recipients by total contributions
    print("\nTop 10 recipients by total contributions:")
    results = query_database(
        db_path,
        """
        SELECT
            "recipient.name",
            "recipient.party",
            COUNT(*) as num_contributions,
            SUM(amount) as total_amount
        FROM contributions
        WHERE "recipient.name" IS NOT NULL
        GROUP BY "recipient.name", "recipient.party"
        ORDER BY total_amount DESC
        LIMIT 10
        """,
    )

    print(f"{'Recipient':<40} {'Party':<6} {'Count':>10} {'Total':>15}")
    print("-" * 75)
    for name, party, count, total in results:
        party = party or "?"
        print(f"{name[:40]:<40} {party:<6} {count:>10,} ${total:>14,.2f}")

    # Contributions by cycle
    print("\nContributions by election cycle:")
    results = query_database(
        db_path,
        """
        SELECT
            cycle,
            COUNT(*) as num_contributions,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM contributions
        GROUP BY cycle
        ORDER BY cycle
        """,
    )

    print(f"{'Cycle':>6} {'Count':>12} {'Total':>18} {'Average':>12}")
    print("-" * 50)
    for cycle, count, total, avg in results:
        print(f"{cycle:>6} {count:>12,} ${total:>17,.2f} ${avg:>11,.2f}")


def cleanup():
    """Remove example databases."""
    for db in ["recent_cycles.duckdb", "california_large.duckdb"]:
        path = Path(db)
        if path.exists():
            path.unlink()
            print(f"Removed {db}")


if __name__ == "__main__":
    import sys

    if "--cleanup" in sys.argv:
        cleanup()
    else:
        example_load_recent_cycles()
        example_load_single_state()
        example_query_database()

        print("\n" + "=" * 60)
        print("Done! Run with --cleanup to remove example databases.")
        print("=" * 60)
