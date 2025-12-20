"""
Load donors table into PostgreSQL database.

Loads all donor records (30.6M) from Phase 2 transformed data.
Uses psycopg3 COPY for fast bulk loading.
"""

import logging
import os
from io import BytesIO
from pathlib import Path

import polars as pl
import psycopg

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
DATA_DIR = Path(__file__).parent.parent / "data" / "transformed"
DONORS_FILE = DATA_DIR / "donors.parquet"
DB_CONNECTION = os.environ.get('DATABASE_URL', '')


def load_donors():
    """Load donors from Parquet to PostgreSQL using COPY."""
    logger.info("=== Loading Donors Table ===\n")

    # Read transformed donors
    logger.info(f"Reading {DONORS_FILE}...")
    donors = pl.read_parquet(DONORS_FILE)
    logger.info(f"✓ Loaded {len(donors):,} donors from Parquet")

    # Display breakdown
    donor_types = donors.group_by('donor_type').agg(pl.len().alias('count')).sort('count', descending=True)
    logger.info(f"\nDonor types:")
    for row in donor_types.iter_rows(named=True):
        logger.info(f"  - {row['donor_type']}: {row['count']:,}")

    # Fix Phase 2 data quality issues:
    # 1. donor_type: 1,526 donors have NULL donor_type - default to 'I' (Individual)
    # 2. name: 133 donors have NULL name - set to 'UNKNOWN DONOR' placeholder
    donors_fixed = donors.with_columns([
        pl.when(pl.col('donor_type').is_null())
          .then(pl.lit('I'))
          .otherwise(pl.col('donor_type'))
          .alias('donor_type'),
        pl.when(pl.col('name').is_null())
          .then(pl.lit('UNKNOWN DONOR'))
          .otherwise(pl.col('name'))
          .alias('name')
    ])

    # Load using psycopg3 COPY
    logger.info(f"\nLoading {len(donors_fixed):,} donors to database...")
    with psycopg.connect(DB_CONNECTION) as conn:
        with conn.cursor() as cursor:
            # Convert to CSV in memory
            csv_buffer = BytesIO()
            donors_fixed.write_csv(csv_buffer)
            csv_buffer.seek(0)

            # COPY to database
            with cursor.copy("COPY donors FROM STDIN WITH (FORMAT CSV, HEADER)") as copy:
                copy.write(csv_buffer.read())

            conn.commit()

            # Verify row count
            cursor.execute("SELECT COUNT(*) FROM donors;")
            db_count = cursor.fetchone()[0]

            if db_count == len(donors):
                logger.info(f"✅ Successfully loaded {db_count:,} donors")
            else:
                raise ValueError(
                    f"Row count mismatch! Expected {len(donors):,}, got {db_count:,}"
                )

            # Display database statistics
            cursor.execute("""
                SELECT
                    donor_type,
                    COUNT(*) as count,
                    SUM(total_contributions_count) as total_contribs,
                    SUM(total_amount) as total_amount
                FROM donors
                GROUP BY donor_type
                ORDER BY count DESC;
            """)
            logger.info("\nDatabase statistics:")
            for row in cursor.fetchall():
                donor_type, count, total_contribs, total_amount = row
                logger.info(
                    f"  - {donor_type}: {count:,} donors, "
                    f"{total_contribs:,} contributions, "
                    f"${total_amount:,.2f} total"
                )

    logger.info("\n✅ Donors table load complete")


if __name__ == "__main__":
    load_donors()
