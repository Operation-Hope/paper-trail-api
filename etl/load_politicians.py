"""
Load politicians table into PostgreSQL database.

Loads all politician records (real + placeholders) from Phase 2 transformed data.
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
POLITICIANS_FILE = DATA_DIR / "politicians.parquet"
DB_CONNECTION = os.environ.get('DATABASE_URL', '')


def load_politicians():
    """Load politicians from Parquet to PostgreSQL using COPY."""
    logger.info("=== Loading Politicians Table ===\n")

    # Read transformed politicians
    logger.info(f"Reading {POLITICIANS_FILE}...")
    politicians = pl.read_parquet(POLITICIANS_FILE)
    logger.info(f"✓ Loaded {len(politicians):,} politicians from Parquet")

    # Display breakdown
    real_count = politicians.filter(pl.col('is_placeholder') == False).height
    placeholder_count = politicians.filter(pl.col('is_placeholder') == True).height
    logger.info(f"  - Real politicians: {real_count:,}")
    logger.info(f"  - Placeholder records: {placeholder_count:,}")

    if placeholder_count > 0:
        placeholder_types = politicians.filter(
            pl.col('is_placeholder') == True
        ).group_by('placeholder_type').agg(pl.len().alias('count'))
        logger.info(f"  - Placeholder types:")
        for row in placeholder_types.iter_rows(named=True):
            logger.info(f"    • {row['placeholder_type']}: {row['count']:,}")

    # Load using psycopg3 COPY
    logger.info(f"\nLoading {len(politicians):,} politicians to database...")

    # Fix Phase 2 data quality issues:
    # Note: icpsr_id is now clean integer from mapping (no string conversion needed)
    # Fix: last_name for placeholders (if any) - fill with full_name
    politicians_fixed = politicians.with_columns([
        # Fix NULL last_name for placeholders
        pl.when(pl.col('last_name').is_null())
          .then(pl.col('full_name'))
          .otherwise(pl.col('last_name'))
          .alias('last_name')
    ])

    # Reorder columns to match database schema
    politicians_ordered = politicians_fixed.select([
        'politician_id', 'first_name', 'last_name', 'full_name', 'party', 'state', 'seat',
        'is_active', 'is_placeholder', 'placeholder_type',
        'icpsr_id', 'bioguide_id', 'nominate_dim1', 'nominate_dim2',
        'first_elected_year', 'last_elected_year'
    ])

    with psycopg.connect(DB_CONNECTION) as conn:
        with conn.cursor() as cursor:
            # Truncate table before loading (CASCADE to handle foreign keys)
            logger.info("Truncating politicians table...")
            cursor.execute("TRUNCATE TABLE politicians CASCADE;")
            logger.info("✓ Table truncated")

            # Convert to CSV in memory
            csv_buffer = BytesIO()
            politicians_ordered.write_csv(csv_buffer)
            csv_buffer.seek(0)

            # COPY to database
            with cursor.copy("COPY politicians FROM STDIN WITH (FORMAT CSV, HEADER)") as copy:
                copy.write(csv_buffer.read())

            conn.commit()

            # Verify row count
            cursor.execute("SELECT COUNT(*) FROM politicians;")
            db_count = cursor.fetchone()[0]

            if db_count == len(politicians):
                logger.info(f"✅ Successfully loaded {db_count:,} politicians")
            else:
                raise ValueError(
                    f"Row count mismatch! Expected {len(politicians):,}, got {db_count:,}"
                )

            # Display sample statistics
            cursor.execute("""
                SELECT
                    is_placeholder,
                    COUNT(*) as count
                FROM politicians
                GROUP BY is_placeholder
                ORDER BY is_placeholder;
            """)
            logger.info("\nDatabase statistics:")
            for row in cursor.fetchall():
                is_placeholder, count = row
                label = "Placeholder" if is_placeholder else "Real politician"
                logger.info(f"  - {label}: {count:,}")

    logger.info("\n✅ Politicians table load complete")


if __name__ == "__main__":
    load_politicians()
