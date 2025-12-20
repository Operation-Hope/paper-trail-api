"""
Load contributions table into PostgreSQL database with checkpointing.

Loads 398.7M contribution records from Phase 2 transformed data in batches
with checkpoint/resume capability for reliability.
Uses psycopg3 COPY for fast bulk loading.
"""

import logging
import os
import sys
from io import BytesIO
from pathlib import Path
from typing import Optional

import polars as pl
import psycopg

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
DATA_DIR = Path(__file__).parent.parent / "data" / "transformed"
CONTRIBUTIONS_FILE = DATA_DIR / "contributions_final.parquet"
CHECKPOINT_FILE = Path(__file__).parent / ".contributions_checkpoint"
DB_CONNECTION = os.environ.get('DATABASE_URL', '')

# Batch configuration for memory management
BATCH_SIZE = 10_000_000  # 10M rows per batch


def get_last_checkpoint() -> int:
    """Read the last checkpoint offset, or 0 if starting fresh."""
    if CHECKPOINT_FILE.exists():
        offset = int(CHECKPOINT_FILE.read_text().strip())
        logger.info(f"📍 Found checkpoint at offset {offset:,}")
        return offset
    return 0


def save_checkpoint(offset: int):
    """Save progress checkpoint."""
    CHECKPOINT_FILE.write_text(str(offset))
    logger.info(f"✓ Checkpoint saved at offset {offset:,}")


def clear_checkpoint():
    """Remove checkpoint file after successful completion."""
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        logger.info("✓ Checkpoint cleared")


def validate_foreign_keys(conn: psycopg.Connection) -> tuple[int, int]:
    """
    Verify that all required foreign keys exist in the database.

    Returns:
        Tuple of (donor_count, politician_count)
    """
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM donors;")
        donor_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM politicians;")
        politician_count = cursor.fetchone()[0]

        logger.info(f"Foreign key validation:")
        logger.info(f"  - Donors: {donor_count:,}")
        logger.info(f"  - Politicians: {politician_count:,}")

        if donor_count == 0:
            raise ValueError("Donors table is empty! Load donors before contributions.")
        if politician_count == 0:
            raise ValueError("Politicians table is empty! Load politicians before contributions.")

        return donor_count, politician_count


def load_contributions_batch(
    batch: pl.DataFrame,
    conn: psycopg.Connection,
    batch_num: int,
    total_batches: int
) -> int:
    """
    Load a single batch of contributions using COPY.

    Returns:
        Number of rows loaded
    """
    logger.info(f"  Batch {batch_num}/{total_batches}: Processing {len(batch):,} rows...")

    # Fix Phase 2 data quality issue: Cap amounts exceeding DECIMAL(10,2) precision
    # Max valid value is $99,999,999.99 (10^8 - 0.01)
    # Only 2 contributions exceed this (both impossible amounts: $250M and $125M)
    MAX_VALID_AMOUNT = 99_999_999.99

    # Log if any amounts need capping (before modifying)
    capped = batch.filter(
        (pl.col('amount') > MAX_VALID_AMOUNT) | (pl.col('amount') < -MAX_VALID_AMOUNT)
    )
    if len(capped) > 0:
        logger.warning(f"  ⚠️  Capping {len(capped)} contribution amounts exceeding DECIMAL(10,2) range")

    # Cap amounts to valid range
    batch_fixed = batch.with_columns([
        pl.when(pl.col('amount') > MAX_VALID_AMOUNT)
          .then(pl.lit(MAX_VALID_AMOUNT))
          .when(pl.col('amount') < -MAX_VALID_AMOUNT)
          .then(pl.lit(-MAX_VALID_AMOUNT))
          .otherwise(pl.col('amount'))
          .alias('amount')
    ])

    with conn.cursor() as cursor:
        # Convert to CSV in memory
        csv_buffer = BytesIO()
        batch_fixed.write_csv(csv_buffer)
        csv_buffer.seek(0)

        # COPY to database
        with cursor.copy("COPY contributions FROM STDIN WITH (FORMAT CSV, HEADER)") as copy:
            copy.write(csv_buffer.read())

        conn.commit()

    return len(batch)


def load_contributions(resume: bool = True):
    """
    Load contributions from Parquet to PostgreSQL using batched COPY.

    Args:
        resume: If True, resume from last checkpoint. If False, start fresh.
    """
    logger.info("=== Loading Contributions Table ===\n")

    # Get total row count
    logger.info(f"Reading metadata from {CONTRIBUTIONS_FILE}...")
    total_rows = pl.scan_parquet(CONTRIBUTIONS_FILE).select(pl.len()).collect()[0, 0]
    logger.info(f"✓ Total contributions: {total_rows:,}")

    # Calculate batches
    total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
    logger.info(f"✓ Will process in {total_batches} batches of {BATCH_SIZE:,} rows\n")

    # Check for resume
    start_offset = 0
    if resume:
        start_offset = get_last_checkpoint()
        if start_offset > 0:
            logger.info(f"Resuming from row {start_offset:,} ({start_offset/total_rows*100:.1f}% complete)\n")

    # Validate foreign keys before loading
    with psycopg.connect(DB_CONNECTION) as conn:
        validate_foreign_keys(conn)
        logger.info("")

    # Process in batches
    rows_loaded = 0
    start_batch = start_offset // BATCH_SIZE

    try:
        for batch_num in range(start_batch, total_batches):
            offset = batch_num * BATCH_SIZE

            logger.info(f"Batch {batch_num + 1}/{total_batches} "
                       f"(rows {offset:,} to {min(offset + BATCH_SIZE, total_rows):,})")

            # Read batch using Polars lazy scan
            batch = (
                pl.scan_parquet(CONTRIBUTIONS_FILE)
                .slice(offset, BATCH_SIZE)
                .collect()
            )

            # Load batch
            with psycopg.connect(DB_CONNECTION) as conn:
                batch_rows = load_contributions_batch(
                    batch, conn, batch_num + 1, total_batches
                )
                rows_loaded += batch_rows

            # Save checkpoint after each batch
            save_checkpoint(offset + batch_rows)

            # Progress update
            percent = (offset + batch_rows) / total_rows * 100
            logger.info(f"  ✓ Progress: {rows_loaded:,}/{total_rows:,} ({percent:.1f}%)\n")

    except Exception as e:
        logger.error(f"\n❌ Error during batch processing: {e}")
        logger.error(f"Progress saved at checkpoint. Re-run to resume from row {offset:,}")
        raise

    # Verify final count
    logger.info("Verifying final count...")
    with psycopg.connect(DB_CONNECTION) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM contributions;")
            db_count = cursor.fetchone()[0]

            if db_count == total_rows:
                logger.info(f"✅ Successfully loaded {db_count:,} contributions")
            else:
                raise ValueError(
                    f"Row count mismatch! Expected {total_rows:,}, got {db_count:,}"
                )

            # Display statistics
            cursor.execute("""
                SELECT
                    COUNT(*) as total_contributions,
                    SUM(amount) as total_amount,
                    MIN(transaction_date) as earliest_date,
                    MAX(transaction_date) as latest_date,
                    AVG(amount) as avg_amount
                FROM contributions;
            """)
            row = cursor.fetchone()
            count, total, earliest, latest, avg = row
            logger.info(f"\nDatabase statistics:")
            logger.info(f"  - Total contributions: {count:,}")
            logger.info(f"  - Total amount: ${total:,.2f}")
            logger.info(f"  - Date range: {earliest} to {latest}")
            logger.info(f"  - Average contribution: ${avg:.2f}")

    # Clear checkpoint on success
    clear_checkpoint()
    logger.info("\n✅ Contributions table load complete")


if __name__ == "__main__":
    # Allow --fresh flag to start from beginning
    resume = "--fresh" not in sys.argv
    if not resume:
        logger.info("⚠️  Starting fresh load (ignoring checkpoint)")
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()

    load_contributions(resume=resume)
