"""
Transform politicians data from DIME recipients to final schema.

Reads raw recipients data and produces deduplicated politicians table.
"""

import polars as pl
import duckdb
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def main():
    """Transform politicians from DIME recipients data."""

    logger.info("="*60)
    logger.info("PHASE 2 - STEP 2.1: Transform Politicians")
    logger.info("="*60 + "\n")

    # Load DIME politicians/recipients (247,614 politician-cycle records)
    recipients_path = Path('data/raw/recipients/recipients_filtered.parquet')
    if not recipients_path.exists():
        raise FileNotFoundError(f"Recipients file not found: {recipients_path}")

    recipients_df = pl.read_parquet(recipients_path)

    logger.info(f'Loaded {len(recipients_df):,} politician-cycle records')
    logger.info(f'Unique politicians (bonica.rid): {recipients_df["bonica.rid"].n_unique():,}')

    # Strip 'nominee' suffix from bonica.rid to match contribution data
    # Presidential nominees have IDs like 'cand100270nominee' in recipients but 'cand100270' in contributions
    recipients_df = recipients_df.with_columns([
        pl.col('bonica.rid').cast(pl.String).str.replace('nominee$', '').alias('bonica.rid')
    ])
    logger.info(f'✓ Stripped nominee suffix from politician IDs')

    # STEP 1: Preserve full politician-cycle history for post-MVP use
    output_dir = Path('data/transformed')
    output_dir.mkdir(parents=True, exist_ok=True)

    full_history_path = output_dir / 'politician_cycles_full.parquet'
    recipients_df.write_parquet(full_history_path)
    logger.info(f'✓ Saved complete politician-cycle history to {full_history_path}')

    # STEP 2: Deduplicate to most recent cycle per politician
    # Sort by cycle descending, then group by bonica.rid and take first (most recent)
    politicians = (
        recipients_df
        .sort('cycle', descending=True)
        .group_by('bonica.rid', maintain_order=True)
        .first()
    )

    logger.info(f'✓ Deduplicated to {len(politicians):,} unique politicians (most recent cycle)')

    # STEP 3: Parse names
    # DIME format: "LAST, First Middle" or "LAST, First" or "LAST,First"
    # Handle edge cases:
    # - Names without comma (committees, organizations)
    # - Names with comma but no space
    # - Names with multiple commas (e.g., "massie, iii, james")
    # - Names with first part that doesn't have space
    politicians = politicians.with_columns([
        # Extract last name (everything before first comma, or full name if no comma)
        pl.when(pl.col('name').str.contains(','))
        .then(pl.col('name').str.split(',').list.first())
        .otherwise(pl.col('name'))
        .alias('last_name'),

        # Extract first name part (everything after first comma)
        # For multiple commas, join parts 1 onwards
        pl.when(pl.col('name').str.contains(','))
        .then(pl.col('name').str.split(',').list.slice(1).list.join(' '))
        .otherwise(pl.lit(None))
        .alias('first_name_raw')
    ]).with_columns([
        # Clean up first name: strip whitespace and get first word
        # Use list.first() which is safer than list.get(0) for empty lists
        pl.when(pl.col('first_name_raw').is_not_null())
        .then(
            pl.col('first_name_raw')
            .str.strip_chars()
            .str.split(' ')
            .list.first()
        )
        .otherwise(pl.lit(None))
        .alias('first_name')
    ]).drop('first_name_raw')

    # STEP 4: Set is_active flag (served in 113th Congress or later, 2013+)
    politicians = politicians.with_columns([
        (pl.col('cycle') >= 2013).alias('is_active')
    ])

    active_count = politicians['is_active'].sum()
    logger.info(f'✓ Active politicians (cycle >= 2013): {active_count:,}')

    # STEP 5: Map party codes to party abbreviations
    # DIME uses numeric codes: 100=Democrat, 200=Republican, others for third parties
    party_map = {
        100: 'D',
        200: 'R',
        328: 'I',  # Independent
    }
    politicians = politicians.with_columns([
        pl.col('party').replace(party_map, default='O').alias('party_abbrev')
    ])

    # STEP 6: Extract first/last elected years from cycle
    # Note: This is an approximation - we're using cycle as proxy for service
    # For more accurate dates, would need to link to legislators dataset
    politicians = politicians.with_columns([
        pl.col('cycle').alias('first_elected_year'),  # Placeholder
        pl.col('cycle').alias('last_elected_year')
    ])

    # STEP 7: Extract ideology scores (already in recipients data)
    # DW-NOMINATE scores from DIME (dwnom1, dwnom2 columns)
    nominate_dim1_col = pl.col('dwnom1').alias('nominate_dim1') if 'dwnom1' in politicians.columns else pl.lit(None).alias('nominate_dim1')
    nominate_dim2_col = pl.col('dwnom2').alias('nominate_dim2') if 'dwnom2' in politicians.columns else pl.lit(None).alias('nominate_dim2')

    politicians = politicians.with_columns([
        nominate_dim1_col,
        nominate_dim2_col
    ])

    # STEP 7.5: Load clean ICPSR mapping (instead of using corrupted DIME ICPSR field)
    icpsr_mapping_path = Path('data/transformed/icpsr_to_bonica_mapping.parquet')
    if icpsr_mapping_path.exists():
        logger.info("Loading clean ICPSR mapping...")
        icpsr_mapping = pl.read_parquet(icpsr_mapping_path)

        # Take most recent congress mapping for each politician
        icpsr_latest = (
            icpsr_mapping
            .sort('congress', descending=True)
            .group_by('bonica_rid', maintain_order=True)
            .first()
            .select(['bonica_rid', 'icpsr'])
        )

        # Cast bonica.rid to string to match mapping file datatype
        politicians = politicians.with_columns([
            pl.col('bonica.rid').cast(pl.Utf8).alias('bonica_rid_str')
        ])

        # Join clean ICPSR mapping with politicians
        politicians = politicians.join(
            icpsr_latest,
            left_on='bonica_rid_str',
            right_on='bonica_rid',
            how='left'
        ).drop('bonica_rid_str')

        logger.info(f"  Mapped clean ICPSR for {politicians.filter(pl.col('icpsr').is_not_null()).height:,} politicians")
    else:
        logger.warning(f"ICPSR mapping not found: {icpsr_mapping_path}")
        politicians = politicians.with_columns([pl.lit(None, dtype=pl.Int32).alias('icpsr')])

    # STEP 7.6: Load bioguide_id mapping
    bioguide_mapping_path = Path('data/transformed/bioguide_to_bonica_mapping.parquet')
    if bioguide_mapping_path.exists():
        logger.info("Loading bioguide_id mapping...")
        bioguide_mapping = pl.read_parquet(bioguide_mapping_path)

        # Take most recent congress mapping for each politician
        bioguide_latest = (
            bioguide_mapping
            .sort('congress', descending=True)
            .group_by('politician_id', maintain_order=True)
            .first()
            .select(['politician_id', 'bioguide_id'])
        )

        # Cast bonica.rid to string to match mapping file datatype (if not already done)
        if 'bonica_rid_str' not in politicians.columns:
            politicians = politicians.with_columns([
                pl.col('bonica.rid').cast(pl.Utf8).alias('bonica_rid_str')
            ])

        # Join bioguide mapping with politicians
        politicians = politicians.join(
            bioguide_latest,
            left_on='bonica_rid_str',
            right_on='politician_id',
            how='left'
        ).drop('bonica_rid_str')

        logger.info(f"  Mapped bioguide_id for {politicians.filter(pl.col('bioguide_id').is_not_null()).height:,} politicians")
    else:
        logger.warning(f"Bioguide mapping not found: {bioguide_mapping_path}")
        politicians = politicians.with_columns([pl.lit(None, dtype=pl.Utf8).alias('bioguide_id')])

    # STEP 8: Create final politicians dataframe
    icpsr_col = pl.col('icpsr').alias('icpsr_id')
    bioguide_col = pl.col('bioguide_id')

    politicians_final = politicians.select([
        pl.col('bonica.rid').alias('politician_id'),
        'first_name',
        'last_name',
        pl.col('name').alias('full_name'),
        pl.col('party_abbrev').alias('party'),
        'state',
        'seat',
        'is_active',
        pl.lit(False).alias('is_placeholder'),  # All DIME records are real politicians
        pl.lit(None, dtype=pl.Utf8).alias('placeholder_type'),  # No placeholders in DIME data
        icpsr_col,
        bioguide_col,
        'nominate_dim1',
        'nominate_dim2',
        'first_elected_year',
        'last_elected_year'
    ])

    # STEP 8.5: Extract missing politicians from contributions and create placeholders
    logger.info('\n=== Checking for missing politicians in contributions ===')
    file_pattern = 'data/raw/contributions/contrib_*_filtered.parquet'

    # Get unique recipient IDs from contributions with their metadata
    contrib_recipients = duckdb.query(f"""
        WITH ranked_recipients AS (
            SELECT
                "bonica.rid",
                "recipient.name",
                "recipient.party",
                "recipient.type",
                ROW_NUMBER() OVER (PARTITION BY "bonica.rid" ORDER BY "date" DESC) as rn
            FROM read_parquet('{file_pattern}')
            WHERE "bonica.rid" IS NOT NULL
        )
        SELECT "bonica.rid", "recipient.name", "recipient.party", "recipient.type"
        FROM ranked_recipients
        WHERE rn = 1
    """).pl()

    # Find recipients in contributions but not in politicians_final
    politician_ids_set = set(politicians_final['politician_id'].to_list())
    contrib_ids_set = set(contrib_recipients['bonica.rid'].to_list())
    missing_ids = contrib_ids_set - politician_ids_set

    if len(missing_ids) > 0:
        logger.info(f'⚠️  Found {len(missing_ids):,} politicians in contributions but not in recipients')
        logger.info(f'   Creating placeholder records for these politicians')

        # Filter to missing recipients and create placeholder records
        missing_recipients = contrib_recipients.filter(pl.col('bonica.rid').is_in(list(missing_ids)))

        # Parse names (same logic as main transform)
        missing_recipients = missing_recipients.with_columns([
            pl.when(pl.col('recipient.name').str.contains(','))
            .then(pl.col('recipient.name').str.split(',').list.first())
            .otherwise(pl.col('recipient.name'))
            .alias('last_name'),

            pl.when(pl.col('recipient.name').str.contains(','))
            .then(pl.col('recipient.name').str.split(',').list.slice(1).list.join(' '))
            .otherwise(pl.lit(None))
            .alias('first_name_raw')
        ]).with_columns([
            pl.when(pl.col('first_name_raw').is_not_null())
            .then(
                pl.col('first_name_raw')
                .str.strip_chars()
                .str.split(' ')
                .list.first()
            )
            .otherwise(pl.lit(None))
            .alias('first_name')
        ]).drop('first_name_raw')

        # Map party codes
        party_map = {100: 'D', 200: 'R', 328: 'I'}
        missing_recipients = missing_recipients.with_columns([
            pl.col('recipient.party').cast(pl.Int32).replace(party_map, default='O').alias('party_abbrev')
        ])

        # Create placeholder politician records
        placeholders = missing_recipients.select([
            pl.col('bonica.rid').alias('politician_id'),
            'first_name',
            'last_name',
            pl.col('recipient.name').alias('full_name'),
            pl.col('party_abbrev').alias('party'),
            pl.lit('XX').alias('state'),  # Unknown state
            pl.lit('unknown').alias('seat'),  # Unknown seat
            pl.lit(True).alias('is_active'),  # Assume active since they have recent contributions
            pl.lit(True).alias('is_placeholder'),  # Mark as placeholder
            pl.lit('missing_from_recipients').alias('placeholder_type'),
            pl.lit(None, dtype=pl.Int32).alias('icpsr_id'),
            pl.lit(None, dtype=pl.Utf8).alias('bioguide_id'),
            pl.lit(None, dtype=pl.Float64).alias('nominate_dim1'),
            pl.lit(None, dtype=pl.Float64).alias('nominate_dim2'),
            pl.lit(None, dtype=pl.Int32).alias('first_elected_year'),
            pl.lit(None, dtype=pl.Int32).alias('last_elected_year')
        ])

        # Ensure schema compatibility before concatenating
        # Cast placeholders to match politicians_final schema
        placeholders = placeholders.cast(politicians_final.schema)

        # Append placeholders to politicians_final
        politicians_final = pl.concat([politicians_final, placeholders])
        logger.info(f'✓ Added {len(placeholders):,} placeholder politicians')
    else:
        logger.info(f'✓ All politicians in contributions are present in recipients')

    # STEP 9: Validation
    assert politicians_final['politician_id'].is_unique().all(), "politician_id must be unique!"
    assert politicians_final['politician_id'].is_not_null().all(), "No null politician_ids!"
    assert len(politicians_final) > 40000, f"Expected 40K+ politicians, got {len(politicians_final)}"

    logger.info(f'\n✓ Validation passed:')
    logger.info(f'  - {len(politicians_final):,} unique politicians')
    logger.info(f'  - No duplicate politician_ids')
    logger.info(f'  - No null politician_ids')

    # STEP 10: Save final output
    output_path = output_dir / 'politicians.parquet'
    politicians_final.write_parquet(output_path)
    logger.info(f'\n✓ Saved to {output_path}')

    # Print summary statistics
    logger.info(f'\nSummary:')
    logger.info(f'  Party distribution:')
    party_counts = politicians_final['party'].value_counts().sort('party')
    for row in party_counts.iter_rows(named=True):
        logger.info(f'    {row["party"]}: {row["count"]:,}')

    logger.info(f'\n  Seat distribution:')
    seat_counts = politicians_final['seat'].value_counts().sort('seat')
    for row in seat_counts.iter_rows(named=True):
        logger.info(f'    {row["seat"]}: {row["count"]:,}')

    logger.info(f'\n  Active vs Inactive:')
    is_active_counts = politicians_final['is_active'].value_counts().sort('is_active', descending=True)
    for row in is_active_counts.iter_rows(named=True):
        status = 'Active' if row['is_active'] else 'Inactive'
        logger.info(f'    {status}: {row["count"]:,}')

    logger.info("\n" + "="*60)
    logger.info("✓ Step 2.1 Complete: Politicians transformation successful")
    logger.info("="*60)


if __name__ == '__main__':
    main()
