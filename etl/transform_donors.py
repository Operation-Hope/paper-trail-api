"""
Transform donors data from DIME contributors to final schema.

Loads pre-deduplicated donors, aggregates contributions, and applies employer normalization.
"""

import polars as pl
import duckdb
import logging
from pathlib import Path

from utils.normalization import normalize_employer_name

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def main():
    """Transform donors from DIME contributors data."""

    logger.info("="*60)
    logger.info("PHASE 2 - STEP 2.2: Transform Donors")
    logger.info("="*60 + "\n")

    # STEP 1: Extract unique donors from contributions with aggregates and latest metadata
    # Using DuckDB for better performance with large contribution files
    logger.info('\n=== Extracting Donors from Contributions via DuckDB ===')

    file_pattern = 'data/raw/contributions/contrib_*_filtered.parquet'

    # Extract donors with aggregates and most recent contributor info
    donors = duckdb.query(f"""
        WITH ranked_contributions AS (
            SELECT
                "bonica.cid",
                "contributor.name",
                "contributor.type",
                "contributor.employer",
                "contributor.occupation",
                "contributor.state",
                "amount",
                "date",
                ROW_NUMBER() OVER (
                    PARTITION BY "bonica.cid"
                    ORDER BY "date" DESC, "amount" DESC
                ) as rn
            FROM read_parquet('{file_pattern}')
            WHERE "bonica.cid" IS NOT NULL
        )
        SELECT
            "bonica.cid",
            "contributor.name" as "most.recent.contributor.name",
            "contributor.type" as "contributor.type",
            "contributor.employer" as "most.recent.contributor.employer",
            "contributor.occupation" as "most.recent.contributor.occupation",
            "contributor.state" as "most.recent.contributor.state",
            COUNT(*) as total_contributions_count,
            SUM(CAST("amount" AS DECIMAL(12,2))) as total_amount
        FROM ranked_contributions
        WHERE rn = 1
        GROUP BY
            "bonica.cid",
            "contributor.name",
            "contributor.type",
            "contributor.employer",
            "contributor.occupation",
            "contributor.state"
    """).pl()  # Convert DuckDB result to Polars

    logger.info(f'✓ Extracted {len(donors):,} unique donors from contributions')
    logger.info(f'✓ Total contributions: {donors["total_contributions_count"].sum():,}')
    logger.info(f'✓ Total amount: ${donors["total_amount"].sum():,.2f}')

    # STEP 2: Optionally enrich with contributors_all.parquet if available
    # contributors_all.parquet may have additional metadata, but is not comprehensive
    contributors_path = Path('data/raw/contributors/contributors_all.parquet')
    if contributors_path.exists():
        logger.info(f'\n=== Enriching with contributors_all.parquet (if available) ===')
        contributors = pl.read_parquet(contributors_path)
        contributors = contributors.filter(pl.col('bonica.cid').is_not_null())

        # Count how many donors are missing from contributors_all
        donor_ids_set = set(donors['bonica.cid'].to_list())
        contrib_ids_set = set(contributors['bonica.cid'].to_list())
        missing_in_contrib = len(donor_ids_set - contrib_ids_set)

        if missing_in_contrib > 0:
            logger.info(f'⚠️  {missing_in_contrib:,} donors from contributions are missing in contributors_all.parquet')
            logger.info(f'   Using contribution metadata for these donors')

        # Use contributors_all as source of truth where available, fallback to contribution metadata
        # Left join to preserve all donors from contributions
        donors_enriched = donors.join(
            contributors.select(['bonica.cid', 'most.recent.contributor.name', 'contributor.type',
                               'most.recent.contributor.employer', 'most.recent.contributor.occupation',
                               'most.recent.contributor.state']),
            on='bonica.cid',
            how='left',
            suffix='_contrib'
        )

        # Coalesce: prefer contributors_all metadata, fallback to contribution metadata
        donors = donors_enriched.with_columns([
            pl.coalesce(['most.recent.contributor.name_contrib', 'most.recent.contributor.name']).alias('most.recent.contributor.name'),
            pl.coalesce(['contributor.type_contrib', 'contributor.type']).alias('contributor.type'),
            pl.coalesce(['most.recent.contributor.employer_contrib', 'most.recent.contributor.employer']).alias('most.recent.contributor.employer'),
            pl.coalesce(['most.recent.contributor.occupation_contrib', 'most.recent.contributor.occupation']).alias('most.recent.contributor.occupation'),
            pl.coalesce(['most.recent.contributor.state_contrib', 'most.recent.contributor.state']).alias('most.recent.contributor.state'),
        ]).select([
            'bonica.cid',
            'most.recent.contributor.name',
            'contributor.type',
            'most.recent.contributor.employer',
            'most.recent.contributor.occupation',
            'most.recent.contributor.state',
            'total_contributions_count',
            'total_amount'
        ])
    else:
        logger.info(f'ℹ️  contributors_all.parquet not found, using contribution metadata only')

    # STEP 4: Validate Employer Normalization Effectiveness
    logger.info('\n=== Employer Normalization Validation ===')

    # Apply normalization and compare before/after
    before_unique = donors['most.recent.contributor.employer'].n_unique()
    donors = donors.with_columns([
        pl.col('most.recent.contributor.employer')
        .map_elements(normalize_employer_name, return_dtype=pl.Utf8)
        .alias('employer_normalized')
    ])
    after_unique = donors['employer_normalized'].n_unique()

    reduction_count = before_unique - after_unique
    reduction_pct = (reduction_count / before_unique) * 100 if before_unique > 0 else 0

    logger.info(f'Employer normalization results:')
    logger.info(f'  Before normalization: {before_unique:,} unique employers')
    logger.info(f'  After normalization:  {after_unique:,} unique employers')
    logger.info(f'  Reduction: {reduction_count:,} ({reduction_pct:.1f}%)')

    # Target: At least 15% reduction in unique employers
    if reduction_pct < 15:
        logger.warning(f'⚠️  WARNING: Employer normalization achieved {reduction_pct:.1f}% reduction (target: ≥15%)')
        logger.warning(f'    Consider reviewing normalization rules in normalize_employer_name()')
    else:
        logger.info(f'✅ Employer normalization target achieved ({reduction_pct:.1f}% ≥ 15%)')

    # Sample normalized results for verification
    logger.info(f'\nSample normalization results:')
    sample = donors.select(['most.recent.contributor.employer', 'employer_normalized']).unique().head(10)
    for row in sample.iter_rows(named=True):
        orig = row['most.recent.contributor.employer']
        norm = row['employer_normalized']
        logger.info(f'  "{orig}" → "{norm}"')

    # STEP 5: Map to final schema
    donors_final = donors.rename({
        'bonica.cid': 'donor_id',
        'most.recent.contributor.name': 'name',
        'contributor.type': 'donor_type',
        'most.recent.contributor.occupation': 'occupation',
        'most.recent.contributor.state': 'state'
    })

    # Use normalized employer
    donors_final = donors_final.rename({'employer_normalized': 'employer'})

    # Add igcat column (industry category) if it doesn't exist
    # DIME data doesn't include this field, so we set it to NULL
    if 'igcat' not in donors_final.columns:
        donors_final = donors_final.with_columns(pl.lit(None).alias('igcat'))

    # Select final columns
    donors_final = donors_final.select([
        'donor_id',
        'name',
        'donor_type',
        'igcat',
        'employer',
        'occupation',
        'state',
        'total_contributions_count',
        'total_amount'
    ])

    # No need to filter zero-contribution donors since all donors are extracted from contributions

    # STEP 6: Validation
    assert donors_final['donor_id'].is_unique().all(), "donor_id must be unique!"
    assert donors_final['donor_id'].is_not_null().all(), "No null donor_ids!"
    # Expect at least 1M donors (full dataset has ~30M, single year has ~6-8M)
    assert len(donors_final) > 1_000_000, f"Expected 1M+ donors, got {len(donors_final)}"
    assert donors_final['total_contributions_count'].min() > 0, "All donors must have >0 contributions!"

    logger.info(f'\n✓ Validation passed:')
    logger.info(f'  - {len(donors_final):,} unique donors')
    logger.info(f'  - No duplicate donor_ids')
    logger.info(f'  - No null donor_ids')
    logger.info(f'  - Total contributions: {donors_final["total_contributions_count"].sum():,}')
    logger.info(f'  - Total amount: ${donors_final["total_amount"].sum():,.2f}')

    # STEP 7: Save final output
    output_dir = Path('data/transformed')
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / 'donors.parquet'
    donors_final.write_parquet(output_path)
    logger.info(f'\n✓ Saved to {output_path}')

    # Print summary statistics
    logger.info(f'\nSummary:')
    logger.info(f'  Donor type distribution:')
    type_counts = donors_final['donor_type'].value_counts().sort('donor_type')
    for row in type_counts.iter_rows(named=True):
        logger.info(f'    {row["donor_type"]}: {row["count"]:,}')

    logger.info(f'\n  FEC interest group category (igcat) distribution:')
    igcat_counts = donors_final['igcat'].value_counts().sort('igcat')
    for row in igcat_counts.iter_rows(named=True):
        igcat_val = row["igcat"] if row["igcat"] is not None else 'NULL'
        logger.info(f'    {igcat_val}: {row["count"]:,}')

    logger.info(f'\n  Top 10 states by donor count:')
    state_counts = donors_final['state'].value_counts().sort('count', descending=True).head(10)
    for row in state_counts.iter_rows(named=True):
        logger.info(f'    {row["state"]}: {row["count"]:,}')

    logger.info(f'\n  Donors with contributions:')
    with_contribs = donors_final.filter(pl.col('total_contributions_count') > 0)
    logger.info(f'    Count: {len(with_contribs):,} ({len(with_contribs)/len(donors_final)*100:.1f}%)')
    logger.info(f'    Total contributions: {with_contribs["total_contributions_count"].sum():,}')
    logger.info(f'    Total amount: ${with_contribs["total_amount"].sum():,.2f}')

    logger.info("\n" + "="*60)
    logger.info("✓ Step 2.2 Complete: Donors transformation successful")
    logger.info("="*60)


if __name__ == '__main__':
    main()
