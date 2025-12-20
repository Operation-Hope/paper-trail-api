"""
Transform contributions data with industry classification.

Implements stratified industry classification, type casting with validation,
and generates industry classification report for manual review.
"""

import polars as pl
import duckdb
import logging
import json
import re
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Any
from tqdm import tqdm

from utils.normalization import normalize_employer_name

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


# DIME occ.standardized → Industry mapping (31% coverage)
DIME_TO_INDUSTRY_MAP = {
    # Legal
    'attorney': 'Legal',
    'lobbyists': 'Legal',

    # Healthcare
    'physician': 'Healthcare',
    'nurse': 'Healthcare',
    'mental_health_professional': 'Healthcare',
    'pharma': 'Pharmaceuticals',

    # Finance & Real Estate
    'finance': 'Finance',
    'real_estate': 'Real Estate',
    'accountants': 'Finance',

    # Technology & Telecom
    'tech': 'Technology',
    'telecom': 'Telecommunications',

    # Energy
    'energy': 'Energy',
    'oil_gas': 'Energy',

    # Business & Corporate
    'corporate_executive': 'Business',
    'small_business': 'Business',
    'marketing_sales': 'Business',

    # Education
    'academics': 'Education',
    'school_teacher': 'Education',

    # Agriculture & Construction
    'agriculture': 'Agriculture',
    'housing_construction': 'Construction',

    # Media & Entertainment
    'media': 'Media',
    'journalist': 'Media',
    'entertainment': 'Entertainment',
    'creative': 'Arts & Entertainment',

    # Hospitality & Services
    'hotels_and_restaurants': 'Hospitality',
    'automotive_industry': 'Transportation',

    # Other Occupations
    'engineer': 'Engineering',
    'social_worker': 'Social Services',
    'clergy': 'Religious Organizations',
    'politicos': 'Government & Politics',
}


# Occupation-specific keywords (high signal for individuals)
OCCUPATION_KEYWORDS = {
    'Technology': ['software engineer', 'programmer', 'developer', 'data scientist', 'systems analyst'],
    'Finance': ['financial advisor', 'investment banker', 'portfolio manager', 'trader', 'analyst'],
    'Healthcare': ['doctor', 'physician', 'surgeon', 'nurse', 'medical assistant', 'dentist'],
    'Legal': ['attorney', 'lawyer', 'paralegal', 'legal counsel'],
    'Education': ['teacher', 'professor', 'educator', 'principal', 'superintendent'],
    'Engineering': ['mechanical engineer', 'civil engineer', 'electrical engineer', 'architect'],
}


# Employer-specific keywords (used for small donations and fallback)
EMPLOYER_KEYWORDS = {
    'Finance': ['bank', 'capital', 'investment', 'securities', 'financial', 'fund', 'asset management',
                'goldman', 'jpmorgan', 'morgan stanley', 'wells fargo', 'citi', 'merrill'],
    'Technology': ['software', 'tech', 'computer', 'microsoft', 'apple', 'google', 'amazon', 'facebook',
                   'meta', 'oracle', 'ibm', 'intel', 'cisco', 'salesforce'],
    'Healthcare': ['hospital', 'medical', 'health', 'clinic', 'healthcare', 'mayo', 'kaiser'],
    'Energy': ['energy', 'power', 'electric', 'utility', 'oil', 'gas', 'petroleum', 'solar', 'wind',
               'exxon', 'chevron', 'shell', 'bp', 'conoco'],
    'Legal': ['law', 'legal', 'counsel', 'litigation', 'attorneys'],
    'Education': ['university', 'college', 'school', 'education'],
    'Real Estate': ['real estate', 'property', 'realty', 'developer'],
    'Pharmaceuticals': ['pharma', 'pharmaceutical', 'drug', 'biotech', 'biopharm', 'pfizer', 'merck',
                        'johnson & johnson', 'abbvie', 'bristol myers'],
    'Telecommunications': ['telecom', 'telephone', 'wireless', 'verizon', 'at&t', 'sprint', 't-mobile'],
    'Agriculture': ['farm', 'agriculture', 'agricultural', 'crop', 'livestock', 'agribusiness'],
    'Manufacturing': ['manufacturing', 'factory', 'industrial', 'production'],
    'Transportation': ['transport', 'airline', 'shipping', 'logistics', 'automotive', 'auto',
                       'delta', 'american airlines', 'united', 'fedex', 'ups'],
    'Media': ['media', 'news', 'publishing', 'broadcast', 'television', 'newspaper'],
    'Entertainment': ['entertainment', 'film', 'movie', 'music', 'studio'],
    'Hospitality': ['hotel', 'restaurant', 'hospitality', 'food service'],
    'Business': ['consultant', 'consulting', 'management', 'business', 'executive', 'entrepreneur'],
    'Engineering': ['engineer', 'engineering'],
    'Social Services': ['social work', 'nonprofit', 'charity', 'foundation'],
    'Religious Organizations': ['church', 'religious', 'ministry', 'clergy', 'faith'],
    'Government & Politics': ['government', 'political', 'campaign', 'policy'],
}


def parse_pac_sponsor(pac_name):
    """Extract corporate sponsor from PAC name and classify."""
    pac_name = pac_name.lower()

    # Remove common PAC suffixes
    sponsor = pac_name.replace('political action committee', '')
    sponsor = sponsor.replace('pac', '').strip()

    # Match against known corporate keywords
    for industry, keywords in EMPLOYER_KEYWORDS.items():
        if any(kw in sponsor for kw in keywords):
            return industry

    return None


def classify_industry_stratified(row, employer_lookup: Dict[str, str]) -> str:
    """
    Stratified industry classification.

    - Large donations (≥$1,000): Prioritize accuracy, flag unknowns for review
    - PACs/Organizations: Use donor_type field for special handling
    - Small donations (<$1,000): Use full keyword approach for coverage
    """
    amount = float(row.get('amount', 0)) if row.get('amount') else 0
    donor_type = str(row.get('contributor.type', '')).strip()

    # STAGE 1: DIME occ.standardized (31% coverage, 100% accurate)
    occ_std = row.get('occ.standardized')
    if occ_std is not None:
        industry = DIME_TO_INDUSTRY_MAP.get(occ_std)
        if industry:
            return industry

    # INSTITUTIONAL DONORS PATH: PACs, Organizations, Corporations
    if donor_type in ['PAC', 'Organization', 'Corporation']:
        # Stage 2A: Employer name lookup
        employer_norm = normalize_employer_name(row.get('contributor.employer'))
        if employer_norm and employer_norm in employer_lookup:
            return employer_lookup[employer_norm]

        # Stage 2B: PAC sponsor parsing
        employer_text = str(row.get('contributor.employer', '')).lower()
        if 'pac' in employer_text or 'committee' in employer_text:
            industry = parse_pac_sponsor(employer_text)
            if industry:
                return industry

        # Stage 2C: Employer keyword matching
        for industry, keywords in EMPLOYER_KEYWORDS.items():
            if any(kw in employer_text for kw in keywords):
                return industry

        # Stage 2D: Flag for manual review (institutional donor without match)
        if amount >= 1000:
            return 'Other - Corporate Review Needed'
        else:
            return 'Other'

    # INDIVIDUAL DONORS PATH

    # Stage 3: Occupation keyword matching
    occupation = str(row.get('contributor.occupation', '')).lower()
    if occupation and occupation not in ['retired', 'homemaker', 'not employed', '']:
        for industry, keywords in OCCUPATION_KEYWORDS.items():
            if any(kw in occupation for kw in keywords):
                return industry

    # Stage 4: Employer name lookup
    employer_norm = normalize_employer_name(row.get('contributor.employer'))
    if employer_norm and employer_norm in employer_lookup:
        return employer_lookup[employer_norm]

    # Stage 5: Employer keyword matching (fallback)
    employer_text = str(row.get('contributor.employer', '')).lower()
    for industry, keywords in EMPLOYER_KEYWORDS.items():
        if any(kw in employer_text for kw in keywords):
            return industry

    # Stage 6: Flag large individual donations for review
    if amount >= 10000:
        return 'Other - High Value Review Needed'

    return 'Other'


@dataclass
class CastingResult:
    """Result of type casting operation with quality metrics."""
    field_name: str
    total_records: int
    successful_casts: int
    null_after_cast: int
    flagged_records: int
    error_samples: List[Dict[str, Any]]
    quality_score: float


def validate_and_cast_date(
    series: pl.Series,
    field_name: str,
    df: pl.DataFrame,
    min_date: str = "1980-01-01",
    max_date: str = "2024-12-31"
) -> tuple[pl.Series, CastingResult]:
    """Validate and cast date field with explicit error tracking."""

    # Identify placeholder values
    placeholders = ["0000-01-01", "1900-01-01", "", "nan", "NaT"]
    placeholder_mask = series.is_in(placeholders) | series.is_null()

    # Parse dates with explicit format validation
    parsed_dates = None
    parse_failures = []

    for date_format in ["%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"]:
        try:
            temp_parsed = series.str.strptime(pl.Date, format=date_format, strict=False)
            if parsed_dates is None:
                parsed_dates = temp_parsed
            else:
                parsed_dates = pl.when(parsed_dates.is_null()).then(temp_parsed).otherwise(parsed_dates)
        except:
            pass

    if parsed_dates is None:
        parsed_dates = pl.Series([None] * len(series), dtype=pl.Date)

    # Collect parse failures
    parse_failures_mask = (~placeholder_mask) & parsed_dates.is_null()
    if parse_failures_mask.sum() > 0:
        failure_indices = parse_failures_mask.arg_true()[:20]
        for idx in failure_indices:
            parse_failures.append({
                'row_index': int(idx),
                'transaction_id': str(df[idx, 'transaction_id']),
                'source_value': str(series[idx]),
                'error_type': 'date_parse_failure'
            })

    # Validate date range
    import datetime
    min_dt = datetime.datetime.strptime(min_date, "%Y-%m-%d").date()
    max_dt = datetime.datetime.strptime(max_date, "%Y-%m-%d").date()

    out_of_range_mask = ((parsed_dates < min_dt) | (parsed_dates > max_dt)) & parsed_dates.is_not_null()

    if out_of_range_mask.sum() > 0:
        oor_indices = out_of_range_mask.arg_true()[:20]
        for idx in oor_indices:
            parse_failures.append({
                'row_index': int(idx),
                'transaction_id': str(df[idx, 'transaction_id']),
                'source_value': str(parsed_dates[idx]),
                'error_type': 'out_of_range'
            })
        parsed_dates = pl.when(out_of_range_mask).then(None).otherwise(parsed_dates)

    # Apply placeholder mask
    result_series = pl.when(placeholder_mask).then(None).otherwise(parsed_dates)

    # Calculate metrics
    successful = result_series.is_not_null().sum()
    total = len(series)
    flagged = len(parse_failures)

    result = CastingResult(
        field_name=field_name,
        total_records=total,
        successful_casts=successful,
        null_after_cast=total - successful,
        flagged_records=flagged,
        error_samples=parse_failures[:20],
        quality_score=successful / total if total > 0 else 0.0
    )

    return result_series, result


def validate_and_cast_numeric(
    series: pl.Series,
    field_name: str,
    df: pl.DataFrame,
    min_value: float = 0.01,
    max_value: float = 100_000_000
) -> tuple[pl.Series, CastingResult]:
    """Validate and cast numeric field with explicit error tracking."""

    # Identify placeholder values
    placeholders = ["", "0", "0.00", "N/A", "NULL", "nan"]
    series_str = series.cast(pl.Utf8).str.strip_chars()
    placeholder_mask = series_str.is_in(placeholders) | series.is_null()

    # Attempt DECIMAL conversion
    temp_numeric = series.cast(pl.Float64, strict=False)
    parse_failures = []

    # Collect parse failures
    parse_failures_mask = (~placeholder_mask) & temp_numeric.is_null()
    if parse_failures_mask.sum() > 0:
        failure_indices = parse_failures_mask.arg_true()[:20]
        for idx in failure_indices:
            parse_failures.append({
                'row_index': int(idx),
                'transaction_id': str(df[idx, 'transaction_id']),
                'source_value': str(series[idx]),
                'error_type': 'numeric_parse_failure'
            })

    # Validate range
    out_of_range_mask = ((temp_numeric < min_value) | (temp_numeric > max_value)) & temp_numeric.is_not_null()

    if out_of_range_mask.sum() > 0:
        oor_indices = out_of_range_mask.arg_true()[:20]
        for idx in oor_indices:
            parse_failures.append({
                'row_index': int(idx),
                'transaction_id': str(df[idx, 'transaction_id']),
                'source_value': str(temp_numeric[idx]),
                'error_type': 'out_of_range'
            })
        temp_numeric = pl.when(out_of_range_mask).then(None).otherwise(temp_numeric)

    # Apply placeholder mask
    validated_numeric = pl.when(placeholder_mask).then(None).otherwise(temp_numeric)

    # Convert to DECIMAL(12,2) for exact precision
    result_series = validated_numeric.cast(pl.Decimal(precision=12, scale=2))

    # Calculate metrics
    successful = result_series.is_not_null().sum()
    total = len(series)
    flagged = len(parse_failures)

    result = CastingResult(
        field_name=field_name,
        total_records=total,
        successful_casts=successful,
        null_after_cast=total - successful,
        flagged_records=flagged,
        error_samples=parse_failures[:20],
        quality_score=successful / total if total > 0 else 0.0
    )

    return result_series, result


def main():
    """Transform contributions from DIME data."""

    logger.info("="*60)
    logger.info("PHASE 2 - STEP 2.3: Transform Contributions")
    logger.info("="*60 + "\n")

    # STEP 1: Build Employer Lookup Table from DIME-classified Data
    logger.info("=== Building Employer→Industry Lookup ===\n")

    file_pattern = 'data/raw/contributions/contrib_*_filtered.parquet'

    logger.info('Processing contributions via DuckDB SQL aggregation...')

    # MEMORY OPTIMIZATION: Build employer lookup using pure DuckDB SQL
    # Uses raw employer names from DIME data (no normalization)
    # This approach avoids Python UDF performance issues (ITERATION 2 failed with 20+ min runtime)
    #
    # Trade-off: Slightly more lookup entries vs. simplicity and performance
    # DIME data already has good employer consistency (31% coverage achieved)
    employer_lookup_df = duckdb.query(f"""
        WITH employer_industry AS (
            SELECT
                "contributor.employer" as employer,
                -- Native SQL industry mapping from DIME occ.standardized
                CASE "occ.standardized"
                    WHEN 'attorney' THEN 'Legal'
                    WHEN 'lobbyists' THEN 'Legal'
                    WHEN 'physician' THEN 'Healthcare'
                    WHEN 'nurse' THEN 'Healthcare'
                    WHEN 'mental_health_professional' THEN 'Healthcare'
                    WHEN 'pharma' THEN 'Pharmaceuticals'
                    WHEN 'finance' THEN 'Finance'
                    WHEN 'real_estate' THEN 'Real Estate'
                    WHEN 'accountants' THEN 'Finance'
                    WHEN 'tech' THEN 'Technology'
                    WHEN 'telecom' THEN 'Telecommunications'
                    WHEN 'energy' THEN 'Energy'
                    WHEN 'oil_gas' THEN 'Energy'
                    WHEN 'corporate_executive' THEN 'Business'
                    WHEN 'small_business' THEN 'Business'
                    WHEN 'marketing_sales' THEN 'Business'
                    WHEN 'academics' THEN 'Education'
                    WHEN 'school_teacher' THEN 'Education'
                    WHEN 'agriculture' THEN 'Agriculture'
                    WHEN 'housing_construction' THEN 'Construction'
                    WHEN 'media' THEN 'Media'
                    WHEN 'journalist' THEN 'Media'
                    WHEN 'entertainment' THEN 'Entertainment'
                    WHEN 'creative' THEN 'Arts & Entertainment'
                    WHEN 'hotels_and_restaurants' THEN 'Hospitality'
                    WHEN 'automotive_industry' THEN 'Transportation'
                    WHEN 'engineer' THEN 'Engineering'
                    WHEN 'social_worker' THEN 'Social Services'
                    WHEN 'clergy' THEN 'Religious Organizations'
                    WHEN 'politicos' THEN 'Government & Politics'
                    ELSE NULL
                END as industry
            FROM read_parquet('{file_pattern}')
            WHERE "occ.standardized" IS NOT NULL
                AND "contributor.employer" IS NOT NULL
                AND TRIM("contributor.employer") != ''
        ),
        counts AS (
            SELECT employer, industry, COUNT(*) as contribution_count
            FROM employer_industry
            WHERE industry IS NOT NULL
            GROUP BY employer, industry
        ),
        ranked AS (
            SELECT employer, industry, contribution_count,
                   ROW_NUMBER() OVER (PARTITION BY employer ORDER BY contribution_count DESC) as rank
            FROM counts
        )
        SELECT employer, industry, contribution_count as count
        FROM ranked WHERE rank = 1
        ORDER BY contribution_count DESC
    """).pl()

    # Convert to Python dict for fast lookups during classification
    employer_lookup = dict(zip(employer_lookup_df['employer'], employer_lookup_df['industry']))

    logger.info(f'✓ Built employer lookup with {len(employer_lookup):,} unique employers via SQL aggregation')

    # Save employer lookup for inspection/debugging
    output_dir = Path('data/transformed')
    output_dir.mkdir(parents=True, exist_ok=True)

    employer_lookup_df.write_csv(output_dir / 'employer_industry_lookup.csv')
    logger.info('✓ Saved employer lookup to employer_industry_lookup.csv\n')

    # STEP 2: Apply Stratified Industry Classification (Full DuckDB SQL)
    logger.info("=== Applying Stratified Industry Classification ===\n")

    # MEMORY OPTIMIZATION: Use full DuckDB SQL classification instead of Python map_elements
    # Previous approach caused OOM on large year files (2016: 21M rows, 2018: 16M rows)
    # New approach: All processing stays in DuckDB (columnar, memory-efficient)
    logger.info('Classifying all contributions via DuckDB SQL...')

    # Register employer lookup as temp table for SQL joins
    # Using raw employer names (no normalization)
    employer_lookup_table = pl.DataFrame([
        {'employer': k, 'industry': v}
        for k, v in employer_lookup.items()
    ])
    duckdb.register('employer_lookup', employer_lookup_table)

    # Create temp directory for classified output
    temp_dir = output_dir / 'temp_classified'
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_output = temp_dir / 'classified_all.parquet'

    # Build complete SQL classification query with stratified logic
    classification_query = f"""
        WITH contributions_base AS (
            SELECT *
            FROM read_parquet('{file_pattern}')
        ),

        -- Stage 1: DIME occ.standardized mapping (31% coverage)
        stage1_classified AS (
            SELECT
                *,
                CASE "occ.standardized"
                    WHEN 'attorney' THEN 'Legal'
                    WHEN 'lobbyists' THEN 'Legal'
                    WHEN 'physician' THEN 'Healthcare'
                    WHEN 'nurse' THEN 'Healthcare'
                    WHEN 'mental_health_professional' THEN 'Healthcare'
                    WHEN 'pharma' THEN 'Pharmaceuticals'
                    WHEN 'finance' THEN 'Finance'
                    WHEN 'real_estate' THEN 'Real Estate'
                    WHEN 'accountants' THEN 'Finance'
                    WHEN 'tech' THEN 'Technology'
                    WHEN 'telecom' THEN 'Telecommunications'
                    WHEN 'energy' THEN 'Energy'
                    WHEN 'oil_gas' THEN 'Energy'
                    WHEN 'corporate_executive' THEN 'Business'
                    WHEN 'small_business' THEN 'Business'
                    WHEN 'marketing_sales' THEN 'Business'
                    WHEN 'academics' THEN 'Education'
                    WHEN 'school_teacher' THEN 'Education'
                    WHEN 'agriculture' THEN 'Agriculture'
                    WHEN 'housing_construction' THEN 'Construction'
                    WHEN 'media' THEN 'Media'
                    WHEN 'journalist' THEN 'Media'
                    WHEN 'entertainment' THEN 'Entertainment'
                    WHEN 'creative' THEN 'Arts & Entertainment'
                    WHEN 'hotels_and_restaurants' THEN 'Hospitality'
                    WHEN 'automotive_industry' THEN 'Transportation'
                    WHEN 'engineer' THEN 'Engineering'
                    WHEN 'social_worker' THEN 'Social Services'
                    WHEN 'clergy' THEN 'Religious Organizations'
                    WHEN 'politicos' THEN 'Government & Politics'
                    ELSE NULL
                END as dime_industry
            FROM contributions_base
        ),

        -- Join with employer lookup table (no normalization needed)
        contributions_with_lookup AS (
            SELECT
                c.*,
                el.industry as employer_lookup_industry
            FROM stage1_classified c
            LEFT JOIN employer_lookup el ON c."contributor.employer" = el.employer
        ),

        -- Apply full stratified classification logic
        classified AS (
            SELECT
                *,
                CASE
                    -- Stage 1: DIME classification (if available)
                    WHEN dime_industry IS NOT NULL THEN dime_industry

                    -- Institutional donors path (PAC, Organization, Corporation)
                    WHEN "contributor.type" IN ('PAC', 'Organization', 'Corporation') THEN
                        CASE
                            -- Stage 2A: Employer lookup
                            WHEN employer_lookup_industry IS NOT NULL THEN employer_lookup_industry

                            -- Stage 2B: Employer keyword matching
                            -- Finance
                            WHEN LOWER("contributor.employer") LIKE '%bank%'
                                OR LOWER("contributor.employer") LIKE '%capital%'
                                OR LOWER("contributor.employer") LIKE '%investment%'
                                OR LOWER("contributor.employer") LIKE '%securities%'
                                OR LOWER("contributor.employer") LIKE '%financial%'
                                OR LOWER("contributor.employer") LIKE '%fund%'
                                OR LOWER("contributor.employer") LIKE '%asset management%'
                                OR LOWER("contributor.employer") LIKE '%goldman%'
                                OR LOWER("contributor.employer") LIKE '%jpmorgan%'
                                OR LOWER("contributor.employer") LIKE '%morgan stanley%'
                                OR LOWER("contributor.employer") LIKE '%wells fargo%'
                                OR LOWER("contributor.employer") LIKE '%citi%'
                                OR LOWER("contributor.employer") LIKE '%merrill%' THEN 'Finance'

                            -- Technology
                            WHEN LOWER("contributor.employer") LIKE '%software%'
                                OR LOWER("contributor.employer") LIKE '%tech%'
                                OR LOWER("contributor.employer") LIKE '%computer%'
                                OR LOWER("contributor.employer") LIKE '%microsoft%'
                                OR LOWER("contributor.employer") LIKE '%apple%'
                                OR LOWER("contributor.employer") LIKE '%google%'
                                OR LOWER("contributor.employer") LIKE '%amazon%'
                                OR LOWER("contributor.employer") LIKE '%facebook%'
                                OR LOWER("contributor.employer") LIKE '%meta%'
                                OR LOWER("contributor.employer") LIKE '%oracle%'
                                OR LOWER("contributor.employer") LIKE '%ibm%'
                                OR LOWER("contributor.employer") LIKE '%intel%'
                                OR LOWER("contributor.employer") LIKE '%cisco%'
                                OR LOWER("contributor.employer") LIKE '%salesforce%' THEN 'Technology'

                            -- Healthcare
                            WHEN LOWER("contributor.employer") LIKE '%hospital%'
                                OR LOWER("contributor.employer") LIKE '%medical%'
                                OR LOWER("contributor.employer") LIKE '%health%'
                                OR LOWER("contributor.employer") LIKE '%clinic%'
                                OR LOWER("contributor.employer") LIKE '%healthcare%'
                                OR LOWER("contributor.employer") LIKE '%mayo%'
                                OR LOWER("contributor.employer") LIKE '%kaiser%' THEN 'Healthcare'

                            -- Energy
                            WHEN LOWER("contributor.employer") LIKE '%energy%'
                                OR LOWER("contributor.employer") LIKE '%power%'
                                OR LOWER("contributor.employer") LIKE '%electric%'
                                OR LOWER("contributor.employer") LIKE '%utility%'
                                OR LOWER("contributor.employer") LIKE '%oil%'
                                OR LOWER("contributor.employer") LIKE '%gas%'
                                OR LOWER("contributor.employer") LIKE '%petroleum%'
                                OR LOWER("contributor.employer") LIKE '%solar%'
                                OR LOWER("contributor.employer") LIKE '%wind%'
                                OR LOWER("contributor.employer") LIKE '%exxon%'
                                OR LOWER("contributor.employer") LIKE '%chevron%'
                                OR LOWER("contributor.employer") LIKE '%shell%'
                                OR LOWER("contributor.employer") LIKE '%bp%'
                                OR LOWER("contributor.employer") LIKE '%conoco%' THEN 'Energy'

                            -- Legal
                            WHEN LOWER("contributor.employer") LIKE '%law%'
                                OR LOWER("contributor.employer") LIKE '%legal%'
                                OR LOWER("contributor.employer") LIKE '%counsel%'
                                OR LOWER("contributor.employer") LIKE '%litigation%'
                                OR LOWER("contributor.employer") LIKE '%attorneys%' THEN 'Legal'

                            -- Education
                            WHEN LOWER("contributor.employer") LIKE '%university%'
                                OR LOWER("contributor.employer") LIKE '%college%'
                                OR LOWER("contributor.employer") LIKE '%school%'
                                OR LOWER("contributor.employer") LIKE '%education%' THEN 'Education'

                            -- Real Estate
                            WHEN LOWER("contributor.employer") LIKE '%real estate%'
                                OR LOWER("contributor.employer") LIKE '%property%'
                                OR LOWER("contributor.employer") LIKE '%realty%'
                                OR LOWER("contributor.employer") LIKE '%developer%' THEN 'Real Estate'

                            -- Pharmaceuticals
                            WHEN LOWER("contributor.employer") LIKE '%pharma%'
                                OR LOWER("contributor.employer") LIKE '%pharmaceutical%'
                                OR LOWER("contributor.employer") LIKE '%drug%'
                                OR LOWER("contributor.employer") LIKE '%biotech%'
                                OR LOWER("contributor.employer") LIKE '%biopharm%'
                                OR LOWER("contributor.employer") LIKE '%pfizer%'
                                OR LOWER("contributor.employer") LIKE '%merck%'
                                OR LOWER("contributor.employer") LIKE '%johnson & johnson%'
                                OR LOWER("contributor.employer") LIKE '%abbvie%'
                                OR LOWER("contributor.employer") LIKE '%bristol myers%' THEN 'Pharmaceuticals'

                            -- Telecommunications
                            WHEN LOWER("contributor.employer") LIKE '%telecom%'
                                OR LOWER("contributor.employer") LIKE '%telephone%'
                                OR LOWER("contributor.employer") LIKE '%wireless%'
                                OR LOWER("contributor.employer") LIKE '%verizon%'
                                OR LOWER("contributor.employer") LIKE '%at&t%'
                                OR LOWER("contributor.employer") LIKE '%sprint%'
                                OR LOWER("contributor.employer") LIKE '%t-mobile%' THEN 'Telecommunications'

                            -- Agriculture
                            WHEN LOWER("contributor.employer") LIKE '%farm%'
                                OR LOWER("contributor.employer") LIKE '%agriculture%'
                                OR LOWER("contributor.employer") LIKE '%agricultural%'
                                OR LOWER("contributor.employer") LIKE '%crop%'
                                OR LOWER("contributor.employer") LIKE '%livestock%'
                                OR LOWER("contributor.employer") LIKE '%agribusiness%' THEN 'Agriculture'

                            -- Manufacturing
                            WHEN LOWER("contributor.employer") LIKE '%manufacturing%'
                                OR LOWER("contributor.employer") LIKE '%factory%'
                                OR LOWER("contributor.employer") LIKE '%industrial%'
                                OR LOWER("contributor.employer") LIKE '%production%' THEN 'Manufacturing'

                            -- Transportation
                            WHEN LOWER("contributor.employer") LIKE '%transport%'
                                OR LOWER("contributor.employer") LIKE '%airline%'
                                OR LOWER("contributor.employer") LIKE '%shipping%'
                                OR LOWER("contributor.employer") LIKE '%logistics%'
                                OR LOWER("contributor.employer") LIKE '%automotive%'
                                OR LOWER("contributor.employer") LIKE '%auto%'
                                OR LOWER("contributor.employer") LIKE '%delta%'
                                OR LOWER("contributor.employer") LIKE '%american airlines%'
                                OR LOWER("contributor.employer") LIKE '%united%'
                                OR LOWER("contributor.employer") LIKE '%fedex%'
                                OR LOWER("contributor.employer") LIKE '%ups%' THEN 'Transportation'

                            -- Media
                            WHEN LOWER("contributor.employer") LIKE '%media%'
                                OR LOWER("contributor.employer") LIKE '%news%'
                                OR LOWER("contributor.employer") LIKE '%publishing%'
                                OR LOWER("contributor.employer") LIKE '%broadcast%'
                                OR LOWER("contributor.employer") LIKE '%television%'
                                OR LOWER("contributor.employer") LIKE '%newspaper%' THEN 'Media'

                            -- Entertainment
                            WHEN LOWER("contributor.employer") LIKE '%entertainment%'
                                OR LOWER("contributor.employer") LIKE '%film%'
                                OR LOWER("contributor.employer") LIKE '%movie%'
                                OR LOWER("contributor.employer") LIKE '%music%'
                                OR LOWER("contributor.employer") LIKE '%studio%' THEN 'Entertainment'

                            -- Hospitality
                            WHEN LOWER("contributor.employer") LIKE '%hotel%'
                                OR LOWER("contributor.employer") LIKE '%restaurant%'
                                OR LOWER("contributor.employer") LIKE '%hospitality%'
                                OR LOWER("contributor.employer") LIKE '%food service%' THEN 'Hospitality'

                            -- Business
                            WHEN LOWER("contributor.employer") LIKE '%consultant%'
                                OR LOWER("contributor.employer") LIKE '%consulting%'
                                OR LOWER("contributor.employer") LIKE '%management%'
                                OR LOWER("contributor.employer") LIKE '%business%'
                                OR LOWER("contributor.employer") LIKE '%executive%'
                                OR LOWER("contributor.employer") LIKE '%entrepreneur%' THEN 'Business'

                            -- Engineering
                            WHEN LOWER("contributor.employer") LIKE '%engineer%'
                                OR LOWER("contributor.employer") LIKE '%engineering%' THEN 'Engineering'

                            -- Social Services
                            WHEN LOWER("contributor.employer") LIKE '%social work%'
                                OR LOWER("contributor.employer") LIKE '%nonprofit%'
                                OR LOWER("contributor.employer") LIKE '%charity%'
                                OR LOWER("contributor.employer") LIKE '%foundation%' THEN 'Social Services'

                            -- Religious Organizations
                            WHEN LOWER("contributor.employer") LIKE '%church%'
                                OR LOWER("contributor.employer") LIKE '%religious%'
                                OR LOWER("contributor.employer") LIKE '%ministry%'
                                OR LOWER("contributor.employer") LIKE '%clergy%'
                                OR LOWER("contributor.employer") LIKE '%faith%' THEN 'Religious Organizations'

                            -- Government & Politics
                            WHEN LOWER("contributor.employer") LIKE '%government%'
                                OR LOWER("contributor.employer") LIKE '%political%'
                                OR LOWER("contributor.employer") LIKE '%campaign%'
                                OR LOWER("contributor.employer") LIKE '%policy%' THEN 'Government & Politics'

                            -- Stage 2D: Flag large institutional donations for review
                            WHEN TRY_CAST(amount AS DOUBLE) >= 1000 THEN 'Other - Corporate Review Needed'
                            ELSE 'Other'
                        END

                    -- Individual donors path
                    ELSE
                        CASE
                            -- Stage 3: Occupation keyword matching
                            WHEN LOWER("contributor.occupation") LIKE '%software engineer%'
                                OR LOWER("contributor.occupation") LIKE '%programmer%'
                                OR LOWER("contributor.occupation") LIKE '%developer%'
                                OR LOWER("contributor.occupation") LIKE '%data scientist%'
                                OR LOWER("contributor.occupation") LIKE '%systems analyst%' THEN 'Technology'

                            WHEN LOWER("contributor.occupation") LIKE '%financial advisor%'
                                OR LOWER("contributor.occupation") LIKE '%investment banker%'
                                OR LOWER("contributor.occupation") LIKE '%portfolio manager%'
                                OR LOWER("contributor.occupation") LIKE '%trader%'
                                OR LOWER("contributor.occupation") LIKE '%analyst%' THEN 'Finance'

                            WHEN LOWER("contributor.occupation") LIKE '%doctor%'
                                OR LOWER("contributor.occupation") LIKE '%physician%'
                                OR LOWER("contributor.occupation") LIKE '%surgeon%'
                                OR LOWER("contributor.occupation") LIKE '%nurse%'
                                OR LOWER("contributor.occupation") LIKE '%medical assistant%'
                                OR LOWER("contributor.occupation") LIKE '%dentist%' THEN 'Healthcare'

                            WHEN LOWER("contributor.occupation") LIKE '%attorney%'
                                OR LOWER("contributor.occupation") LIKE '%lawyer%'
                                OR LOWER("contributor.occupation") LIKE '%paralegal%'
                                OR LOWER("contributor.occupation") LIKE '%legal counsel%' THEN 'Legal'

                            WHEN LOWER("contributor.occupation") LIKE '%teacher%'
                                OR LOWER("contributor.occupation") LIKE '%professor%'
                                OR LOWER("contributor.occupation") LIKE '%educator%'
                                OR LOWER("contributor.occupation") LIKE '%principal%'
                                OR LOWER("contributor.occupation") LIKE '%superintendent%' THEN 'Education'

                            WHEN LOWER("contributor.occupation") LIKE '%mechanical engineer%'
                                OR LOWER("contributor.occupation") LIKE '%civil engineer%'
                                OR LOWER("contributor.occupation") LIKE '%electrical engineer%'
                                OR LOWER("contributor.occupation") LIKE '%architect%'
                                OR LOWER("contributor.occupation") LIKE '%engineer%' THEN 'Engineering'

                            -- Stage 4: Employer lookup (already joined above)
                            WHEN employer_lookup_industry IS NOT NULL THEN employer_lookup_industry

                            -- Stage 5: Employer keyword matching (reuse institutional keywords)
                            WHEN LOWER("contributor.employer") LIKE '%bank%'
                                OR LOWER("contributor.employer") LIKE '%capital%'
                                OR LOWER("contributor.employer") LIKE '%investment%' THEN 'Finance'

                            WHEN LOWER("contributor.employer") LIKE '%tech%'
                                OR LOWER("contributor.employer") LIKE '%software%'
                                OR LOWER("contributor.employer") LIKE '%google%'
                                OR LOWER("contributor.employer") LIKE '%microsoft%' THEN 'Technology'

                            WHEN LOWER("contributor.employer") LIKE '%hospital%'
                                OR LOWER("contributor.employer") LIKE '%health%'
                                OR LOWER("contributor.employer") LIKE '%medical%' THEN 'Healthcare'

                            WHEN LOWER("contributor.employer") LIKE '%energy%'
                                OR LOWER("contributor.employer") LIKE '%oil%'
                                OR LOWER("contributor.employer") LIKE '%electric%' THEN 'Energy'

                            WHEN LOWER("contributor.employer") LIKE '%law%'
                                OR LOWER("contributor.employer") LIKE '%legal%' THEN 'Legal'

                            WHEN LOWER("contributor.employer") LIKE '%university%'
                                OR LOWER("contributor.employer") LIKE '%school%' THEN 'Education'

                            -- Stage 6: Flag large individual donations for review
                            WHEN TRY_CAST(amount AS DOUBLE) >= 10000 THEN 'Other - High Value Review Needed'

                            ELSE 'Other'
                        END
                END as industry
            FROM contributions_with_lookup
        )

        SELECT * FROM classified
    """

    # Execute SQL query - writes directly to temp output (minimal memory usage)
    duckdb.query(f"COPY ({classification_query}) TO '{temp_output}' (FORMAT PARQUET)")

    logger.info(f"✓ Classified all contributions via SQL and wrote to temp file\n")

    # STEP 3: Report Classification Metrics (using DuckDB for memory efficiency)
    logger.info("=== Industry Classification Report ===\n")

    # Use DuckDB to query classified temp file without loading all into memory
    temp_file_str = str(temp_output)

    total = duckdb.query(f"SELECT COUNT(*) as cnt FROM read_parquet('{temp_file_str}')").fetchone()[0]
    logger.info(f"Total contributions: {total:,}")

    # DIME coverage
    dime_coverage = duckdb.query(f"""
        SELECT COUNT(*) as cnt
        FROM read_parquet('{temp_file_str}')
        WHERE "occ.standardized" IS NOT NULL
    """).fetchone()[0]
    logger.info(f"DIME occ.standardized coverage: {dime_coverage:,} ({100*dime_coverage/total:.1f}%)")

    # Donor type breakdown
    logger.info("\nDonor type distribution:")
    donor_types = duckdb.query(f"""
        SELECT "contributor.type", COUNT(*) as count
        FROM read_parquet('{temp_file_str}')
        GROUP BY "contributor.type"
        ORDER BY "contributor.type"
    """).pl()
    for row in donor_types.iter_rows(named=True):
        dtype = row['contributor.type']
        count = row['count']
        logger.info(f"  {dtype}: {count:,} ({100*count/total:.1f}%)")

    # Industry distribution
    logger.info("\nIndustry distribution (top 20):")
    industry_dist = duckdb.query(f"""
        SELECT industry, COUNT(*) as count
        FROM read_parquet('{temp_file_str}')
        GROUP BY industry
        ORDER BY count DESC
        LIMIT 20
    """).pl()
    for row in industry_dist.iter_rows(named=True):
        logger.info(f"  {row['industry']}: {row['count']:,} ({100*row['count']/total:.1f}%)")

    # Large donations accuracy
    large_count = duckdb.query(f"""
        SELECT COUNT(*) as cnt
        FROM read_parquet('{temp_file_str}')
        WHERE CAST(amount AS DOUBLE) >= 1000
    """).fetchone()[0]
    logger.info(f"\nLarge donations (≥$1,000): {large_count:,} ({100*large_count/total:.1f}%)")

    large_other = duckdb.query(f"""
        SELECT COUNT(*) as cnt
        FROM read_parquet('{temp_file_str}')
        WHERE CAST(amount AS DOUBLE) >= 1000 AND industry LIKE 'Other%'
    """).fetchone()[0]
    logger.info(f"  Classified as 'Other': {large_other:,} ({100*large_other/large_count:.1f}%)")

    # PAC accuracy
    pac_count = duckdb.query(f"""
        SELECT COUNT(*) as cnt
        FROM read_parquet('{temp_file_str}')
        WHERE "contributor.type" = 'PAC'
    """).fetchone()[0]
    if pac_count > 0:
        logger.info(f"\nPAC contributions: {pac_count:,}")
        pac_other = duckdb.query(f"""
            SELECT COUNT(*) as cnt
            FROM read_parquet('{temp_file_str}')
            WHERE "contributor.type" = 'PAC' AND industry LIKE 'Other%'
        """).fetchone()[0]
        logger.info(f"  Classified as 'Other': {pac_other:,} ({100*pac_other/pac_count:.1f}%)")

    # Manual review needed
    review_needed_count = duckdb.query(f"""
        SELECT COUNT(*) as cnt
        FROM read_parquet('{temp_file_str}')
        WHERE industry LIKE '%Review Needed%'
    """).fetchone()[0]
    logger.info(f"\nFlagged for manual review: {review_needed_count:,} ({100*review_needed_count/total:.2f}%)\n")

    # STEP 4-7: Use DuckDB to create final output with all transformations
    logger.info("=== Creating Final Output with DuckDB ===\n")

    # Use DuckDB to process classified data with transformations applied
    logger.info("Processing final schema, type casting, and filtering via DuckDB...")

    # Create final output using DuckDB SQL (memory efficient)
    output_path = output_dir / 'contributions_final.parquet'

    duckdb.query(f"""
        COPY (
            SELECT
                "transaction.id" as transaction_id,
                "bonica.cid" as donor_id,
                "bonica.rid" as recipient_id,
                CAST(amount AS DECIMAL(12,2)) as amount,
                CAST(date AS DATE) as transaction_date,
                industry,
                CAST(cycle AS INTEGER) as election_cycle,
                "contributor.name" as raw_contributor_name,
                "contributor.employer" as raw_employer
            FROM read_parquet('{temp_file_str}')
            WHERE
                "transaction.id" IS NOT NULL
                AND "bonica.cid" IS NOT NULL
                AND "bonica.rid" IS NOT NULL
                AND TRY_CAST(amount AS DECIMAL(12,2)) IS NOT NULL
                AND TRY_CAST(date AS DATE) IS NOT NULL
                AND TRY_CAST(date AS DATE) >= DATE '1980-01-01'
                AND TRY_CAST(date AS DATE) <= DATE '2024-12-31'
        ) TO '{output_path}' (FORMAT PARQUET)
    """)

    logger.info(f"✓ Created final contributions file: {output_path}\n")

    # Get final counts and validation metrics
    logger.info("=== Final Validation Metrics ===\n")

    final_count = duckdb.query(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]
    logger.info(f"Final contribution count: {final_count:,}")

    # Calculate quality metrics
    total_before_filter = total
    filtered_count = total_before_filter - final_count
    quality_score = final_count / total_before_filter if total_before_filter > 0 else 0

    logger.info(f"Records before filtering: {total_before_filter:,}")
    logger.info(f"Records filtered out: {filtered_count:,} ({100*filtered_count/total_before_filter:.2f}%)")
    logger.info(f"Quality score: {quality_score:.2%}")

    MIN_QUALITY_THRESHOLD = 0.99
    if quality_score < MIN_QUALITY_THRESHOLD:
        logger.error(f"✗ Quality threshold not met: {quality_score:.2%} < {MIN_QUALITY_THRESHOLD:.0%}")
        raise ValueError(f"Type casting quality below threshold: {quality_score:.2%}")

    logger.info(f"✓ Type casting validation passed (quality: {quality_score:.2%} ≥ 99%)\n")

    # Verify no NULLs in required fields
    null_check = duckdb.query(f"""
        SELECT
            SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) as null_transaction_id,
            SUM(CASE WHEN donor_id IS NULL THEN 1 ELSE 0 END) as null_donor_id,
            SUM(CASE WHEN recipient_id IS NULL THEN 1 ELSE 0 END) as null_recipient_id,
            SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) as null_amount,
            SUM(CASE WHEN transaction_date IS NULL THEN 1 ELSE 0 END) as null_transaction_date
        FROM read_parquet('{output_path}')
    """).fetchone()

    total_nulls = sum(null_check)
    assert total_nulls == 0, f"Required fields still contain NULLs! {null_check}"
    logger.info("✓ NULL policy enforced: No NULLs in required fields\n")

    # Summary statistics
    summary = duckdb.query(f"""
        SELECT
            COUNT(*) as total_contributions,
            SUM(amount) as total_amount,
            MIN(transaction_date) as min_date,
            MAX(transaction_date) as max_date,
            MIN(election_cycle) as min_cycle,
            MAX(election_cycle) as max_cycle
        FROM read_parquet('{output_path}')
    """).fetchone()

    logger.info(f'Summary:')
    logger.info(f'  Total contributions: {summary[0]:,}')
    logger.info(f'  Total amount: ${summary[1]:,.2f}')
    logger.info(f'  Date range: {summary[2]} to {summary[3]}')
    logger.info(f'  Election cycles: {summary[4]} to {summary[5]}')

    # Clean up temp files
    logger.info("\nCleaning up temporary files...")
    import shutil
    shutil.rmtree(temp_dir)
    logger.info("✓ Temporary files removed")

    logger.info("\n" + "="*60)
    logger.info("✓ Step 2.3 Complete: Contributions transformation successful")
    logger.info("="*60)
    logger.info("\nNEXT STEP: Manual validation of industry classification (Step 2.3.5)")
    logger.info("Review data/transformed/employer_industry_lookup.csv for top employers")


if __name__ == '__main__':
    main()
