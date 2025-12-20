"""
Text normalization utilities for ETL processing.

Provides functions for normalizing employer names, occupation strings,
and other text fields to improve data deduplication and matching.
"""

import re
from typing import Final

# Self-employment indicators
SELF_EMPLOYED_PATTERNS: Final[frozenset[str]] = frozenset({
    'SELF', 'SELF-EMPLOYED', 'SELF EMPLOYED', 'SELF EMPLOYEE',
    'RETIRED', 'HOMEMAKER', 'NOT EMPLOYED', 'UNEMPLOYED', 'N/A', 'NONE'
})

# Legal entity suffixes to remove
LEGAL_SUFFIXES: Final[tuple[str, ...]] = (
    r'\s+INC\b', r'\s+INCORPORATED\b',
    r'\s+LLC\b', r'\s+LLP\b',
    r'\s+CORP\b', r'\s+CORPORATION\b',
    r'\s+LTD\b', r'\s+LIMITED\b',
    r'\s+CO\b', r'\s+COMPANY\b',
    r'\s+LP\b', r'\s+PC\b', r'\s+PA\b', r'\s+PLLC\b',
    r'\s+USA\b',
    r'\s+U\s+S\s+A\b', r'\s+SERVICES\b', r'\s+GROUP\b',
    r'\s+STORE\s+SUPPORT\b', r'\s+PRODUCT\s+AUTHORITY\b',
    r'\s+DISTRIBUTION\s+CENTER\b', r'\s+DISTRIBUTION\b'
)

# Common abbreviations and expansions
ABBREVIATION_MAP: Final[dict[str, str]] = {
    'UNIVERSITY OF': 'UNIV',
    'UNIVERSITY': 'UNIV',
    ' AND ': ' & ',
    'INTERNATIONAL': 'INTL',
    'INCORPORATED': '',
    'CORPORATION': '',
    'DEPARTMENT': 'DEPT',
    'GOVERNMENT': 'GOVT',
    'FEDERAL': 'FED',
    'RETIRED ': '',
    ' RETIRED': ''
}


def normalize_employer_name(name: str | None) -> str:
    """
    Normalize employer name for deduplication.

    Normalizes employer names to improve deduplication by:
    - Removing legal entity suffixes (INC, LLC, CORP, etc.)
    - Standardizing common abbreviations
    - Handling self-employment consistently
    - Removing punctuation while preserving ampersands
    - Collapsing whitespace

    Args:
        name: Raw employer name string, or None

    Returns:
        Normalized employer name, or 'SELF-EMPLOYED' for empty/self-employed values

    Examples:
        >>> normalize_employer_name("Google Inc.")
        'GOOGLE'
        >>> normalize_employer_name("self employed")
        'SELF-EMPLOYED'
        >>> normalize_employer_name("The University of California")
        'UNIV CALIFORNIA'
    """
    if name is None or name == '':
        return 'SELF-EMPLOYED'

    name = str(name).strip().upper()

    # Handle self-employment variations
    if name in SELF_EMPLOYED_PATTERNS or name.startswith('SELF '):
        return 'SELF-EMPLOYED'

    # Remove "THE" prefix early
    if name.startswith('THE '):
        name = name[4:]

    # Remove punctuation EXCEPT ampersand (keep corporate partnerships distinct)
    name = re.sub(r'[^\w\s&]', ' ', name)

    # Normalize whitespace after punctuation removal
    name = re.sub(r'\s+', ' ', name).strip()

    # Remove common legal entity suffixes
    for suffix in LEGAL_SUFFIXES:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)

    # Normalize whitespace again after suffix removal
    name = re.sub(r'\s+', ' ', name).strip()

    # Handle common website/domain variations
    name = re.sub(r'COM$', '', name)
    name = re.sub(r'NET$', '', name)
    name = re.sub(r'ORG$', '', name)

    # Apply common abbreviations and expansions
    for old, new in ABBREVIATION_MAP.items():
        name = name.replace(old, new)

    # Final whitespace normalization
    name = re.sub(r'\s+', ' ', name).strip()

    return name if name else 'SELF-EMPLOYED'
