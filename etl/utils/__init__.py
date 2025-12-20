"""
Shared utilities for ETL scripts.

This module provides common functionality used across multiple ETL scripts
to eliminate code duplication and ensure consistent behavior.
"""

from .normalization import normalize_employer_name

__all__ = ["normalize_employer_name"]
