"""Voteview CSV to Parquet converter with streaming and validation.

This module provides tools for converting Voteview congressional voting
data from CSV format to Parquet, with lossless conversion guaranteed
through three-tier validation.

Supported file types:
- members: Congressional member information with NOMINATE scores
- rollcalls: Roll call vote metadata
- votes: Individual vote records (26M+ rows)

Example usage:
    from voteview_converter import convert_voteview_file, FileType

    result = convert_voteview_file(
        "HSall_members.csv",
        "members.parquet",
        FileType.MEMBERS,
    )
    print(f"Converted {result.row_count:,} rows")
"""

from .converter import ConversionResult, StreamingStats, convert_voteview_file
from .exceptions import (
    ChecksumMismatchError,
    CSVParseError,
    RowCountMismatchError,
    SampleMismatchError,
    SchemaValidationError,
    VoteviewConversionError,
)
from .schema import FileType, FileTypeConfig, get_config

__all__ = [
    # Core functions
    "convert_voteview_file",
    "get_config",
    # Data classes
    "ConversionResult",
    "FileType",
    "FileTypeConfig",
    "StreamingStats",
    # Exceptions
    "CSVParseError",
    "ChecksumMismatchError",
    "RowCountMismatchError",
    "SampleMismatchError",
    "SchemaValidationError",
    "VoteviewConversionError",
]
