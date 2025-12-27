"""DIME CSV to Parquet converter with robust error handling and validation."""

from .converter import ConversionResult, StreamingStats, convert_dime_file
from .exceptions import (
    ChecksumMismatchError,
    CSVParseError,
    DIMEConversionError,
    RowCountMismatchError,
    SampleMismatchError,
    SchemaValidationError,
)
from .schema import FileType, FileTypeConfig, get_config

__all__ = [
    "CSVParseError",
    "ChecksumMismatchError",
    "ConversionResult",
    "DIMEConversionError",
    "FileType",
    "FileTypeConfig",
    "RowCountMismatchError",
    "SampleMismatchError",
    "SchemaValidationError",
    "StreamingStats",
    "convert_dime_file",
    "get_config",
]
