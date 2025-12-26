"""DIME CSV to Parquet converter with robust error handling and validation."""

from .converter import convert_dime_file, ConversionResult, StreamingStats
from .exceptions import (
    DIMEConversionError,
    CSVParseError,
    RowCountMismatchError,
    ChecksumMismatchError,
    SampleMismatchError,
    SchemaValidationError,
)
from .schema import FileType, FileTypeConfig, get_config

__all__ = [
    "convert_dime_file",
    "ConversionResult",
    "StreamingStats",
    "FileType",
    "FileTypeConfig",
    "get_config",
    "DIMEConversionError",
    "CSVParseError",
    "RowCountMismatchError",
    "ChecksumMismatchError",
    "SampleMismatchError",
    "SchemaValidationError",
]
