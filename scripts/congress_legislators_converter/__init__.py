"""Congress Legislators CSV to Parquet converter with streaming and validation.

This module provides tools for downloading and converting Congress Legislators
data from unitedstates.github.io to Parquet format, with lossless conversion
guaranteed through three-tier validation.

Supported file types:
- current: Currently serving legislators
- historical: All historical legislators

Data source: https://unitedstates.github.io/congress-legislators/

Example usage:
    from congress_legislators_converter import (
        convert_legislators_file,
        download_file,
        FileType,
    )

    # Download and convert
    csv_path = download_file(FileType.CURRENT, Path("./data"))
    result = convert_legislators_file(
        csv_path,
        "legislators-current.parquet",
        FileType.CURRENT,
    )
    print(f"Converted {result.row_count:,} rows")
"""

from .converter import ConversionResult, StreamingStats, convert_legislators_file
from .downloader import download_all, download_file
from .exceptions import (
    ChecksumMismatchError,
    CongressLegislatorsConversionError,
    CSVParseError,
    DownloadError,
    RowCountMismatchError,
    SampleMismatchError,
    SchemaValidationError,
)
from .schema import FileType, FileTypeConfig, get_config

__all__ = [
    # Core functions
    "convert_legislators_file",
    "download_file",
    "download_all",
    "get_config",
    # Data classes
    "ConversionResult",
    "FileType",
    "FileTypeConfig",
    "StreamingStats",
    # Exceptions
    "CongressLegislatorsConversionError",
    "CSVParseError",
    "ChecksumMismatchError",
    "DownloadError",
    "RowCountMismatchError",
    "SampleMismatchError",
    "SchemaValidationError",
]
