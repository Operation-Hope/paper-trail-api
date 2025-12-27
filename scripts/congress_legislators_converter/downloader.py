"""Download Congress Legislators CSV files from unitedstates.github.io."""

from pathlib import Path
from urllib.request import urlopen

from .exceptions import DownloadError
from .schema import FILE_URLS, FileType


def download_file(file_type: FileType, output_dir: Path) -> Path:
    """
    Download a Congress Legislators CSV file.

    Args:
        file_type: Type of file to download (CURRENT or HISTORICAL)
        output_dir: Directory to save the downloaded file

    Returns:
        Path to the downloaded file

    Raises:
        DownloadError: If download fails
    """
    url = FILE_URLS[file_type]
    filename = f"legislators-{file_type.value}.csv"
    output_path = output_dir / filename

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"  Downloading {filename} from {url}...")

    try:
        with urlopen(url, timeout=60) as response:
            if response.status != 200:
                raise DownloadError(
                    source_path=output_path,
                    message="HTTP error downloading file",
                    url=url,
                    status_code=response.status,
                )

            content = response.read()
            output_path.write_bytes(content)

            size_kb = len(content) / 1024
            print(f"  Downloaded {size_kb:.1f} KB to {output_path}")

    except TimeoutError as e:
        raise DownloadError(
            source_path=output_path,
            message="Download timed out",
            url=url,
        ) from e
    except OSError as e:
        raise DownloadError(
            source_path=output_path,
            message=f"Network error: {e}",
            url=url,
        ) from e

    return output_path


def download_all(output_dir: Path) -> dict[FileType, Path]:
    """
    Download all Congress Legislators CSV files.

    Args:
        output_dir: Directory to save the downloaded files

    Returns:
        Dictionary mapping FileType to downloaded file paths

    Raises:
        DownloadError: If any download fails
    """
    results: dict[FileType, Path] = {}

    for file_type in FileType:
        path = download_file(file_type, output_dir)
        results[file_type] = path

    return results
