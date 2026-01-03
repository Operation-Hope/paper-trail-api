"""Entry point for running the legislator crosswalk extractor as a module.

Usage:
    python -m legislator_crosswalk output.parquet
    python -m legislator_crosswalk crosswalk.parquet --no-validate
"""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
