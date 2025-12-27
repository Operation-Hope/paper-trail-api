"""Entry point for running the distinct legislators extractor as a module.

Usage:
    python -m distinct_legislators output.parquet
    python -m distinct_legislators legislators.parquet --min-congress 100
"""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
