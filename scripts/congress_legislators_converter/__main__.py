"""Entry point for running the Congress Legislators converter as a module.

Usage:
    python -m congress_legislators_converter download --output-dir ./data
    python -m congress_legislators_converter convert source.csv output.parquet
    python -m congress_legislators_converter all --output-dir ./data
"""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
