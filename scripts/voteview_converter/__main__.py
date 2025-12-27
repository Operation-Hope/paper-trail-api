"""Entry point for running the Voteview converter as a module.

Usage:
    python -m voteview_converter source.csv output.parquet
    python -m voteview_converter HSall_votes.csv votes.parquet --batch-size 200000
"""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
