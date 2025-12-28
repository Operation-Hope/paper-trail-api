"""Entry point for running the module with python -m."""

import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())
