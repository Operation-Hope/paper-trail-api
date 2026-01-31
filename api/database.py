"""Database connection and dependency for FastAPI."""

import os
from collections.abc import AsyncGenerator
from typing import Annotated

import psycopg
from fastapi import Depends
from psycopg.rows import dict_row

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/paper_trail_dev")


async def get_db() -> AsyncGenerator[psycopg.AsyncConnection]:
    """Create a database connection for request scope."""
    async with await psycopg.AsyncConnection.connect(DATABASE_URL, row_factory=dict_row) as conn:
        yield conn


DbDep = Annotated[psycopg.AsyncConnection, Depends(get_db)]
