"""Shared pytest fixtures for Paper Trail API tests."""

import pytest


@pytest.fixture
def sample_contribution_row() -> dict:
    """Sample contribution row matching DIME schema."""
    return {
        "cycle": 2024,
        "transaction.id": "TEST123",
        "transaction.type": "15",
        "amount": 500.00,
        "date": "2024-06-15",
        "bonica.cid": "CID123",
        "contributor.name": "John Doe",
        "contributor.type": "I",
        "contributor.state": "CA",
        "contributor.occupation": "Engineer",
        "contributor.employer": "Tech Corp",
        "contributor.cfscore": -0.5,
        "bonica.rid": "RID456",
        "recipient.name": "Jane Smith",
        "recipient.party": "100",
        "recipient.type": "CAND",
        "recipient.state": "CA",
        "candidate.cfscore": -0.3,
        "seat": "federal:senate",
        "election.type": "G",
        "occ.standardized": "ENGINEERING",
    }


@pytest.fixture
def sample_rows() -> list[dict]:
    """Multiple sample rows for testing filters."""
    return [
        {
            "cycle": 2024,
            "amount": 1000.00,
            "date": "2024-01-15",
            "contributor.state": "CA",
            "recipient.state": "CA",
            "seat": "federal:senate",
        },
        {
            "cycle": 2022,
            "amount": 250.00,
            "date": "2022-06-20",
            "contributor.state": "NY",
            "recipient.state": "NY",
            "seat": "federal:house",
        },
        {
            "cycle": 2020,
            "amount": 5000.00,
            "date": "2020-03-10",
            "contributor.state": "TX",
            "recipient.state": "TX",
            "seat": "state:governor",
        },
        {
            "cycle": 2024,
            "amount": 50.00,
            "date": "2024-09-01",
            "contributor.state": "FL",
            "recipient.state": "FL",
            "seat": "federal:house",
        },
    ]


@pytest.fixture
def temp_duckdb_path(tmp_path):
    """Temporary DuckDB database path for testing."""
    return tmp_path / "test.duckdb"
