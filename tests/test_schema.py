"""Tests for schema definitions."""

import sys

sys.path.insert(0, str(__file__).rsplit("/", 2)[0] + "/duckdb_loader")

from duckdb_loader.schema import (
    ALL_COLUMNS,
    CONTRIBUTIONS_COLUMNS,
    _get_column_type,
    create_indexes,
    create_schema,
)


class TestColumnDefinitions:
    """Tests for column definitions."""

    def test_contributions_columns_not_empty(self):
        """Default columns list is not empty."""
        assert len(CONTRIBUTIONS_COLUMNS) > 0

    def test_all_columns_superset(self):
        """ALL_COLUMNS contains all CONTRIBUTIONS_COLUMNS."""
        for col in CONTRIBUTIONS_COLUMNS:
            assert col in ALL_COLUMNS, f"Missing column: {col}"

    def test_all_columns_count(self):
        """ALL_COLUMNS has expected count."""
        assert len(ALL_COLUMNS) == 45

    def test_core_columns_present(self):
        """Core columns are in default set."""
        core = ["cycle", "amount", "date", "contributor.name", "recipient.name"]
        for col in core:
            assert col in CONTRIBUTIONS_COLUMNS


class TestColumnTypes:
    """Tests for column type mapping."""

    def test_integer_columns(self):
        """Integer columns map correctly."""
        assert _get_column_type("cycle") == "INTEGER"

    def test_float_columns(self):
        """Float columns map correctly."""
        assert _get_column_type("amount") == "DOUBLE"
        assert _get_column_type("contributor.cfscore") == "DOUBLE"
        assert _get_column_type("latitude") == "DOUBLE"

    def test_string_columns(self):
        """String columns default to VARCHAR."""
        assert _get_column_type("contributor.name") == "VARCHAR"
        assert _get_column_type("unknown.column") == "VARCHAR"


class TestCreateSchema:
    """Tests for schema creation."""

    def test_create_schema_default_columns(self, temp_duckdb_path):
        """Creates schema with default columns."""
        import duckdb

        conn = duckdb.connect(str(temp_duckdb_path))
        create_schema(conn)

        # Verify table exists
        result = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='contributions'"
        ).fetchone()
        assert result is not None

        # Verify columns
        cols = conn.execute("PRAGMA table_info(contributions)").fetchall()
        col_names = [c[1] for c in cols]
        assert "cycle" in col_names
        assert "amount" in col_names

        conn.close()

    def test_create_schema_custom_columns(self, temp_duckdb_path):
        """Creates schema with custom columns."""
        import duckdb

        conn = duckdb.connect(str(temp_duckdb_path))
        custom_cols = ["cycle", "amount", "contributor.name"]
        create_schema(conn, columns=custom_cols)

        cols = conn.execute("PRAGMA table_info(contributions)").fetchall()
        col_names = [c[1] for c in cols]
        assert len(col_names) == 3
        assert "cycle" in col_names

        conn.close()

    def test_create_schema_custom_table_name(self, temp_duckdb_path):
        """Creates schema with custom table name."""
        import duckdb

        conn = duckdb.connect(str(temp_duckdb_path))
        create_schema(conn, table_name="my_contributions")

        result = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='my_contributions'"
        ).fetchone()
        assert result is not None

        conn.close()


class TestCreateIndexes:
    """Tests for index creation."""

    def test_create_indexes(self, temp_duckdb_path):
        """Creates indexes on table."""
        import duckdb

        conn = duckdb.connect(str(temp_duckdb_path))
        create_schema(conn)
        create_indexes(conn)

        # DuckDB stores index info differently, just verify no errors
        conn.close()

    def test_create_indexes_missing_columns(self, temp_duckdb_path):
        """Index creation handles missing columns gracefully."""
        import duckdb

        conn = duckdb.connect(str(temp_duckdb_path))
        # Create with minimal columns
        create_schema(conn, columns=["cycle", "amount"])
        # Should not raise despite missing indexed columns
        create_indexes(conn)

        conn.close()
