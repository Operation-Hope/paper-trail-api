"""Tests for legislator crosswalk schema definitions."""

import pyarrow as pa

from scripts.legislator_crosswalk.schema import (
    ALLOWED_SOURCE_DOMAINS,
    CROSSWALK_COLUMNS,
    CROSSWALK_SCHEMA,
    DIME_RECIPIENTS_URL,
    EXTRACTION_QUERY,
    KEY_COLUMNS,
    YEAR_SUFFIX_LENGTH,
    validate_source_url,
)


class TestConstants:
    """Tests for schema constants."""

    def test_year_suffix_length_is_four(self):
        """YEAR_SUFFIX_LENGTH is 4 (DIME stores ICPSR as {icpsr}{year})."""
        assert YEAR_SUFFIX_LENGTH == 4

    def test_allowed_domains_not_empty(self):
        """ALLOWED_SOURCE_DOMAINS has at least one domain."""
        assert len(ALLOWED_SOURCE_DOMAINS) > 0

    def test_huggingface_in_allowed_domains(self):
        """HuggingFace is an allowed domain."""
        assert "huggingface.co" in ALLOWED_SOURCE_DOMAINS

    def test_dime_recipients_url_uses_huggingface(self):
        """Default DIME URL points to HuggingFace."""
        assert "huggingface.co" in DIME_RECIPIENTS_URL

    def test_dime_recipients_url_is_parquet(self):
        """Default DIME URL is a parquet file."""
        assert DIME_RECIPIENTS_URL.endswith(".parquet")


class TestValidateSourceUrl:
    """Tests for URL validation."""

    def test_valid_huggingface_url(self):
        """Accepts valid HuggingFace URLs."""
        assert validate_source_url("https://huggingface.co/datasets/test/data.parquet")

    def test_valid_huggingface_subdomain(self):
        """Accepts HuggingFace subdomains."""
        assert validate_source_url("https://cdn-lfs.huggingface.co/some/file.parquet")

    def test_rejects_github_url(self):
        """Rejects GitHub URLs (not in allowed list)."""
        assert not validate_source_url("https://github.com/user/repo/file.parquet")

    def test_rejects_arbitrary_url(self):
        """Rejects arbitrary URLs."""
        assert not validate_source_url("https://malicious-site.com/data.parquet")

    def test_rejects_empty_url(self):
        """Handles empty URLs gracefully."""
        assert not validate_source_url("")

    def test_rejects_non_url(self):
        """Handles non-URL strings."""
        assert not validate_source_url("not a url")

    def test_rejects_local_path(self):
        """Rejects local file paths."""
        assert not validate_source_url("/path/to/local/file.parquet")

    def test_rejects_similar_domain(self):
        """Rejects domains that look similar but aren't allowed."""
        assert not validate_source_url("https://huggingface.co.evil.com/data.parquet")


class TestCrosswalkSchema:
    """Tests for PyArrow schema definition."""

    def test_schema_is_valid_pyarrow_schema(self):
        """CROSSWALK_SCHEMA is a valid PyArrow schema."""
        assert isinstance(CROSSWALK_SCHEMA, pa.Schema)

    def test_schema_has_icpsr_field(self):
        """Schema includes icpsr field."""
        assert "icpsr" in CROSSWALK_SCHEMA.names

    def test_schema_has_bonica_rid_field(self):
        """Schema includes bonica_rid field."""
        assert "bonica_rid" in CROSSWALK_SCHEMA.names

    def test_icpsr_is_not_nullable(self):
        """icpsr field is not nullable."""
        icpsr_field = CROSSWALK_SCHEMA.field("icpsr")
        assert not icpsr_field.nullable

    def test_bonica_rid_is_not_nullable(self):
        """bonica_rid field is not nullable."""
        bonica_rid_field = CROSSWALK_SCHEMA.field("bonica_rid")
        assert not bonica_rid_field.nullable

    def test_icpsr_is_string_type(self):
        """icpsr is stored as string (for DIME compatibility)."""
        icpsr_field = CROSSWALK_SCHEMA.field("icpsr")
        assert icpsr_field.type == pa.string()

    def test_schema_has_metadata_fields(self):
        """Schema includes metadata fields."""
        expected = ["recipient_name", "party", "state", "seat", "fec_id"]
        for field_name in expected:
            assert field_name in CROSSWALK_SCHEMA.names


class TestCrosswalkColumns:
    """Tests for column list constants."""

    def test_columns_match_schema(self):
        """CROSSWALK_COLUMNS matches schema field names."""
        assert set(CROSSWALK_COLUMNS) == set(CROSSWALK_SCHEMA.names)

    def test_key_columns_are_subset(self):
        """KEY_COLUMNS are in CROSSWALK_COLUMNS."""
        for col in KEY_COLUMNS:
            assert col in CROSSWALK_COLUMNS

    def test_key_columns_has_icpsr_and_bonica_rid(self):
        """KEY_COLUMNS includes both key fields."""
        assert "icpsr" in KEY_COLUMNS
        assert "bonica_rid" in KEY_COLUMNS


class TestExtractionQuery:
    """Tests for extraction SQL query."""

    def test_query_uses_year_suffix_constant(self):
        """Query uses YEAR_SUFFIX_LENGTH constant, not hardcoded 4."""
        # The query should have been formatted with the constant
        assert str(YEAR_SUFFIX_LENGTH) in EXTRACTION_QUERY

    def test_query_has_placeholder_for_source_url(self):
        """Query has placeholder for source URL."""
        assert "{source_url}" in EXTRACTION_QUERY

    def test_query_selects_icpsr(self):
        """Query selects icpsr column."""
        assert "icpsr" in EXTRACTION_QUERY.lower()

    def test_query_selects_bonica_rid(self):
        """Query selects bonica_rid column."""
        assert "bonica_rid" in EXTRACTION_QUERY.lower()

    def test_query_filters_null_icpsr(self):
        """Query filters out null ICPSR values."""
        assert 'ICPSR" IS NOT NULL' in EXTRACTION_QUERY

    def test_query_groups_by_key_columns(self):
        """Query groups by icpsr and bonica_rid."""
        assert "GROUP BY icpsr, bonica_rid" in EXTRACTION_QUERY
