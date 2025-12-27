"""Tests for filter implementations."""

from datetime import date

from duckdb_loader.filters import (
    AmountFilter,
    CompositeFilter,
    CycleFilter,
    DateFilter,
    StateFilter,
    federal_races,
    large_donors,
    recent_cycles,
    single_state,
)


class TestCycleFilter:
    """Tests for CycleFilter."""

    def test_single_cycle_match(self, sample_rows):
        """Filter matches rows with specified cycle."""
        f = CycleFilter(cycles=[2024])
        matches = [r for r in sample_rows if f.apply(r)]
        assert len(matches) == 2
        assert all(r["cycle"] == 2024 for r in matches)

    def test_multiple_cycles_match(self, sample_rows):
        """Filter matches rows with any of specified cycles."""
        f = CycleFilter(cycles=[2022, 2024])
        matches = [r for r in sample_rows if f.apply(r)]
        assert len(matches) == 3

    def test_no_match(self, sample_rows):
        """Filter excludes rows not matching cycles."""
        f = CycleFilter(cycles=[2018])
        matches = [r for r in sample_rows if f.apply(r)]
        assert len(matches) == 0

    def test_describe_single(self):
        """Describe returns readable string for single cycle."""
        f = CycleFilter(cycles=[2024])
        assert f.describe() == "cycle = 2024"

    def test_describe_multiple(self):
        """Describe returns readable string for multiple cycles."""
        f = CycleFilter(cycles=[2022, 2024])
        assert f.describe() == "cycle in [2022, 2024]"

    def test_to_sql_single(self):
        """Generates correct SQL for single cycle."""
        f = CycleFilter(cycles=[2024])
        assert f.to_sql() == "cycle = 2024"

    def test_to_sql_multiple(self):
        """Generates correct SQL for multiple cycles."""
        f = CycleFilter(cycles=[2022, 2024])
        assert f.to_sql() == "cycle IN (2022, 2024)"


class TestStateFilter:
    """Tests for StateFilter."""

    def test_contributor_state_match(self, sample_rows):
        """Filter matches by contributor state."""
        f = StateFilter(states=["CA"], field="contributor.state")
        matches = [r for r in sample_rows if f.apply(r)]
        assert len(matches) == 1
        assert matches[0]["contributor.state"] == "CA"

    def test_multiple_states(self, sample_rows):
        """Filter matches any of multiple states."""
        f = StateFilter(states=["CA", "NY"], field="contributor.state")
        matches = [r for r in sample_rows if f.apply(r)]
        assert len(matches) == 2

    def test_to_sql(self):
        """Generates correct SQL for state filter."""
        f = StateFilter(states=["CA", "NY"], field="contributor.state")
        sql = f.to_sql()
        assert '"contributor.state"' in sql
        assert "'CA'" in sql
        assert "'NY'" in sql


class TestAmountFilter:
    """Tests for AmountFilter."""

    def test_min_amount(self, sample_rows):
        """Filter by minimum amount."""
        f = AmountFilter(min_amount=500.0)
        matches = [r for r in sample_rows if f.apply(r)]
        assert len(matches) == 2
        assert all(r["amount"] >= 500.0 for r in matches)

    def test_max_amount(self, sample_rows):
        """Filter by maximum amount."""
        f = AmountFilter(max_amount=500.0)
        matches = [r for r in sample_rows if f.apply(r)]
        assert len(matches) == 2
        assert all(r["amount"] <= 500.0 for r in matches)

    def test_range(self, sample_rows):
        """Filter by amount range."""
        f = AmountFilter(min_amount=100.0, max_amount=1000.0)
        matches = [r for r in sample_rows if f.apply(r)]
        assert len(matches) == 2

    def test_null_amount(self):
        """Filter excludes rows with null amount."""
        f = AmountFilter(min_amount=100.0)
        assert not f.apply({"amount": None})

    def test_to_sql_min(self):
        """Generates SQL for min amount."""
        f = AmountFilter(min_amount=500.0)
        assert f.to_sql() == "amount >= 500.0"

    def test_to_sql_range(self):
        """Generates SQL for amount range."""
        f = AmountFilter(min_amount=100.0, max_amount=1000.0)
        assert f.to_sql() == "amount >= 100.0 AND amount <= 1000.0"

    def test_to_sql_none(self):
        """Returns None when no bounds set."""
        f = AmountFilter()
        assert f.to_sql() is None


class TestDateFilter:
    """Tests for DateFilter."""

    def test_start_date(self, sample_rows):
        """Filter by start date."""
        f = DateFilter(start_date=date(2024, 1, 1))
        matches = [r for r in sample_rows if f.apply(r)]
        assert len(matches) == 2

    def test_end_date(self, sample_rows):
        """Filter by end date."""
        f = DateFilter(end_date=date(2022, 12, 31))
        matches = [r for r in sample_rows if f.apply(r)]
        assert len(matches) == 2

    def test_date_range(self, sample_rows):
        """Filter by date range."""
        f = DateFilter(start_date=date(2020, 1, 1), end_date=date(2022, 12, 31))
        matches = [r for r in sample_rows if f.apply(r)]
        assert len(matches) == 2

    def test_invalid_date(self):
        """Filter excludes rows with invalid dates."""
        f = DateFilter(start_date=date(2020, 1, 1))
        assert not f.apply({"date": "invalid"})
        assert not f.apply({"date": None})
        assert not f.apply({"date": ""})

    def test_to_sql(self):
        """Generates SQL for date filter."""
        f = DateFilter(start_date=date(2024, 1, 1), end_date=date(2024, 12, 31))
        sql = f.to_sql()
        assert "date >= '2024-01-01'" in sql
        assert "date <= '2024-12-31'" in sql


class TestCompositeFilter:
    """Tests for CompositeFilter."""

    def test_all_filters_apply(self, sample_rows):
        """Composite filter requires all filters to match."""
        f = CompositeFilter(
            filters=[
                CycleFilter(cycles=[2024]),
                AmountFilter(min_amount=100.0),
            ]
        )
        matches = [r for r in sample_rows if f.apply(r)]
        assert len(matches) == 1
        assert matches[0]["cycle"] == 2024
        assert matches[0]["amount"] >= 100.0

    def test_describe(self):
        """Describe combines filter descriptions."""
        f = CompositeFilter(
            filters=[
                CycleFilter(cycles=[2024]),
                AmountFilter(min_amount=100.0),
            ]
        )
        desc = f.describe()
        assert "cycle = 2024" in desc
        assert "amount >= $100.00" in desc

    def test_to_sql(self):
        """Generates combined SQL."""
        f = CompositeFilter(
            filters=[
                CycleFilter(cycles=[2024]),
                AmountFilter(min_amount=100.0),
            ]
        )
        sql = f.to_sql()
        assert "(cycle = 2024)" in sql
        assert "(amount >= 100.0)" in sql
        assert " AND " in sql


class TestPresetFilters:
    """Tests for preset filter functions."""

    def test_recent_cycles_default(self):
        """recent_cycles returns last 4 cycles by default."""
        f = recent_cycles()
        assert len(f.cycles) == 4
        assert 2024 in f.cycles
        assert 2018 in f.cycles

    def test_recent_cycles_custom(self):
        """recent_cycles accepts custom count."""
        f = recent_cycles(n=2)
        assert len(f.cycles) == 2

    def test_large_donors_default(self):
        """large_donors uses $1000 threshold by default."""
        f = large_donors()
        assert f.min_amount == 1000.0

    def test_large_donors_custom(self):
        """large_donors accepts custom threshold."""
        f = large_donors(min_amount=5000.0)
        assert f.min_amount == 5000.0

    def test_federal_races(self, sample_rows):
        """federal_races filters to federal seats."""
        f = federal_races()
        matches = [r for r in sample_rows if f.apply(r)]
        assert len(matches) == 3
        assert all("federal:" in r["seat"] for r in matches)

    def test_single_state(self):
        """single_state creates OR filter for state."""
        f = single_state("CA")
        # Should match if contributor OR recipient is in CA
        assert f.apply({"contributor.state": "CA", "recipient.state": "NY"})
        assert f.apply({"contributor.state": "NY", "recipient.state": "CA"})
        assert not f.apply({"contributor.state": "NY", "recipient.state": "TX"})
