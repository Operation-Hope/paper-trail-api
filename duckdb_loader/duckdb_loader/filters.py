"""Filter definitions for subsetting DIME data during loading."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Any


class Filter(ABC):
    """Base class for data filters."""

    @abstractmethod
    def apply(self, row: dict[str, Any]) -> bool:
        """Return True if row should be included."""
        ...

    @abstractmethod
    def describe(self) -> str:
        """Human-readable description of the filter."""
        ...

    def to_sql(self) -> str | None:
        """Generate SQL WHERE clause for this filter.

        Returns None if filter cannot be expressed as SQL.
        """
        return None


@dataclass
class CycleFilter(Filter):
    """Filter by election cycle(s)."""

    cycles: list[int]

    def apply(self, row: dict[str, Any]) -> bool:
        return row.get("cycle") in self.cycles

    def describe(self) -> str:
        if len(self.cycles) == 1:
            return f"cycle = {self.cycles[0]}"
        return f"cycle in {self.cycles}"

    def to_sql(self) -> str:
        if len(self.cycles) == 1:
            return f"cycle = {self.cycles[0]}"
        cycles_str = ", ".join(str(c) for c in self.cycles)
        return f"cycle IN ({cycles_str})"


@dataclass
class StateFilter(Filter):
    """Filter by contributor or recipient state."""

    states: list[str]
    field: str = "contributor.state"  # or "recipient.state"

    def apply(self, row: dict[str, Any]) -> bool:
        return row.get(self.field) in self.states

    def describe(self) -> str:
        states_str = ", ".join(self.states)
        return f"{self.field} in [{states_str}]"

    def to_sql(self) -> str:
        states_str = ", ".join(f"'{s}'" for s in self.states)
        return f'"{self.field}" IN ({states_str})'


@dataclass
class AmountFilter(Filter):
    """Filter by contribution amount range."""

    min_amount: float | None = None
    max_amount: float | None = None

    def apply(self, row: dict[str, Any]) -> bool:
        amount = row.get("amount")
        if amount is None:
            return False
        if self.min_amount is not None and amount < self.min_amount:
            return False
        if self.max_amount is not None and amount > self.max_amount:
            return False
        return True

    def describe(self) -> str:
        parts = []
        if self.min_amount is not None:
            parts.append(f"amount >= ${self.min_amount:,.2f}")
        if self.max_amount is not None:
            parts.append(f"amount <= ${self.max_amount:,.2f}")
        return " AND ".join(parts) if parts else "no amount filter"

    def to_sql(self) -> str | None:
        parts = []
        if self.min_amount is not None:
            parts.append(f"amount >= {self.min_amount}")
        if self.max_amount is not None:
            parts.append(f"amount <= {self.max_amount}")
        return " AND ".join(parts) if parts else None


@dataclass
class DateFilter(Filter):
    """Filter by transaction date range."""

    start_date: date | None = None
    end_date: date | None = None

    def apply(self, row: dict[str, Any]) -> bool:
        date_str = row.get("date")
        if not date_str:
            return False
        try:
            row_date = date.fromisoformat(date_str[:10])
        except (ValueError, TypeError):
            return False
        if self.start_date and row_date < self.start_date:
            return False
        if self.end_date and row_date > self.end_date:
            return False
        return True

    def describe(self) -> str:
        parts = []
        if self.start_date:
            parts.append(f"date >= {self.start_date}")
        if self.end_date:
            parts.append(f"date <= {self.end_date}")
        return " AND ".join(parts) if parts else "no date filter"

    def to_sql(self) -> str | None:
        parts = []
        if self.start_date:
            parts.append(f"date >= '{self.start_date}'")
        if self.end_date:
            parts.append(f"date <= '{self.end_date}'")
        return " AND ".join(parts) if parts else None


@dataclass
class CompositeFilter(Filter):
    """Combine multiple filters with AND logic."""

    filters: list[Filter]

    def apply(self, row: dict[str, Any]) -> bool:
        return all(f.apply(row) for f in self.filters)

    def describe(self) -> str:
        return " AND ".join(f"({f.describe()})" for f in self.filters)

    def to_sql(self) -> str | None:
        sql_parts = []
        for f in self.filters:
            sql = f.to_sql()
            if sql is None:
                return None  # Can't convert if any filter lacks SQL support
            sql_parts.append(f"({sql})")
        return " AND ".join(sql_parts) if sql_parts else None


# Preset filters for common use cases
def recent_cycles(n: int = 4) -> CycleFilter:
    """Filter to last N election cycles (default: 4 = 2018-2024)."""
    current_year = 2024
    cycles = [current_year - (2 * i) for i in range(n)]
    return CycleFilter(cycles=sorted(cycles))


def large_donors(min_amount: float = 1000.0) -> AmountFilter:
    """Filter to contributions above threshold (default: $1,000)."""
    return AmountFilter(min_amount=min_amount)


def federal_races() -> Filter:
    """Filter to federal races only (President, Senate, House)."""

    @dataclass
    class SeatFilter(Filter):
        def apply(self, row: dict[str, Any]) -> bool:
            seat = row.get("seat", "")
            if not seat:
                return False
            return seat.startswith(("federal:", "president", "senate", "house"))

        def describe(self) -> str:
            return "federal races only"

    return SeatFilter()


def single_state(state: str, include_recipients: bool = True) -> Filter:
    """Filter to a single state (contributors and optionally recipients)."""
    filters = [StateFilter(states=[state.upper()], field="contributor.state")]
    if include_recipients:
        filters.append(StateFilter(states=[state.upper()], field="recipient.state"))

        # Return rows matching either filter
        @dataclass
        class OrFilter(Filter):
            state: str

            def apply(self, row: dict[str, Any]) -> bool:
                return (
                    row.get("contributor.state") == self.state
                    or row.get("recipient.state") == self.state
                )

            def describe(self) -> str:
                return f"contributor.state = {self.state} OR recipient.state = {self.state}"

        return OrFilter(state=state.upper())
    return filters[0]
