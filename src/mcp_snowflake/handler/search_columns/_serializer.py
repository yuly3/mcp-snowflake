"""Search columns result model and serializers.

This module provides a format-agnostic SearchColumnsResult class and a serializer
interface following the double-dispatch (Visitor) pattern.
"""

from abc import ABC, abstractmethod

import attrs

from kernel.table_metadata import DataBase


@attrs.define(frozen=True, slots=True)
class SearchColumnsTableEntry:
    """A single table entry in search results.

    Attributes
    ----------
    schema : str
        Schema name.
    table : str
        Table name.
    columns_json : str
        Minified JSON array of matched columns.
    """

    schema: str
    table: str
    columns_json: str


@attrs.define(frozen=True)
class SearchColumnsResult:
    """Format-agnostic representation of a search columns result.

    Attributes
    ----------
    database : DataBase
        Database name.
    tables : list[SearchColumnsTableEntry]
        Matched tables with their column information.
    """

    database: DataBase
    tables: list[SearchColumnsTableEntry]

    @property
    def table_count(self) -> int:
        """Return the number of matched tables."""
        return len(self.tables)

    def serialize_with(self, serializer: "SearchColumnsResultSerializer") -> str:
        """Serialize this result using the given serializer (double dispatch)."""
        serializer.visit_metadata(self.database, self.table_count)
        for entry in self.tables:
            serializer.visit_table(entry)
        return serializer.finish()


class SearchColumnsResultSerializer(ABC):
    """Abstract visitor that serializes a :class:`SearchColumnsResult`."""

    @abstractmethod
    def visit_metadata(self, database: DataBase, table_count: int) -> None:
        """Receive top-level metadata."""
        ...

    @abstractmethod
    def visit_table(self, entry: SearchColumnsTableEntry) -> None:
        """Receive a single table entry."""
        ...

    @abstractmethod
    def finish(self) -> str:
        """Produce the final serialized string."""
        ...


# ---------------------------------------------------------------------------
# Concrete serializers
# ---------------------------------------------------------------------------


class CompactSearchColumnsResultSerializer(SearchColumnsResultSerializer):
    """Serialize a :class:`SearchColumnsResult` in a compact, token-efficient format.

    Output example::

        database: MY_DB
        table_count: 2

        schema: PUBLIC
        table: ORDERS
        columns: [{"name":"ORDER_ID","type":"NUMBER"}]

        schema: PUBLIC
        table: CUSTOMERS
        columns: [{"name":"CUSTOMER_ID","type":"NUMBER"},{"name":"UNIT_ID","type":"NUMBER"}]

    Empty result renders as::

        database: MY_DB
        table_count: 0
    """

    def __init__(self) -> None:
        self._lines: list[str] = []

    def visit_metadata(self, database: DataBase, table_count: int) -> None:
        self._lines.append(f"database: {database}")
        self._lines.append(f"table_count: {table_count}")

    def visit_table(self, entry: SearchColumnsTableEntry) -> None:
        self._lines.append("")
        self._lines.append(f"schema: {entry.schema}")
        self._lines.append(f"table: {entry.table}")
        self._lines.append(f"columns: {entry.columns_json}")

    def finish(self) -> str:
        return "\n".join(self._lines)
