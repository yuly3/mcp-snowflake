"""List tables result model and serializers.

This module provides a format-agnostic ListTablesResult class and a serializer
interface following the double-dispatch (Visitor) pattern.
"""

from abc import ABC, abstractmethod

import attrs

from kernel.table_metadata import DataBase, Schema


@attrs.define(frozen=True)
class ListTablesResult:
    """Format-agnostic representation of a list tables result.

    Attributes
    ----------
    database : DataBase
        Database name.
    schema : Schema
        Schema name.
    tables : list[str]
        Ordered list of table names.
    """

    database: DataBase
    schema: Schema
    tables: list[str]

    @property
    def table_count(self) -> int:
        """Return the number of tables."""
        return len(self.tables)

    def serialize_with(self, serializer: "ListTablesResultSerializer") -> str:
        """Serialize this result using the given serializer (double dispatch)."""
        serializer.visit_metadata(self.database, self.schema, self.table_count)
        serializer.visit_tables(self.tables)
        return serializer.finish()


class ListTablesResultSerializer(ABC):
    """Abstract visitor that serializes a :class:`ListTablesResult`."""

    @abstractmethod
    def visit_metadata(self, database: DataBase, schema: Schema, table_count: int) -> None:
        """Receive top-level metadata."""
        ...

    @abstractmethod
    def visit_tables(self, tables: list[str]) -> None:
        """Receive the list of table names."""
        ...

    @abstractmethod
    def finish(self) -> str:
        """Produce the final serialized string."""
        ...


# ---------------------------------------------------------------------------
# Concrete serializers
# ---------------------------------------------------------------------------


class CompactListTablesResultSerializer(ListTablesResultSerializer):
    """Serialize a :class:`ListTablesResult` in a compact, token-efficient format.

    Output example::

        database: MY_DB
        schema: PUBLIC
        table_count: 3
        tables: TABLE1, TABLE2, TABLE3

    Empty list renders as::

        database: MY_DB
        schema: PUBLIC
        table_count: 0
        tables: (none)
    """

    def __init__(self) -> None:
        self._lines: list[str] = []

    def visit_metadata(self, database: DataBase, schema: Schema, table_count: int) -> None:
        self._lines.append(f"database: {database}")
        self._lines.append(f"schema: {schema}")
        self._lines.append(f"table_count: {table_count}")

    def visit_tables(self, tables: list[str]) -> None:
        if tables:
            self._lines.append(f"tables: {', '.join(tables)}")
        else:
            self._lines.append("tables: (none)")

    def finish(self) -> str:
        return "\n".join(self._lines)
