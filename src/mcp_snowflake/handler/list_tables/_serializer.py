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
    views : list[str]
        Ordered list of view names.
    """

    database: DataBase
    schema: Schema
    tables: list[str]
    views: list[str]

    @property
    def object_count(self) -> int:
        """Return the total number of objects (tables + views)."""
        return len(self.tables) + len(self.views)

    def serialize_with(self, serializer: "ListTablesResultSerializer") -> str:
        """Serialize this result using the given serializer (double dispatch)."""
        serializer.visit_metadata(self.database, self.schema, self.object_count)
        serializer.visit_tables(self.tables)
        serializer.visit_views(self.views)
        return serializer.finish()


class ListTablesResultSerializer(ABC):
    """Abstract visitor that serializes a :class:`ListTablesResult`."""

    @abstractmethod
    def visit_metadata(self, database: DataBase, schema: Schema, object_count: int) -> None:
        """Receive top-level metadata."""
        ...

    @abstractmethod
    def visit_tables(self, tables: list[str]) -> None:
        """Receive the list of table names."""
        ...

    @abstractmethod
    def visit_views(self, views: list[str]) -> None:
        """Receive the list of view names."""
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
        object_count: 5
        tables: TABLE1, TABLE2, TABLE3
        views: VIEW1, VIEW2

    Empty lists render as::

        database: MY_DB
        schema: PUBLIC
        object_count: 0
        tables: (none)
        views: (none)
    """

    def __init__(self) -> None:
        self._lines: list[str] = []

    def visit_metadata(self, database: DataBase, schema: Schema, object_count: int) -> None:
        self._lines.append(f"database: {database}")
        self._lines.append(f"schema: {schema}")
        self._lines.append(f"object_count: {object_count}")

    def visit_tables(self, tables: list[str]) -> None:
        self._append_name_list("tables", tables)

    def visit_views(self, views: list[str]) -> None:
        self._append_name_list("views", views)

    def _append_name_list(self, label: str, names: list[str]) -> None:
        if names:
            self._lines.append(f"{label}: {', '.join(names)}")
        else:
            self._lines.append(f"{label}: (none)")

    def finish(self) -> str:
        return "\n".join(self._lines)
