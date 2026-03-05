"""List views result model and serializers.

This module provides a format-agnostic ListViewsResult class and a serializer
interface following the double-dispatch (Visitor) pattern.
"""

from abc import ABC, abstractmethod

import attrs

from kernel.table_metadata import DataBase, Schema


@attrs.define(frozen=True)
class ListViewsResult:
    """Format-agnostic representation of a list views result.

    Attributes
    ----------
    database : DataBase
        Database name.
    schema : Schema
        Schema name.
    views : list[str]
        Ordered list of view names.
    """

    database: DataBase
    schema: Schema
    views: list[str]

    @property
    def view_count(self) -> int:
        """Return the number of views."""
        return len(self.views)

    def serialize_with(self, serializer: "ListViewsResultSerializer") -> str:
        """Serialize this result using the given serializer (double dispatch)."""
        serializer.visit_metadata(self.database, self.schema, self.view_count)
        serializer.visit_views(self.views)
        return serializer.finish()


class ListViewsResultSerializer(ABC):
    """Abstract visitor that serializes a :class:`ListViewsResult`."""

    @abstractmethod
    def visit_metadata(self, database: DataBase, schema: Schema, view_count: int) -> None:
        """Receive top-level metadata."""
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


class CompactListViewsResultSerializer(ListViewsResultSerializer):
    """Serialize a :class:`ListViewsResult` in a compact, token-efficient format.

    Output example::

        database: MY_DB
        schema: PUBLIC
        view_count: 3
        views: VIEW1, VIEW2, VIEW3

    Empty list renders as::

        database: MY_DB
        schema: PUBLIC
        view_count: 0
        views: (none)
    """

    def __init__(self) -> None:
        self._lines: list[str] = []

    def visit_metadata(self, database: DataBase, schema: Schema, view_count: int) -> None:
        self._lines.append(f"database: {database}")
        self._lines.append(f"schema: {schema}")
        self._lines.append(f"view_count: {view_count}")

    def visit_views(self, views: list[str]) -> None:
        if views:
            self._lines.append(f"views: {', '.join(views)}")
        else:
            self._lines.append("views: (none)")

    def finish(self) -> str:
        return "\n".join(self._lines)
