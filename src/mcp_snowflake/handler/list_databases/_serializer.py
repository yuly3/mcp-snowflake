"""List databases result model and serializers.

This module provides a format-agnostic ListDatabasesResult class and a serializer
interface following the double-dispatch (Visitor) pattern.
"""

from abc import ABC, abstractmethod

import attrs


@attrs.define(frozen=True)
class ListDatabasesResult:
    """Format-agnostic representation of a list databases result.

    Attributes
    ----------
    databases : list[str]
        Ordered list of database names.
    """

    databases: list[str]

    @property
    def database_count(self) -> int:
        """Return the number of databases."""
        return len(self.databases)

    def serialize_with(self, serializer: "ListDatabasesResultSerializer") -> str:
        """Serialize this result using the given serializer (double dispatch)."""
        serializer.visit_metadata(self.database_count)
        serializer.visit_databases(self.databases)
        return serializer.finish()


class ListDatabasesResultSerializer(ABC):
    """Abstract visitor that serializes a :class:`ListDatabasesResult`."""

    @abstractmethod
    def visit_metadata(self, database_count: int) -> None:
        """Receive top-level metadata."""
        ...

    @abstractmethod
    def visit_databases(self, databases: list[str]) -> None:
        """Receive the list of database names."""
        ...

    @abstractmethod
    def finish(self) -> str:
        """Produce the final serialized string."""
        ...


# ---------------------------------------------------------------------------
# Concrete serializers
# ---------------------------------------------------------------------------


class CompactListDatabasesResultSerializer(ListDatabasesResultSerializer):
    """Serialize a :class:`ListDatabasesResult` in a compact, token-efficient format.

    Output example::

        database_count: 3
        databases: MY_DB, ANALYTICS, RAW

    Empty list renders as::

        database_count: 0
        databases: (none)
    """

    def __init__(self) -> None:
        self._lines: list[str] = []

    def visit_metadata(self, database_count: int) -> None:
        self._lines.append(f"database_count: {database_count}")

    def visit_databases(self, databases: list[str]) -> None:
        if databases:
            self._lines.append(f"databases: {', '.join(databases)}")
        else:
            self._lines.append("databases: (none)")

    def finish(self) -> str:
        return "\n".join(self._lines)
