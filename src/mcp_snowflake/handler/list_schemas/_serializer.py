"""List schemas result model and serializers.

This module provides a format-agnostic ListSchemasResult class and a serializer
interface following the double-dispatch (Visitor) pattern.
"""

from abc import ABC, abstractmethod

import attrs

from kernel.table_metadata import DataBase


@attrs.define(frozen=True)
class ListSchemasResult:
    """Format-agnostic representation of a list schemas result.

    Attributes
    ----------
    database : DataBase
        Database name.
    schemas : list[str]
        Ordered list of schema names.
    """

    database: DataBase
    schemas: list[str]

    @property
    def schema_count(self) -> int:
        """Return the number of schemas."""
        return len(self.schemas)

    def serialize_with(self, serializer: "ListSchemasResultSerializer") -> str:
        """Serialize this result using the given serializer (double dispatch)."""
        serializer.visit_metadata(self.database, self.schema_count)
        serializer.visit_schemas(self.schemas)
        return serializer.finish()


class ListSchemasResultSerializer(ABC):
    """Abstract visitor that serializes a :class:`ListSchemasResult`."""

    @abstractmethod
    def visit_metadata(self, database: DataBase, schema_count: int) -> None:
        """Receive top-level metadata."""
        ...

    @abstractmethod
    def visit_schemas(self, schemas: list[str]) -> None:
        """Receive the list of schema names."""
        ...

    @abstractmethod
    def finish(self) -> str:
        """Produce the final serialized string."""
        ...


# ---------------------------------------------------------------------------
# Concrete serializers
# ---------------------------------------------------------------------------


class CompactListSchemasResultSerializer(ListSchemasResultSerializer):
    """Serialize a :class:`ListSchemasResult` in a compact, token-efficient format.

    Output example::

        database: MY_DB
        schema_count: 3
        schemas: PUBLIC, INFORMATION_SCHEMA, RAW

    Empty list renders as::

        database: MY_DB
        schema_count: 0
        schemas: (none)
    """

    def __init__(self) -> None:
        self._lines: list[str] = []

    def visit_metadata(self, database: DataBase, schema_count: int) -> None:
        self._lines.append(f"database: {database}")
        self._lines.append(f"schema_count: {schema_count}")

    def visit_schemas(self, schemas: list[str]) -> None:
        if schemas:
            self._lines.append(f"schemas: {', '.join(schemas)}")
        else:
            self._lines.append("schemas: (none)")

    def finish(self) -> str:
        return "\n".join(self._lines)
