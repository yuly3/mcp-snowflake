"""Sample table data result model and serializers.

This module provides a format-agnostic SampleTableDataResult class and a serializer
interface following the double-dispatch (Visitor) pattern.
"""

import json
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence

import attrs

from cattrs_converter import Jsonable
from kernel.table_metadata import DataBase, Schema, Table


@attrs.define(frozen=True)
class SampleTableDataResult:
    """Format-agnostic representation of a sample table data result.

    Attributes
    ----------
    database : DataBase
        Database name.
    schema : Schema
        Schema name.
    table : Table
        Table name.
    sample_size : int
        Requested sample size.
    columns : list[str]
        Ordered column names.
    rows : Sequence[Mapping[str, Jsonable]]
        Result rows, each mapping column names to JSON-compatible values.
    warnings : list[str]
        Deduplicated warning messages produced during data processing.
    """

    database: DataBase
    schema: Schema
    table: Table
    sample_size: int
    columns: list[str]
    rows: Sequence[Mapping[str, Jsonable]]
    warnings: list[str]

    @property
    def actual_rows(self) -> int:
        """Return the number of result rows."""
        return len(self.rows)

    def serialize_with(self, serializer: "SampleTableDataResultSerializer") -> str:
        """Serialize this result using the given serializer (double dispatch)."""
        serializer.visit_metadata(
            self.database,
            self.schema,
            self.table,
            self.sample_size,
            self.actual_rows,
        )
        for index, row in enumerate(self.rows):
            serializer.visit_row(index, row)
        serializer.visit_warnings(self.warnings)
        return serializer.finish()


class SampleTableDataResultSerializer(ABC):
    """Abstract visitor that serializes a :class:`SampleTableDataResult`."""

    @abstractmethod
    def visit_metadata(
        self,
        database: DataBase,
        schema: Schema,
        table: Table,
        sample_size: int,
        actual_rows: int,
    ) -> None:
        """Receive top-level metadata."""
        ...

    @abstractmethod
    def visit_row(self, index: int, row: Mapping[str, Jsonable]) -> None:
        """Receive a single result row."""
        ...

    @abstractmethod
    def visit_warnings(self, warnings: list[str]) -> None:
        """Receive warning messages."""
        ...

    @abstractmethod
    def finish(self) -> str:
        """Produce the final serialized string."""
        ...


# ---------------------------------------------------------------------------
# Concrete serializers
# ---------------------------------------------------------------------------


class CompactSampleTableDataResultSerializer(SampleTableDataResultSerializer):
    """Serialize a :class:`SampleTableDataResult` in a compact, token-efficient format.

    Output example::

        database: test_db
        schema: test_schema
        table: users
        sample_size: 10
        actual_rows: 2

        row1:
        id: 1
        name: "Alice"

        row2:
        id: 2
        name: "Bob"

    Semi-structured values (``dict`` / ``list``) are rendered as inline
    JSON to preserve structure.
    """

    def __init__(self) -> None:
        self._lines: list[str] = []

    def visit_metadata(
        self,
        database: DataBase,
        schema: Schema,
        table: Table,
        sample_size: int,
        actual_rows: int,
    ) -> None:
        self._lines.append(f"database: {database}")
        self._lines.append(f"schema: {schema}")
        self._lines.append(f"table: {table}")
        self._lines.append(f"sample_size: {sample_size}")
        self._lines.append(f"actual_rows: {actual_rows}")

    def visit_row(self, index: int, row: Mapping[str, Jsonable]) -> None:
        self._lines.append("")
        self._lines.append(f"row{index + 1}:")
        for col, value in row.items():
            self._lines.append(f"{col}: {_format_compact_value(value)}")

    def visit_warnings(self, warnings: list[str]) -> None:
        if warnings:
            self._lines.append("")
            self._lines.append("warnings:")
            for w in warnings:
                self._lines.append(f"- {w}")

    def finish(self) -> str:
        return "\n".join(self._lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _format_compact_value(value: Jsonable) -> str:
    """Format a single value for the compact serializer.

    * ``dict`` and ``list`` (semi-structured data) are rendered as JSON.
    * ``str`` values are always rendered as JSON strings.
    * ``None`` is rendered as ``null``.
    * All other scalars use their natural ``str()`` representation.
    """
    if isinstance(value, dict | list):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)
