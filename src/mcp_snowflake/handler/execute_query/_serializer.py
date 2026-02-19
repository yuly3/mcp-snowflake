"""Query result model and serializers for execute_query.

This module provides a format-agnostic QueryResult class and a serializer
interface following the double-dispatch (Visitor) pattern, inspired by
Rust's serde.  The data structure drives the serialization by calling
back into the serializer, while each serializer controls the output format.
"""

import json
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence

import attrs

from cattrs_converter import Jsonable


@attrs.define(frozen=True)
class QueryResult:
    """Format-agnostic representation of a query execution result.

    This class holds the raw result data and delegates formatting to an
    external ``QueryResultSerializer`` via the :meth:`serialize_with`
    method (double dispatch).

    Attributes
    ----------
    execution_time_ms : int
        Elapsed time for query execution in milliseconds.
    columns : list[str]
        Ordered column names.
    rows : Sequence[Mapping[str, Jsonable]]
        Result rows, each mapping column names to JSON-compatible values.
    warnings : list[str]
        Deduplicated warning messages produced during data processing.
    """

    execution_time_ms: int
    columns: list[str]
    rows: Sequence[Mapping[str, Jsonable]]
    warnings: list[str]

    @property
    def row_count(self) -> int:
        """Return the number of result rows."""
        return len(self.rows)

    def serialize_with(self, serializer: "QueryResultSerializer") -> str:
        """Serialize this result using the given serializer.

        The data structure drives the serialization: it calls back into
        the *serializer* for each logical piece of the result (metadata,
        rows, warnings).  This is the "accept" side of the Visitor /
        double-dispatch pattern.

        Parameters
        ----------
        serializer : QueryResultSerializer
            The serializer (visitor) to use.

        Returns
        -------
        str
            The serialized string representation.
        """
        serializer.visit_metadata(self.execution_time_ms, self.row_count)
        for index, row in enumerate(self.rows):
            serializer.visit_row(index, row)
        serializer.visit_warnings(self.warnings)
        return serializer.finish()


class QueryResultSerializer(ABC):
    """Abstract visitor that serializes a :class:`QueryResult`.

    Concrete subclasses implement each ``visit_*`` / ``finish`` method to
    produce a specific output format (JSON, compact text, etc.).
    """

    @abstractmethod
    def visit_metadata(self, execution_time_ms: int, row_count: int) -> None:
        """Receive top-level metadata."""
        ...

    @abstractmethod
    def visit_row(self, index: int, row: Mapping[str, Jsonable]) -> None:
        """Receive a single result row.

        Parameters
        ----------
        index : int
            Zero-based row index.
        row : Mapping[str, Jsonable]
            Column-name → value mapping.
        """
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


class JsonQueryResultSerializer(QueryResultSerializer):
    """Serialize a :class:`QueryResult` as a JSON document.

    Output structure::

        {
          "query_result": {
            "execution_time_ms": ...,
            "row_count": ...,
            "columns": [...],
            "rows": [...],
            "warnings": [...]
          }
        }
    """

    def __init__(self, columns: list[str]) -> None:
        self._columns = columns
        self._execution_time_ms: int = 0
        self._row_count: int = 0
        self._rows: list[Mapping[str, Jsonable]] = []
        self._warnings: list[str] = []

    def visit_metadata(self, execution_time_ms: int, row_count: int) -> None:
        self._execution_time_ms = execution_time_ms
        self._row_count = row_count

    def visit_row(self, index: int, row: Mapping[str, Jsonable]) -> None:  # noqa: ARG002
        self._rows.append(row)

    def visit_warnings(self, warnings: list[str]) -> None:
        self._warnings = warnings

    def finish(self) -> str:
        return json.dumps(
            {
                "query_result": {
                    "execution_time_ms": self._execution_time_ms,
                    "row_count": self._row_count,
                    "columns": self._columns,
                    "rows": [dict(r) for r in self._rows],
                    "warnings": self._warnings,
                }
            },
            indent=2,
        )


class CompactQueryResultSerializer(QueryResultSerializer):
    """Serialize a :class:`QueryResult` in a compact, token-efficient format.

    Output example::

        execution_time_ms: 1234
        row_count: 2

        row1:
        col1: value1
        col2: value2

        row2:
        col1: value3
        col2: value4

    Semi-structured values (``dict`` / ``list``) are rendered as inline
    JSON to preserve structure.
    """

    def __init__(self) -> None:
        self._lines: list[str] = []

    def visit_metadata(self, execution_time_ms: int, row_count: int) -> None:
        self._lines.append(f"execution_time_ms: {execution_time_ms}")
        self._lines.append(f"row_count: {row_count}")

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
