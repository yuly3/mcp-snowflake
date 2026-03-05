"""Result model and serializers for analyze_table_statistics.

This module provides a format-agnostic AnalyzeTableStatisticsResult class
and a serializer interface following the double-dispatch (Visitor) pattern.
"""

import json
from abc import ABC, abstractmethod
from typing import cast

import attrs

from kernel.table_metadata import DataBase, Schema, Table

from .models import StatsDict, TopValue


@attrs.define(frozen=True)
class UnsupportedColumnInfo:
    """Column that could not be analyzed due to unsupported data type."""

    name: str
    data_type: str


@attrs.define(frozen=True)
class AnalyzeTableStatisticsResult:
    """Format-agnostic representation of a table statistics analysis result.

    Attributes
    ----------
    database : DataBase
        Database name.
    schema : Schema
        Schema name.
    table : Table
        Table name.
    total_rows : int
        Total row count of the table.
    column_statistics : dict[str, StatsDict]
        Per-column statistics keyed by column name.
    unsupported_columns : list[UnsupportedColumnInfo]
        Columns that were skipped due to unsupported data types.
    include_statistics_metadata : bool
        Whether to include statistics metadata section.
    """

    database: DataBase
    schema: Schema
    table: Table
    total_rows: int
    column_statistics: dict[str, StatsDict]
    unsupported_columns: list[UnsupportedColumnInfo]
    include_statistics_metadata: bool

    @property
    def analyzed_columns(self) -> int:
        """Return the number of analyzed columns."""
        return len(self.column_statistics)

    def serialize_with(self, serializer: "AnalyzeTableStatisticsResultSerializer") -> str:
        """Serialize this result using the given serializer (double dispatch)."""
        serializer.visit_table_info(
            self.database,
            self.schema,
            self.table,
            self.total_rows,
            self.analyzed_columns,
        )
        for name, stats in self.column_statistics.items():
            serializer.visit_column(name, stats)
        serializer.visit_unsupported_columns(self.unsupported_columns)
        serializer.visit_statistics_metadata(include=self.include_statistics_metadata)
        return serializer.finish()


class AnalyzeTableStatisticsResultSerializer(ABC):
    """Abstract visitor that serializes an AnalyzeTableStatisticsResult."""

    @abstractmethod
    def visit_table_info(
        self,
        database: DataBase,
        schema: Schema,
        table: Table,
        total_rows: int,
        analyzed_columns: int,
    ) -> None:
        """Receive top-level table metadata."""
        ...

    @abstractmethod
    def visit_column(self, name: str, stats: StatsDict) -> None:
        """Receive statistics for a single column."""
        ...

    @abstractmethod
    def visit_unsupported_columns(self, columns: list[UnsupportedColumnInfo]) -> None:
        """Receive list of unsupported columns."""
        ...

    @abstractmethod
    def visit_statistics_metadata(self, *, include: bool) -> None:
        """Receive statistics metadata flag."""
        ...

    @abstractmethod
    def finish(self) -> str:
        """Produce the final serialized string."""
        ...


# ---------------------------------------------------------------------------
# Concrete serializers
# ---------------------------------------------------------------------------


class CompactAnalyzeTableStatisticsResultSerializer(AnalyzeTableStatisticsResultSerializer):
    """Serialize an AnalyzeTableStatisticsResult in compact, token-efficient format."""

    def __init__(self) -> None:
        self._lines: list[str] = []

    def visit_table_info(
        self,
        database: DataBase,
        schema: Schema,
        table: Table,
        total_rows: int,
        analyzed_columns: int,
    ) -> None:
        self._lines.append(f"database: {database}")
        self._lines.append(f"schema: {schema}")
        self._lines.append(f"table: {table}")
        self._lines.append(f"total_rows: {total_rows}")
        self._lines.append(f"analyzed_columns: {analyzed_columns}")

    def visit_column(self, name: str, stats: StatsDict) -> None:
        self._lines.append("")
        self._lines.append(f"column: {name}")
        for key, value in stats.items():
            if key == "top_values":
                self._lines.append("top_values:")
                for tv in cast("list[TopValue[str]]", value):
                    self._lines.append(f"- {_format_top_value_entry(tv)}")
            elif key == "quality_profile":
                self._lines.append("quality_profile:")
                for qk, qv in cast("dict[str, object]", value).items():
                    self._lines.append(f"  {qk}: {qv}")
            else:
                self._lines.append(f"{key}: {value}")

    def visit_unsupported_columns(self, columns: list[UnsupportedColumnInfo]) -> None:
        if columns:
            self._lines.append("")
            self._lines.append("unsupported_columns:")
            for col in columns:
                self._lines.append(f"- {col.name} ({col.data_type})")

    def visit_statistics_metadata(self, *, include: bool) -> None:
        if include:
            self._lines.append("")
            self._lines.append("statistics_metadata:")
            self._lines.append("quality_profile_counting_mode: exact")
            self._lines.append("distribution_metrics_mode: approximate")

    def finish(self) -> str:
        return "\n".join(self._lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _format_top_value_entry(tv: TopValue[str]) -> str:
    """Format a TopValue entry for compact output."""
    if tv.value is None:
        return f"null: {tv.count}"
    return f"{json.dumps(tv.value, ensure_ascii=False)}: {tv.count}"
