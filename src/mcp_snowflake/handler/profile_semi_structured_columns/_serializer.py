"""Result model and serializers for profile_semi_structured_columns.

This module provides a format-agnostic ProfileSemiStructuredColumnsResult class
and a serializer interface following the double-dispatch (Visitor) pattern.
"""

import json
from abc import ABC, abstractmethod
from typing import cast

import attrs

from kernel.table_metadata import DataBase, Schema, Table

from .models import ColumnProfileDict, PathProfileDict, TopValue


@attrs.define(frozen=True)
class UnsupportedColumnInfo:
    """Column that could not be profiled due to unsupported data type."""

    name: str
    data_type: str


@attrs.define(frozen=True)
class ProfileSemiStructuredColumnsResult:
    """Format-agnostic representation of a semi-structured profiling result.

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
    sampled_rows : int
        Number of rows sampled for profiling.
    analyzed_column_names : list[str]
        Names of analyzed columns.
    column_profiles : dict[str, ColumnProfileDict]
        Per-column profile data.
    path_profiles : list[PathProfileDict]
        Path-level profile data.
    warnings : list[str]
        Warning messages.
    unsupported_columns : list[UnsupportedColumnInfo]
        Columns skipped due to unsupported data types.
    """

    database: DataBase
    schema: Schema
    table: Table
    total_rows: int
    sampled_rows: int
    analyzed_column_names: list[str]
    column_profiles: dict[str, ColumnProfileDict]
    path_profiles: list[PathProfileDict]
    warnings: list[str]
    unsupported_columns: list[UnsupportedColumnInfo]

    @property
    def analyzed_columns(self) -> int:
        """Return the number of analyzed columns."""
        return len(self.column_profiles)

    def serialize_with(self, serializer: "ProfileSemiStructuredColumnsResultSerializer") -> str:
        """Serialize this result using the given serializer (double dispatch)."""
        serializer.visit_profile_info(
            self.database,
            self.schema,
            self.table,
            self.total_rows,
            self.sampled_rows,
            self.analyzed_columns,
        )
        for name, profile in self.column_profiles.items():
            serializer.visit_column_profile(name, profile)
        for path_profile in self.path_profiles:
            serializer.visit_path_profile(path_profile)
        serializer.visit_unsupported_columns(self.unsupported_columns)
        serializer.visit_warnings(self.warnings)
        return serializer.finish()


class ProfileSemiStructuredColumnsResultSerializer(ABC):
    """Abstract visitor that serializes a ProfileSemiStructuredColumnsResult."""

    @abstractmethod
    def visit_profile_info(
        self,
        database: DataBase,
        schema: Schema,
        table: Table,
        total_rows: int,
        sampled_rows: int,
        analyzed_columns: int,
    ) -> None:
        """Receive top-level profile metadata."""
        ...

    @abstractmethod
    def visit_column_profile(self, name: str, profile: ColumnProfileDict) -> None:
        """Receive profile data for a single column."""
        ...

    @abstractmethod
    def visit_path_profile(self, path_profile: PathProfileDict) -> None:
        """Receive a single path-level profile."""
        ...

    @abstractmethod
    def visit_unsupported_columns(self, columns: list[UnsupportedColumnInfo]) -> None:
        """Receive list of unsupported columns."""
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


class CompactProfileSemiStructuredColumnsResultSerializer(ProfileSemiStructuredColumnsResultSerializer):
    """Serialize a ProfileSemiStructuredColumnsResult in compact, token-efficient format."""

    def __init__(self) -> None:
        self._lines: list[str] = []

    def visit_profile_info(
        self,
        database: DataBase,
        schema: Schema,
        table: Table,
        total_rows: int,
        sampled_rows: int,
        analyzed_columns: int,
    ) -> None:
        self._lines.append(f"database: {database}")
        self._lines.append(f"schema: {schema}")
        self._lines.append(f"table: {table}")
        self._lines.append(f"total_rows: {total_rows}")
        self._lines.append(f"sampled_rows: {sampled_rows}")
        self._lines.append(f"analyzed_columns: {analyzed_columns}")

    def visit_column_profile(self, name: str, profile: ColumnProfileDict) -> None:
        self._lines.append("")
        self._lines.append(f"column_profile: {name}")
        for key, value in profile.items():
            if key == "top_level_type_distribution":
                self._lines.append("top_level_type_distribution:")
                for tk, tv in cast("dict[str, int]", value).items():
                    self._lines.append(f"  {tk}: {tv}")
            elif key == "array_length_stats":
                self._lines.append("array_length_stats:")
                for sk, sv in cast("dict[str, object]", value).items():
                    self._lines.append(f"  {sk}: {sv}")
            elif key == "top_level_keys_top_k":
                self._lines.append("top_level_keys_top_k:")
                for tv in cast("list[TopValue[str]]", value):
                    self._lines.append(f"- {_format_top_value_entry(tv)}")
            else:
                self._lines.append(f"{key}: {value}")

    def visit_path_profile(self, path_profile: PathProfileDict) -> None:
        self._lines.append("")
        self._lines.append(f"path_profile: {path_profile['path']}")
        for key, value in path_profile.items():
            if key == "path":
                continue  # Already in header
            if key == "value_type_distribution":
                self._lines.append("value_type_distribution:")
                for tk, tv in cast("dict[str, int]", value).items():
                    self._lines.append(f"  {tk}: {tv}")
            elif key == "top_values":
                self._lines.append("top_values:")
                for tv in cast("list[TopValue[str]]", value):
                    self._lines.append(f"- {_format_top_value_entry(tv)}")
            else:
                self._lines.append(f"{key}: {value}")

    def visit_unsupported_columns(self, columns: list[UnsupportedColumnInfo]) -> None:
        if columns:
            self._lines.append("")
            self._lines.append("unsupported_columns:")
            for col in columns:
                self._lines.append(f"- {col.name} ({col.data_type})")

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


def _format_top_value_entry(tv: TopValue[str]) -> str:
    """Format a TopValue entry for compact output."""
    if tv.value is None:
        return f"null: {tv.count}"
    return f"{json.dumps(tv.value, ensure_ascii=False)}: {tv.count}"
