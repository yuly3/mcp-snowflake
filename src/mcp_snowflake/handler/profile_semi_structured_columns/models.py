from collections.abc import Awaitable
from typing import Literal, NotRequired, Protocol, TypedDict

import attrs
from attrs import validators
from pydantic import BaseModel, Field

from kernel.table_metadata import DataBase, Schema, Table, TableColumn

from ..describe_table import EffectDescribeTable


class SemiStructuredProfileResultParseError(Exception):
    """Raised when semi-structured profile query results cannot be parsed."""


@attrs.define(frozen=True, slots=True)
class TopValue[T]:
    """Type-safe representation of a top value entry with count."""

    value: T | None
    count: int = attrs.field(validator=[validators.ge(0)])


class TopLevelTypeDistributionDict(TypedDict):
    """Top-level data type distribution for a semi-structured column."""

    OBJECT: int
    ARRAY: int
    STRING: int
    NUMBER: int
    BOOLEAN: int
    NULL: int


class ArrayLengthStatsDict(TypedDict):
    """Array length summary statistics."""

    min: int
    max: int
    p25: float
    p50: float
    p75: float


class ColumnProfileDict(TypedDict):
    """Column-level profile for a semi-structured column."""

    column_type: Literal["VARIANT", "ARRAY", "OBJECT"]
    null_count: int
    non_null_count: int
    null_ratio: float
    top_level_type_distribution: TopLevelTypeDistributionDict
    array_length_stats: NotRequired[ArrayLengthStatsDict]
    top_level_keys_top_k: NotRequired[list[TopValue[str]]]


class PathProfileDict(TypedDict):
    """Path-level profile for values inside semi-structured columns."""

    column: str
    path: str
    path_depth: int
    value_type_distribution: dict[str, int]
    distinct_count_approx: int
    null_ratio: float
    top_values: NotRequired[list[TopValue[str]]]


class UnsupportedColumnDict(TypedDict):
    """Column that cannot be profiled by this tool."""

    name: str
    data_type: str


class ProfileInfoDict(TypedDict):
    """Summary metadata for profile execution."""

    database: str
    schema: str
    table: str
    total_rows: int
    sampled_rows: int
    analyzed_columns: int
    analyzed_column_names: list[str]
    unsupported_columns: NotRequired[list[UnsupportedColumnDict]]


class SemiStructuredProfileDict(TypedDict):
    """Main payload for semi-structured profiling results."""

    profile_info: ProfileInfoDict
    column_profiles: dict[str, ColumnProfileDict]
    path_profiles: list[PathProfileDict]
    warnings: list[str]


class ProfileSemiStructuredColumnsJsonResponse(TypedDict):
    """Typed JSON response payload for profile_semi_structured_columns tool."""

    semi_structured_profile: SemiStructuredProfileDict


class ProfileSemiStructuredColumnsArgs(BaseModel):
    """Arguments for profiling semi-structured columns."""

    database: DataBase
    schema_: Schema = Field(alias="schema")
    table_: Table = Field(alias="table")
    columns: list[str] = Field(default_factory=list)
    sample_rows: int = Field(default=10_000, ge=1, le=200_000)
    max_depth: int = Field(default=4, ge=1, le=20)
    top_k_limit: int = Field(default=20, ge=1, le=100)
    include_path_stats: bool = Field(default=True)
    include_value_samples: bool = Field(default=False)


@attrs.define(frozen=True, slots=True)
class SemiStructuredProfileParseResult:
    """Structured profile result produced by adapter layer."""

    total_rows: int
    sampled_rows: int
    column_profiles: dict[str, ColumnProfileDict]
    path_profiles: list[PathProfileDict]
    warnings: list[str]


class EffectProfileSemiStructuredColumns(EffectDescribeTable, Protocol):
    """Protocol for dependencies required by semi-structured profiling."""

    def profile_semi_structured_columns(
        self,
        database: DataBase,
        schema: Schema,
        table: Table,
        columns_to_profile: list[TableColumn],
        sample_rows: int,
        max_depth: int,
        top_k_limit: int,
        include_path_stats: bool,  # noqa: FBT001
        include_value_samples: bool,  # noqa: FBT001
    ) -> Awaitable[SemiStructuredProfileParseResult]:
        """Execute profiling queries and return parsed results."""
        ...


@attrs.define(frozen=True, slots=True)
class SemiStructuredColumnDoesNotExist:
    """Error result when some requested columns do not exist."""

    existed_columns: list[TableColumn]
    not_existed_columns: list[str]


@attrs.define(frozen=True, slots=True)
class NoSemiStructuredColumns:
    """Error result when no selected columns are semi-structured."""

    unsupported_columns: list[TableColumn]
