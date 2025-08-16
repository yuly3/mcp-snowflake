"""Table metadata domain models using attrs."""

from typing import NewType

import attrs

from .data_types import SnowflakeDataType, StatisticsSupportDataType

DataBase = NewType("DataBase", str)
Schema = NewType("Schema", str)
Table = NewType("Table", str)


def _to_snowflake_data_type(value: str | SnowflakeDataType) -> SnowflakeDataType:
    """Convert str or SnowflakeDataType to SnowflakeDataType with validation."""
    if isinstance(value, SnowflakeDataType):
        return value

    sf_type = SnowflakeDataType.from_raw_str(value)
    if sf_type is None:
        raise ValueError(f"Unsupported Snowflake data type: {value}")
    return sf_type


@attrs.define(frozen=True, slots=True)
class TableColumn:
    """Domain model for a table column."""

    name: str
    data_type: SnowflakeDataType = attrs.field(converter=_to_snowflake_data_type)
    nullable: bool
    ordinal_position: int
    default_value: str | None = None
    comment: str | None = None

    @property
    def statistics_type(self) -> StatisticsSupportDataType | None:
        """Get the statistics support data type for this column."""
        return StatisticsSupportDataType.from_snowflake_type(self.data_type)


@attrs.define(frozen=True, slots=True)
class TableInfo:
    """Domain model for table information."""

    database: DataBase
    schema: Schema
    name: str
    column_count: int
    columns: list[TableColumn]
