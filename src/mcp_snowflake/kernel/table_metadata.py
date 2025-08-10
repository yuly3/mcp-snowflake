"""Table metadata domain models using attrs."""

import attrs
from cattrs import Converter

from .data_types import SnowflakeDataType, StatisticsSupportDataType

# Default cattrs converter for this module
converter = Converter()


@attrs.define(frozen=True, slots=True)
class TableColumn:
    """Domain model for a table column."""

    name: str
    data_type: str
    nullable: bool
    ordinal_position: int
    default_value: str | None = None
    comment: str | None = None

    @property
    def snowflake_type(self) -> SnowflakeDataType:
        """Get the Snowflake data type representation for this column."""
        return SnowflakeDataType(self.data_type)

    @property
    def statistics_type(self) -> StatisticsSupportDataType:
        """Get the statistics support data type for this column."""
        return StatisticsSupportDataType.from_snowflake_type(self.snowflake_type)


@attrs.define(frozen=True, slots=True)
class TableInfo:
    """Domain model for table information."""

    database: str
    schema: str
    name: str
    column_count: int
    columns: list[TableColumn]
