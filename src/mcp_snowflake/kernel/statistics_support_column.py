"""Statistics support column wrapper for type safety."""

from typing import Self

import attrs

from .data_types import SnowflakeDataType, StatisticsSupportDataType
from .table_metadata import TableColumn


@attrs.define(frozen=True, slots=True)
class StatisticsSupportColumn:
    """Column wrapper that guarantees statistics support.

    This wrapper ensures that the wrapped column has a valid
    StatisticsSupportDataType, eliminating Optional type checks
    in downstream statistics processing.
    """

    base: TableColumn
    statistics_type: StatisticsSupportDataType

    @property
    def name(self) -> str:
        """Get column name."""
        return self.base.name

    @property
    def data_type(self) -> SnowflakeDataType:
        """Get Snowflake data type."""
        return self.base.data_type

    @property
    def nullable(self) -> bool:
        """Get nullable flag."""
        return self.base.nullable

    @property
    def ordinal_position(self) -> int:
        """Get ordinal position."""
        return self.base.ordinal_position

    @classmethod
    def from_table_column(cls, col: TableColumn) -> Self | None:
        """Convert TableColumn to StatisticsSupportColumn if supported.

        Parameters
        ----------
        col : TableColumn
            The table column to convert.

        Returns
        -------
        Self | None
            The converted column or None if statistics not supported.
        """
        stats_type = col.statistics_type
        if stats_type is None:
            return None
        return cls(base=col, statistics_type=stats_type)
