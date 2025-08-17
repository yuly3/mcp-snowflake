"""Internal classes for table statistics analysis."""

import attrs

from kernel.statistics_support_column import StatisticsSupportColumn
from kernel.table_metadata import TableColumn


@attrs.define(frozen=True, slots=True)
class ColumnDoesNotExist:
    """Error result when some requested columns don't exist in the table.

    Attributes
    ----------
    existed_columns : list[TableColumn]
        Columns that exist in the table among the requested ones.
    not_existed_columns : list[str]
        Column names that don't exist in the table.
    """

    existed_columns: list[TableColumn]
    not_existed_columns: list[str]


@attrs.define(frozen=True, slots=True)
class ClassifiedColumns:
    """Result of column classification for statistics analysis.

    Attributes
    ----------
    supported_columns : list[StatisticsSupportColumn]
        Columns that support statistics analysis.
    unsupported_columns : list[TableColumn]
        Columns that don't support statistics analysis.
    """

    supported_columns: list[StatisticsSupportColumn]
    unsupported_columns: list[TableColumn]


@attrs.define(frozen=True, slots=True)
class NoSupportedColumns:
    """Result when no columns support statistics analysis.

    Attributes
    ----------
    unsupported_columns : list[TableColumn]
        All columns that don't support statistics analysis.
    """

    unsupported_columns: list[TableColumn]
