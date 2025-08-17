"""Internal classes for table statistics analysis."""

import attrs

from kernel.statistics_support_column import StatisticsSupportColumn
from kernel.table_metadata import TableColumn


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
