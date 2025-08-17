"""Column analysis utilities for table statistics."""

from collections.abc import Sequence

from kernel.statistics_support_column import StatisticsSupportColumn
from kernel.table_metadata import TableColumn

from ._types import ClassifiedColumns, ColumnDoesNotExist, NoSupportedColumns


def select_and_classify_columns(
    all_columns: list[TableColumn],
    requested_columns: Sequence[str],
) -> ClassifiedColumns | ColumnDoesNotExist | NoSupportedColumns:
    """Select and classify columns into supported and unsupported for analysis.

    Parameters
    ----------
    all_columns : list[TableColumn]
        All available columns from the table.
    requested_columns : Sequence[str]
        Columns requested for analysis. Empty list means all columns.

    Returns
    -------
    ClassifiedColumns | ColumnDoesNotExist | NoSupportedColumns
        ClassifiedColumns containing supported and unsupported columns,
        ColumnDoesNotExist if any requested columns don't exist in the table,
        or NoSupportedColumns if no columns support statistics analysis.
    """
    # Filter columns if specified
    if requested_columns:
        requested_columns_set = set(requested_columns)
        columns_to_analyze = [
            col for col in all_columns if col.name in requested_columns_set
        ]
        if len(columns_to_analyze) != len(requested_columns):
            found_columns = {col.name for col in columns_to_analyze}
            not_found_columns = requested_columns_set - found_columns
            return ColumnDoesNotExist(
                existed_columns=columns_to_analyze,
                not_existed_columns=list(not_found_columns),
            )
    else:
        columns_to_analyze = all_columns

    # Classify columns into supported and unsupported
    supported_columns: list[StatisticsSupportColumn] = []
    unsupported_columns: list[TableColumn] = []

    for col in columns_to_analyze:
        stats_col = StatisticsSupportColumn.from_table_column(col)
        if stats_col is None:
            unsupported_columns.append(col)
        else:
            supported_columns.append(stats_col)

    # Check if no supported columns
    if not supported_columns:
        return NoSupportedColumns(unsupported_columns=unsupported_columns)

    return ClassifiedColumns(
        supported_columns=supported_columns,
        unsupported_columns=unsupported_columns,
    )
