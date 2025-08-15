"""Column analysis utilities for table statistics."""

from collections.abc import Sequence

import mcp.types as types

from ...kernel.statistics_support_column import StatisticsSupportColumn
from ...kernel.table_metadata import TableColumn

# Type alias for unsupported column information (column, reason)
UnsupportedInfo = tuple[TableColumn, str]


def select_and_classify_columns(
    all_columns: list[TableColumn],
    requested_columns: Sequence[str],
) -> types.TextContent | tuple[list[StatisticsSupportColumn], list[UnsupportedInfo]]:
    """Select and classify columns into supported and unsupported for analysis.

    Parameters
    ----------
    all_columns : list[TableColumn]
        All available columns from the table.
    requested_columns : Sequence[str]
        Columns requested for analysis. Empty list means all columns.

    Returns
    -------
    types.TextContent | tuple[list[StatisticsSupportColumn], list[UnsupportedInfo]]
        Error message or tuple of (supported_columns, unsupported_info).
        UnsupportedInfo is (TableColumn, reason) pairs.
        supported_columns are now StatisticsSupportColumn instances.
    """
    # Filter columns if specified
    if requested_columns:
        columns_to_analyze = [
            col for col in all_columns if col.name in requested_columns
        ]
        if len(columns_to_analyze) != len(requested_columns):
            found_columns: set[str] = {col.name for col in columns_to_analyze}
            missing_columns = set(requested_columns) - found_columns
            return types.TextContent(
                type="text",
                text=f"Error: Columns not found in table: {', '.join(missing_columns)}",
            )
    else:
        columns_to_analyze = all_columns

    if not columns_to_analyze:
        return types.TextContent(
            type="text",
            text="Error: No columns to analyze",
        )

    # Classify columns into supported and unsupported
    supported_columns: list[StatisticsSupportColumn] = []
    unsupported_info: list[UnsupportedInfo] = []

    for col in columns_to_analyze:
        stats_col = StatisticsSupportColumn.from_table_column(col)
        if stats_col is None:
            # Generate the same error message format as before
            reason = f"Unsupported Snowflake data type for statistics: {col.data_type.raw_type}"
            unsupported_info.append((col, reason))
        else:
            supported_columns.append(stats_col)

    return supported_columns, unsupported_info
