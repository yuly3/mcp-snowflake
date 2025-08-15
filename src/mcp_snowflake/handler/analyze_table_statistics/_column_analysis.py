"""Column analysis utilities for table statistics."""

from collections.abc import Sequence

import mcp.types as types

from ...kernel.table_metadata import TableColumn

# Type alias for unsupported column information (column, reason)
UnsupportedInfo = tuple[TableColumn, str]


def select_and_classify_columns(
    all_columns: list[TableColumn],
    requested_columns: Sequence[str],
) -> types.TextContent | tuple[list[TableColumn], list[UnsupportedInfo]]:
    """Select and classify columns into supported and unsupported for analysis.

    Parameters
    ----------
    all_columns : list[TableColumn]
        All available columns from the table.
    requested_columns : Sequence[str]
        Columns requested for analysis. Empty list means all columns.

    Returns
    -------
    types.TextContent | tuple[list[TableColumn], list[UnsupportedInfo]]
        Error message or tuple of (supported_columns, unsupported_info).
        UnsupportedInfo is (TableColumn, reason) pairs.
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
    supported_columns: list[TableColumn] = []
    unsupported_info: list[UnsupportedInfo] = []

    for col in columns_to_analyze:
        try:
            # Access the properties to validate support
            _ = col.statistics_type
        except ValueError as e:
            unsupported_info.append((col, str(e)))
        else:
            supported_columns.append(col)

    return supported_columns, unsupported_info
