"""Column analysis utilities for table statistics."""

from collections.abc import Iterable, Sequence

import mcp.types as types

from ...kernel.table_metadata import TableColumn
from ._types import ColumnInfo


def create_column_info_list(columns: Iterable[TableColumn]) -> list[ColumnInfo]:
    """Convert dict-based column information to ColumnInfo objects.

    Parameters
    ----------
    columns : Iterable[TableColumn]
        List of TableColumn objects.

    Returns
    -------
    list[ColumnInfo]
        List of ColumnInfo objects.

    Raises
    ------
    ValueError
        If any column has unsupported data type or missing required keys.
    """
    column_infos: list[ColumnInfo] = []
    unsupported_columns: list[str] = []

    for col in columns:
        try:
            column_info = ColumnInfo.from_table_column(col)
        except ValueError:
            unsupported_columns.append(f"{col.name} ({col.data_type})")
        else:
            column_infos.append(column_info)

    if unsupported_columns:
        raise ValueError(
            f"Unsupported column types found: {', '.join(unsupported_columns)}"
        )

    return column_infos


def validate_and_select_columns(
    all_columns: list[TableColumn],
    requested_columns: Sequence[str],
) -> list[ColumnInfo] | types.TextContent:
    """Validate and select columns for analysis.

    Parameters
    ----------
    all_columns : list[dict[str, Any]]
        All available columns from the table.
    requested_columns : Sequence[str]
        Columns requested for analysis. Empty list means all columns.

    Returns
    -------
    list[ColumnInfo] | types.TextContent
        List of ColumnInfo objects or an error message.
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

    try:
        column_info_objects = create_column_info_list(columns_to_analyze)
    except ValueError as e:
        # Extract column information from the error
        error_msg = str(e)
        return types.TextContent(
            type="text",
            text=f"Error: {error_msg}",
        )
    else:
        return column_info_objects
