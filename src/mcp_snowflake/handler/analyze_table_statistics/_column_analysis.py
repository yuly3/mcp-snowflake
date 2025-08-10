"""Column analysis utilities for table statistics."""

from collections.abc import Iterable, Sequence

import mcp.types as types

from ...kernel.table_metadata import TableColumn


def ensure_supported_columns(columns: Iterable[TableColumn]) -> list[TableColumn]:
    """Validate that all columns are supported for statistics analysis.

    Parameters
    ----------
    columns : Iterable[TableColumn]
        List of TableColumn objects to validate.

    Returns
    -------
    list[TableColumn]
        List of supported TableColumn objects.

    Raises
    ------
    ValueError
        If any column has unsupported data type.
    """
    column_list: list[TableColumn] = []
    unsupported_columns: list[str] = []

    for col in columns:
        try:
            # Access the properties to validate support
            _ = col.snowflake_type
            _ = col.statistics_type
        except ValueError:
            unsupported_columns.append(f"{col.name} ({col.data_type})")
        else:
            column_list.append(col)

    if unsupported_columns:
        raise ValueError(
            f"Unsupported column types found: {', '.join(unsupported_columns)}"
        )

    return column_list


def validate_and_select_columns(
    all_columns: list[TableColumn],
    requested_columns: Sequence[str],
) -> list[TableColumn] | types.TextContent:
    """Validate and select columns for analysis.

    Parameters
    ----------
    all_columns : list[TableColumn]
        All available columns from the table.
    requested_columns : Sequence[str]
        Columns requested for analysis. Empty list means all columns.

    Returns
    -------
    list[TableColumn] | types.TextContent
        List of TableColumn objects or an error message.
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
        supported_columns = ensure_supported_columns(columns_to_analyze)
    except ValueError as e:
        # Extract column information from the error
        error_msg = str(e)
        return types.TextContent(
            type="text",
            text=f"Error: {error_msg}",
        )
    else:
        return supported_columns
