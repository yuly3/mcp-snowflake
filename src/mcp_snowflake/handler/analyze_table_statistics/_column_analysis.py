"""Column analysis utilities for table statistics."""

from typing import Any

import mcp.types as types


def classify_column_type(data_type: str) -> str:
    """Classify column data type into numeric, string, or date.

    Parameters
    ----------
    data_type : str
        The Snowflake data type to classify.

    Returns
    -------
    str
        The classified type: "numeric", "string", or "date".

    Raises
    ------
    ValueError
        If the data type is not supported.
    """
    data_type_upper = data_type.upper()

    # Check numeric types
    if any(
        numeric_type in data_type_upper
        for numeric_type in ["NUMBER", "INT", "FLOAT", "DOUBLE", "DECIMAL"]
    ):
        return "numeric"

    # Check date types
    if any(date_type in data_type_upper for date_type in ["DATE", "TIMESTAMP", "TIME"]):
        return "date"

    # Check string types
    if any(
        string_type in data_type_upper
        for string_type in ["VARCHAR", "CHAR", "TEXT", "STRING"]
    ):
        return "string"

    # If none of the supported types match, raise an exception
    raise ValueError(f"Unsupported column data type: {data_type}")


def validate_and_select_columns(
    all_columns: list[dict[str, Any]],
    requested_columns: list[str],
) -> tuple[list[dict[str, Any]] | None, list[types.Content] | None]:
    """Validate and select columns for analysis.

    Parameters
    ----------
    all_columns : list[dict[str, Any]]
        All available columns from the table.
    requested_columns : list[str]
        Columns requested for analysis. Empty list means all columns.

    Returns
    -------
    tuple[list[dict[str, Any]] | None, list[types.Content] | None]
        A tuple of (columns, error_content). One will be None and the other will have data.
    """
    # Filter columns if specified
    if requested_columns:
        columns_to_analyze = [
            col for col in all_columns if col["name"] in requested_columns
        ]
        if len(columns_to_analyze) != len(requested_columns):
            found_columns = {col["name"] for col in columns_to_analyze}
            missing_columns = set(requested_columns) - found_columns
            return (
                None,
                [
                    types.TextContent(
                        type="text",
                        text=f"Error: Columns not found in table: {', '.join(missing_columns)}",
                    )
                ],
            )
    else:
        columns_to_analyze = all_columns

    if not columns_to_analyze:
        return (
            None,
            [
                types.TextContent(
                    type="text",
                    text="Error: No columns to analyze",
                )
            ],
        )

    # Check for unsupported column types
    unsupported_columns: list[str] = []
    for col in columns_to_analyze:
        try:
            _ = classify_column_type(col["data_type"])
        except ValueError:
            unsupported_columns.append(f"{col['name']} ({col['data_type']})")

    if unsupported_columns:
        return (
            None,
            [
                types.TextContent(
                    type="text",
                    text=f"Error: Unsupported column types found: {', '.join(unsupported_columns)}",
                )
            ],
        )

    return (columns_to_analyze, None)
