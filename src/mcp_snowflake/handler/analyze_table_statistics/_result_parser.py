"""Result parsing utilities for table statistics analysis."""

import json
import logging
from typing import Any

from ._types import ColumnInfo, DateStatsDict, NumericStatsDict, StringStatsDict

logger = logging.getLogger(__name__)


def parse_statistics_result(
    result_row: dict[str, Any],
    columns_info: list[ColumnInfo],
) -> dict[str, NumericStatsDict | StringStatsDict | DateStatsDict]:
    """Parse the statistics query result into structured column statistics.

    Parameters
    ----------
    result_row : dict[str, Any]
        Raw query result row containing all statistics.
    columns_info : list[ColumnInfo]
        Column information including name and data_type.

    Returns
    -------
    dict[str, NumericStatsDict | StringStatsDict | DateStatsDict]
        Parsed column statistics keyed by column name.
    """
    column_statistics: dict[
        str, NumericStatsDict | StringStatsDict | DateStatsDict
    ] = {}

    for col_info in columns_info:
        col_name = col_info.name
        data_type = col_info.snowflake_type.raw_type
        col_type = col_info.statistics_type.type_name
        prefix = f"{col_type}_{col_name}".upper()  # Convert to uppercase for Snowflake

        match col_type:
            case "numeric":
                stats = NumericStatsDict(
                    column_type="numeric",
                    data_type=data_type,
                    count=result_row[f"{prefix}_COUNT"],
                    null_count=result_row[f"{prefix}_NULL_COUNT"],
                    distinct_count_approx=result_row[f"{prefix}_DISTINCT"],
                    min=float(result_row[f"{prefix}_MIN"])
                    if result_row[f"{prefix}_MIN"] is not None
                    else 0.0,
                    max=float(result_row[f"{prefix}_MAX"])
                    if result_row[f"{prefix}_MAX"] is not None
                    else 0.0,
                    avg=float(result_row[f"{prefix}_AVG"])
                    if result_row[f"{prefix}_AVG"] is not None
                    else 0.0,
                    percentile_25=float(result_row[f"{prefix}_Q1"])
                    if result_row[f"{prefix}_Q1"] is not None
                    else 0.0,
                    percentile_50=float(result_row[f"{prefix}_MEDIAN"])
                    if result_row[f"{prefix}_MEDIAN"] is not None
                    else 0.0,
                    percentile_75=float(result_row[f"{prefix}_Q3"])
                    if result_row[f"{prefix}_Q3"] is not None
                    else 0.0,
                )
            case "string":
                # Parse APPROX_TOP_K result from JSON string
                top_values_raw = result_row[f"{prefix}_TOP_VALUES"]
                try:
                    top_values = json.loads(top_values_raw) if top_values_raw else []
                except (json.JSONDecodeError, TypeError):
                    logger.warning(
                        f"Failed to parse top_values for column {col_name}: {top_values_raw}"
                    )
                    top_values = []

                stats = StringStatsDict(
                    column_type="string",
                    data_type=data_type,
                    count=result_row[f"{prefix}_COUNT"],
                    null_count=result_row[f"{prefix}_NULL_COUNT"],
                    distinct_count_approx=result_row[f"{prefix}_DISTINCT"],
                    min_length=result_row[f"{prefix}_MIN_LENGTH"] or 0,
                    max_length=result_row[f"{prefix}_MAX_LENGTH"] or 0,
                    top_values=top_values,
                )
            case "date":
                min_date = result_row[f"{prefix}_MIN"]
                max_date = result_row[f"{prefix}_MAX"]

                stats = DateStatsDict(
                    column_type="date",
                    data_type=data_type,
                    count=result_row[f"{prefix}_COUNT"],
                    null_count=result_row[f"{prefix}_NULL_COUNT"],
                    distinct_count_approx=result_row[f"{prefix}_DISTINCT"],
                    min_date=str(min_date) if min_date is not None else "",
                    max_date=str(max_date) if max_date is not None else "",
                    date_range_days=result_row[f"{prefix}_RANGE_DAYS"] or 0,
                )

        column_statistics[col_name] = stats

    return column_statistics
