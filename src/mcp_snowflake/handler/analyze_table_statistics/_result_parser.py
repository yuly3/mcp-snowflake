"""Result parsing utilities for table statistics analysis."""

import json
import logging
from collections.abc import Iterable, Mapping
from typing import Any

from expression import option

from ._types import (
    BooleanStatsDict,
    ColumnInfo,
    DateStatsDict,
    NumericStatsDict,
    StatsDict,
    StringStatsDict,
)

logger = logging.getLogger(__name__)


def parse_statistics_result(
    result_row: Mapping[str, Any],
    columns_info: Iterable[ColumnInfo],
) -> dict[str, StatsDict]:
    """Parse the statistics query result into structured column statistics.

    Parameters
    ----------
    result_row : Mapping[str, Any]
        Raw query result row containing all statistics.
    columns_info : Iterable[ColumnInfo]
        Column information including name and data_type.

    Returns
    -------
    dict[str, StatsDict]
        Parsed column statistics keyed by column name.
    """
    column_statistics: dict[str, StatsDict] = {}

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
                    min=float(option.unwrap_or(result_row[f"{prefix}_MIN"], 0.0)),
                    max=float(option.unwrap_or(result_row[f"{prefix}_MAX"], 0.0)),
                    avg=float(option.unwrap_or(result_row[f"{prefix}_AVG"], 0.0)),
                    percentile_25=float(
                        option.unwrap_or(result_row[f"{prefix}_Q1"], 0.0)
                    ),
                    percentile_50=float(
                        option.unwrap_or(result_row[f"{prefix}_MEDIAN"], 0.0)
                    ),
                    percentile_75=float(
                        option.unwrap_or(result_row[f"{prefix}_Q3"], 0.0)
                    ),
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
                    min_date=str(option.unwrap_or(min_date, "")),
                    max_date=str(option.unwrap_or(max_date, "")),
                    date_range_days=result_row[f"{prefix}_RANGE_DAYS"] or 0,
                )
            case "boolean":
                stats = BooleanStatsDict(
                    column_type="boolean",
                    data_type=data_type,
                    count=result_row[f"{prefix}_COUNT"],
                    null_count=result_row[f"{prefix}_NULL_COUNT"],
                    true_count=result_row[f"{prefix}_TRUE_COUNT"],
                    false_count=result_row[f"{prefix}_FALSE_COUNT"],
                    true_percentage=float(
                        option.unwrap_or(result_row[f"{prefix}_TRUE_PERCENTAGE"], 0.0)
                    ),
                    false_percentage=float(
                        option.unwrap_or(result_row[f"{prefix}_FALSE_PERCENTAGE"], 0.0)
                    ),
                    true_percentage_with_nulls=float(
                        option.unwrap_or(
                            result_row[f"{prefix}_TRUE_PERCENTAGE_WITH_NULLS"],
                            0.0,
                        )
                    ),
                    false_percentage_with_nulls=float(
                        option.unwrap_or(
                            result_row[f"{prefix}_FALSE_PERCENTAGE_WITH_NULLS"],
                            0.0,
                        )
                    ),
                )

        column_statistics[col_name] = stats

    return column_statistics
