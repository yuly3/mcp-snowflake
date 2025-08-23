"""Result parsing utilities for table statistics analysis."""

import json
import logging
from collections.abc import Iterable, Mapping
from typing import Any, cast

from expression import option
from expression.contract import contract
from kernel.statistics_support_column import StatisticsSupportColumn

from .models import (
    BooleanStatsDict,
    DateStatsDict,
    NumericStatsDict,
    StatisticsResultParseError,
    StatsDict,
    StringStatsDict,
    TableStatisticsParseResult,
    TopValue,
)

logger = logging.getLogger(__name__)


@contract(known_err=(StatisticsResultParseError,))
def parse_statistics_result(
    result_row: Mapping[str, Any],
    columns_info: Iterable[StatisticsSupportColumn],
) -> TableStatisticsParseResult:
    """Parse the statistics query result into structured column statistics.

    Parameters
    ----------
    result_row : Mapping[str, Any]
        Single result row from statistics query.
    columns_info : Iterable[StatisticsSupportColumn]
        Column information with statistics support guaranteed.

    Returns
    -------
    TableStatisticsParseResult
        Parsed statistics containing total_rows and column statistics.

    Raises
    ------
    StatisticsResultParseError
        If TOTAL_ROWS is missing/None, or if string column TOP_VALUES is None,
        or if JSON parsing fails for TOP_VALUES.
    """
    # Extract total_rows with fallback to 0 if missing or None
    total_rows_raw = result_row.get("TOTAL_ROWS")
    if total_rows_raw is None:
        logger.error("TOTAL_ROWS missing from statistics result")
        raise StatisticsResultParseError("TOTAL_ROWS missing from statistics result")
    total_rows = int(total_rows_raw)

    column_statistics: dict[str, StatsDict] = {}

    for col_info in columns_info:
        col_name = col_info.name
        data_type = col_info.data_type.raw_type  # Use raw_type for JSON serialization
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

                if top_values_raw is None:
                    logger.error(
                        f"{prefix}_TOP_VALUES is required for string column but was None",
                        extra={"column": col_name, "prefix": prefix},
                    )
                    raise StatisticsResultParseError(
                        f"{prefix}_TOP_VALUES is required for string column but was None"
                    )

                try:
                    raw_values = (
                        cast("list[Any]", json.loads(top_values_raw))
                        if top_values_raw
                        else []
                    )
                except (json.JSONDecodeError, TypeError) as e:
                    logger.error(
                        f"Failed to parse {prefix}_TOP_VALUES JSON",
                        extra={
                            "column": col_name,
                            "raw_value": top_values_raw,
                            "error": str(e),
                        },
                    )
                    raise StatisticsResultParseError(
                        f"Failed to parse {prefix}_TOP_VALUES JSON: {top_values_raw!r}"
                    ) from e

                top_values = parse_top_values(
                    raw_values,
                    str,
                    col_name,
                )

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

    return TableStatisticsParseResult(
        total_rows=total_rows,
        column_statistics=column_statistics,
    )


@contract(known_err=(StatisticsResultParseError,))
def parse_top_values[T](
    raw_top_values: list[Any],
    value_cls: type[T],
    column_name: str,
) -> list[TopValue[T]]:
    """Parse raw top values from the database result.

    Parameters
    ----------
    raw_top_values : list[Any]
        Raw top values from the database result, expected to be a list of lists.
    value_cls : type[T]
        The type of the values in the top values list.
    column_name : str
        Name of the column being processed (for error messages).

    Returns
    -------
    list[TopValue[T]]
        A list of TopValue instances parsed from the raw input.

    Raises
    ------
    StatisticsResultParseError
        If any element has invalid structure, type, or negative count.
    """
    top_values: list[TopValue[T]] = []
    for item in raw_top_values:
        if not isinstance(item, list) or len(item) != 2:
            logger.error(
                f"Invalid top_values element structure for column {column_name}",
                extra={"column": column_name, "invalid_item": item},
            )
            raise StatisticsResultParseError(
                f"Invalid top_values element for column {column_name}: {item!r}"
            )

        value_raw, count_raw = cast("list[Any]", item)

        # Check value type
        if not (isinstance(value_raw, value_cls) or value_raw is None):
            logger.error(
                f"Invalid value type in top_values for column {column_name}",
                extra={
                    "column": column_name,
                    "expected_type": value_cls.__name__,
                    "value": value_raw,
                    "item": item,
                },
            )
            raise StatisticsResultParseError(
                f"Invalid top_values element for column {column_name}: {item!r}"
            )

        # Check and convert count
        try:
            count_int = int(count_raw)
            top_value = TopValue(value_raw, count_int)
        except (ValueError, TypeError) as e:
            logger.error(
                f"Invalid count in top_values for column {column_name}",
                extra={
                    "column": column_name,
                    "count_raw": count_raw,
                    "item": item,
                    "error": str(e),
                },
            )
            raise StatisticsResultParseError(
                f"Invalid top_values element for column {column_name}: {item!r}"
            ) from e
        top_values.append(top_value)

    return top_values
