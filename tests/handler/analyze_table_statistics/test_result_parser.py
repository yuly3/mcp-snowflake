"""Tests for result parsing functionality."""

from typing import TYPE_CHECKING, cast

from kernel.statistics_support_column import StatisticsSupportColumn
from kernel.table_metadata import TableColumn
from mcp_snowflake.handler.analyze_table_statistics._result_parser import (
    parse_statistics_result,
)

if TYPE_CHECKING:
    from mcp_snowflake.handler.analyze_table_statistics._types import (
        BooleanStatsDict,
        DateStatsDict,
        NumericStatsDict,
        StringStatsDict,
    )


def _convert_to_statistics_support_columns(
    columns: list[TableColumn],
) -> list[StatisticsSupportColumn]:
    """Convert TableColumns to StatisticsSupportColumns for testing."""
    result = []
    for col in columns:
        stats_col = StatisticsSupportColumn.from_table_column(col)
        if stats_col is not None:
            result.append(stats_col)
    return result


class TestParseStatisticsResult:
    """Test parse_statistics_result function."""

    def test_parse_numeric_column(self) -> None:
        """Test parsing numeric column statistics."""
        columns_info = [
            TableColumn(
                name="price",
                data_type="NUMBER(10,2)",
                nullable=False,
                ordinal_position=2,
            )
        ]

        result_row = {
            "TOTAL_ROWS": 1000,
            "NUMERIC_PRICE_COUNT": 1000,
            "NUMERIC_PRICE_NULL_COUNT": 0,
            "NUMERIC_PRICE_MIN": 10.5,
            "NUMERIC_PRICE_MAX": 999.99,
            "NUMERIC_PRICE_AVG": 505.25,
            "NUMERIC_PRICE_Q1": 250.0,
            "NUMERIC_PRICE_MEDIAN": 500.0,
            "NUMERIC_PRICE_Q3": 750.0,
            "NUMERIC_PRICE_DISTINCT": 950,
        }

        column_stats = parse_statistics_result(
            result_row, _convert_to_statistics_support_columns(columns_info)
        )

        assert len(column_stats) == 1
        price_stats = cast("NumericStatsDict", column_stats["price"])
        assert price_stats["column_type"] == "numeric"
        assert price_stats["data_type"] == "NUMBER(10,2)"
        assert price_stats["count"] == 1000
        assert price_stats["null_count"] == 0
        assert price_stats["min"] == 10.5
        assert price_stats["max"] == 999.99
        assert price_stats["avg"] == 505.25
        assert price_stats["percentile_25"] == 250.0
        assert price_stats["percentile_50"] == 500.0
        assert price_stats["percentile_75"] == 750.0
        assert price_stats["distinct_count_approx"] == 950

    def test_parse_string_column(self) -> None:
        """Test parsing string column statistics."""
        columns_info = [
            TableColumn(
                name="status",
                data_type="VARCHAR(10)",
                nullable=False,
                ordinal_position=1,
            )
        ]

        result_row = {
            "TOTAL_ROWS": 1000,
            "STRING_STATUS_COUNT": 1000,
            "STRING_STATUS_NULL_COUNT": 0,
            "STRING_STATUS_MIN_LENGTH": 1,
            "STRING_STATUS_MAX_LENGTH": 10,
            "STRING_STATUS_DISTINCT": 3,
            "STRING_STATUS_TOP_VALUES": '[["active", 400], ["inactive", 350], ["pending", 250]]',
        }

        column_stats = parse_statistics_result(
            result_row, _convert_to_statistics_support_columns(columns_info)
        )

        assert len(column_stats) == 1
        status_stats = cast("StringStatsDict", column_stats["status"])
        assert status_stats["column_type"] == "string"
        assert status_stats["data_type"] == "VARCHAR(10)"
        assert status_stats["count"] == 1000
        assert status_stats["null_count"] == 0
        assert status_stats["min_length"] == 1
        assert status_stats["max_length"] == 10
        assert status_stats["distinct_count_approx"] == 3
        assert status_stats["top_values"] == [
            ["active", 400],
            ["inactive", 350],
            ["pending", 250],
        ]

    def test_parse_date_column(self) -> None:
        """Test parsing date column statistics."""
        columns_info = [
            TableColumn(
                name="created_date",
                data_type="DATE",
                nullable=False,
                ordinal_position=3,
            )
        ]

        result_row = {
            "TOTAL_ROWS": 1000,
            "DATE_CREATED_DATE_COUNT": 1000,
            "DATE_CREATED_DATE_NULL_COUNT": 0,
            "DATE_CREATED_DATE_MIN": "2023-01-01",
            "DATE_CREATED_DATE_MAX": "2023-12-31",
            "DATE_CREATED_DATE_RANGE_DAYS": 364,
            "DATE_CREATED_DATE_DISTINCT": 300,
        }

        column_stats = parse_statistics_result(
            result_row, _convert_to_statistics_support_columns(columns_info)
        )

        assert len(column_stats) == 1
        date_stats = cast("DateStatsDict", column_stats["created_date"])
        assert date_stats["column_type"] == "date"
        assert date_stats["data_type"] == "DATE"
        assert date_stats["count"] == 1000
        assert date_stats["null_count"] == 0
        assert date_stats["min_date"] == "2023-01-01"
        assert date_stats["max_date"] == "2023-12-31"
        assert date_stats["date_range_days"] == 364
        assert date_stats["distinct_count_approx"] == 300

    def test_parse_mixed_columns(self) -> None:
        """Test parsing mixed column types."""
        columns_info = [
            TableColumn(
                name="price",
                data_type="NUMBER(10,2)",
                nullable=False,
                ordinal_position=1,
            ),
            TableColumn(
                name="status",
                data_type="VARCHAR(1)",
                nullable=False,
                ordinal_position=2,
            ),
            TableColumn(
                name="created_date",
                data_type="DATE",
                nullable=False,
                ordinal_position=3,
            ),
        ]

        result_row = {
            "TOTAL_ROWS": 1000,
            # Numeric column results
            "NUMERIC_PRICE_COUNT": 1000,
            "NUMERIC_PRICE_NULL_COUNT": 0,
            "NUMERIC_PRICE_MIN": 10.5,
            "NUMERIC_PRICE_MAX": 999.99,
            "NUMERIC_PRICE_AVG": 505.25,
            "NUMERIC_PRICE_Q1": 250.0,
            "NUMERIC_PRICE_MEDIAN": 500.0,
            "NUMERIC_PRICE_Q3": 750.0,
            "NUMERIC_PRICE_DISTINCT": 950,
            # String column results
            "STRING_STATUS_COUNT": 1000,
            "STRING_STATUS_NULL_COUNT": 0,
            "STRING_STATUS_MIN_LENGTH": 1,
            "STRING_STATUS_MAX_LENGTH": 1,
            "STRING_STATUS_DISTINCT": 3,
            "STRING_STATUS_TOP_VALUES": '[["A", 400], ["B", 350], ["C", 250]]',
            # Date column results
            "DATE_CREATED_DATE_COUNT": 1000,
            "DATE_CREATED_DATE_NULL_COUNT": 0,
            "DATE_CREATED_DATE_MIN": "2023-01-01",
            "DATE_CREATED_DATE_MAX": "2023-12-31",
            "DATE_CREATED_DATE_RANGE_DAYS": 364,
            "DATE_CREATED_DATE_DISTINCT": 300,
        }

        column_stats = parse_statistics_result(
            result_row, _convert_to_statistics_support_columns(columns_info)
        )

        assert len(column_stats) == 3

        # Check numeric column stats
        price_stats = cast("NumericStatsDict", column_stats["price"])
        assert price_stats["column_type"] == "numeric"
        assert price_stats["avg"] == 505.25

        # Check string column stats
        status_stats = cast("StringStatsDict", column_stats["status"])
        assert status_stats["column_type"] == "string"
        assert status_stats["top_values"] == [["A", 400], ["B", 350], ["C", 250]]

        # Check date column stats
        date_stats = cast("DateStatsDict", column_stats["created_date"])
        assert date_stats["column_type"] == "date"
        assert date_stats["date_range_days"] == 364

    def test_parse_with_null_values(self) -> None:
        """Test parsing with null values in the result."""
        columns_info = [
            TableColumn(
                name="price",
                data_type="NUMBER(10,2)",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        result_row = {
            "TOTAL_ROWS": 1000,
            "NUMERIC_PRICE_COUNT": 500,
            "NUMERIC_PRICE_NULL_COUNT": 500,
            "NUMERIC_PRICE_MIN": None,
            "NUMERIC_PRICE_MAX": None,
            "NUMERIC_PRICE_AVG": None,
            "NUMERIC_PRICE_Q1": None,
            "NUMERIC_PRICE_MEDIAN": None,
            "NUMERIC_PRICE_Q3": None,
            "NUMERIC_PRICE_DISTINCT": 0,
        }

        column_stats = parse_statistics_result(
            result_row, _convert_to_statistics_support_columns(columns_info)
        )

        price_stats = cast("NumericStatsDict", column_stats["price"])
        assert price_stats["min"] == 0.0  # Default for None
        assert price_stats["max"] == 0.0
        assert price_stats["avg"] == 0.0
        assert price_stats["percentile_25"] == 0.0
        assert price_stats["percentile_50"] == 0.0
        assert price_stats["percentile_75"] == 0.0

    def test_parse_invalid_json_top_values(self) -> None:
        """Test parsing with invalid JSON in top_values."""
        columns_info = [
            TableColumn(
                name="status",
                data_type="VARCHAR(10)",
                nullable=False,
                ordinal_position=2,
            ),
        ]

        result_row = {
            "TOTAL_ROWS": 1000,
            "STRING_STATUS_COUNT": 1000,
            "STRING_STATUS_NULL_COUNT": 0,
            "STRING_STATUS_MIN_LENGTH": 1,
            "STRING_STATUS_MAX_LENGTH": 10,
            "STRING_STATUS_DISTINCT": 3,
            "STRING_STATUS_TOP_VALUES": "invalid_json",
        }

        column_stats = parse_statistics_result(
            result_row, _convert_to_statistics_support_columns(columns_info)
        )

        status_stats = cast("StringStatsDict", column_stats["status"])
        assert status_stats["top_values"] == []  # Should default to empty list

    def test_parse_empty_top_values(self) -> None:
        """Test parsing with empty top_values."""
        columns_info = [
            TableColumn(
                name="status",
                data_type="VARCHAR(10)",
                nullable=False,
                ordinal_position=2,
            ),
        ]

        result_row = {
            "TOTAL_ROWS": 1000,
            "STRING_STATUS_COUNT": 1000,
            "STRING_STATUS_NULL_COUNT": 0,
            "STRING_STATUS_MIN_LENGTH": 1,
            "STRING_STATUS_MAX_LENGTH": 10,
            "STRING_STATUS_DISTINCT": 3,
            "STRING_STATUS_TOP_VALUES": None,
        }

        column_stats = parse_statistics_result(
            result_row, _convert_to_statistics_support_columns(columns_info)
        )

        status_stats = cast("StringStatsDict", column_stats["status"])
        assert status_stats["top_values"] == []

    def test_parse_boolean_column(self) -> None:
        """Test parsing boolean column statistics."""
        columns_info = [
            TableColumn(
                name="is_active",
                data_type="BOOLEAN",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        result_row = {
            "TOTAL_ROWS": 1000,
            "BOOLEAN_IS_ACTIVE_COUNT": 950,
            "BOOLEAN_IS_ACTIVE_NULL_COUNT": 50,
            "BOOLEAN_IS_ACTIVE_TRUE_COUNT": 720,
            "BOOLEAN_IS_ACTIVE_FALSE_COUNT": 230,
            "BOOLEAN_IS_ACTIVE_TRUE_PERCENTAGE": 75.79,
            "BOOLEAN_IS_ACTIVE_FALSE_PERCENTAGE": 24.21,
            "BOOLEAN_IS_ACTIVE_TRUE_PERCENTAGE_WITH_NULLS": 72.0,
            "BOOLEAN_IS_ACTIVE_FALSE_PERCENTAGE_WITH_NULLS": 23.0,
        }

        column_stats = parse_statistics_result(
            result_row, _convert_to_statistics_support_columns(columns_info)
        )

        assert len(column_stats) == 1
        boolean_stats = cast("BooleanStatsDict", column_stats["is_active"])
        assert boolean_stats["column_type"] == "boolean"
        assert boolean_stats["data_type"] == "BOOLEAN"
        assert boolean_stats["count"] == 950
        assert boolean_stats["null_count"] == 50
        assert boolean_stats["true_count"] == 720
        assert boolean_stats["false_count"] == 230
        assert boolean_stats["true_percentage"] == 75.79
        assert boolean_stats["false_percentage"] == 24.21
        assert boolean_stats["true_percentage_with_nulls"] == 72.0
        assert boolean_stats["false_percentage_with_nulls"] == 23.0

    def test_parse_boolean_all_null(self) -> None:
        """Test parsing boolean column with all null values."""
        columns_info = [
            TableColumn(
                name="is_active",
                data_type="BOOLEAN",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        result_row = {
            "TOTAL_ROWS": 1000,
            "BOOLEAN_IS_ACTIVE_COUNT": 0,
            "BOOLEAN_IS_ACTIVE_NULL_COUNT": 1000,
            "BOOLEAN_IS_ACTIVE_TRUE_COUNT": 0,
            "BOOLEAN_IS_ACTIVE_FALSE_COUNT": 0,
            "BOOLEAN_IS_ACTIVE_TRUE_PERCENTAGE": 0.0,  # DIV0NULL returns 0.0
            "BOOLEAN_IS_ACTIVE_FALSE_PERCENTAGE": 0.0,
            "BOOLEAN_IS_ACTIVE_TRUE_PERCENTAGE_WITH_NULLS": 0.0,
            "BOOLEAN_IS_ACTIVE_FALSE_PERCENTAGE_WITH_NULLS": 0.0,
        }

        column_stats = parse_statistics_result(
            result_row, _convert_to_statistics_support_columns(columns_info)
        )

        boolean_stats = cast("BooleanStatsDict", column_stats["is_active"])
        assert boolean_stats["count"] == 0
        assert boolean_stats["null_count"] == 1000
        assert boolean_stats["true_count"] == 0
        assert boolean_stats["false_count"] == 0
        assert boolean_stats["true_percentage"] == 0.0
        assert boolean_stats["false_percentage"] == 0.0
        assert boolean_stats["true_percentage_with_nulls"] == 0.0
        assert boolean_stats["false_percentage_with_nulls"] == 0.0
