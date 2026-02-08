"""Tests for result parsing functionality."""

from typing import TYPE_CHECKING, cast

import pytest

from kernel.table_metadata import TableColumn
from mcp_snowflake.adapter.analyze_table_statistics_handler.result_parser import (
    parse_statistics_result,
)
from mcp_snowflake.handler.analyze_table_statistics.models import (
    StatisticsResultParseError,
    TopValue,
)

from ...handler.analyze_table_statistics._utils import (
    convert_to_statistics_support_columns,
)

if TYPE_CHECKING:
    from mcp_snowflake.handler.analyze_table_statistics.models import (
        BooleanStatsDict,
        DateStatsDict,
        NumericStatsDict,
        StringStatsDict,
    )


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
            ),
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

        parsed = parse_statistics_result(
            result_row,
            convert_to_statistics_support_columns(columns_info),
        )

        assert parsed.total_rows == 1000
        assert len(parsed.column_statistics) == 1
        price_stats = cast("NumericStatsDict", parsed.column_statistics["price"])
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
            ),
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

        parsed = parse_statistics_result(
            result_row,
            convert_to_statistics_support_columns(columns_info),
        )

        assert parsed.total_rows == 1000
        assert len(parsed.column_statistics) == 1
        status_stats = cast("StringStatsDict", parsed.column_statistics["status"])
        assert status_stats["column_type"] == "string"
        assert status_stats["data_type"] == "VARCHAR(10)"
        assert status_stats["count"] == 1000
        assert status_stats["null_count"] == 0
        assert status_stats["min_length"] == 1
        assert status_stats["max_length"] == 10
        assert status_stats["distinct_count_approx"] == 3
        assert status_stats["top_values"] == [
            TopValue("active", 400),
            TopValue("inactive", 350),
            TopValue("pending", 250),
        ]

    def test_parse_date_column(self) -> None:
        """Test parsing date column statistics."""
        columns_info = [
            TableColumn(
                name="created_date",
                data_type="DATE",
                nullable=False,
                ordinal_position=3,
            ),
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

        parsed = parse_statistics_result(
            result_row,
            convert_to_statistics_support_columns(columns_info),
        )

        assert parsed.total_rows == 1000
        assert len(parsed.column_statistics) == 1
        date_stats = cast("DateStatsDict", parsed.column_statistics["created_date"])
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

        parsed = parse_statistics_result(
            result_row,
            convert_to_statistics_support_columns(columns_info),
        )

        assert parsed.total_rows == 1000
        assert len(parsed.column_statistics) == 3

        # Check numeric column stats
        price_stats = cast("NumericStatsDict", parsed.column_statistics["price"])
        assert price_stats["column_type"] == "numeric"
        assert price_stats["avg"] == 505.25

        # Check string column stats
        status_stats = cast("StringStatsDict", parsed.column_statistics["status"])
        assert status_stats["column_type"] == "string"
        assert status_stats["top_values"] == [
            TopValue("A", 400),
            TopValue("B", 350),
            TopValue("C", 250),
        ]

        # Check date column stats
        date_stats = cast("DateStatsDict", parsed.column_statistics["created_date"])
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

        parsed = parse_statistics_result(
            result_row,
            convert_to_statistics_support_columns(columns_info),
        )

        price_stats = cast("NumericStatsDict", parsed.column_statistics["price"])
        assert price_stats["min"] == 0.0  # Default for None
        assert price_stats["max"] == 0.0
        assert price_stats["avg"] == 0.0
        assert price_stats["percentile_25"] == 0.0
        assert price_stats["percentile_50"] == 0.0
        assert price_stats["percentile_75"] == 0.0

    def test_parse_invalid_json_top_values(self) -> None:
        """Test parsing with invalid JSON in top_values - should raise exception."""
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

        with pytest.raises(
            StatisticsResultParseError,
            match="Failed to parse STRING_STATUS_TOP_VALUES JSON",
        ):
            _ = parse_statistics_result(
                result_row,
                convert_to_statistics_support_columns(columns_info),
            )

    def test_parse_empty_top_values(self) -> None:
        """Test parsing with empty top_values - should raise exception since None is not allowed."""
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

        with pytest.raises(
            StatisticsResultParseError,
            match="STRING_STATUS_TOP_VALUES is required for string column but was None",
        ):
            _ = parse_statistics_result(
                result_row,
                convert_to_statistics_support_columns(columns_info),
            )

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

        parsed = parse_statistics_result(
            result_row,
            convert_to_statistics_support_columns(columns_info),
        )

        assert parsed.total_rows == 1000
        assert len(parsed.column_statistics) == 1
        boolean_stats = cast("BooleanStatsDict", parsed.column_statistics["is_active"])
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

        parsed = parse_statistics_result(
            result_row,
            convert_to_statistics_support_columns(columns_info),
        )

        boolean_stats = cast("BooleanStatsDict", parsed.column_statistics["is_active"])
        assert boolean_stats["count"] == 0
        assert boolean_stats["null_count"] == 1000
        assert boolean_stats["true_count"] == 0
        assert boolean_stats["false_count"] == 0
        assert boolean_stats["true_percentage"] == 0.0
        assert boolean_stats["false_percentage"] == 0.0
        assert boolean_stats["true_percentage_with_nulls"] == 0.0
        assert boolean_stats["false_percentage_with_nulls"] == 0.0

    def test_parse_float_count_normalization(self) -> None:
        """Test parsing with float count values that get converted to int."""
        columns_info = [
            TableColumn(
                name="status",
                data_type="VARCHAR(10)",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        result_row = {
            "TOTAL_ROWS": 1000,
            "STRING_STATUS_COUNT": 1000,
            "STRING_STATUS_NULL_COUNT": 0,
            "STRING_STATUS_MIN_LENGTH": 1,
            "STRING_STATUS_MAX_LENGTH": 10,
            "STRING_STATUS_DISTINCT": 3,
            # Float count values should be converted to int
            "STRING_STATUS_TOP_VALUES": '[["active", 400.0], ["inactive", 350.5], ["pending", 250.9]]',
        }

        parsed = parse_statistics_result(
            result_row,
            convert_to_statistics_support_columns(columns_info),
        )

        status_stats = cast("StringStatsDict", parsed.column_statistics["status"])
        assert status_stats["top_values"] == [
            TopValue("active", 400),  # 400.0 → 400
            TopValue("inactive", 350),  # 350.5 → 350 (int() truncates)
            TopValue("pending", 250),  # 250.9 → 250
        ]

    def test_parse_negative_count_skipping(self) -> None:
        """Test parsing with negative count values - should raise exception."""
        columns_info = [
            TableColumn(
                name="status",
                data_type="VARCHAR(10)",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        result_row = {
            "TOTAL_ROWS": 1000,
            "STRING_STATUS_COUNT": 1000,
            "STRING_STATUS_NULL_COUNT": 0,
            "STRING_STATUS_MIN_LENGTH": 1,
            "STRING_STATUS_MAX_LENGTH": 10,
            "STRING_STATUS_DISTINCT": 3,
            # Mix of valid and negative counts
            "STRING_STATUS_TOP_VALUES": '[["bad", -1], ["good", 100], ["invalid", -5], ["ok", 50]]',
        }

        with pytest.raises(
            StatisticsResultParseError,
            match="Invalid top_values element for column status",
        ):
            _ = parse_statistics_result(
                result_row,
                convert_to_statistics_support_columns(columns_info),
            )

    def test_error_missing_total_rows(self) -> None:
        """Test parsing with missing TOTAL_ROWS - should raise exception."""
        columns_info = [
            TableColumn(
                name="price",
                data_type="NUMBER(10,2)",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        result_row = {
            # TOTAL_ROWS is missing
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

        with pytest.raises(
            StatisticsResultParseError,
            match="TOTAL_ROWS missing from statistics result",
        ):
            _ = parse_statistics_result(
                result_row,
                convert_to_statistics_support_columns(columns_info),
            )

    def test_error_top_values_wrong_shape(self) -> None:
        """Test parsing with wrong shape top_values elements - should raise exception."""
        columns_info = [
            TableColumn(
                name="status",
                data_type="VARCHAR(10)",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        result_row = {
            "TOTAL_ROWS": 1000,
            "STRING_STATUS_COUNT": 1000,
            "STRING_STATUS_NULL_COUNT": 0,
            "STRING_STATUS_MIN_LENGTH": 1,
            "STRING_STATUS_MAX_LENGTH": 10,
            "STRING_STATUS_DISTINCT": 3,
            # Wrong shape: 3 elements instead of 2
            "STRING_STATUS_TOP_VALUES": '[["A", 1, "extra"], ["B", 2]]',
        }

        with pytest.raises(
            StatisticsResultParseError,
            match="Invalid top_values element for column status",
        ):
            _ = parse_statistics_result(
                result_row,
                convert_to_statistics_support_columns(columns_info),
            )

    def test_error_top_values_value_type_mismatch(self) -> None:
        """Test parsing with value type mismatch in top_values - should raise exception."""
        columns_info = [
            TableColumn(
                name="status",
                data_type="VARCHAR(10)",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        result_row = {
            "TOTAL_ROWS": 1000,
            "STRING_STATUS_COUNT": 1000,
            "STRING_STATUS_NULL_COUNT": 0,
            "STRING_STATUS_MIN_LENGTH": 1,
            "STRING_STATUS_MAX_LENGTH": 10,
            "STRING_STATUS_DISTINCT": 3,
            # Value type mismatch: number instead of string
            "STRING_STATUS_TOP_VALUES": '[["A", 1], [123, 2]]',
        }

        with pytest.raises(
            StatisticsResultParseError,
            match="Invalid top_values element for column status",
        ):
            _ = parse_statistics_result(
                result_row,
                convert_to_statistics_support_columns(columns_info),
            )

    def test_parse_quality_profile_for_string_column(self) -> None:
        """Test quality_profile parsing for string column."""
        columns_info = [
            TableColumn(
                name="status",
                data_type="VARCHAR(10)",
                nullable=True,
                ordinal_position=1,
            ),
        ]

        result_row = {
            "TOTAL_ROWS": 10,
            "STRING_STATUS_COUNT": 8,
            "STRING_STATUS_NULL_COUNT": 2,
            "STRING_STATUS_MIN_LENGTH": 0,
            "STRING_STATUS_MAX_LENGTH": 10,
            "STRING_STATUS_DISTINCT": 4,
            "STRING_STATUS_TOP_VALUES": '[["active", 4], ["", 2], ["pending", 2]]',
            "STRING_STATUS_EMPTY_STRING_COUNT": 2,
        }

        parsed = parse_statistics_result(
            result_row,
            convert_to_statistics_support_columns(columns_info),
        )
        status_stats = cast("StringStatsDict", parsed.column_statistics["status"])
        quality_profile = status_stats.get("quality_profile")
        assert quality_profile is not None
        assert quality_profile["null_count"] == 2
        assert quality_profile["null_ratio"] == 0.2
        assert quality_profile["empty_string_count"] == 2
        assert quality_profile["empty_string_ratio"] == 0.25
        assert "blank_string_count" not in quality_profile

    def test_parse_quality_profile_with_blank_string_enabled(self) -> None:
        """Test blank string quality profile parsing for string column."""
        columns_info = [
            TableColumn(
                name="status",
                data_type="VARCHAR(10)",
                nullable=True,
                ordinal_position=1,
            ),
        ]

        result_row = {
            "TOTAL_ROWS": 10,
            "STRING_STATUS_COUNT": 8,
            "STRING_STATUS_NULL_COUNT": 2,
            "STRING_STATUS_MIN_LENGTH": 0,
            "STRING_STATUS_MAX_LENGTH": 10,
            "STRING_STATUS_DISTINCT": 4,
            "STRING_STATUS_TOP_VALUES": '[["active", 4], ["", 2], ["pending", 2]]',
            "STRING_STATUS_EMPTY_STRING_COUNT": 2,
            "STRING_STATUS_BLANK_STRING_COUNT": 3,
        }

        parsed = parse_statistics_result(
            result_row,
            convert_to_statistics_support_columns(columns_info),
            include_null_empty_profile=True,
            include_blank_string_profile=True,
        )
        status_stats = cast("StringStatsDict", parsed.column_statistics["status"])
        quality_profile = status_stats.get("quality_profile")
        assert quality_profile is not None
        assert quality_profile.get("blank_string_count") == 3
        assert quality_profile.get("blank_string_ratio") == 0.375

    def test_parse_quality_profile_ratio_zero_denominator(self) -> None:
        """Test ratio fallback to 0.0 when denominator is zero."""
        columns_info = [
            TableColumn(
                name="status",
                data_type="VARCHAR(10)",
                nullable=True,
                ordinal_position=1,
            ),
        ]

        result_row = {
            "TOTAL_ROWS": 0,
            "STRING_STATUS_COUNT": 0,
            "STRING_STATUS_NULL_COUNT": 0,
            "STRING_STATUS_MIN_LENGTH": None,
            "STRING_STATUS_MAX_LENGTH": None,
            "STRING_STATUS_DISTINCT": 0,
            "STRING_STATUS_TOP_VALUES": "[]",
            "STRING_STATUS_EMPTY_STRING_COUNT": 0,
            "STRING_STATUS_BLANK_STRING_COUNT": 0,
        }

        parsed = parse_statistics_result(
            result_row,
            convert_to_statistics_support_columns(columns_info),
            include_null_empty_profile=True,
            include_blank_string_profile=True,
        )
        status_stats = cast("StringStatsDict", parsed.column_statistics["status"])
        quality_profile = status_stats.get("quality_profile")
        assert quality_profile is not None
        assert quality_profile["null_ratio"] == 0.0
        assert quality_profile["empty_string_ratio"] == 0.0
        assert quality_profile.get("blank_string_ratio") == 0.0

    def test_parse_without_quality_profile_for_backward_compatibility(self) -> None:
        """Test no quality_profile field when include_null_empty_profile is disabled."""
        columns_info = [
            TableColumn(
                name="price",
                data_type="NUMBER(10,2)",
                nullable=False,
                ordinal_position=1,
            ),
            TableColumn(
                name="status",
                data_type="VARCHAR(10)",
                nullable=True,
                ordinal_position=2,
            ),
        ]

        result_row = {
            "TOTAL_ROWS": 100,
            "NUMERIC_PRICE_COUNT": 100,
            "NUMERIC_PRICE_NULL_COUNT": 0,
            "NUMERIC_PRICE_MIN": 1.0,
            "NUMERIC_PRICE_MAX": 100.0,
            "NUMERIC_PRICE_AVG": 50.5,
            "NUMERIC_PRICE_Q1": 25.0,
            "NUMERIC_PRICE_MEDIAN": 50.0,
            "NUMERIC_PRICE_Q3": 75.0,
            "NUMERIC_PRICE_DISTINCT": 100,
            "STRING_STATUS_COUNT": 90,
            "STRING_STATUS_NULL_COUNT": 10,
            "STRING_STATUS_MIN_LENGTH": 1,
            "STRING_STATUS_MAX_LENGTH": 10,
            "STRING_STATUS_DISTINCT": 3,
            "STRING_STATUS_TOP_VALUES": '[["A", 40], ["B", 30], ["", 20]]',
        }

        parsed = parse_statistics_result(
            result_row,
            convert_to_statistics_support_columns(columns_info),
            include_null_empty_profile=False,
            include_blank_string_profile=True,
        )
        price_stats = cast("NumericStatsDict", parsed.column_statistics["price"])
        status_stats = cast("StringStatsDict", parsed.column_statistics["status"])
        assert "quality_profile" not in price_stats
        assert "quality_profile" not in status_stats
