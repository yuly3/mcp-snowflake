"""Success cases tests for analyze_table_statistics handler."""

import pytest

from kernel.table_metadata import DataBase, Schema, Table
from mcp_snowflake.handler.analyze_table_statistics import (
    AnalyzeTableStatisticsArgs,
    handle_analyze_table_statistics,
)

from ....mock_effect_handler import MockAnalyzeTableStatistics
from .test_fixtures import (
    create_mixed_analysis_result,
    create_test_table_info,
)


class TestSuccessCases:
    """Test successful analysis cases."""

    @pytest.mark.asyncio
    async def test_successful_analysis_comprehensive(self) -> None:
        """Test comprehensive successful table statistics analysis with mixed column types."""
        # Create table with numeric, string, and boolean columns
        table_data = create_test_table_info(
            [
                ("id", "NUMBER(10,0)", False, 1),
                ("name", "VARCHAR(50)", True, 2),
                ("price", "NUMBER(10,2)", True, 3),
                ("status", "VARCHAR(10)", True, 4),
                ("is_active", "BOOLEAN", True, 5),
            ],
        )

        # Create comprehensive query result with all column types
        query_result = create_mixed_analysis_result(
            numeric_columns=["id", "price"],
            string_columns=["name", "status"],
            boolean_columns=["is_active"],
            total_rows=1000,
        )

        mock_effect = MockAnalyzeTableStatistics(
            table_info=table_data,
            statistics_result=query_result,
        )

        args = AnalyzeTableStatisticsArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("test_table"),
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        # Should return structured data, not content list
        assert isinstance(result, dict)
        assert "table_statistics" in result

        # Check table_info
        table_stats = result["table_statistics"]
        table_info = table_stats["table_info"]
        assert table_info["database"] == "test_db"
        assert table_info["schema"] == "test_schema"
        assert table_info["table"] == "test_table"
        assert table_info["total_rows"] == 1000
        assert table_info["analyzed_columns"] == 5

        # Check column statistics
        column_stats = table_stats["column_statistics"]
        assert len(column_stats) == 5

        # Verify numeric columns
        assert column_stats["id"]["column_type"] == "numeric"
        assert column_stats["id"]["data_type"] == "NUMBER(10,0)"
        assert column_stats["price"]["column_type"] == "numeric"

        # Verify string columns
        assert column_stats["name"]["column_type"] == "string"
        assert column_stats["status"]["column_type"] == "string"

        # Verify boolean column
        assert column_stats["is_active"]["column_type"] == "boolean"

        # Verify detailed column statistics structure
        numeric_stats = column_stats["id"]
        column_stats = table_stats["column_statistics"]
        assert len(column_stats) == 5
        assert "id" in column_stats
        assert "name" in column_stats
        assert "price" in column_stats
        assert "status" in column_stats
        assert "is_active" in column_stats

        # Verify column types
        assert column_stats["id"]["column_type"] == "numeric"
        assert column_stats["name"]["column_type"] == "string"
        assert column_stats["price"]["column_type"] == "numeric"
        assert column_stats["status"]["column_type"] == "string"
        assert column_stats["is_active"]["column_type"] == "boolean"

        # Verify numeric statistics structure
        numeric_stats = column_stats["id"]
        assert "min" in numeric_stats
        assert "max" in numeric_stats
        assert "avg" in numeric_stats
        assert "percentile_50" in numeric_stats

        # Verify string statistics structure
        string_stats = column_stats["name"]
        assert "min_length" in string_stats
        assert "max_length" in string_stats
        assert "distinct_count_approx" in string_stats
        assert "top_values" in string_stats

        # Verify boolean statistics structure
        boolean_stats = column_stats["is_active"]
        assert "true_count" in boolean_stats
        assert "false_count" in boolean_stats
        assert "true_percentage" in boolean_stats
        assert "false_percentage" in boolean_stats

    @pytest.mark.asyncio
    async def test_handle_boolean_column_success(self) -> None:
        """Test successful handling of boolean column analysis."""
        table_data = create_test_table_info(
            [
                ("is_active", "BOOLEAN", True, 1),
            ],
        )

        query_result = {
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

        mock_effect = MockAnalyzeTableStatistics(table_data, query_result)

        args = AnalyzeTableStatisticsArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("test_table"),
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        # Should return structured data, not content list
        assert isinstance(result, dict)
        assert "table_statistics" in result

        # Check table_info
        table_stats = result["table_statistics"]
        table_info = table_stats["table_info"]
        assert table_info["total_rows"] == 1000
        assert table_info["analyzed_columns"] == 1

        # Check boolean column statistics
        column_stats = table_stats["column_statistics"]
        boolean_stats = column_stats["is_active"]
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

    @pytest.mark.asyncio
    async def test_basic_numeric_string_analysis(self) -> None:
        """Test basic successful analysis with numeric and string columns only."""
        table_data = create_test_table_info(
            [
                ("id", "NUMBER(10,0)", False, 1),
                ("name", "VARCHAR(50)", True, 2),
            ],
        )

        query_result = {
            "TOTAL_ROWS": 100,
            "NUMERIC_ID_COUNT": 100,
            "NUMERIC_ID_NULL_COUNT": 0,
            "NUMERIC_ID_MIN": 1.0,
            "NUMERIC_ID_MAX": 100.0,
            "NUMERIC_ID_AVG": 50.5,
            "NUMERIC_ID_Q1": 25.0,
            "NUMERIC_ID_MEDIAN": 50.0,
            "NUMERIC_ID_Q3": 75.0,
            "NUMERIC_ID_DISTINCT": 100,
            "STRING_NAME_COUNT": 100,
            "STRING_NAME_NULL_COUNT": 0,
            "STRING_NAME_MIN_LENGTH": 3,
            "STRING_NAME_MAX_LENGTH": 20,
            "STRING_NAME_DISTINCT": 95,
            "STRING_NAME_TOP_VALUES": '[["John", 5], ["Jane", 3]]',
        }

        mock_effect = MockAnalyzeTableStatistics(
            table_data,
            query_result,
        )

        args = AnalyzeTableStatisticsArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("test_table"),
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        # Should return structured data, not content list
        assert isinstance(result, dict)
        assert "table_statistics" in result

        # Check table_info
        table_stats = result["table_statistics"]
        table_info = table_stats["table_info"]
        assert table_info["total_rows"] == 100
        assert table_info["analyzed_columns"] == 2

        # Check column statistics
        column_stats = table_stats["column_statistics"]
        assert "id" in column_stats
        assert "name" in column_stats

        # Verify specific statistics values
        id_stats = column_stats["id"]
        assert id_stats["column_type"] == "numeric"
        assert id_stats["min"] == 1.0
        assert id_stats["max"] == 100.0
        assert id_stats["avg"] == 50.5

        name_stats = column_stats["name"]
        assert name_stats["column_type"] == "string"
        assert name_stats["min_length"] == 3
        assert name_stats["max_length"] == 20
        assert name_stats["distinct_count_approx"] == 95
