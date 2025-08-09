"""Tests for main handler functionality."""

import json
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

import pytest

from mcp_snowflake.handler.analyze_table_statistics import (
    AnalyzeTableStatisticsArgs,
    handle_analyze_table_statistics,
)

if TYPE_CHECKING:
    import mcp.types as types


class MockEffectHandler:
    """Mock implementation of EffectAnalyzeTableStatistics protocol."""

    def __init__(
        self,
        table_data: dict[str, Any] | None = None,
        query_result: list[dict[str, Any]] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.table_data = table_data or {}
        self.query_result = query_result or []
        self.should_raise = should_raise

    async def describe_table(
        self,
        database: str,  # noqa: ARG002
        schema: str,  # noqa: ARG002
        table_name: str,  # noqa: ARG002
    ) -> dict[str, Any]:
        if self.should_raise:
            raise self.should_raise
        return self.table_data

    async def execute_query(
        self,
        query: str,  # noqa: ARG002
        query_timeout: timedelta | None = None,  # noqa: ARG002
    ) -> list[dict[str, Any]]:
        if self.should_raise:
            raise self.should_raise
        return self.query_result


class TestHandleAnalyzeTableStatistics:
    """Test handle_analyze_table_statistics function."""

    @pytest.mark.asyncio
    async def test_successful_analysis(self) -> None:
        """Test successful table statistics analysis."""
        table_data = {
            "database": "test_db",
            "schema_name": "test_schema",
            "name": "test_table",
            "column_count": 2,
            "columns": [
                {"name": "id", "data_type": "NUMBER(10,0)"},
                {"name": "name", "data_type": "VARCHAR(50)"},
            ],
        }

        query_result = [
            {
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
        ]

        mock_effect = MockEffectHandler(
            table_data=table_data,
            query_result=query_result,
        )

        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        assert len(result) == 2

        # Check summary text
        summary_content = cast("types.TextContent", result[0])
        assert "Table Statistics Analysis" in summary_content.text
        assert "100 total rows" in summary_content.text

        # Parse and check JSON response
        json_content = cast("types.TextContent", result[1])
        json_response = json.loads(json_content.text)
        table_stats = json_response["table_statistics"]

        assert table_stats["table_info"]["total_rows"] == 100
        assert table_stats["table_info"]["analyzed_columns"] == 2
        assert "id" in table_stats["column_statistics"]
        assert "name" in table_stats["column_statistics"]

    @pytest.mark.asyncio
    async def test_unsupported_column_type(self) -> None:
        """Test handler with unsupported column types."""
        table_data = {
            "database": "test_db",
            "schema_name": "test_schema",
            "name": "test_table",
            "column_count": 2,
            "columns": [
                {"name": "id", "data_type": "NUMBER(10,0)"},
                {"name": "metadata", "data_type": "VARIANT"},  # Unsupported type
            ],
        }

        mock_effect = MockEffectHandler(table_data=table_data)

        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        assert len(result) == 1
        error_content = cast("types.TextContent", result[0])
        assert "Error: Unsupported column types found" in error_content.text
        assert "metadata (VARIANT)" in error_content.text

    @pytest.mark.asyncio
    async def test_specific_columns_selection(self) -> None:
        """Test analysis with specific columns selected."""
        table_data = {
            "database": "test_db",
            "schema_name": "test_schema",
            "name": "test_table",
            "column_count": 3,
            "columns": [
                {"name": "id", "data_type": "NUMBER(10,0)"},
                {"name": "name", "data_type": "VARCHAR(50)"},
                {"name": "date", "data_type": "DATE"},
            ],
        }

        query_result = [
            {
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
            }
        ]

        mock_effect = MockEffectHandler(
            table_data=table_data, query_result=query_result
        )

        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
            columns=["id"],  # Only analyze ID column
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        # Parse JSON response
        json_content = cast("types.TextContent", result[1])
        json_response = json.loads(json_content.text)
        table_stats = json_response["table_statistics"]

        # Should only have 1 analyzed column
        assert table_stats["table_info"]["analyzed_columns"] == 1
        assert "id" in table_stats["column_statistics"]
        assert "name" not in table_stats["column_statistics"]
        assert "date" not in table_stats["column_statistics"]

    @pytest.mark.asyncio
    async def test_missing_columns_error(self) -> None:
        """Test error when requested columns don't exist."""
        table_data = {
            "database": "test_db",
            "schema_name": "test_schema",
            "name": "test_table",
            "column_count": 1,
            "columns": [
                {"name": "id", "data_type": "NUMBER(10,0)"},
            ],
        }

        mock_effect = MockEffectHandler(table_data=table_data)

        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
            columns=["id", "nonexistent"],  # nonexistent column
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        assert len(result) == 1
        error_content = cast("types.TextContent", result[0])
        assert "Error: Columns not found in table: nonexistent" in error_content.text

    @pytest.mark.asyncio
    async def test_describe_table_error(self) -> None:
        """Test error handling when describe_table fails."""
        mock_effect = MockEffectHandler(
            should_raise=Exception("Database connection failed")
        )

        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        assert len(result) == 1
        error_content = cast("types.TextContent", result[0])
        assert "Error getting table information" in error_content.text

    @pytest.mark.asyncio
    async def test_execute_query_error(self) -> None:
        """Test error handling when query execution fails."""
        table_data = {
            "database": "test_db",
            "schema_name": "test_schema",
            "name": "test_table",
            "column_count": 1,
            "columns": [
                {"name": "id", "data_type": "NUMBER(10,0)"},
            ],
        }

        # Mock that returns table data but fails on query execution
        class MockEffectWithQueryError:
            async def describe_table(
                self,
                database: str,  # noqa: ARG002
                schema: str,  # noqa: ARG002
                table_name: str,  # noqa: ARG002
            ) -> dict[str, Any]:
                return table_data

            async def execute_query(
                self,
                query: str,  # noqa: ARG002
                query_timeout: timedelta | None = None,  # noqa: ARG002
            ) -> list[dict[str, Any]]:
                raise Exception("Query execution failed")

        mock_effect = MockEffectWithQueryError()

        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        assert len(result) == 1
        error_content = cast("types.TextContent", result[0])
        assert "Error executing statistics query" in error_content.text

    @pytest.mark.asyncio
    async def test_empty_query_result(self) -> None:
        """Test error handling when query returns no results."""
        table_data = {
            "database": "test_db",
            "schema_name": "test_schema",
            "name": "test_table",
            "column_count": 1,
            "columns": [
                {"name": "id", "data_type": "NUMBER(10,0)"},
            ],
        }

        # Return empty query result
        mock_effect = MockEffectHandler(table_data=table_data, query_result=[])

        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        assert len(result) == 1
        error_content = cast("types.TextContent", result[0])
        assert "Error executing statistics query" in error_content.text

    @pytest.mark.asyncio
    async def test_custom_top_k_limit(self) -> None:
        """Test that custom top_k_limit is used in query generation."""
        table_data = {
            "database": "test_db",
            "schema_name": "test_schema",
            "name": "test_table",
            "column_count": 1,
            "columns": [
                {"name": "status", "data_type": "VARCHAR(10)"},
            ],
        }

        query_result = [
            {
                "TOTAL_ROWS": 100,
                "STRING_STATUS_COUNT": 100,
                "STRING_STATUS_NULL_COUNT": 0,
                "STRING_STATUS_MIN_LENGTH": 6,
                "STRING_STATUS_MAX_LENGTH": 8,
                "STRING_STATUS_DISTINCT": 3,
                "STRING_STATUS_TOP_VALUES": '[["active", 50], ["pending", 30], ["inactive", 20]]',
            }
        ]

        # Track what query was executed
        executed_queries: list[str] = []

        class MockEffectWithQueryTracking:
            async def describe_table(
                self,
                database: str,  # noqa: ARG002
                schema: str,  # noqa: ARG002
                table_name: str,  # noqa: ARG002
            ) -> dict[str, Any]:
                return table_data

            async def execute_query(
                self,
                query: str,
                query_timeout: timedelta | None = None,  # noqa: ARG002
            ) -> list[dict[str, Any]]:
                executed_queries.append(query)
                return query_result

        mock_effect = MockEffectWithQueryTracking()

        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
            top_k_limit=25,  # Custom limit
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        # Check that the query contains the custom top_k_limit
        assert len(executed_queries) == 1
        query = executed_queries[0]
        assert 'APPROX_TOP_K("status", 25)' in query

        # Verify result is successful
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_handle_boolean_column_success(self) -> None:
        """Test successful handling of boolean column analysis."""
        table_data = {
            "columns": [
                {"name": "is_active", "data_type": "BOOLEAN"},
            ]
        }

        query_result = [
            {
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
        ]

        mock_effect = MockEffectHandler(table_data, query_result)

        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        # Verify response structure
        assert len(result) == 2
        assert result[0].type == "text"
        assert result[1].type == "text"

        # Check summary text includes boolean columns
        summary_text = result[0].text
        assert "- Boolean: 1 columns" in summary_text

        # Parse JSON response and verify structure
        json_response = json.loads(result[1].text)
        table_stats = json_response["table_statistics"]

        # Check table info
        assert table_stats["table_info"]["total_rows"] == 1000
        assert table_stats["table_info"]["analyzed_columns"] == 1

        # Check boolean column statistics
        boolean_stats = table_stats["column_statistics"]["is_active"]
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
    async def test_handle_mixed_columns_including_boolean(self) -> None:
        """Test handling mixed column types including boolean."""
        table_data = {
            "columns": [
                {"name": "price", "data_type": "NUMBER(10,2)"},
                {"name": "status", "data_type": "VARCHAR(10)"},
                {"name": "is_active", "data_type": "BOOLEAN"},
            ]
        }

        query_result = [
            {
                "TOTAL_ROWS": 1000,
                # Numeric stats
                "NUMERIC_PRICE_COUNT": 1000,
                "NUMERIC_PRICE_NULL_COUNT": 0,
                "NUMERIC_PRICE_MIN": 10.5,
                "NUMERIC_PRICE_MAX": 999.99,
                "NUMERIC_PRICE_AVG": 505.25,
                "NUMERIC_PRICE_Q1": 250.0,
                "NUMERIC_PRICE_MEDIAN": 500.0,
                "NUMERIC_PRICE_Q3": 750.0,
                "NUMERIC_PRICE_DISTINCT": 950,
                # String stats
                "STRING_STATUS_COUNT": 1000,
                "STRING_STATUS_NULL_COUNT": 0,
                "STRING_STATUS_MIN_LENGTH": 6,
                "STRING_STATUS_MAX_LENGTH": 8,
                "STRING_STATUS_DISTINCT": 3,
                "STRING_STATUS_TOP_VALUES": '[["active", 400], ["inactive", 350], ["pending", 250]]',
                # Boolean stats
                "BOOLEAN_IS_ACTIVE_COUNT": 950,
                "BOOLEAN_IS_ACTIVE_NULL_COUNT": 50,
                "BOOLEAN_IS_ACTIVE_TRUE_COUNT": 720,
                "BOOLEAN_IS_ACTIVE_FALSE_COUNT": 230,
                "BOOLEAN_IS_ACTIVE_TRUE_PERCENTAGE": 75.79,
                "BOOLEAN_IS_ACTIVE_FALSE_PERCENTAGE": 24.21,
                "BOOLEAN_IS_ACTIVE_TRUE_PERCENTAGE_WITH_NULLS": 72.0,
                "BOOLEAN_IS_ACTIVE_FALSE_PERCENTAGE_WITH_NULLS": 23.0,
            }
        ]

        mock_effect = MockEffectHandler(table_data, query_result)

        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        # Check summary includes all column types
        summary_text = cast("types.TextContent", result[0]).text
        assert "- Numeric: 1 columns" in summary_text
        assert "- String: 1 columns" in summary_text
        assert "- Boolean: 1 columns" in summary_text

        # Parse and verify JSON response
        json_response = json.loads(cast("types.TextContent", result[1]).text)
        column_stats = json_response["table_statistics"]["column_statistics"]

        assert len(column_stats) == 3
        assert "price" in column_stats
        assert "status" in column_stats
        assert "is_active" in column_stats

        # Verify each column type
        assert column_stats["price"]["column_type"] == "numeric"
        assert column_stats["status"]["column_type"] == "string"
        assert column_stats["is_active"]["column_type"] == "boolean"
