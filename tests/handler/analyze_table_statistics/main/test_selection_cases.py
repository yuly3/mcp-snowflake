"""Column selection and boundary value tests for analyze_table_statistics handler."""

import json
from typing import TYPE_CHECKING, Any, cast

import pytest

from mcp_snowflake.adapter.analyze_table_statistics_handler import (
    generate_statistics_sql,
)
from mcp_snowflake.handler.analyze_table_statistics import (
    AnalyzeTableStatisticsArgs,
    handle_analyze_table_statistics,
)
from mcp_snowflake.kernel.table_metadata import TableInfo

from .test_fixtures import MockEffectHandler, create_test_table_info

if TYPE_CHECKING:
    import mcp.types as types


class TestColumnSelection:
    """Test column selection and boundary value scenarios."""

    @pytest.mark.asyncio
    async def test_specific_columns_selection(self) -> None:
        """Test analysis with specific columns selected."""
        table_data = create_test_table_info(
            [
                ("id", "NUMBER(10,0)", False, 1),
                ("name", "VARCHAR(50)", True, 2),
                ("date", "DATE", True, 3),
            ]
        )

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
            table_data=table_data,
            query_result=query_result,
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

        # Verify the selected column has complete stats
        id_stats = table_stats["column_statistics"]["id"]
        assert id_stats["column_type"] == "numeric"
        assert id_stats["min"] == 1.0
        assert id_stats["max"] == 100.0

    @pytest.mark.asyncio
    async def test_custom_top_k_limit(self) -> None:
        """Test that custom top_k_limit is used in query generation."""
        table_data = create_test_table_info(
            [
                ("status", "VARCHAR(10)", True, 1),
            ]
        )

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
            ) -> TableInfo:
                return table_data

            async def analyze_table_statistics(
                self,
                database: str,
                schema_name: str,
                table_name: str,
                columns_to_analyze: Any,
                top_k_limit: int,
            ) -> dict[str, Any]:
                """Execute statistics query and track the top_k_limit."""
                # Simulate SQL generation and execution for verification
                query = generate_statistics_sql(
                    database,
                    schema_name,
                    table_name,
                    columns_to_analyze,
                    top_k_limit,
                )
                executed_queries.append(query)

                if not query_result:
                    raise ValueError("No data returned from statistics query")
                return query_result[0]

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
    async def test_multiple_columns_selection(self) -> None:
        """Test analysis with multiple specific columns selected."""
        table_data = create_test_table_info(
            [
                ("id", "NUMBER(10,0)", False, 1),
                ("name", "VARCHAR(50)", True, 2),
                ("price", "NUMBER(10,2)", True, 3),
                ("date", "DATE", True, 4),
                ("status", "VARCHAR(20)", True, 5),
            ]
        )

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
                "NUMERIC_PRICE_COUNT": 100,
                "NUMERIC_PRICE_NULL_COUNT": 10,
                "NUMERIC_PRICE_MIN": 10.5,
                "NUMERIC_PRICE_MAX": 999.99,
                "NUMERIC_PRICE_AVG": 505.25,
                "NUMERIC_PRICE_Q1": 250.0,
                "NUMERIC_PRICE_MEDIAN": 500.0,
                "NUMERIC_PRICE_Q3": 750.0,
                "NUMERIC_PRICE_DISTINCT": 90,
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
            columns=["id", "price"],  # Only analyze numeric columns
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        # Parse JSON response
        json_content = cast("types.TextContent", result[1])
        json_response = json.loads(json_content.text)
        table_stats = json_response["table_statistics"]

        # Should only have 2 analyzed columns
        assert table_stats["table_info"]["analyzed_columns"] == 2
        assert "id" in table_stats["column_statistics"]
        assert "price" in table_stats["column_statistics"]

        # Should not have unselected columns
        assert "name" not in table_stats["column_statistics"]
        assert "date" not in table_stats["column_statistics"]
        assert "status" not in table_stats["column_statistics"]

        # Verify both selected columns have complete stats
        id_stats = table_stats["column_statistics"]["id"]
        price_stats = table_stats["column_statistics"]["price"]

        assert id_stats["column_type"] == "numeric"
        assert price_stats["column_type"] == "numeric"
        assert price_stats["null_count"] == 10  # Different from id

    @pytest.mark.asyncio
    async def test_single_column_table_analysis(self) -> None:
        """Test analysis of a table with only one column."""
        table_data = create_test_table_info(
            [
                ("single_col", "VARCHAR(100)", True, 1),
            ]
        )

        query_result = [
            {
                "TOTAL_ROWS": 50,
                "STRING_SINGLE_COL_COUNT": 45,
                "STRING_SINGLE_COL_NULL_COUNT": 5,
                "STRING_SINGLE_COL_MIN_LENGTH": 1,
                "STRING_SINGLE_COL_MAX_LENGTH": 50,
                "STRING_SINGLE_COL_DISTINCT": 40,
                "STRING_SINGLE_COL_TOP_VALUES": '[["value1", 3], ["value2", 2]]',
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

        # Check summary shows single column
        summary_content = cast("types.TextContent", result[0])
        assert "50 total rows" in summary_content.text
        assert "- String: 1 columns" in summary_content.text

        # Parse JSON and verify single column
        json_content = cast("types.TextContent", result[1])
        json_response = json.loads(json_content.text)
        table_stats = json_response["table_statistics"]

        assert table_stats["table_info"]["analyzed_columns"] == 1
        assert "single_col" in table_stats["column_statistics"]

        col_stats = table_stats["column_statistics"]["single_col"]
        assert col_stats["column_type"] == "string"
        assert col_stats["count"] == 45
        assert col_stats["null_count"] == 5
