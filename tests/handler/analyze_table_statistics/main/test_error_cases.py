"""Error handling tests for analyze_table_statistics handler."""

from typing import TYPE_CHECKING, Any, cast

import pytest

from kernel.table_metadata import TableInfo
from mcp_snowflake.handler.analyze_table_statistics import (
    AnalyzeTableStatisticsArgs,
    handle_analyze_table_statistics,
)

from .test_fixtures import MockEffectHandler, create_test_table_info

if TYPE_CHECKING:
    import mcp.types as types


class TestErrorHandling:
    """Test error handling scenarios."""

    @pytest.mark.asyncio
    async def test_unsupported_column_type(self) -> None:
        """Test handler with all unsupported column types."""
        table_data = create_test_table_info(
            [
                ("metadata", "VARIANT", True, 1),  # All unsupported
                ("config", "OBJECT", True, 2),  # All unsupported
            ],
        )

        mock_effect = MockEffectHandler(table_data=table_data)

        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema="test_schema",
            table="test_table",
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        assert len(result) == 1
        error_content = cast("types.TextContent", result[0])
        assert "Error: No supported columns for statistics" in error_content.text
        assert "metadata(VARIANT)" in error_content.text
        assert "config(OBJECT)" in error_content.text

    @pytest.mark.asyncio
    async def test_missing_columns_error(self) -> None:
        """Test error when requested columns don't exist."""
        table_data = create_test_table_info(
            [
                ("id", "NUMBER(10,0)", False, 1),
            ],
        )

        mock_effect = MockEffectHandler(table_data=table_data)

        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema="test_schema",
            table="test_table",
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
            should_raise=Exception("Database connection failed"),
        )

        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema="test_schema",
            table="test_table",
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        assert len(result) == 1
        error_content = cast("types.TextContent", result[0])
        assert "Error getting table information" in error_content.text
        assert "Database connection failed" in error_content.text

    @pytest.mark.asyncio
    async def test_execute_query_error(self) -> None:
        """Test error handling when query execution fails."""
        table_data = create_test_table_info(
            [
                ("id", "NUMBER(10,0)", False, 1),
            ],
        )

        # Mock that returns table data but fails on query execution
        class MockEffectWithQueryError:
            async def describe_table(
                self,
                database: str,  # noqa: ARG002
                schema: str,  # noqa: ARG002
                table: str,  # noqa: ARG002
            ) -> TableInfo:
                return table_data

            async def analyze_table_statistics(
                self,
                database: str,  # noqa: ARG002
                schema: str,  # noqa: ARG002
                table: str,  # noqa: ARG002
                columns_to_analyze: Any,  # noqa: ARG002
                top_k_limit: int,  # noqa: ARG002
            ) -> dict[str, Any]:
                raise Exception("Query execution failed")

        mock_effect = MockEffectWithQueryError()

        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema="test_schema",
            table="test_table",
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        assert len(result) == 1
        error_content = cast("types.TextContent", result[0])
        assert "Error executing statistics query" in error_content.text
        assert "Query execution failed" in error_content.text

    @pytest.mark.asyncio
    async def test_no_columns_to_analyze_error(self) -> None:
        """Test error when no columns are available for analysis."""
        table_data = create_test_table_info([])  # Empty columns list

        mock_effect = MockEffectHandler(table_data=table_data)

        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema="test_schema",
            table="test_table",
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        assert len(result) == 1
        error_content = cast("types.TextContent", result[0])
        assert "Error: No columns to analyze" in error_content.text
