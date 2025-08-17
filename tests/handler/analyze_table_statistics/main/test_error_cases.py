"""Error handling tests for analyze_table_statistics handler."""

from typing import TYPE_CHECKING, cast

import pytest

from kernel.table_metadata import DataBase, Schema, Table
from mcp_snowflake.handler.analyze_table_statistics import (
    AnalyzeTableStatisticsArgs,
    handle_analyze_table_statistics,
)

from ....mock_effect_handler import MockAnalyzeTableStatistics
from .test_fixtures import create_test_table_info

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

        mock_effect = MockAnalyzeTableStatistics(table_info=table_data)

        args = AnalyzeTableStatisticsArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("test_table"),
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

        mock_effect = MockAnalyzeTableStatistics(table_info=table_data)

        args = AnalyzeTableStatisticsArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("test_table"),
            columns=["id", "nonexistent"],  # nonexistent column
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        assert len(result) == 1
        error_content = cast("types.TextContent", result[0])
        assert "Error: Columns not found in table: nonexistent" in error_content.text
