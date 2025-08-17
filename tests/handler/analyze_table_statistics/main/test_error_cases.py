"""Error handling tests for analyze_table_statistics handler."""

import pytest

from kernel.table_metadata import DataBase, Schema, Table
from mcp_snowflake.handler.analyze_table_statistics import (
    AnalyzeTableStatisticsArgs,
    handle_analyze_table_statistics,
)
from mcp_snowflake.handler.analyze_table_statistics._types import ColumnDoesNotExist

from ....mock_effect_handler import MockAnalyzeTableStatistics
from .test_fixtures import create_test_table_info


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

        # Should return ColumnDoesNotExist error for no supported columns
        assert isinstance(result, ColumnDoesNotExist)
        assert not result.not_existed_columns  # No missing columns
        # Should have unsupported columns
        unsupported_names = [col.name for col in result.existed_columns]
        assert "metadata" in unsupported_names
        assert "config" in unsupported_names

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

        # Should return ColumnDoesNotExist error for missing columns
        assert isinstance(result, ColumnDoesNotExist)
        assert "nonexistent" in result.not_existed_columns
