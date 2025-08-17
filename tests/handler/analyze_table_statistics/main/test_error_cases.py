"""Error handling tests for analyze_table_statistics handler."""

import pytest

from kernel.table_metadata import DataBase, Schema, Table
from mcp_snowflake.handler.analyze_table_statistics import (
    AnalyzeTableStatisticsArgs,
    handle_analyze_table_statistics,
)
from mcp_snowflake.handler.analyze_table_statistics._types import (
    ColumnDoesNotExist,
    NoSupportedColumns,
)

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

        # Should return NoSupportedColumns for no supported columns
        assert isinstance(result, NoSupportedColumns)

        # Should contain all unsupported columns
        assert len(result.unsupported_columns) == 2
        unsupported_names = [col.name for col in result.unsupported_columns]
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

    @pytest.mark.asyncio
    async def test_no_supported_columns_returns_no_supported_columns(self) -> None:
        """Test that handler returns NoSupportedColumns when no columns are supported."""
        # Table with only unsupported columns
        table_data = create_test_table_info(
            [
                ("JSON_DATA", "VARIANT", True, 1),
                ("BINARY_DATA", "BINARY", True, 2),
            ],
        )

        mock_effect = MockAnalyzeTableStatistics(table_info=table_data)
        args = AnalyzeTableStatisticsArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("test_table"),
            columns=[],  # Analyze all columns
            top_k_limit=10,
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        # Should return NoSupportedColumns, not ColumnDoesNotExist
        assert isinstance(result, NoSupportedColumns)
        assert len(result.unsupported_columns) == 2
        assert result.unsupported_columns[0].name == "JSON_DATA"
        assert result.unsupported_columns[1].name == "BINARY_DATA"
