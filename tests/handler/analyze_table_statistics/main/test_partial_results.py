"""Partial results tests for analyze_table_statistics handler with unsupported columns."""

import pytest

from kernel.table_metadata import DataBase, Schema, Table
from mcp_snowflake.handler.analyze_table_statistics import (
    AnalyzeTableStatisticsArgs,
    NoSupportedColumns,
    handle_analyze_table_statistics,
)

from ....mock_effect_handler import MockAnalyzeTableStatistics
from .test_fixtures import create_mixed_analysis_result, create_test_table_info


class TestPartialResults:
    """Test partial results when some columns are unsupported."""

    @pytest.mark.asyncio
    async def test_mixed_supported_and_unsupported_columns(self) -> None:
        """Test when some columns are supported and some are unsupported."""
        # Create table with mixed supported and unsupported columns
        table_data = create_test_table_info(
            [
                ("id", "NUMBER(10,0)", False, 1),
                ("name", "VARCHAR(50)", True, 2),
                ("metadata", "VARIANT", True, 3),
                ("config", "OBJECT", True, 4),
            ],
        )

        # Query result should only include supported columns
        query_result = create_mixed_analysis_result(
            numeric_columns=["id"],
            string_columns=["name"],
            boolean_columns=[],
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

        # Should return structured data
        assert isinstance(result, dict)
        assert "table_statistics" in result

        table_stats = result["table_statistics"]

        # Check that only supported columns are in column_statistics
        column_stats = table_stats["column_statistics"]
        assert len(column_stats) == 2
        assert "id" in column_stats
        assert "name" in column_stats
        assert "metadata" not in column_stats
        assert "config" not in column_stats

        # Check unsupported_columns field
        assert "unsupported_columns" in table_stats
        unsupported = table_stats["unsupported_columns"]
        assert len(unsupported) == 2

        # Check unsupported column details
        unsupported_names = {col["name"] for col in unsupported}
        assert unsupported_names == {"metadata", "config"}

        for col in unsupported:
            assert "name" in col
            assert "data_type" in col
            assert "reason" not in col  # Should not include reason
            assert len(col) == 2  # Only name and data_type

        # Verify data types
        metadata_col = next(col for col in unsupported if col["name"] == "metadata")
        config_col = next(col for col in unsupported if col["name"] == "config")
        assert metadata_col["data_type"] == "VARIANT"
        assert config_col["data_type"] == "OBJECT"

    @pytest.mark.asyncio
    async def test_all_unsupported_columns_error(self) -> None:
        """Test error when all columns are unsupported."""
        # Create table with only unsupported columns
        table_data = create_test_table_info(
            [
                ("metadata", "VARIANT", True, 1),
                ("config", "OBJECT", True, 2),
                ("data", "ARRAY", True, 3),
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
        assert len(result.unsupported_columns) == 3
        unsupported_names = [col.name for col in result.unsupported_columns]
        assert "metadata" in unsupported_names
        assert "config" in unsupported_names
        assert "data" in unsupported_names

    @pytest.mark.asyncio
    async def test_requested_columns_with_unsupported(self) -> None:
        """Test with specific requested columns where some are unsupported."""
        # Create table with mixed columns
        table_data = create_test_table_info(
            [
                ("id", "NUMBER(10,0)", False, 1),
                ("metadata", "VARIANT", True, 2),
                ("name", "VARCHAR(50)", True, 3),
            ],
        )

        # Query result for only the supported requested column
        query_result = create_mixed_analysis_result(
            numeric_columns=["id"],
            string_columns=[],
            boolean_columns=[],
            total_rows=500,
        )

        mock_effect = MockAnalyzeTableStatistics(
            table_info=table_data,
            statistics_result=query_result,
        )

        args = AnalyzeTableStatisticsArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("test_table"),
            columns=["id", "metadata"],  # Request both supported and unsupported
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        # Should return structured data
        assert isinstance(result, dict)
        assert "table_statistics" in result

        table_stats = result["table_statistics"]

        # Only id should be analyzed
        column_stats = table_stats["column_statistics"]
        assert len(column_stats) == 1
        assert "id" in column_stats

        # metadata should be in unsupported_columns
        unsupported = table_stats.get("unsupported_columns", [])
        assert len(unsupported) == 1
        assert unsupported[0]["name"] == "metadata"
        assert unsupported[0]["data_type"] == "VARIANT"

    @pytest.mark.asyncio
    async def test_all_supported_columns_no_unsupported_field(self) -> None:
        """Test that unsupported_columns field is omitted when all columns are supported."""
        # Create table with only supported columns
        table_data = create_test_table_info(
            [
                ("id", "NUMBER(10,0)", False, 1),
                ("name", "VARCHAR(50)", True, 2),
                ("active", "BOOLEAN", True, 3),
            ],
        )

        query_result = create_mixed_analysis_result(
            numeric_columns=["id"],
            string_columns=["name"],
            boolean_columns=["active"],
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

        # Should return structured data (no unsupported columns)
        assert isinstance(result, dict)
        assert "table_statistics" in result

        table_stats = result["table_statistics"]
        assert "unsupported_columns" not in table_stats

        # All columns should be analyzed
        column_stats = table_stats["column_statistics"]
        assert len(column_stats) == 3
        assert "id" in column_stats
        assert "name" in column_stats
        assert "active" in column_stats
