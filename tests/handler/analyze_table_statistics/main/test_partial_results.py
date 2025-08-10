"""Partial results tests for analyze_table_statistics handler with unsupported columns."""

import json
from typing import TYPE_CHECKING, cast

import pytest

if TYPE_CHECKING:
    from mcp.types import TextContent

from mcp_snowflake.handler.analyze_table_statistics import (
    AnalyzeTableStatisticsArgs,
    handle_analyze_table_statistics,
)

from .test_fixtures import (
    MockEffectHandler,
    create_mixed_analysis_result,
    create_test_table_info,
)


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
            ]
        )

        # Query result should only include supported columns
        query_result = [
            create_mixed_analysis_result(
                numeric_columns=["id"],
                string_columns=["name"],
                boolean_columns=[],
                total_rows=1000,
            )
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

        # Should return 2 content items (text + JSON)
        assert len(result) == 2

        text_content = cast("TextContent", result[0])
        json_content = cast("TextContent", result[1])

        # Check text content includes unsupported note
        assert (
            "Note: Some columns were not analyzed due to unsupported data types"
            in text_content.text
        )
        assert "2 column(s) skipped" in text_content.text

        # Parse JSON response
        parsed = json.loads(json_content.text)
        table_stats = parsed["table_statistics"]

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
            ]
        )

        mock_effect = MockEffectHandler(
            table_data=table_data,
            query_result=[],  # No query should be executed
        )

        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        # Should return single TextContent with error
        assert len(result) == 1
        error_content = cast("TextContent", result[0])

        # Check error message format
        error_text = error_content.text
        assert "Error: No supported columns for statistics" in error_text
        assert "Unsupported columns:" in error_text

        # Check that columns are listed in "name(type)" format
        assert "metadata(VARIANT)" in error_text
        assert "config(OBJECT)" in error_text
        assert "data(ARRAY)" in error_text

    @pytest.mark.asyncio
    async def test_requested_columns_with_unsupported(self) -> None:
        """Test with specific requested columns where some are unsupported."""
        # Create table with mixed columns
        table_data = create_test_table_info(
            [
                ("id", "NUMBER(10,0)", False, 1),
                ("metadata", "VARIANT", True, 2),
                ("name", "VARCHAR(50)", True, 3),
            ]
        )

        # Query result for only the supported requested column
        query_result = [
            create_mixed_analysis_result(
                numeric_columns=["id"],
                string_columns=[],
                boolean_columns=[],
                total_rows=500,
            )
        ]

        mock_effect = MockEffectHandler(
            table_data=table_data,
            query_result=query_result,
        )

        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
            columns=["id", "metadata"],  # Request both supported and unsupported
        )

        result = await handle_analyze_table_statistics(args, mock_effect)

        # Should return 2 content items (text + JSON)
        assert len(result) == 2

        text_content = cast("TextContent", result[0])
        json_content = cast("TextContent", result[1])

        # Check text includes unsupported note
        assert "1 column(s) skipped" in text_content.text

        # Parse JSON and check structure
        parsed = json.loads(json_content.text)
        table_stats = parsed["table_statistics"]

        # Only id should be analyzed
        column_stats = table_stats["column_statistics"]
        assert len(column_stats) == 1
        assert "id" in column_stats

        # metadata should be in unsupported_columns
        unsupported = table_stats["unsupported_columns"]
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
            ]
        )

        query_result = [
            create_mixed_analysis_result(
                numeric_columns=["id"],
                string_columns=["name"],
                boolean_columns=["active"],
                total_rows=1000,
            )
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
        text_content = cast("TextContent", result[0])
        json_content = cast("TextContent", result[1])

        # Text should not mention unsupported columns
        assert "Note: Some columns were not analyzed" not in text_content.text
        assert "skipped" not in text_content.text

        # JSON should not have unsupported_columns field
        parsed = json.loads(json_content.text)
        table_stats = parsed["table_statistics"]
        assert "unsupported_columns" not in table_stats

        # All columns should be analyzed
        column_stats = table_stats["column_statistics"]
        assert len(column_stats) == 3
        assert "id" in column_stats
        assert "name" in column_stats
        assert "active" in column_stats
