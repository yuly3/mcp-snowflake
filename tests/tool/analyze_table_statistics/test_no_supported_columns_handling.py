"""Test for tool.perform with NoSupportedColumns handling."""

import pytest

from cattrs_converter import JsonImmutableConverter
from kernel.table_metadata import DataBase, Schema, TableColumn, TableInfo
from mcp_snowflake.tool.analyze_table_statistics import AnalyzeTableStatisticsTool

from ...mock_effect_handler import MockAnalyzeTableStatistics


class TestAnalyzeTableStatisticsToolNoSupportedColumns:
    """Test tool behavior with NoSupportedColumns."""

    @pytest.mark.asyncio
    async def test_perform_with_no_supported_columns_message(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test that tool correctly formats NoSupportedColumns as error message."""
        # Create table info with only unsupported columns
        table_info = TableInfo(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            name="test_table",
            column_count=2,
            columns=[
                TableColumn(
                    name="JSON_DATA",
                    data_type="VARIANT",
                    nullable=True,
                    default_value=None,
                    comment="JSON data",
                    ordinal_position=1,
                ),
                TableColumn(
                    name="BINARY_DATA",
                    data_type="BINARY",
                    nullable=True,
                    default_value=None,
                    comment="Binary data",
                    ordinal_position=2,
                ),
            ],
        )

        # Create tool instance with mock effect
        mock_effect = MockAnalyzeTableStatistics(table_info=table_info)
        tool = AnalyzeTableStatisticsTool(json_converter, mock_effect)

        # Perform analysis
        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
        }
        result = await tool.perform(arguments)

        # Should return single TextContent with no supported columns message
        assert len(result) == 1
        text_content = result[0]
        assert text_content.type == "text"
        text = text_content.text

        # Should contain the expected error message format
        assert "Error: No supported columns for statistics" in text
        assert "Unsupported columns:" in text
        assert "JSON_DATA(VARIANT)" in text
        assert "BINARY_DATA(BINARY)" in text
