"""Test for AnalyzeTableStatisticsTool - Column Error Cases."""

import mcp.types as types
import pytest

from kernel.table_metadata import DataBase, Schema, TableColumn, TableInfo
from mcp_snowflake.tool.analyze_table_statistics import AnalyzeTableStatisticsTool

from ...mock_effect_handler import MockAnalyzeTableStatistics


class TestAnalyzeTableStatisticsToolColumnErrors:
    """Test AnalyzeTableStatisticsTool column error cases."""

    @pytest.mark.asyncio
    async def test_perform_with_nonexistent_columns(self) -> None:
        """Test with columns that don't exist in the table."""
        # Set up mock table info with limited columns
        table_info = TableInfo(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            name="test_table",
            column_count=2,
            columns=[
                TableColumn(
                    name="ID",
                    data_type="NUMBER(10,0)",
                    nullable=False,
                    default_value=None,
                    comment="Primary key",
                    ordinal_position=1,
                ),
                TableColumn(
                    name="NAME",
                    data_type="VARCHAR(100)",
                    nullable=True,
                    default_value=None,
                    comment="User name",
                    ordinal_position=2,
                ),
            ],
        )

        mock_effect = MockAnalyzeTableStatistics(table_info=table_info)
        tool = AnalyzeTableStatisticsTool(mock_effect)

        # Request analysis for columns that don't exist
        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "columns": [
                "ID",
                "NONEXISTENT",
                "ALSO_MISSING",
            ],  # NONEXISTENT and ALSO_MISSING don't exist
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Columns not found in table:" in result[0].text
        assert "NONEXISTENT" in result[0].text
        assert "ALSO_MISSING" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_no_supported_columns(self) -> None:
        """Test with table that has no supported columns for statistics."""
        # Set up mock table info with only unsupported column types
        table_info = TableInfo(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            name="test_table",
            column_count=2,
            columns=[
                TableColumn(
                    name="JSON_DATA",
                    data_type="VARIANT",  # Unsupported type
                    nullable=True,
                    default_value=None,
                    comment="JSON data",
                    ordinal_position=1,
                ),
                TableColumn(
                    name="BINARY_DATA",
                    data_type="BINARY",  # Unsupported type
                    nullable=True,
                    default_value=None,
                    comment="Binary data",
                    ordinal_position=2,
                ),
            ],
        )

        mock_effect = MockAnalyzeTableStatistics(table_info=table_info)
        tool = AnalyzeTableStatisticsTool(mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: No supported columns for statistics" in result[0].text
        assert "Unsupported columns:" in result[0].text
        assert "JSON_DATA(VARIANT)" in result[0].text
        assert "BINARY_DATA(BINARY)" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_mixed_existing_nonexisting_columns(self) -> None:
        """Test with a mix of existing and non-existing columns."""
        # Set up mock table info
        table_info = TableInfo(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            name="test_table",
            column_count=2,
            columns=[
                TableColumn(
                    name="ID",
                    data_type="NUMBER(10,0)",
                    nullable=False,
                    default_value=None,
                    comment="Primary key",
                    ordinal_position=1,
                ),
                TableColumn(
                    name="NAME",
                    data_type="VARCHAR(100)",
                    nullable=True,
                    default_value=None,
                    comment="User name",
                    ordinal_position=2,
                ),
            ],
        )

        mock_effect = MockAnalyzeTableStatistics(table_info=table_info)
        tool = AnalyzeTableStatisticsTool(mock_effect)

        # Request analysis with mix of existing and non-existing columns
        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "columns": [
                "ID",
                "NONEXISTENT",
                "NAME",
                "MISSING",
            ],  # ID and NAME exist, others don't
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Columns not found in table:" in result[0].text
        # Should list the missing columns
        assert "NONEXISTENT" in result[0].text
        assert "MISSING" in result[0].text
        # Should NOT list the existing columns in error
        assert "ID" not in result[0].text or "not found" not in result[0].text
        assert "NAME" not in result[0].text or "not found" not in result[0].text
