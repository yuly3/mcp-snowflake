"""Test for AnalyzeTableStatisticsTool - Success Cases."""

import mcp.types as types
import pytest

from kernel.table_metadata import DataBase, Schema, TableColumn, TableInfo
from mcp_snowflake.tool.analyze_table_statistics import AnalyzeTableStatisticsTool

from ...mock_effect_handler import MockAnalyzeTableStatistics


class TestAnalyzeTableStatisticsToolSuccess:
    """Test AnalyzeTableStatisticsTool success cases."""

    def test_name_property(self) -> None:
        """Test name property."""
        mock_effect = MockAnalyzeTableStatistics()
        tool = AnalyzeTableStatisticsTool(mock_effect)
        assert tool.name == "analyze_table_statistics"

    def test_definition_property(self) -> None:
        """Test definition property."""
        mock_effect = MockAnalyzeTableStatistics()
        tool = AnalyzeTableStatisticsTool(mock_effect)
        definition = tool.definition

        assert definition.name == "analyze_table_statistics"
        assert definition.description is not None
        assert "analyze" in definition.description.lower()
        assert "statistics" in definition.description.lower()
        assert "snowflake" in definition.description.lower()
        assert definition.inputSchema is not None

        # Check required fields
        input_schema = definition.inputSchema
        assert input_schema["type"] == "object"
        assert set(input_schema["required"]) == {"database", "schema", "table"}

        # Check properties
        properties = input_schema["properties"]
        assert "database" in properties
        assert "schema" in properties
        assert "table" in properties
        assert "columns" in properties
        assert "top_k_limit" in properties
        assert "include_null_empty_profile" in properties
        assert "include_blank_string_profile" in properties

        # Check optional properties defaults
        assert properties["columns"]["default"] == []
        assert properties["top_k_limit"]["default"] == 10
        assert properties["top_k_limit"]["minimum"] == 1
        assert properties["top_k_limit"]["maximum"] == 100
        assert properties["include_null_empty_profile"]["default"] is True
        assert properties["include_blank_string_profile"]["default"] is False

    @pytest.mark.asyncio
    async def test_perform_success_basic(self) -> None:
        """Test basic successful statistics analysis returns compact format."""
        table_info = TableInfo(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            name="test_table",
            column_count=4,
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
                TableColumn(
                    name="CREATED_DATE",
                    data_type="DATE",
                    nullable=True,
                    default_value=None,
                    comment="Creation date",
                    ordinal_position=3,
                ),
                TableColumn(
                    name="IS_ACTIVE",
                    data_type="BOOLEAN",
                    nullable=True,
                    default_value=None,
                    comment="Active status",
                    ordinal_position=4,
                ),
            ],
        )

        statistics_result = {
            "TOTAL_ROWS": 5000,
            "NUMERIC_ID_COUNT": 5000,
            "NUMERIC_ID_NULL_COUNT": 0,
            "NUMERIC_ID_MIN": 1,
            "NUMERIC_ID_MAX": 5000,
            "NUMERIC_ID_AVG": 2500.5,
            "NUMERIC_ID_Q1": 1250.25,
            "NUMERIC_ID_MEDIAN": 2500.5,
            "NUMERIC_ID_Q3": 3750.75,
            "NUMERIC_ID_DISTINCT": 5000,
            "STRING_NAME_COUNT": 4900,
            "STRING_NAME_NULL_COUNT": 100,
            "STRING_NAME_MIN_LENGTH": 3,
            "STRING_NAME_MAX_LENGTH": 20,
            "STRING_NAME_DISTINCT": 4500,
            "STRING_NAME_TOP_VALUES": '[["John", 50], ["Jane", 45]]',
            "DATE_CREATED_DATE_COUNT": 4950,
            "DATE_CREATED_DATE_NULL_COUNT": 50,
            "DATE_CREATED_DATE_MIN": "2023-01-01",
            "DATE_CREATED_DATE_MAX": "2024-12-31",
            "DATE_CREATED_DATE_DISTINCT": 365,
            "DATE_CREATED_DATE_RANGE_DAYS": 730,
            "BOOLEAN_IS_ACTIVE_COUNT": 4950,
            "BOOLEAN_IS_ACTIVE_NULL_COUNT": 50,
            "BOOLEAN_IS_ACTIVE_TRUE_COUNT": 3000,
            "BOOLEAN_IS_ACTIVE_FALSE_COUNT": 1950,
            "BOOLEAN_IS_ACTIVE_TRUE_PERCENTAGE": 60.61,
            "BOOLEAN_IS_ACTIVE_FALSE_PERCENTAGE": 39.39,
            "BOOLEAN_IS_ACTIVE_TRUE_PERCENTAGE_WITH_NULLS": 60.0,
            "BOOLEAN_IS_ACTIVE_FALSE_PERCENTAGE_WITH_NULLS": 39.0,
            "BOOLEAN_IS_ACTIVE_DISTINCT": 2,
        }

        mock_effect = MockAnalyzeTableStatistics(
            table_info=table_info,
            statistics_result=statistics_result,
        )
        tool = AnalyzeTableStatisticsTool(mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)

        text = result[0].text
        assert text.startswith("database: test_db")
        assert "schema: test_schema" in text
        assert "table: test_table" in text
        assert "total_rows: 5000" in text
        assert "analyzed_columns: 4" in text
        assert "\ncolumn: ID\n" in text
        assert "column_type: numeric" in text
        assert "\ncolumn: NAME\n" in text
        assert "column_type: string" in text
        assert '"John": 50' in text

    @pytest.mark.asyncio
    async def test_perform_with_unsupported_columns(self) -> None:
        """Test statistics analysis with unsupported column types."""
        table_info = TableInfo(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            name="test_table",
            column_count=3,
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
                    name="DATA_BLOB",
                    data_type="BINARY",
                    nullable=True,
                    default_value=None,
                    comment="Binary data",
                    ordinal_position=2,
                ),
                TableColumn(
                    name="METADATA",
                    data_type="OBJECT",
                    nullable=True,
                    default_value=None,
                    comment="JSON metadata",
                    ordinal_position=3,
                ),
            ],
        )

        statistics_result = {
            "TOTAL_ROWS": 500,
            "NUMERIC_ID_COUNT": 500,
            "NUMERIC_ID_NULL_COUNT": 0,
            "NUMERIC_ID_MIN": 1,
            "NUMERIC_ID_MAX": 500,
            "NUMERIC_ID_AVG": 250.5,
            "NUMERIC_ID_Q1": 125.0,
            "NUMERIC_ID_MEDIAN": 250.5,
            "NUMERIC_ID_Q3": 375.0,
            "NUMERIC_ID_DISTINCT": 500,
        }

        mock_effect = MockAnalyzeTableStatistics(
            table_info=table_info,
            statistics_result=statistics_result,
        )
        tool = AnalyzeTableStatisticsTool(mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)

        text = result[0].text
        assert "unsupported_columns:" in text
        assert "DATA_BLOB (BINARY)" in text or "METADATA (OBJECT)" in text

    @pytest.mark.asyncio
    async def test_perform_with_blank_string_profile_enabled(self) -> None:
        """Test quality profile fields when blank string profile is enabled."""
        table_info = TableInfo(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            name="test_table",
            column_count=1,
            columns=[
                TableColumn(
                    name="NAME",
                    data_type="VARCHAR(100)",
                    nullable=True,
                    default_value=None,
                    comment="User name",
                    ordinal_position=1,
                ),
            ],
        )

        statistics_result = {
            "TOTAL_ROWS": 10,
            "STRING_NAME_COUNT": 8,
            "STRING_NAME_NULL_COUNT": 2,
            "STRING_NAME_MIN_LENGTH": 0,
            "STRING_NAME_MAX_LENGTH": 20,
            "STRING_NAME_DISTINCT": 4,
            "STRING_NAME_TOP_VALUES": '[["John", 4], ["", 2], ["Jane", 2]]',
            "STRING_NAME_EMPTY_STRING_COUNT": 2,
            "STRING_NAME_BLANK_STRING_COUNT": 3,
        }

        mock_effect = MockAnalyzeTableStatistics(
            table_info=table_info,
            statistics_result=statistics_result,
        )
        tool = AnalyzeTableStatisticsTool(mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "include_null_empty_profile": True,
            "include_blank_string_profile": True,
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)

        text = result[0].text
        assert "quality_profile:" in text
        assert "null_ratio: 0.2" in text
        assert "empty_string_ratio: 0.25" in text
        assert "blank_string_ratio: 0.375" in text
        assert "statistics_metadata:" in text
        assert "quality_profile_counting_mode: exact" in text

    @pytest.mark.asyncio
    async def test_perform_with_minimal_table(self) -> None:
        """Test with minimal table (no columns)."""
        mock_effect = MockAnalyzeTableStatistics()
        tool = AnalyzeTableStatisticsTool(mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        error_text = result[0].text
        assert "Error: No supported columns for statistics" in error_text
