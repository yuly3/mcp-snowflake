"""Test for AnalyzeTableStatisticsTool - Success Cases."""

import json

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

        # Check optional properties defaults
        assert properties["columns"]["default"] == []
        assert properties["top_k_limit"]["default"] == 10
        assert properties["top_k_limit"]["minimum"] == 1
        assert properties["top_k_limit"]["maximum"] == 100

    @pytest.mark.asyncio
    async def test_perform_success_basic(self) -> None:
        """Test basic successful statistics analysis."""
        # Set up mock table info with various column types
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

        # Mock statistics result with sample data
        statistics_result = {
            "TOTAL_ROWS": 5000,
            # Numeric column (ID)
            "NUMERIC_ID_COUNT": 5000,
            "NUMERIC_ID_NULL_COUNT": 0,
            "NUMERIC_ID_MIN": 1,
            "NUMERIC_ID_MAX": 5000,
            "NUMERIC_ID_AVG": 2500.5,
            "NUMERIC_ID_Q1": 1250.25,
            "NUMERIC_ID_MEDIAN": 2500.5,
            "NUMERIC_ID_Q3": 3750.75,
            "NUMERIC_ID_DISTINCT": 5000,
            # String column (NAME)
            "STRING_NAME_COUNT": 4900,
            "STRING_NAME_NULL_COUNT": 100,
            "STRING_NAME_MIN_LENGTH": 3,
            "STRING_NAME_MAX_LENGTH": 20,
            "STRING_NAME_DISTINCT": 4500,
            "STRING_NAME_TOP_VALUES": '[["John", 50], ["Jane", 45]]',
            # Date column (CREATED_DATE)
            "DATE_CREATED_DATE_COUNT": 4950,
            "DATE_CREATED_DATE_NULL_COUNT": 50,
            "DATE_CREATED_DATE_MIN": "2023-01-01",
            "DATE_CREATED_DATE_MAX": "2024-12-31",
            "DATE_CREATED_DATE_DISTINCT": 365,
            "DATE_CREATED_DATE_RANGE_DAYS": 730,
            # Boolean column (IS_ACTIVE)
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
            table_info=table_info, statistics_result=statistics_result
        )
        tool = AnalyzeTableStatisticsTool(mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
        }
        result = await tool.perform(arguments)

        assert len(result) == 2
        assert all(isinstance(content, types.TextContent) for content in result)

        # First content should be summary
        assert isinstance(result[0], types.TextContent)
        summary_text = result[0].text
        assert (
            "Table Statistics Analysis: test_db.test_schema.test_table" in summary_text
        )
        assert "5,000 total rows" in summary_text
        assert "Successfully analyzed" in summary_text

        # Second content should be JSON
        assert isinstance(result[1], types.TextContent)
        json_text = result[1].text
        response_data = json.loads(json_text)

        assert "table_statistics" in response_data
        table_stats = response_data["table_statistics"]

        # Verify table info
        assert table_stats["table_info"]["database"] == "test_db"
        assert table_stats["table_info"]["schema"] == "test_schema"
        assert table_stats["table_info"]["table"] == "test_table"
        assert table_stats["table_info"]["total_rows"] == 5000

        # Verify column statistics exist
        assert "column_statistics" in table_stats
        column_stats = table_stats["column_statistics"]

        # Should have statistics for supported column types
        expected_columns = {"ID", "NAME", "CREATED_DATE", "IS_ACTIVE"}
        actual_columns = set(column_stats.keys())
        assert actual_columns.issubset(expected_columns)

    @pytest.mark.asyncio
    async def test_perform_with_specific_columns(self) -> None:
        """Test statistics analysis with specific columns specified."""
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

        statistics_result = {
            "TOTAL_ROWS": 1000,
            "NUMERIC_ID_COUNT": 1000,
            "NUMERIC_ID_NULL_COUNT": 0,
            "NUMERIC_ID_MIN": 1,
            "NUMERIC_ID_MAX": 1000,
            "NUMERIC_ID_AVG": 500.5,
            "NUMERIC_ID_Q1": 250.0,
            "NUMERIC_ID_MEDIAN": 500.0,
            "NUMERIC_ID_Q3": 750.0,
            "NUMERIC_ID_DISTINCT": 1000,
        }

        mock_effect = MockAnalyzeTableStatistics(
            table_info=table_info, statistics_result=statistics_result
        )
        tool = AnalyzeTableStatisticsTool(mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "columns": ["ID"],  # Only analyze ID column
        }
        result = await tool.perform(arguments)

        assert len(result) == 2
        assert isinstance(result[0], types.TextContent)
        summary_text = result[0].text
        assert "test_db.test_schema.test_table" in summary_text

        # Verify JSON response
        assert isinstance(result[1], types.TextContent)
        json_text = result[1].text
        response_data = json.loads(json_text)
        assert "table_statistics" in response_data

    @pytest.mark.asyncio
    async def test_perform_with_custom_top_k_limit(self) -> None:
        """Test statistics analysis with custom top_k_limit."""
        table_info = TableInfo(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            name="test_table",
            column_count=1,
            columns=[
                TableColumn(
                    name="CATEGORY",
                    data_type="VARCHAR(50)",
                    nullable=True,
                    default_value=None,
                    comment="Category name",
                    ordinal_position=1,
                ),
            ],
        )

        statistics_result = {
            "TOTAL_ROWS": 2000,
            "STRING_CATEGORY_COUNT": 1990,
            "STRING_CATEGORY_NULL_COUNT": 10,
            "STRING_CATEGORY_MIN_LENGTH": 1,
            "STRING_CATEGORY_MAX_LENGTH": 10,
            "STRING_CATEGORY_DISTINCT": 25,
            "STRING_CATEGORY_TOP_VALUES": '[["A", 200], ["B", 150]]',
        }

        mock_effect = MockAnalyzeTableStatistics(
            table_info=table_info, statistics_result=statistics_result
        )
        tool = AnalyzeTableStatisticsTool(mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "top_k_limit": 5,  # Custom limit
        }
        result = await tool.perform(arguments)

        assert len(result) == 2
        assert isinstance(result[0], types.TextContent)
        summary_text = result[0].text
        assert "test_db.test_schema.test_table" in summary_text

        assert isinstance(result[1], types.TextContent)
        json_text = result[1].text
        response_data = json.loads(json_text)
        assert "table_statistics" in response_data

    @pytest.mark.asyncio
    async def test_perform_with_minimal_table(self) -> None:
        """Test with minimal table (no columns)."""
        # Use default table info from MockAnalyzeTableStatistics (no columns)
        mock_effect = MockAnalyzeTableStatistics()
        tool = AnalyzeTableStatisticsTool(mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
        }
        result = await tool.perform(arguments)

        # With no columns, should return error message
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        error_text = result[0].text
        assert "Error: No supported columns for statistics" in error_text

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
            table_info=table_info, statistics_result=statistics_result
        )
        tool = AnalyzeTableStatisticsTool(mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
        }
        result = await tool.perform(arguments)

        assert len(result) == 2
        assert isinstance(result[0], types.TextContent)
        summary_text = result[0].text
        assert "test_db.test_schema.test_table" in summary_text

        # Should mention unsupported columns if any
        assert isinstance(result[1], types.TextContent)
        json_text = result[1].text
        response_data = json.loads(json_text)
        table_stats = response_data["table_statistics"]

        # May have unsupported_columns field if binary/object columns are not supported
        if "unsupported_columns" in table_stats:
            unsupported = table_stats["unsupported_columns"]
            assert isinstance(unsupported, list)
