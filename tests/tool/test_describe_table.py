"""Test for DescribeTableTool."""

import json

import mcp.types as types
import pytest
from snowflake.connector import (
    DataError,
    IntegrityError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)

from expression.contract import ContractViolationError
from kernel.table_metadata import DataBase, Schema, TableColumn, TableInfo
from mcp_snowflake.tool.describe_table import DescribeTableTool

from ..mock_effect_handler import MockDescribeTable


class TestDescribeTableTool:
    """Test DescribeTableTool."""

    def test_name_property(self) -> None:
        """Test name property."""
        mock_effect = MockDescribeTable()
        tool = DescribeTableTool(mock_effect)
        assert tool.name == "describe_table"

    def test_definition_property(self) -> None:
        """Test definition property."""
        mock_effect = MockDescribeTable()
        tool = DescribeTableTool(mock_effect)
        definition = tool.definition

        assert definition.name == "describe_table"
        assert definition.description is not None
        assert "structure" in definition.description
        assert "columns" in definition.description
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

    @pytest.mark.asyncio
    async def test_perform_success(self) -> None:
        """Test successful table description."""
        # Prepare mock data
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

        mock_effect = MockDescribeTable(table_info=table_info)
        tool = DescribeTableTool(mock_effect)

        # Execute
        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
        }
        result = await tool.perform(arguments)

        # Verify
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"

        # Parse and verify JSON content
        response_data = json.loads(result[0].text)
        assert "table_info" in response_data
        table_info_dict = response_data["table_info"]
        assert table_info_dict["database"] == "test_db"
        assert table_info_dict["schema"] == "test_schema"
        assert table_info_dict["name"] == "test_table"
        assert table_info_dict["column_count"] == 2
        assert len(table_info_dict["columns"]) == 2

        # Verify column details
        columns = table_info_dict["columns"]
        id_column = next((col for col in columns if col["name"] == "ID"), None)
        assert id_column is not None
        assert id_column["data_type"] == "NUMBER(10,0)"
        assert id_column["nullable"] is False
        assert id_column["comment"] == "Primary key"
        assert id_column["ordinal_position"] == 1

        name_column = next((col for col in columns if col["name"] == "NAME"), None)
        assert name_column is not None
        assert name_column["data_type"] == "VARCHAR(100)"
        assert name_column["nullable"] is True
        assert name_column["comment"] == "User name"
        assert name_column["ordinal_position"] == 2

    @pytest.mark.asyncio
    async def test_perform_with_empty_arguments(self) -> None:
        """Test with empty arguments."""
        mock_effect = MockDescribeTable()
        tool = DescribeTableTool(mock_effect)

        result = await tool.perform(None)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Invalid arguments for describe_table:" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_invalid_arguments(self) -> None:
        """Test with invalid arguments."""
        mock_effect = MockDescribeTable()
        tool = DescribeTableTool(mock_effect)

        # Missing required fields
        arguments = {"database": "test_db"}  # Missing schema and table
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Invalid arguments for describe_table:" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_empty_dict_arguments(self) -> None:
        """Test with empty dict arguments."""
        mock_effect = MockDescribeTable()
        tool = DescribeTableTool(mock_effect)

        result = await tool.perform({})

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Invalid arguments for describe_table:" in result[0].text

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("exception", "expected_message_prefix"),
        [
            (TimeoutError("Connection timeout"), "Error: Query timed out:"),
            (
                ProgrammingError("SQL syntax error"),
                "Error: SQL syntax error or other programming error:",
            ),
            (
                OperationalError("Database connection error"),
                "Error: Database operation related error:",
            ),
            (DataError("Invalid data"), "Error: Data processing related error:"),
            (
                IntegrityError("Constraint violation"),
                "Error: Referential integrity constraint violation:",
            ),
            (
                NotSupportedError("Feature not supported"),
                "Error: Unsupported database feature used:",
            ),
            (
                ContractViolationError("Contract violation"),
                "Error: Unexpected error:",
            ),
        ],
    )
    async def test_perform_with_exceptions(
        self,
        exception: Exception,
        expected_message_prefix: str,
    ) -> None:
        """Test exception handling in perform method."""
        mock_effect = MockDescribeTable(should_raise=exception)
        tool = DescribeTableTool(mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert result[0].text.startswith(expected_message_prefix)
        assert str(exception) in result[0].text

    @pytest.mark.asyncio
    async def test_perform_minimal_table_info(self) -> None:
        """Test with minimal table information."""
        # Use default table info from MockDescribeTable
        mock_effect = MockDescribeTable()
        tool = DescribeTableTool(mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"

        # Parse and verify JSON content
        response_data = json.loads(result[0].text)
        assert "table_info" in response_data
        table_info_dict = response_data["table_info"]
        assert table_info_dict["database"] == "default_db"
        assert table_info_dict["schema"] == "default_schema"
        assert table_info_dict["name"] == "default_table"
        assert table_info_dict["column_count"] == 0
        assert table_info_dict["columns"] == []

    @pytest.mark.asyncio
    async def test_perform_with_complex_column_types(self) -> None:
        """Test with various column data types."""
        table_info = TableInfo(
            database=DataBase("complex_db"),
            schema=Schema("complex_schema"),
            name="complex_table",
            column_count=5,
            columns=[
                TableColumn(
                    name="INT_COL",
                    data_type="NUMBER(10,0)",
                    nullable=False,
                    default_value="0",
                    comment=None,
                    ordinal_position=1,
                ),
                TableColumn(
                    name="TEXT_COL",
                    data_type="VARCHAR(16777216)",
                    nullable=True,
                    default_value=None,
                    comment="Long text column",
                    ordinal_position=2,
                ),
                TableColumn(
                    name="DATE_COL",
                    data_type="DATE",
                    nullable=True,
                    default_value="CURRENT_DATE",
                    comment="Date column",
                    ordinal_position=3,
                ),
                TableColumn(
                    name="DECIMAL_COL",
                    data_type="NUMBER(18,2)",
                    nullable=False,
                    default_value="0.00",
                    comment="Decimal column",
                    ordinal_position=4,
                ),
                TableColumn(
                    name="BOOLEAN_COL",
                    data_type="BOOLEAN",
                    nullable=True,
                    default_value=None,
                    comment="Boolean column",
                    ordinal_position=5,
                ),
            ],
        )

        mock_effect = MockDescribeTable(table_info=table_info)
        tool = DescribeTableTool(mock_effect)

        arguments = {
            "database": "complex_db",
            "schema": "complex_schema",
            "table": "complex_table",
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)

        # Parse and verify JSON content
        response_data = json.loads(result[0].text)
        table_info_dict = response_data["table_info"]
        assert table_info_dict["column_count"] == 5

        columns = table_info_dict["columns"]
        assert len(columns) == 5

        # Verify specific columns
        int_col = next((col for col in columns if col["name"] == "INT_COL"), None)
        assert int_col is not None
        assert int_col["data_type"] == "NUMBER(10,0)"
        assert int_col["default_value"] == "0"
        assert int_col["nullable"] is False

        text_col = next((col for col in columns if col["name"] == "TEXT_COL"), None)
        assert text_col is not None
        assert text_col["data_type"] == "VARCHAR(16777216)"
        assert text_col["comment"] == "Long text column"

        boolean_col = next(
            (col for col in columns if col["name"] == "BOOLEAN_COL"), None
        )
        assert boolean_col is not None
        assert boolean_col["data_type"] == "BOOLEAN"
        assert boolean_col["default_value"] is None
