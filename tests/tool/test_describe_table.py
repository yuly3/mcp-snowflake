"""Test for DescribeTableTool."""

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
        """Test successful table description outputs compact format."""
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

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        text = result[0].text

        assert "database: test_db" in text
        assert "schema: test_schema" in text
        assert "table: test_table" in text
        assert "column_count: 2" in text
        assert "name: ID" in text
        assert "type: NUMBER(10,0)" in text
        assert "nullable: false" in text
        assert "comment: Primary key" in text
        assert "name: NAME" in text
        assert "type: VARCHAR(100)" in text
        assert "nullable: true" in text
        assert "comment: User name" in text

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
        text = result[0].text

        assert "database: default_db" in text
        assert "schema: default_schema" in text
        assert "table: default_table" in text
        assert "column_count: 0" in text
