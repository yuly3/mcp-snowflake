"""Test for ListTablesTool."""

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
from kernel.table_metadata import Table
from mcp_snowflake.tool.list_tables import ListTablesTool

from ..mock_effect_handler import MockListTables


class TestListTablesTool:
    """Test ListTablesTool."""

    def test_name_property(self) -> None:
        """Test name property."""
        mock_effect = MockListTables()
        tool = ListTablesTool(mock_effect)
        assert tool.name == "list_tables"

    def test_definition_property(self) -> None:
        """Test definition property."""
        mock_effect = MockListTables()
        tool = ListTablesTool(mock_effect)
        definition = tool.definition

        assert definition.name == "list_tables"
        assert definition.description is not None
        assert "tables" in definition.description
        assert "database" in definition.description
        assert "schema" in definition.description
        assert definition.inputSchema is not None

        # Check required fields
        input_schema = definition.inputSchema
        assert input_schema["type"] == "object"
        assert set(input_schema["required"]) == {"database", "schema"}

        # Check properties
        properties = input_schema["properties"]
        assert "database" in properties
        assert "schema" in properties

    @pytest.mark.asyncio
    async def test_perform_success(self) -> None:
        """Test successful perform with default data."""
        mock_effect = MockListTables()
        tool = ListTablesTool(mock_effect)

        arguments = {"database": "TEST_DB", "schema": "TEST_SCHEMA"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"

        # Verify JSON structure
        response_data = json.loads(result[0].text)
        assert "tables_info" in response_data
        tables_info = response_data["tables_info"]
        assert tables_info["database"] == "TEST_DB"
        assert tables_info["schema"] == "TEST_SCHEMA"
        assert tables_info["tables"] == ["CUSTOMERS", "ORDERS"]

    @pytest.mark.asyncio
    async def test_perform_with_custom_data(self) -> None:
        """Test perform with custom table data."""
        custom_tables = [Table("TABLE1"), Table("TABLE2"), Table("TABLE3")]
        mock_effect = MockListTables(result_data=custom_tables)
        tool = ListTablesTool(mock_effect)

        arguments = {"database": "CUSTOM_DB", "schema": "CUSTOM_SCHEMA"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)

        response_data = json.loads(result[0].text)
        tables_info = response_data["tables_info"]
        assert tables_info["database"] == "CUSTOM_DB"
        assert tables_info["schema"] == "CUSTOM_SCHEMA"
        assert tables_info["tables"] == ["TABLE1", "TABLE2", "TABLE3"]

    @pytest.mark.asyncio
    async def test_perform_with_empty_tables(self) -> None:
        """Test perform with empty table list."""
        mock_effect = MockListTables(result_data=[])
        tool = ListTablesTool(mock_effect)

        arguments = {"database": "EMPTY_DB", "schema": "EMPTY_SCHEMA"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        response_data = json.loads(result[0].text)
        tables_info = response_data["tables_info"]
        assert tables_info["database"] == "EMPTY_DB"
        assert tables_info["schema"] == "EMPTY_SCHEMA"
        assert tables_info["tables"] == []

    @pytest.mark.asyncio
    async def test_perform_with_empty_arguments(self) -> None:
        """Test perform with empty arguments."""
        mock_effect = MockListTables()
        tool = ListTablesTool(mock_effect)

        result = await tool.perform({})

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert result[0].text.startswith("Error: Invalid arguments for list_tables:")

    @pytest.mark.asyncio
    async def test_perform_with_none_arguments(self) -> None:
        """Test perform with None arguments."""
        mock_effect = MockListTables()
        tool = ListTablesTool(mock_effect)

        result = await tool.perform(None)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].text.startswith("Error: Invalid arguments for list_tables:")

    @pytest.mark.asyncio
    async def test_perform_with_missing_database(self) -> None:
        """Test perform with missing database argument."""
        mock_effect = MockListTables()
        tool = ListTablesTool(mock_effect)

        arguments = {"schema": "TEST_SCHEMA"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].text.startswith("Error: Invalid arguments for list_tables:")

    @pytest.mark.asyncio
    async def test_perform_with_missing_schema(self) -> None:
        """Test perform with missing schema argument."""
        mock_effect = MockListTables()
        tool = ListTablesTool(mock_effect)

        arguments = {"database": "TEST_DB"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].text.startswith("Error: Invalid arguments for list_tables:")

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
        mock_effect = MockListTables(should_raise=exception)
        tool = ListTablesTool(mock_effect)

        arguments = {"database": "TEST_DB", "schema": "TEST_SCHEMA"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert result[0].text.startswith(expected_message_prefix)
        assert str(exception) in result[0].text
