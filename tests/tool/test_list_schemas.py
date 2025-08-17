"""Test for ListSchemasTool."""

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
from kernel.table_metadata import Schema
from mcp_snowflake.tool.list_schemas import ListSchemasTool

from ..mock_effect_handler import MockListSchemas


class TestListSchemasTool:
    """Test ListSchemasTool."""

    def test_name_property(self) -> None:
        """Test name property."""
        mock_effect = MockListSchemas()
        tool = ListSchemasTool(mock_effect)
        assert tool.name == "list_schemas"

    def test_definition_property(self) -> None:
        """Test definition property."""
        mock_effect = MockListSchemas()
        tool = ListSchemasTool(mock_effect)
        definition = tool.definition

        assert definition.name == "list_schemas"
        assert definition.description is not None
        assert "schemas" in definition.description
        assert "database" in definition.description
        assert definition.inputSchema is not None

        # Check required fields
        input_schema = definition.inputSchema
        assert input_schema["type"] == "object"
        assert set(input_schema["required"]) == {"database"}

        # Check properties
        properties = input_schema["properties"]
        assert "database" in properties

    @pytest.mark.asyncio
    async def test_perform_success(self) -> None:
        """Test successful perform with default data."""
        mock_effect = MockListSchemas()
        tool = ListSchemasTool(mock_effect)

        arguments = {"database": "TEST_DB"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"

        # Verify JSON structure
        response_data = json.loads(result[0].text)
        assert "schemas_info" in response_data
        schemas_info = response_data["schemas_info"]
        assert schemas_info["database"] == "TEST_DB"
        assert schemas_info["schemas"] == ["INFORMATION_SCHEMA", "PUBLIC"]

    @pytest.mark.asyncio
    async def test_perform_with_custom_data(self) -> None:
        """Test perform with custom schema data."""
        custom_schemas = [Schema("SCHEMA1"), Schema("SCHEMA2"), Schema("SCHEMA3")]
        mock_effect = MockListSchemas(result_data=custom_schemas)
        tool = ListSchemasTool(mock_effect)

        arguments = {"database": "CUSTOM_DB"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)

        response_data = json.loads(result[0].text)
        schemas_info = response_data["schemas_info"]
        assert schemas_info["database"] == "CUSTOM_DB"
        assert schemas_info["schemas"] == ["SCHEMA1", "SCHEMA2", "SCHEMA3"]

    @pytest.mark.asyncio
    async def test_perform_with_empty_schemas(self) -> None:
        """Test perform with empty schema list."""
        mock_effect = MockListSchemas(result_data=[])
        tool = ListSchemasTool(mock_effect)

        arguments = {"database": "EMPTY_DB"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        response_data = json.loads(result[0].text)
        schemas_info = response_data["schemas_info"]
        assert schemas_info["database"] == "EMPTY_DB"
        assert schemas_info["schemas"] == []

    @pytest.mark.asyncio
    async def test_perform_with_empty_arguments(self) -> None:
        """Test perform with empty arguments."""
        mock_effect = MockListSchemas()
        tool = ListSchemasTool(mock_effect)

        result = await tool.perform({})

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert result[0].text.startswith("Error: Invalid arguments for list_schemas:")

    @pytest.mark.asyncio
    async def test_perform_with_none_arguments(self) -> None:
        """Test perform with None arguments."""
        mock_effect = MockListSchemas()
        tool = ListSchemasTool(mock_effect)

        result = await tool.perform(None)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].text.startswith("Error: Invalid arguments for list_schemas:")

    @pytest.mark.asyncio
    async def test_perform_with_invalid_arguments(self) -> None:
        """Test perform with invalid arguments."""
        mock_effect = MockListSchemas()
        tool = ListSchemasTool(mock_effect)

        arguments = {"invalid_field": "value"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].text.startswith("Error: Invalid arguments for list_schemas:")

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
        mock_effect = MockListSchemas(should_raise=exception)
        tool = ListSchemasTool(mock_effect)

        arguments = {"database": "TEST_DB"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert result[0].text.startswith(expected_message_prefix)
        assert str(exception) in result[0].text
