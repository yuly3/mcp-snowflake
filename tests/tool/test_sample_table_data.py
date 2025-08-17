"""Test for SampleTableDataTool."""

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

from cattrs_converter import JsonImmutableConverter
from expression.contract import ContractViolationError
from mcp_snowflake.tool.sample_table_data import SampleTableDataTool

from ..mock_effect_handler import MockSampleTableData


class TestSampleTableDataTool:
    """Test SampleTableDataTool."""

    def test_name_property(self) -> None:
        """Test name property."""
        converter = JsonImmutableConverter()
        mock_effect = MockSampleTableData()
        tool = SampleTableDataTool(converter, mock_effect)
        assert tool.name == "sample_table_data"

    def test_definition_property(self) -> None:
        """Test definition property."""
        converter = JsonImmutableConverter()
        mock_effect = MockSampleTableData()
        tool = SampleTableDataTool(converter, mock_effect)
        definition = tool.definition

        assert definition.name == "sample_table_data"
        assert definition.description is not None
        assert "sample data" in definition.description.lower()
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
        assert "sample_size" in properties
        assert "columns" in properties

    @pytest.mark.asyncio
    async def test_perform_success(self) -> None:
        """Test successful sample table data operation."""
        converter = JsonImmutableConverter()
        mock_data = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
        ]
        mock_effect = MockSampleTableData(result_data=mock_data)
        tool = SampleTableDataTool(converter, mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "users",
            "sample_size": 2,
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"

        # Parse JSON response
        response_data = json.loads(result[0].text)
        assert "sample_data" in response_data

        sample_data = response_data["sample_data"]
        assert sample_data["database"] == "test_db"
        assert sample_data["schema"] == "test_schema"
        assert sample_data["table"] == "users"
        assert sample_data["sample_size"] == 2
        assert sample_data["actual_rows"] == 2
        assert len(sample_data["rows"]) == 2

    @pytest.mark.asyncio
    async def test_perform_minimal_data(self) -> None:
        """Test with minimal data set."""
        converter = JsonImmutableConverter()
        mock_data = [{"id": 1}]
        mock_effect = MockSampleTableData(result_data=mock_data)
        tool = SampleTableDataTool(converter, mock_effect)

        arguments = {
            "database": "db",
            "schema": "schema",
            "table": "table",
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        response_data = json.loads(result[0].text)
        assert response_data["sample_data"]["actual_rows"] == 1

    @pytest.mark.asyncio
    async def test_perform_with_columns(self) -> None:
        """Test with specific columns specified."""
        converter = JsonImmutableConverter()
        mock_data = [{"name": "Alice"}, {"name": "Bob"}]
        mock_effect = MockSampleTableData(result_data=mock_data)
        tool = SampleTableDataTool(converter, mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "users",
            "columns": ["name"],
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        response_data = json.loads(result[0].text)
        sample_data = response_data["sample_data"]
        assert "name" in sample_data["columns"]
        assert len(sample_data["rows"]) == 2

    @pytest.mark.asyncio
    async def test_perform_empty_result(self) -> None:
        """Test with empty result set."""
        converter = JsonImmutableConverter()
        mock_effect = MockSampleTableData(result_data=[])
        tool = SampleTableDataTool(converter, mock_effect)

        arguments = {
            "database": "empty_db",
            "schema": "empty_schema",
            "table": "empty_table",
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        response_data = json.loads(result[0].text)
        sample_data = response_data["sample_data"]
        assert sample_data["actual_rows"] == 0
        assert len(sample_data["rows"]) == 0
        assert len(sample_data["columns"]) == 0

    @pytest.mark.asyncio
    async def test_perform_with_empty_arguments(self) -> None:
        """Test with empty arguments."""
        converter = JsonImmutableConverter()
        mock_effect = MockSampleTableData()
        tool = SampleTableDataTool(converter, mock_effect)

        result = await tool.perform({})

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Invalid arguments for sample_table_data:" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_invalid_arguments(self) -> None:
        """Test with invalid arguments."""
        converter = JsonImmutableConverter()
        mock_effect = MockSampleTableData()
        tool = SampleTableDataTool(converter, mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            # Missing required 'table' field
            "sample_size": "invalid_type",  # Invalid type
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Error: Invalid arguments for sample_table_data:" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_none_arguments(self) -> None:
        """Test with None arguments."""
        converter = JsonImmutableConverter()
        mock_effect = MockSampleTableData()
        tool = SampleTableDataTool(converter, mock_effect)

        result = await tool.perform(None)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Error: Invalid arguments for sample_table_data:" in result[0].text

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
        converter = JsonImmutableConverter()
        mock_effect = MockSampleTableData(should_raise=exception)
        tool = SampleTableDataTool(converter, mock_effect)

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
