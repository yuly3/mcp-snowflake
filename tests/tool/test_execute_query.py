"""Test for ExecuteQueryTool."""

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
from mcp_snowflake.tool.execute_query import ExecuteQueryTool

from ..mock_effect_handler import MockExecuteQuery


class TestExecuteQueryTool:
    """Test ExecuteQueryTool."""

    def test_name_property(self) -> None:
        """Test name property."""
        converter = JsonImmutableConverter()
        mock_effect = MockExecuteQuery()
        tool = ExecuteQueryTool(converter, mock_effect)
        assert tool.name == "execute_query"

    def test_definition_property(self) -> None:
        """Test definition property."""
        converter = JsonImmutableConverter()
        mock_effect = MockExecuteQuery()
        tool = ExecuteQueryTool(converter, mock_effect)
        definition = tool.definition

        assert definition.name == "execute_query"
        assert definition.description is not None
        assert "execute" in definition.description.lower()
        assert "sql" in definition.description.lower()
        assert definition.inputSchema is not None

        # Check required fields
        input_schema = definition.inputSchema
        assert input_schema["type"] == "object"
        assert set(input_schema["required"]) == {"sql"}

        # Check properties
        properties = input_schema["properties"]
        assert "sql" in properties
        assert "timeout_seconds" in properties
        assert properties["timeout_seconds"]["maximum"] == 300
        assert properties["timeout_seconds"]["description"] == "Query timeout in seconds (default: 30, max: 300)"

    def test_definition_property_with_custom_timeout_max(self) -> None:
        """Test definition property with custom timeout max."""
        converter = JsonImmutableConverter()
        mock_effect = MockExecuteQuery()
        tool = ExecuteQueryTool(converter, mock_effect, timeout_seconds_max=1800)
        definition = tool.definition

        assert definition.inputSchema is not None
        timeout = definition.inputSchema["properties"]["timeout_seconds"]
        assert timeout["maximum"] == 1800
        assert timeout["description"] == "Query timeout in seconds (default: 30, max: 1800)"

    @pytest.mark.asyncio
    async def test_perform_success(self) -> None:
        """Test successful query execution."""
        converter = JsonImmutableConverter()
        mock_data = [
            {"id": 1, "name": "Alice", "count": 10},
            {"id": 2, "name": "Bob", "count": 20},
        ]
        mock_effect = MockExecuteQuery(result_data=mock_data)
        tool = ExecuteQueryTool(converter, mock_effect)

        arguments = {
            "sql": "SELECT id, name, count FROM users LIMIT 2",
            "timeout_seconds": 30,
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"

        # Parse JSON response
        response_data = json.loads(result[0].text)
        assert "query_result" in response_data

        query_result = response_data["query_result"]
        assert query_result["row_count"] == 2
        assert "execution_time_ms" in query_result
        assert len(query_result["rows"]) == 2
        assert query_result["columns"] == ["id", "name", "count"]

    @pytest.mark.asyncio
    async def test_perform_minimal_query(self) -> None:
        """Test with minimal SQL query."""
        converter = JsonImmutableConverter()
        mock_data = [{"result": "ok"}]
        mock_effect = MockExecuteQuery(result_data=mock_data)
        tool = ExecuteQueryTool(converter, mock_effect)

        arguments = {"sql": "SELECT 1"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        response_data = json.loads(result[0].text)
        assert response_data["query_result"]["row_count"] == 1

    @pytest.mark.asyncio
    async def test_perform_complex_query(self) -> None:
        """Test with complex query and results."""
        converter = JsonImmutableConverter()
        mock_data = [
            {"department": "Engineering", "avg_salary": 95000.0, "count": 15},
            {"department": "Marketing", "avg_salary": 75000.0, "count": 8},
            {"department": "Sales", "avg_salary": 85000.0, "count": 12},
        ]
        mock_effect = MockExecuteQuery(result_data=mock_data)
        tool = ExecuteQueryTool(converter, mock_effect)

        arguments = {
            "sql": "SELECT department, AVG(salary) as avg_salary, COUNT(*) as count FROM employees GROUP BY department",
            "timeout_seconds": 60,
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        response_data = json.loads(result[0].text)
        query_result = response_data["query_result"]
        assert query_result["row_count"] == 3
        assert "department" in query_result["columns"]
        assert "avg_salary" in query_result["columns"]

    @pytest.mark.asyncio
    async def test_perform_empty_result(self) -> None:
        """Test with empty result set."""
        converter = JsonImmutableConverter()
        mock_effect = MockExecuteQuery(result_data=[])
        tool = ExecuteQueryTool(converter, mock_effect)

        arguments = {"sql": "SELECT * FROM empty_table"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        response_data = json.loads(result[0].text)
        query_result = response_data["query_result"]
        assert query_result["row_count"] == 0
        assert len(query_result["rows"]) == 0
        assert len(query_result["columns"]) == 0

    @pytest.mark.asyncio
    async def test_perform_write_operation_blocked(self) -> None:
        """Test that write operations are blocked."""
        converter = JsonImmutableConverter()
        mock_effect = MockExecuteQuery()
        tool = ExecuteQueryTool(converter, mock_effect)

        arguments = {"sql": "INSERT INTO users VALUES (1, 'Alice')"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Error:" in result[0].text
        assert "Write operations are not allowed" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_empty_arguments(self) -> None:
        """Test with empty arguments."""
        converter = JsonImmutableConverter()
        mock_effect = MockExecuteQuery()
        tool = ExecuteQueryTool(converter, mock_effect)

        result = await tool.perform({})

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Invalid arguments for execute_query:" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_invalid_arguments(self) -> None:
        """Test with invalid arguments."""
        converter = JsonImmutableConverter()
        mock_effect = MockExecuteQuery()
        tool = ExecuteQueryTool(converter, mock_effect)

        arguments = {
            # Missing required 'sql' field
            "timeout_seconds": "invalid_type",  # Invalid type
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Error: Invalid arguments for execute_query:" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_none_arguments(self) -> None:
        """Test with None arguments."""
        converter = JsonImmutableConverter()
        mock_effect = MockExecuteQuery()
        tool = ExecuteQueryTool(converter, mock_effect)

        result = await tool.perform(None)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Error: Invalid arguments for execute_query:" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_timeout_exceeds_custom_max(self) -> None:
        """Test validation error when timeout exceeds configured max."""
        converter = JsonImmutableConverter()
        mock_effect = MockExecuteQuery()
        tool = ExecuteQueryTool(converter, mock_effect, timeout_seconds_max=45)

        result = await tool.perform({"sql": "SELECT 1", "timeout_seconds": 46})

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Error: Invalid arguments for execute_query:" in result[0].text
        assert "less than or equal to 45" in result[0].text

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
        mock_effect = MockExecuteQuery(should_raise=exception)
        tool = ExecuteQueryTool(converter, mock_effect)

        arguments = {"sql": "SELECT * FROM test_table"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert result[0].text.startswith(expected_message_prefix)
        assert str(exception) in result[0].text
