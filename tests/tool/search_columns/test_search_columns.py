"""Test for SearchColumnsTool."""

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
from mcp_snowflake.handler.errors import MissingResponseColumnError
from mcp_snowflake.tool.search_columns import SearchColumnsTool

from ...mock_effect_handler import MockSearchColumns


class TestSearchColumnsTool:
    """Test SearchColumnsTool."""

    def test_name_property(self) -> None:
        tool = SearchColumnsTool(MockSearchColumns())
        assert tool.name == "search_columns"

    def test_definition_property(self) -> None:
        tool = SearchColumnsTool(MockSearchColumns())
        definition = tool.definition

        assert definition.name == "search_columns"
        assert definition.inputSchema is not None
        assert set(definition.inputSchema["required"]) == {"database"}
        assert "column_name_pattern" in definition.inputSchema["properties"]
        assert "data_type" in definition.inputSchema["properties"]

    @pytest.mark.asyncio
    async def test_perform_success(self) -> None:
        tool = SearchColumnsTool(MockSearchColumns())

        result = await tool.perform({"database": "DB", "column_name_pattern": "%id%"})

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        text = result[0].text
        assert "database: DB" in text
        assert "table_count: 2" in text
        assert "schema: PUBLIC" in text

    @pytest.mark.asyncio
    async def test_perform_with_empty_results(self) -> None:
        tool = SearchColumnsTool(MockSearchColumns(result_data=[]))

        result = await tool.perform({"database": "DB", "data_type": "VARIANT"})

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        text = result[0].text
        assert "table_count: 0" in text

    @pytest.mark.asyncio
    async def test_perform_validation_error_missing_filters(self) -> None:
        tool = SearchColumnsTool(MockSearchColumns())

        result = await tool.perform({"database": "DB"})

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Error: Invalid arguments for search_columns:" in result[0].text
        assert "At least one" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_validation_error_missing_database(self) -> None:
        tool = SearchColumnsTool(MockSearchColumns())

        result = await tool.perform({"column_name_pattern": "%id%"})

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Error: Invalid arguments for search_columns:" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_none_arguments(self) -> None:
        tool = SearchColumnsTool(MockSearchColumns())

        result = await tool.perform(None)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Error: Invalid arguments for search_columns:" in result[0].text

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
                MissingResponseColumnError("Missing COLUMNS"),
                "Error: Missing required columns in Snowflake response:",
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
        tool = SearchColumnsTool(MockSearchColumns(should_raise=exception))

        result = await tool.perform({"database": "DB", "column_name_pattern": "%id%"})

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].text.startswith(expected_message_prefix)
        assert str(exception) in result[0].text
