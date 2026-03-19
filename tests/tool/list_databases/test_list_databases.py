"""Test for ListDatabasesTool."""

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
from kernel.table_metadata import DataBase
from mcp_snowflake.tool.list_databases import ListDatabasesTool

from ...mock_effect_handler import MockListDatabases


class TestListDatabasesTool:
    """Test ListDatabasesTool."""

    def test_name_property(self) -> None:
        """Test name property."""
        mock_effect = MockListDatabases()
        tool = ListDatabasesTool(mock_effect)
        assert tool.name == "list_databases"

    def test_definition_property(self) -> None:
        """Test definition property."""
        mock_effect = MockListDatabases()
        tool = ListDatabasesTool(mock_effect)
        definition = tool.definition

        assert definition.name == "list_databases"
        assert definition.description is not None
        assert definition.inputSchema is not None

        input_schema = definition.inputSchema
        assert input_schema["type"] == "object"
        assert "required" not in input_schema

    @pytest.mark.asyncio
    async def test_perform_success(self) -> None:
        """Test successful perform with default data."""
        mock_effect = MockListDatabases()
        tool = ListDatabasesTool(mock_effect)

        result = await tool.perform({})

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        text = result[0].text

        assert "database_count: 2" in text
        assert "databases: ANALYTICS, RAW" in text

    @pytest.mark.asyncio
    async def test_perform_with_custom_data(self) -> None:
        """Test perform with custom database data."""
        custom_databases = [DataBase("DB1"), DataBase("DB2"), DataBase("DB3")]
        mock_effect = MockListDatabases(result_data=custom_databases)
        tool = ListDatabasesTool(mock_effect)

        result = await tool.perform({})

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        text = result[0].text

        assert "database_count: 3" in text
        assert "databases: DB1, DB2, DB3" in text

    @pytest.mark.asyncio
    async def test_perform_with_empty_databases(self) -> None:
        """Test perform with empty database list."""
        mock_effect = MockListDatabases(result_data=[])
        tool = ListDatabasesTool(mock_effect)

        result = await tool.perform({})

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        text = result[0].text

        assert "database_count: 0" in text
        assert "databases: (none)" in text

    @pytest.mark.asyncio
    async def test_perform_with_none_arguments(self) -> None:
        """Test perform with None arguments."""
        mock_effect = MockListDatabases()
        tool = ListDatabasesTool(mock_effect)

        result = await tool.perform(None)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "database_count: 2" in result[0].text

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
        mock_effect = MockListDatabases(should_raise=exception)
        tool = ListDatabasesTool(mock_effect)

        result = await tool.perform({})

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert result[0].text.startswith(expected_message_prefix)
        assert str(exception) in result[0].text
