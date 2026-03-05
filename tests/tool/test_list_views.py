"""Test for ListViewsTool."""

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
from kernel.table_metadata import View
from mcp_snowflake.tool.list_views import ListViewsTool

from ..mock_effect_handler import MockListViews


class TestListViewsTool:
    """Test ListViewsTool."""

    def test_name_property(self) -> None:
        """Test name property."""
        mock_effect = MockListViews()
        tool = ListViewsTool(mock_effect)
        assert tool.name == "list_views"

    def test_definition_property(self) -> None:
        """Test definition property."""
        mock_effect = MockListViews()
        tool = ListViewsTool(mock_effect)
        definition = tool.definition

        assert definition.name == "list_views"
        assert definition.description is not None
        assert "views" in definition.description
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
        assert "filter" in properties
        assert properties["filter"]["type"] == "object"
        assert set(properties["filter"]["required"]) == {"type", "value"}
        assert properties["filter"]["properties"]["type"]["enum"] == ["contains"]

    @pytest.mark.asyncio
    async def test_perform_success(self) -> None:
        """Test successful perform with default data."""
        mock_effect = MockListViews()
        tool = ListViewsTool(mock_effect)

        arguments = {"database": "TEST_DB", "schema": "TEST_SCHEMA"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        text = result[0].text

        assert "database: TEST_DB" in text
        assert "schema: TEST_SCHEMA" in text
        assert "view_count: 2" in text
        assert "views: CUSTOMER_VIEW, ORDER_SUMMARY_VIEW" in text

    @pytest.mark.asyncio
    async def test_perform_with_custom_data(self) -> None:
        """Test perform with custom view data."""
        custom_views = [View("VIEW1"), View("VIEW2"), View("VIEW3")]
        mock_effect = MockListViews(result_data=custom_views)
        tool = ListViewsTool(mock_effect)

        arguments = {"database": "CUSTOM_DB", "schema": "CUSTOM_SCHEMA"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        text = result[0].text

        assert "database: CUSTOM_DB" in text
        assert "schema: CUSTOM_SCHEMA" in text
        assert "view_count: 3" in text
        assert "views: VIEW1, VIEW2, VIEW3" in text

    @pytest.mark.asyncio
    async def test_perform_with_empty_views(self) -> None:
        """Test perform with empty view list."""
        mock_effect = MockListViews(result_data=[])
        tool = ListViewsTool(mock_effect)

        arguments = {"database": "EMPTY_DB", "schema": "EMPTY_SCHEMA"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        text = result[0].text

        assert "database: EMPTY_DB" in text
        assert "schema: EMPTY_SCHEMA" in text
        assert "view_count: 0" in text
        assert "views: (none)" in text

    @pytest.mark.asyncio
    async def test_perform_with_filter(self) -> None:
        """Test perform with contains filter."""
        custom_views = [View("ORDERS"), View("ORDER_ITEMS"), View("CUSTOMERS")]
        mock_effect = MockListViews(result_data=custom_views)
        tool = ListViewsTool(mock_effect)

        arguments = {
            "database": "CUSTOM_DB",
            "schema": "CUSTOM_SCHEMA",
            "filter": {"type": "contains", "value": "ord"},
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        text = result[0].text

        assert "view_count: 2" in text
        assert "views: ORDERS, ORDER_ITEMS" in text

    @pytest.mark.asyncio
    async def test_perform_with_empty_arguments(self) -> None:
        """Test perform with empty arguments."""
        mock_effect = MockListViews()
        tool = ListViewsTool(mock_effect)

        result = await tool.perform({})

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert result[0].text.startswith("Error: Invalid arguments for list_views:")

    @pytest.mark.asyncio
    async def test_perform_with_none_arguments(self) -> None:
        """Test perform with None arguments."""
        mock_effect = MockListViews()
        tool = ListViewsTool(mock_effect)

        result = await tool.perform(None)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].text.startswith("Error: Invalid arguments for list_views:")

    @pytest.mark.asyncio
    async def test_perform_with_missing_database(self) -> None:
        """Test perform with missing database argument."""
        mock_effect = MockListViews()
        tool = ListViewsTool(mock_effect)

        arguments = {"schema": "TEST_SCHEMA"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].text.startswith("Error: Invalid arguments for list_views:")

    @pytest.mark.asyncio
    async def test_perform_with_missing_schema(self) -> None:
        """Test perform with missing schema argument."""
        mock_effect = MockListViews()
        tool = ListViewsTool(mock_effect)

        arguments = {"database": "TEST_DB"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].text.startswith("Error: Invalid arguments for list_views:")

    @pytest.mark.asyncio
    async def test_perform_with_invalid_filter_type(self) -> None:
        """Test perform with invalid filter type."""
        mock_effect = MockListViews()
        tool = ListViewsTool(mock_effect)

        arguments = {
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
            "filter": {"type": "starts_with", "value": "ord"},
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].text.startswith("Error: Invalid arguments for list_views:")

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
        mock_effect = MockListViews(should_raise=exception)
        tool = ListViewsTool(mock_effect)

        arguments = {"database": "TEST_DB", "schema": "TEST_SCHEMA"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert result[0].text.startswith(expected_message_prefix)
        assert str(exception) in result[0].text
