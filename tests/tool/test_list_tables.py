"""Test for ListTablesTool."""

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
from kernel.table_metadata import ObjectKind, SchemaObject
from mcp_snowflake.handler.errors import MissingResponseColumnError
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
        assert "views" in definition.description
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
        assert "oneOf" in properties["filter"]
        assert len(properties["filter"]["oneOf"]) == 2

    @pytest.mark.asyncio
    async def test_perform_success(self) -> None:
        """Test successful perform with default data."""
        mock_effect = MockListTables()
        tool = ListTablesTool(mock_effect)

        arguments = {"database": "TEST_DB", "schema": "TEST_SCHEMA"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        text = result[0].text

        assert "database: TEST_DB" in text
        assert "schema: TEST_SCHEMA" in text
        assert "object_count: 3" in text
        assert "tables: CUSTOMERS, ORDERS" in text
        assert "views: CUSTOMER_VIEW" in text

    @pytest.mark.asyncio
    async def test_perform_with_custom_data(self) -> None:
        """Test perform with custom object data."""
        custom_objects = [
            SchemaObject(name="TABLE1", kind=ObjectKind.TABLE),
            SchemaObject(name="TABLE2", kind=ObjectKind.TABLE),
            SchemaObject(name="VIEW1", kind=ObjectKind.VIEW),
        ]
        mock_effect = MockListTables(result_data=custom_objects)
        tool = ListTablesTool(mock_effect)

        arguments = {"database": "CUSTOM_DB", "schema": "CUSTOM_SCHEMA"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        text = result[0].text

        assert "database: CUSTOM_DB" in text
        assert "schema: CUSTOM_SCHEMA" in text
        assert "object_count: 3" in text
        assert "tables: TABLE1, TABLE2" in text
        assert "views: VIEW1" in text

    @pytest.mark.asyncio
    async def test_perform_with_empty_objects(self) -> None:
        """Test perform with empty object list."""
        mock_effect = MockListTables(result_data=[])
        tool = ListTablesTool(mock_effect)

        arguments = {"database": "EMPTY_DB", "schema": "EMPTY_SCHEMA"}
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        text = result[0].text

        assert "database: EMPTY_DB" in text
        assert "schema: EMPTY_SCHEMA" in text
        assert "object_count: 0" in text
        assert "tables: (none)" in text
        assert "views: (none)" in text

    @pytest.mark.asyncio
    async def test_perform_with_contains_filter(self) -> None:
        """Test perform with contains filter."""
        custom_objects = [
            SchemaObject(name="ORDERS", kind=ObjectKind.TABLE),
            SchemaObject(name="ORDER_ITEMS", kind=ObjectKind.TABLE),
            SchemaObject(name="CUSTOMERS", kind=ObjectKind.TABLE),
            SchemaObject(name="ORDER_VIEW", kind=ObjectKind.VIEW),
        ]
        mock_effect = MockListTables(result_data=custom_objects)
        tool = ListTablesTool(mock_effect)

        arguments = {
            "database": "CUSTOM_DB",
            "schema": "CUSTOM_SCHEMA",
            "filter": {"type": "contains", "value": "ord"},
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        text = result[0].text

        assert "object_count: 3" in text
        assert "tables: ORDERS, ORDER_ITEMS" in text
        assert "views: ORDER_VIEW" in text

    @pytest.mark.asyncio
    async def test_perform_with_object_type_filter_table(self) -> None:
        """Test perform with object_type filter for TABLE."""
        custom_objects = [
            SchemaObject(name="TABLE1", kind=ObjectKind.TABLE),
            SchemaObject(name="VIEW1", kind=ObjectKind.VIEW),
        ]
        mock_effect = MockListTables(result_data=custom_objects)
        tool = ListTablesTool(mock_effect)

        arguments = {
            "database": "DB",
            "schema": "SCH",
            "filter": {"type": "object_type", "value": "TABLE"},
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        text = result[0].text
        assert "object_count: 1" in text
        assert "tables: TABLE1" in text
        assert "views: (none)" in text

    @pytest.mark.asyncio
    async def test_perform_with_object_type_filter_view(self) -> None:
        """Test perform with object_type filter for VIEW."""
        custom_objects = [
            SchemaObject(name="TABLE1", kind=ObjectKind.TABLE),
            SchemaObject(name="VIEW1", kind=ObjectKind.VIEW),
        ]
        mock_effect = MockListTables(result_data=custom_objects)
        tool = ListTablesTool(mock_effect)

        arguments = {
            "database": "DB",
            "schema": "SCH",
            "filter": {"type": "object_type", "value": "VIEW"},
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        text = result[0].text
        assert "object_count: 1" in text
        assert "tables: (none)" in text
        assert "views: VIEW1" in text

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
    async def test_perform_with_invalid_filter_type(self) -> None:
        """Test perform with invalid filter type."""
        mock_effect = MockListTables()
        tool = ListTablesTool(mock_effect)

        arguments = {
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
            "filter": {"type": "starts_with", "value": "ord"},
        }
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
                MissingResponseColumnError("missing required columns: kind"),
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
