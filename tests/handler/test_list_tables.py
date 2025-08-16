import mcp.types as types
import pytest
from pydantic import ValidationError

from mcp_snowflake.handler import ListTablesArgs, handle_list_tables


class MockEffectHandler:
    """Mock implementation of _EffectListTables protocol."""

    def __init__(
        self,
        tables: list[str] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.tables = tables or []
        self.should_raise = should_raise

    async def list_tables(self, database: str, schema: str) -> list[str]:  # noqa: ARG002
        if self.should_raise:
            raise self.should_raise
        return self.tables


class TestListTablesArgs:
    """Test ListTablesArgs validation."""

    def test_valid_args(self) -> None:
        """Test valid arguments."""
        args = ListTablesArgs(database="test_db", schema="test_schema")
        assert args.database == "test_db"
        assert args.schema_ == "test_schema"

    def test_missing_database(self) -> None:
        """Test missing database argument."""
        with pytest.raises(ValidationError):
            _ = ListTablesArgs.model_validate({"schema": "test_schema"})

    def test_missing_schema(self) -> None:
        """Test missing schema argument."""
        with pytest.raises(ValidationError):
            _ = ListTablesArgs.model_validate({"database": "test_db"})

    def test_missing_both_args(self) -> None:
        """Test missing both arguments."""
        with pytest.raises(ValidationError):
            _ = ListTablesArgs.model_validate({})

    def test_empty_database(self) -> None:
        """Test empty database string."""
        args = ListTablesArgs(database="", schema="test_schema")
        assert args.database == ""
        assert args.schema_ == "test_schema"

    def test_empty_schema(self) -> None:
        """Test empty schema string."""
        args = ListTablesArgs(database="test_db", schema="")
        assert args.database == "test_db"
        assert args.schema_ == ""


class TestHandleListTables:
    """Test handle_list_tables function."""

    @pytest.mark.asyncio
    async def test_successful_list_tables(self) -> None:
        """Test successful table listing."""
        # Arrange
        args = ListTablesArgs(database="test_db", schema="test_schema")
        mock_tables = ["table1", "table2", "table3"]
        effect_handler = MockEffectHandler(tables=mock_tables)

        # Act
        result = await handle_list_tables(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Table list for schema 'test_db.test_schema':" in result[0].text
        assert "- table1" in result[0].text
        assert "- table2" in result[0].text
        assert "- table3" in result[0].text

    @pytest.mark.asyncio
    async def test_empty_tables_list(self) -> None:
        """Test when no tables are returned."""
        # Arrange
        args = ListTablesArgs(database="empty_db", schema="empty_schema")
        effect_handler = MockEffectHandler(tables=[])

        # Act
        result = await handle_list_tables(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Table list for schema 'empty_db.empty_schema':" in result[0].text
        # Should not contain any table entries
        assert "- " not in result[0].text

    @pytest.mark.asyncio
    async def test_effect_handler_exception(self) -> None:
        """Test exception handling from effect handler."""
        # Arrange
        args = ListTablesArgs(database="error_db", schema="error_schema")
        error_message = "Connection failed"
        effect_handler = MockEffectHandler(should_raise=Exception(error_message))

        # Act
        result = await handle_list_tables(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Failed to retrieve tables:" in result[0].text
        assert error_message in result[0].text

    @pytest.mark.asyncio
    async def test_with_standard_table_names(self) -> None:
        """Test with typical table names."""
        # Arrange
        args = ListTablesArgs(database="production_db", schema="public")
        effect_handler = MockEffectHandler(tables=["users", "orders", "products"])

        # Act
        result = await handle_list_tables(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Table list for schema 'production_db.public':" in result[0].text
        assert "- users" in result[0].text
        assert "- orders" in result[0].text
        assert "- products" in result[0].text

    @pytest.mark.asyncio
    async def test_single_table(self) -> None:
        """Test with single table result."""
        # Arrange
        args = ListTablesArgs(database="single_db", schema="single_schema")
        effect_handler = MockEffectHandler(tables=["ONLY_TABLE"])

        # Act
        result = await handle_list_tables(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Table list for schema 'single_db.single_schema':" in result[0].text
        assert "- ONLY_TABLE" in result[0].text
        # Should only contain one table line
        assert result[0].text.count("- ") == 1

    @pytest.mark.asyncio
    async def test_case_sensitive_table_names(self) -> None:
        """Test with case-sensitive table names."""
        # Arrange
        args = ListTablesArgs(database="case_db", schema="case_schema")
        effect_handler = MockEffectHandler(tables=["MyTable", "my_table", "MY_TABLE"])

        # Act
        result = await handle_list_tables(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Table list for schema 'case_db.case_schema':" in result[0].text
        assert "- MyTable" in result[0].text
        assert "- my_table" in result[0].text
        assert "- MY_TABLE" in result[0].text
