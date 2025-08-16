import pytest
from pydantic import ValidationError

from kernel.table_metadata import DataBase, Schema, Table
from mcp_snowflake.handler import ListTablesArgs, handle_list_tables

from ._utils import assert_list_output, assert_single_text


class MockEffectHandler:
    """Mock implementation of _EffectListTables protocol."""

    def __init__(
        self,
        tables: list[Table] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.tables = tables or []
        self.should_raise = should_raise

    async def list_tables(self, database: DataBase, schema: Schema) -> list[Table]:  # noqa: ARG002
        if self.should_raise:
            raise self.should_raise
        return self.tables


class TestListTablesArgs:
    """Test ListTablesArgs validation."""

    def test_valid_args(self) -> None:
        """Test valid arguments."""
        args = ListTablesArgs(
            database=DataBase("test_db"), schema=Schema("test_schema")
        )
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
        args = ListTablesArgs(database=DataBase(""), schema=Schema("test_schema"))
        assert args.database == ""
        assert args.schema_ == "test_schema"

    def test_empty_schema(self) -> None:
        """Test empty schema string."""
        args = ListTablesArgs(database=DataBase("test_db"), schema=Schema(""))
        assert args.database == "test_db"
        assert args.schema_ == ""


class TestHandleListTables:
    """Test handle_list_tables function."""

    @pytest.mark.asyncio
    async def test_successful_list_tables(self) -> None:
        """Test successful table listing."""
        # Arrange
        args = ListTablesArgs(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
        )
        mock_tables = [Table("table1"), Table("table2"), Table("table3")]
        effect_handler = MockEffectHandler(tables=mock_tables)

        # Act
        result = await handle_list_tables(args, effect_handler)

        # Assert
        content = assert_single_text(result)
        assert_list_output(
            content.text,
            "Table list for schema 'test_db.test_schema':",
            mock_tables,
        )

    @pytest.mark.asyncio
    async def test_empty_tables_list(self) -> None:
        """Test when no tables are returned."""
        # Arrange
        args = ListTablesArgs(
            database=DataBase("empty_db"),
            schema=Schema("empty_schema"),
        )
        effect_handler = MockEffectHandler(tables=[])

        # Act
        result = await handle_list_tables(args, effect_handler)

        # Assert
        content = assert_single_text(result)
        assert "Table list for schema 'empty_db.empty_schema':" in content.text
        # Should not contain any table entries
        assert "- " not in content.text

    @pytest.mark.asyncio
    async def test_effect_handler_exception(self) -> None:
        """Test exception handling from effect handler."""
        # Arrange
        args = ListTablesArgs(
            database=DataBase("error_db"),
            schema=Schema("error_schema"),
        )
        error_message = "Connection failed"
        effect_handler = MockEffectHandler(should_raise=Exception(error_message))

        # Act
        result = await handle_list_tables(args, effect_handler)

        # Assert
        content = assert_single_text(result)
        assert "Error: Failed to retrieve tables:" in content.text
        assert error_message in content.text

    @pytest.mark.asyncio
    async def test_with_standard_table_names(self) -> None:
        """Test with typical table names."""
        # Arrange
        args = ListTablesArgs(
            database=DataBase("production_db"),
            schema=Schema("public"),
        )
        effect_handler = MockEffectHandler(
            tables=[Table("users"), Table("orders"), Table("products")]
        )

        # Act
        result = await handle_list_tables(args, effect_handler)

        # Assert
        content = assert_single_text(result)
        assert_list_output(
            content.text,
            "Table list for schema 'production_db.public':",
            ["users", "orders", "products"],
        )

    @pytest.mark.asyncio
    async def test_single_table(self) -> None:
        """Test with single table result."""
        # Arrange
        args = ListTablesArgs(
            database=DataBase("single_db"),
            schema=Schema("single_schema"),
        )
        effect_handler = MockEffectHandler(tables=[Table("ONLY_TABLE")])

        # Act
        result = await handle_list_tables(args, effect_handler)

        # Assert
        content = assert_single_text(result)
        assert_list_output(
            content.text,
            "Table list for schema 'single_db.single_schema':",
            ["ONLY_TABLE"],
        )
        # Should only contain one table line
        assert content.text.count("- ") == 1

    @pytest.mark.asyncio
    async def test_case_sensitive_table_names(self) -> None:
        """Test with case-sensitive table names."""
        # Arrange
        args = ListTablesArgs(
            database=DataBase("case_db"),
            schema=Schema("case_schema"),
        )
        effect_handler = MockEffectHandler(
            tables=[Table("MyTable"), Table("my_table"), Table("MY_TABLE")]
        )

        # Act
        result = await handle_list_tables(args, effect_handler)

        # Assert
        content = assert_single_text(result)
        assert_list_output(
            content.text,
            "Table list for schema 'case_db.case_schema':",
            ["MyTable", "my_table", "MY_TABLE"],
        )
