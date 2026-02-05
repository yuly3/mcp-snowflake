import pytest
from pydantic import ValidationError

from kernel.table_metadata import DataBase, Schema, Table
from mcp_snowflake.handler import ListTablesArgs, handle_list_tables

from ..mock_effect_handler import MockListTables


class TestListTablesArgs:
    """Test ListTablesArgs validation."""

    def test_valid_args(self) -> None:
        """Test valid arguments."""
        args = ListTablesArgs.model_validate({"database": "test_db", "schema": "test_schema"})
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
        args = ListTablesArgs.model_validate({"database": "", "schema": "test_schema"})
        assert args.database == DataBase("")
        assert args.schema_ == "test_schema"

    def test_empty_schema(self) -> None:
        """Test empty schema string."""
        args = ListTablesArgs.model_validate({"database": "test_db", "schema": ""})
        assert args.database == "test_db"
        assert args.schema_ == Schema("")


class TestHandleListTables:
    """Test handle_list_tables function."""

    @pytest.mark.asyncio
    async def test_successful_list_tables(self) -> None:
        """Test successful table listing."""
        # Arrange
        args = ListTablesArgs.model_validate({"database": "test_db", "schema": "test_schema"})
        mock_tables = [Table("table1"), Table("table2"), Table("table3")]
        effect_handler = MockListTables(result_data=mock_tables)

        # Act
        result = await handle_list_tables(args, effect_handler)

        # Assert
        tables_info = result["tables_info"]
        assert tables_info["database"] == "test_db"
        assert tables_info["schema"] == "test_schema"
        assert tables_info["tables"] == ["table1", "table2", "table3"]

    @pytest.mark.asyncio
    async def test_empty_tables_list(self) -> None:
        """Test when no tables are returned."""
        # Arrange
        args = ListTablesArgs.model_validate({"database": "empty_db", "schema": "empty_schema"})
        effect_handler = MockListTables(result_data=[])

        # Act
        result = await handle_list_tables(args, effect_handler)

        # Assert
        tables_info = result["tables_info"]
        assert tables_info["database"] == "empty_db"
        assert tables_info["schema"] == "empty_schema"
        assert tables_info["tables"] == []

    @pytest.mark.asyncio
    async def test_effect_handler_exception(self) -> None:
        """Test exception handling from effect handler."""
        # Arrange
        args = ListTablesArgs.model_validate({"database": "error_db", "schema": "error_schema"})
        error_message = "Connection failed"
        effect_handler = MockListTables(should_raise=Exception(error_message))

        # Act
        with pytest.raises(Exception, match=error_message):
            _ = await handle_list_tables(args, effect_handler)

    @pytest.mark.asyncio
    async def test_with_standard_table_names(self) -> None:
        """Test with typical table names."""
        # Arrange
        args = ListTablesArgs.model_validate({"database": "production_db", "schema": "public"})
        effect_handler = MockListTables(result_data=[Table("users"), Table("orders"), Table("products")])

        # Act
        result = await handle_list_tables(args, effect_handler)

        # Assert
        tables_info = result["tables_info"]
        assert tables_info["database"] == "production_db"
        assert tables_info["schema"] == "public"
        assert tables_info["tables"] == ["users", "orders", "products"]

    @pytest.mark.asyncio
    async def test_single_table(self) -> None:
        """Test with single table result."""
        # Arrange
        args = ListTablesArgs.model_validate({"database": "single_db", "schema": "single_schema"})
        effect_handler = MockListTables(result_data=[Table("ONLY_TABLE")])

        # Act
        result = await handle_list_tables(args, effect_handler)

        # Assert
        tables_info = result["tables_info"]
        assert tables_info["database"] == "single_db"
        assert tables_info["schema"] == "single_schema"
        assert tables_info["tables"] == ["ONLY_TABLE"]

    @pytest.mark.asyncio
    async def test_case_sensitive_table_names(self) -> None:
        """Test with case-sensitive table names."""
        # Arrange
        args = ListTablesArgs.model_validate({"database": "case_db", "schema": "case_schema"})
        effect_handler = MockListTables(result_data=[Table("MyTable"), Table("my_table"), Table("MY_TABLE")])

        # Act
        result = await handle_list_tables(args, effect_handler)

        # Assert
        tables_info = result["tables_info"]
        assert tables_info["database"] == "case_db"
        assert tables_info["schema"] == "case_schema"
        assert tables_info["tables"] == ["MyTable", "my_table", "MY_TABLE"]
