import pytest
from pydantic import ValidationError

from kernel.table_metadata import DataBase, Schema, Table
from mcp_snowflake.handler import ListTablesArgs, handle_list_tables

from ...mock_effect_handler import MockListTables


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

    def test_valid_filter_contains(self) -> None:
        """Test valid contains filter."""
        args = ListTablesArgs.model_validate({
            "database": "test_db",
            "schema": "test_schema",
            "filter": {"type": "contains", "value": "ord"},
        })
        assert args.filter_ is not None
        assert args.filter_.type_ == "contains"
        assert args.filter_.value == "ord"

    def test_invalid_filter_type(self) -> None:
        """Test invalid filter type."""
        with pytest.raises(ValidationError):
            _ = ListTablesArgs.model_validate({
                "database": "test_db",
                "schema": "test_schema",
                "filter": {"type": "starts_with", "value": "ord"},
            })

    def test_invalid_filter_empty_value(self) -> None:
        """Test filter with empty value."""
        with pytest.raises(ValidationError):
            _ = ListTablesArgs.model_validate({
                "database": "test_db",
                "schema": "test_schema",
                "filter": {"type": "contains", "value": ""},
            })


class TestHandleListTables:
    """Test handle_list_tables function."""

    @pytest.mark.asyncio
    async def test_successful_list_tables(self) -> None:
        """Test successful table listing returns ListTablesResult."""
        args = ListTablesArgs.model_validate({"database": "test_db", "schema": "test_schema"})
        mock_tables = [Table("table1"), Table("table2"), Table("table3")]
        effect_handler = MockListTables(result_data=mock_tables)

        result = await handle_list_tables(args, effect_handler)

        assert result.database == "test_db"
        assert result.schema == "test_schema"
        assert result.tables == ["table1", "table2", "table3"]

    @pytest.mark.asyncio
    async def test_empty_tables_list(self) -> None:
        """Test when no tables are returned."""
        args = ListTablesArgs.model_validate({"database": "empty_db", "schema": "empty_schema"})
        effect_handler = MockListTables(result_data=[])

        result = await handle_list_tables(args, effect_handler)

        assert result.database == "empty_db"
        assert result.schema == "empty_schema"
        assert result.tables == []

    @pytest.mark.asyncio
    async def test_effect_handler_exception(self) -> None:
        """Test exception handling from effect handler."""
        args = ListTablesArgs.model_validate({"database": "error_db", "schema": "error_schema"})
        error_message = "Connection failed"
        effect_handler = MockListTables(should_raise=Exception(error_message))

        with pytest.raises(Exception, match=error_message):
            _ = await handle_list_tables(args, effect_handler)

    @pytest.mark.asyncio
    async def test_with_standard_table_names(self) -> None:
        """Test with typical table names."""
        args = ListTablesArgs.model_validate({"database": "production_db", "schema": "public"})
        effect_handler = MockListTables(result_data=[Table("users"), Table("orders"), Table("products")])

        result = await handle_list_tables(args, effect_handler)

        assert result.database == "production_db"
        assert result.schema == "public"
        assert result.tables == ["users", "orders", "products"]

    @pytest.mark.asyncio
    async def test_single_table(self) -> None:
        """Test with single table result."""
        args = ListTablesArgs.model_validate({"database": "single_db", "schema": "single_schema"})
        effect_handler = MockListTables(result_data=[Table("ONLY_TABLE")])

        result = await handle_list_tables(args, effect_handler)

        assert result.database == "single_db"
        assert result.schema == "single_schema"
        assert result.tables == ["ONLY_TABLE"]

    @pytest.mark.asyncio
    async def test_case_sensitive_table_names(self) -> None:
        """Test with case-sensitive table names."""
        args = ListTablesArgs.model_validate({"database": "case_db", "schema": "case_schema"})
        effect_handler = MockListTables(result_data=[Table("MyTable"), Table("my_table"), Table("MY_TABLE")])

        result = await handle_list_tables(args, effect_handler)

        assert result.database == "case_db"
        assert result.schema == "case_schema"
        assert result.tables == ["MyTable", "my_table", "MY_TABLE"]

    @pytest.mark.asyncio
    async def test_filter_contains_applies_case_insensitive_match(self) -> None:
        """Test that contains filter is applied case-insensitively."""
        args = ListTablesArgs.model_validate({
            "database": "case_db",
            "schema": "case_schema",
            "filter": {"type": "contains", "value": "ord"},
        })
        effect_handler = MockListTables(result_data=[Table("Orders"), Table("order_items"), Table("CUSTOMERS")])

        result = await handle_list_tables(args, effect_handler)

        assert result.tables == ["Orders", "order_items"]

    @pytest.mark.asyncio
    async def test_filter_contains_with_no_match(self) -> None:
        """Test contains filter when no names match."""
        args = ListTablesArgs.model_validate({
            "database": "case_db",
            "schema": "case_schema",
            "filter": {"type": "contains", "value": "xyz"},
        })
        effect_handler = MockListTables(result_data=[Table("Orders"), Table("CUSTOMERS")])

        result = await handle_list_tables(args, effect_handler)

        assert result.tables == []
