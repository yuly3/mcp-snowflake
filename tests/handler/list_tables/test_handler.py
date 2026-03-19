import pytest
from pydantic import ValidationError

from kernel.table_metadata import DataBase, ObjectKind, Schema, SchemaObject
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

    def test_valid_filter_object_type(self) -> None:
        """Test valid object_type filter."""
        args = ListTablesArgs.model_validate({
            "database": "test_db",
            "schema": "test_schema",
            "filter": {"type": "object_type", "value": "TABLE"},
        })
        assert args.filter_ is not None
        assert args.filter_.type_ == "object_type"
        assert args.filter_.value == "TABLE"

    def test_valid_filter_object_type_view(self) -> None:
        """Test valid object_type filter with VIEW."""
        args = ListTablesArgs.model_validate({
            "database": "test_db",
            "schema": "test_schema",
            "filter": {"type": "object_type", "value": "VIEW"},
        })
        assert args.filter_ is not None
        assert args.filter_.type_ == "object_type"
        assert args.filter_.value == "VIEW"

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

    def test_invalid_object_type_value(self) -> None:
        """Test object_type filter with invalid value."""
        with pytest.raises(ValidationError):
            _ = ListTablesArgs.model_validate({
                "database": "test_db",
                "schema": "test_schema",
                "filter": {"type": "object_type", "value": "MATERIALIZED_VIEW"},
            })


class TestHandleListTables:
    """Test handle_list_tables function."""

    @pytest.mark.asyncio
    async def test_successful_list_tables(self) -> None:
        """Test successful listing returns both tables and views."""
        args = ListTablesArgs.model_validate({"database": "test_db", "schema": "test_schema"})
        mock_objects = [
            SchemaObject(name="table1", kind=ObjectKind.TABLE),
            SchemaObject(name="table2", kind=ObjectKind.TABLE),
            SchemaObject(name="view1", kind=ObjectKind.VIEW),
        ]
        effect_handler = MockListTables(result_data=mock_objects)

        result = await handle_list_tables(args, effect_handler)

        assert result.database == "test_db"
        assert result.schema == "test_schema"
        assert result.tables == ["table1", "table2"]
        assert result.views == ["view1"]

    @pytest.mark.asyncio
    async def test_empty_objects_list(self) -> None:
        """Test when no objects are returned."""
        args = ListTablesArgs.model_validate({"database": "empty_db", "schema": "empty_schema"})
        effect_handler = MockListTables(result_data=[])

        result = await handle_list_tables(args, effect_handler)

        assert result.database == "empty_db"
        assert result.schema == "empty_schema"
        assert result.tables == []
        assert result.views == []

    @pytest.mark.asyncio
    async def test_effect_handler_exception(self) -> None:
        """Test exception handling from effect handler."""
        args = ListTablesArgs.model_validate({"database": "error_db", "schema": "error_schema"})
        error_message = "Connection failed"
        effect_handler = MockListTables(should_raise=Exception(error_message))

        with pytest.raises(Exception, match=error_message):
            _ = await handle_list_tables(args, effect_handler)

    @pytest.mark.asyncio
    async def test_with_standard_object_names(self) -> None:
        """Test with typical object names."""
        args = ListTablesArgs.model_validate({"database": "production_db", "schema": "public"})
        effect_handler = MockListTables(
            result_data=[
                SchemaObject(name="users", kind=ObjectKind.TABLE),
                SchemaObject(name="orders", kind=ObjectKind.TABLE),
                SchemaObject(name="user_summary", kind=ObjectKind.VIEW),
            ]
        )

        result = await handle_list_tables(args, effect_handler)

        assert result.database == "production_db"
        assert result.schema == "public"
        assert result.tables == ["users", "orders"]
        assert result.views == ["user_summary"]

    @pytest.mark.asyncio
    async def test_single_table(self) -> None:
        """Test with single table result."""
        args = ListTablesArgs.model_validate({"database": "single_db", "schema": "single_schema"})
        effect_handler = MockListTables(result_data=[SchemaObject(name="ONLY_TABLE", kind=ObjectKind.TABLE)])

        result = await handle_list_tables(args, effect_handler)

        assert result.tables == ["ONLY_TABLE"]
        assert result.views == []

    @pytest.mark.asyncio
    async def test_filter_contains_applies_case_insensitive_match(self) -> None:
        """Test that contains filter is applied case-insensitively."""
        args = ListTablesArgs.model_validate({
            "database": "case_db",
            "schema": "case_schema",
            "filter": {"type": "contains", "value": "ord"},
        })
        effect_handler = MockListTables(
            result_data=[
                SchemaObject(name="Orders", kind=ObjectKind.TABLE),
                SchemaObject(name="order_items", kind=ObjectKind.TABLE),
                SchemaObject(name="CUSTOMERS", kind=ObjectKind.TABLE),
                SchemaObject(name="order_summary", kind=ObjectKind.VIEW),
            ]
        )

        result = await handle_list_tables(args, effect_handler)

        assert result.tables == ["Orders", "order_items"]
        assert result.views == ["order_summary"]

    @pytest.mark.asyncio
    async def test_filter_contains_with_no_match(self) -> None:
        """Test contains filter when no names match."""
        args = ListTablesArgs.model_validate({
            "database": "case_db",
            "schema": "case_schema",
            "filter": {"type": "contains", "value": "xyz"},
        })
        effect_handler = MockListTables(
            result_data=[
                SchemaObject(name="Orders", kind=ObjectKind.TABLE),
                SchemaObject(name="CUSTOMERS", kind=ObjectKind.VIEW),
            ]
        )

        result = await handle_list_tables(args, effect_handler)

        assert result.tables == []
        assert result.views == []

    @pytest.mark.asyncio
    async def test_filter_object_type_table(self) -> None:
        """Test object_type filter for TABLE."""
        args = ListTablesArgs.model_validate({
            "database": "test_db",
            "schema": "test_schema",
            "filter": {"type": "object_type", "value": "TABLE"},
        })
        effect_handler = MockListTables(
            result_data=[
                SchemaObject(name="table1", kind=ObjectKind.TABLE),
                SchemaObject(name="view1", kind=ObjectKind.VIEW),
                SchemaObject(name="table2", kind=ObjectKind.TABLE),
            ]
        )

        result = await handle_list_tables(args, effect_handler)

        assert result.tables == ["table1", "table2"]
        assert result.views == []

    @pytest.mark.asyncio
    async def test_filter_object_type_view(self) -> None:
        """Test object_type filter for VIEW."""
        args = ListTablesArgs.model_validate({
            "database": "test_db",
            "schema": "test_schema",
            "filter": {"type": "object_type", "value": "VIEW"},
        })
        effect_handler = MockListTables(
            result_data=[
                SchemaObject(name="table1", kind=ObjectKind.TABLE),
                SchemaObject(name="view1", kind=ObjectKind.VIEW),
                SchemaObject(name="view2", kind=ObjectKind.VIEW),
            ]
        )

        result = await handle_list_tables(args, effect_handler)

        assert result.tables == []
        assert result.views == ["view1", "view2"]

    @pytest.mark.asyncio
    async def test_object_count(self) -> None:
        """Test object_count reflects total objects."""
        args = ListTablesArgs.model_validate({"database": "db", "schema": "sch"})
        effect_handler = MockListTables(
            result_data=[
                SchemaObject(name="t1", kind=ObjectKind.TABLE),
                SchemaObject(name="t2", kind=ObjectKind.TABLE),
                SchemaObject(name="v1", kind=ObjectKind.VIEW),
            ]
        )

        result = await handle_list_tables(args, effect_handler)

        assert result.object_count == 3
