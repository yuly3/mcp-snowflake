import pytest
from pydantic import ValidationError

from kernel.table_metadata import DataBase, Schema
from mcp_snowflake.handler import ListSchemasArgs, handle_list_schemas

from ...mock_effect_handler import MockListSchemas


class TestListSchemasArgs:
    """Test ListSchemasArgs validation."""

    def test_valid_args(self) -> None:
        """Test valid arguments."""
        args = ListSchemasArgs(database=DataBase("test_db"))
        assert args.database == "test_db"

    def test_missing_database(self) -> None:
        """Test missing database argument."""
        with pytest.raises(ValidationError):
            _ = ListSchemasArgs.model_validate({})

    def test_empty_database(self) -> None:
        """Test empty database string."""
        args = ListSchemasArgs(database=DataBase(""))
        assert args.database == DataBase("")


class TestHandleListSchemas:
    """Test handle_list_schemas function."""

    @pytest.mark.asyncio
    async def test_successful_list_schemas(self) -> None:
        """Test successful schema listing returns ListSchemasResult."""
        args = ListSchemasArgs(database=DataBase("test_db"))
        mock_schemas = [Schema("schema1"), Schema("schema2"), Schema("schema3")]
        effect_handler = MockListSchemas(result_data=mock_schemas)

        result = await handle_list_schemas(args, effect_handler)

        assert result.database == "test_db"
        assert result.schemas == ["schema1", "schema2", "schema3"]

    @pytest.mark.asyncio
    async def test_empty_schemas_list(self) -> None:
        """Test when no schemas are returned."""
        args = ListSchemasArgs(database=DataBase("empty_db"))
        effect_handler = MockListSchemas(result_data=[])

        result = await handle_list_schemas(args, effect_handler)

        assert result.database == "empty_db"
        assert result.schemas == []

    @pytest.mark.asyncio
    async def test_effect_handler_exception(self) -> None:
        """Test exception handling from effect handler."""
        args = ListSchemasArgs(database=DataBase("error_db"))
        error_message = "Connection failed"
        effect_handler = MockListSchemas(should_raise=Exception(error_message))

        with pytest.raises(Exception, match=error_message):
            _ = await handle_list_schemas(args, effect_handler)

    @pytest.mark.asyncio
    async def test_with_standard_snowflake_schemas(self) -> None:
        """Test with typical Snowflake schema names."""
        args = ListSchemasArgs(database=DataBase("snowflake_db"))
        effect_handler = MockListSchemas(
            result_data=[Schema("PUBLIC"), Schema("INFORMATION_SCHEMA")],
        )

        result = await handle_list_schemas(args, effect_handler)

        assert result.database == "snowflake_db"
        assert result.schemas == ["PUBLIC", "INFORMATION_SCHEMA"]

    @pytest.mark.asyncio
    async def test_single_schema(self) -> None:
        """Test with single schema result."""
        args = ListSchemasArgs(database=DataBase("single_db"))
        effect_handler = MockListSchemas(result_data=[Schema("ONLY_SCHEMA")])

        result = await handle_list_schemas(args, effect_handler)

        assert result.database == "single_db"
        assert result.schemas == ["ONLY_SCHEMA"]
