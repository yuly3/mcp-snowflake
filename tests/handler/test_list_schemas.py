import pytest
from pydantic import ValidationError

from kernel.table_metadata import DataBase, Schema
from mcp_snowflake.handler import ListSchemasArgs, handle_list_schemas

from ..mock_effect_handler import MockListSchemas


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
        """Test successful schema listing."""
        # Arrange
        args = ListSchemasArgs(database=DataBase("test_db"))
        mock_schemas = [Schema("schema1"), Schema("schema2"), Schema("schema3")]
        effect_handler = MockListSchemas(result_data=mock_schemas)

        # Act
        result = await handle_list_schemas(args, effect_handler)

        # Assert
        schemas_info = result["schemas_info"]
        assert schemas_info["database"] == "test_db"
        assert schemas_info["schemas"] == ["schema1", "schema2", "schema3"]

    @pytest.mark.asyncio
    async def test_empty_schemas_list(self) -> None:
        """Test when no schemas are returned."""
        # Arrange
        args = ListSchemasArgs(database=DataBase("empty_db"))
        effect_handler = MockListSchemas(result_data=[])

        # Act
        result = await handle_list_schemas(args, effect_handler)

        # Assert
        schemas_info = result["schemas_info"]
        assert schemas_info["database"] == "empty_db"
        assert schemas_info["schemas"] == []

    @pytest.mark.asyncio
    async def test_effect_handler_exception(self) -> None:
        """Test exception handling from effect handler."""
        # Arrange
        args = ListSchemasArgs(database=DataBase("error_db"))
        error_message = "Connection failed"
        effect_handler = MockListSchemas(should_raise=Exception(error_message))

        # Act
        with pytest.raises(Exception, match=error_message):
            _ = await handle_list_schemas(args, effect_handler)

    @pytest.mark.asyncio
    async def test_with_standard_snowflake_schemas(self) -> None:
        """Test with typical Snowflake schema names."""
        # Arrange
        args = ListSchemasArgs(database=DataBase("snowflake_db"))
        effect_handler = MockListSchemas(
            result_data=[Schema("PUBLIC"), Schema("INFORMATION_SCHEMA")],
        )

        # Act
        result = await handle_list_schemas(args, effect_handler)

        # Assert
        schemas_info = result["schemas_info"]
        assert schemas_info["database"] == "snowflake_db"
        assert schemas_info["schemas"] == ["PUBLIC", "INFORMATION_SCHEMA"]

    @pytest.mark.asyncio
    async def test_single_schema(self) -> None:
        """Test with single schema result."""
        # Arrange
        args = ListSchemasArgs(database=DataBase("single_db"))
        effect_handler = MockListSchemas(result_data=[Schema("ONLY_SCHEMA")])

        # Act
        result = await handle_list_schemas(args, effect_handler)

        # Assert
        schemas_info = result["schemas_info"]
        assert schemas_info["database"] == "single_db"
        assert schemas_info["schemas"] == ["ONLY_SCHEMA"]
