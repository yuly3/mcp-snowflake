import pytest
from pydantic import ValidationError

from kernel.table_metadata import DataBase, Schema
from mcp_snowflake.handler import ListSchemasArgs, handle_list_schemas

from ._utils import assert_list_output, assert_single_text


class MockEffectHandler:
    """Mock implementation of _EffectListSchemas protocol."""

    def __init__(
        self,
        schemas: list[Schema] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.schemas = schemas or []
        self.should_raise = should_raise

    async def list_schemas(self, database: DataBase) -> list[Schema]:  # noqa: ARG002
        if self.should_raise:
            raise self.should_raise
        return self.schemas


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
        effect_handler = MockEffectHandler(schemas=mock_schemas)

        # Act
        result = await handle_list_schemas(args, effect_handler)

        # Assert
        content = assert_single_text(result)
        assert_list_output(
            content.text,
            "Schema list for database 'test_db':",
            mock_schemas,
        )

    @pytest.mark.asyncio
    async def test_empty_schemas_list(self) -> None:
        """Test when no schemas are returned."""
        # Arrange
        args = ListSchemasArgs(database=DataBase("empty_db"))
        effect_handler = MockEffectHandler(schemas=[])

        # Act
        result = await handle_list_schemas(args, effect_handler)

        # Assert
        content = assert_single_text(result)
        assert "Schema list for database 'empty_db':" in content.text
        # Should not contain any schema entries
        assert "- " not in content.text

    @pytest.mark.asyncio
    async def test_effect_handler_exception(self) -> None:
        """Test exception handling from effect handler."""
        # Arrange
        args = ListSchemasArgs(database=DataBase("error_db"))
        error_message = "Connection failed"
        effect_handler = MockEffectHandler(should_raise=Exception(error_message))

        # Act
        result = await handle_list_schemas(args, effect_handler)

        # Assert
        content = assert_single_text(result)
        assert "Error: Failed to retrieve schemas:" in content.text
        assert error_message in content.text

    @pytest.mark.asyncio
    async def test_with_standard_snowflake_schemas(self) -> None:
        """Test with typical Snowflake schema names."""
        # Arrange
        args = ListSchemasArgs(database=DataBase("snowflake_db"))
        effect_handler = MockEffectHandler(
            schemas=[Schema("PUBLIC"), Schema("INFORMATION_SCHEMA")],
        )

        # Act
        result = await handle_list_schemas(args, effect_handler)

        # Assert
        content = assert_single_text(result)
        assert_list_output(
            content.text,
            "Schema list for database 'snowflake_db':",
            ["PUBLIC", "INFORMATION_SCHEMA"],
        )

    @pytest.mark.asyncio
    async def test_single_schema(self) -> None:
        """Test with single schema result."""
        # Arrange
        args = ListSchemasArgs(database=DataBase("single_db"))
        effect_handler = MockEffectHandler(schemas=[Schema("ONLY_SCHEMA")])

        # Act
        result = await handle_list_schemas(args, effect_handler)

        # Assert
        content = assert_single_text(result)
        assert_list_output(
            content.text,
            "Schema list for database 'single_db':",
            ["ONLY_SCHEMA"],
        )
        # Should only contain one schema line
        assert content.text.count("- ") == 1
