import mcp.types as types
import pytest
from pydantic import ValidationError

from mcp_snowflake.handler import ListSchemasArgs, handle_list_schemas


class MockEffectHandler:
    """Mock implementation of _EffectListSchemas protocol."""

    def __init__(
        self,
        schemas: list[str] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.schemas = schemas or []
        self.should_raise = should_raise

    async def list_schemas(self, database: str) -> list[str]:  # noqa: ARG002
        if self.should_raise:
            raise self.should_raise
        return self.schemas


class TestListSchemasArgs:
    """Test ListSchemasArgs validation."""

    def test_valid_args(self) -> None:
        """Test valid arguments."""
        args = ListSchemasArgs(database="test_db")
        assert args.database == "test_db"

    def test_missing_database(self) -> None:
        """Test missing database argument."""
        with pytest.raises(ValidationError):
            _ = ListSchemasArgs.model_validate({})

    def test_empty_database(self) -> None:
        """Test empty database string."""
        args = ListSchemasArgs(database="")
        assert args.database == ""


class TestHandleListSchemas:
    """Test handle_list_schemas function."""

    @pytest.mark.asyncio
    async def test_successful_list_schemas(self) -> None:
        """Test successful schema listing."""
        # Arrange
        args = ListSchemasArgs(database="test_db")
        mock_schemas = ["schema1", "schema2", "schema3"]
        effect_handler = MockEffectHandler(schemas=mock_schemas)

        # Act
        result = await handle_list_schemas(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Schema list for database 'test_db':" in result[0].text
        assert "- schema1" in result[0].text
        assert "- schema2" in result[0].text
        assert "- schema3" in result[0].text

    @pytest.mark.asyncio
    async def test_empty_schemas_list(self) -> None:
        """Test when no schemas are returned."""
        # Arrange
        args = ListSchemasArgs(database="empty_db")
        effect_handler = MockEffectHandler(schemas=[])

        # Act
        result = await handle_list_schemas(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Schema list for database 'empty_db':" in result[0].text
        # Should not contain any schema entries
        assert "- " not in result[0].text

    @pytest.mark.asyncio
    async def test_effect_handler_exception(self) -> None:
        """Test exception handling from effect handler."""
        # Arrange
        args = ListSchemasArgs(database="error_db")
        error_message = "Connection failed"
        effect_handler = MockEffectHandler(should_raise=Exception(error_message))

        # Act
        result = await handle_list_schemas(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Failed to retrieve schemas:" in result[0].text
        assert error_message in result[0].text

    @pytest.mark.asyncio
    async def test_with_standard_snowflake_schemas(self) -> None:
        """Test with typical Snowflake schema names."""
        # Arrange
        args = ListSchemasArgs(database="snowflake_db")
        effect_handler = MockEffectHandler(schemas=["PUBLIC", "INFORMATION_SCHEMA"])

        # Act
        result = await handle_list_schemas(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Schema list for database 'snowflake_db':" in result[0].text
        assert "- PUBLIC" in result[0].text
        assert "- INFORMATION_SCHEMA" in result[0].text

    @pytest.mark.asyncio
    async def test_single_schema(self) -> None:
        """Test with single schema result."""
        # Arrange
        args = ListSchemasArgs(database="single_db")
        effect_handler = MockEffectHandler(schemas=["ONLY_SCHEMA"])

        # Act
        result = await handle_list_schemas(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Schema list for database 'single_db':" in result[0].text
        assert "- ONLY_SCHEMA" in result[0].text
        # Should only contain one schema line
        assert result[0].text.count("- ") == 1
