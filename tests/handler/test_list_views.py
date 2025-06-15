import mcp.types as types
import pytest
from pydantic import ValidationError

from mcp_snowflake.handler import ListViewsArgs, handle_list_views


class MockEffectHandler:
    """Mock implementation of _EffectListViews protocol."""

    def __init__(
        self,
        views: list[str] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.views = views or []
        self.should_raise = should_raise

    async def list_views(self, database: str, schema: str) -> list[str]:  # noqa: ARG002
        if self.should_raise:
            raise self.should_raise
        return self.views


class TestListViewsArgs:
    """Test ListViewsArgs validation."""

    def test_valid_args(self) -> None:
        """Test valid arguments."""
        args = ListViewsArgs(database="test_db", schema_name="test_schema")
        assert args.database == "test_db"
        assert args.schema_name == "test_schema"

    def test_missing_database(self) -> None:
        """Test missing database argument."""
        with pytest.raises(ValidationError):
            _ = ListViewsArgs.model_validate({"schema_name": "test_schema"})

    def test_missing_schema_name(self) -> None:
        """Test missing schema_name argument."""
        with pytest.raises(ValidationError):
            _ = ListViewsArgs.model_validate({"database": "test_db"})

    def test_missing_both_args(self) -> None:
        """Test missing both arguments."""
        with pytest.raises(ValidationError):
            _ = ListViewsArgs.model_validate({})

    def test_empty_database(self) -> None:
        """Test empty database string."""
        args = ListViewsArgs(database="", schema_name="test_schema")
        assert args.database == ""
        assert args.schema_name == "test_schema"

    def test_empty_schema_name(self) -> None:
        """Test empty schema_name string."""
        args = ListViewsArgs(database="test_db", schema_name="")
        assert args.database == "test_db"
        assert args.schema_name == ""


class TestHandleListViews:
    """Test handle_list_views function."""

    @pytest.mark.asyncio
    async def test_successful_list_views(self) -> None:
        """Test successful view listing."""
        # Arrange
        args = ListViewsArgs(database="test_db", schema_name="test_schema")
        mock_views = ["view1", "view2", "view3"]
        effect_handler = MockEffectHandler(views=mock_views)

        # Act
        result = await handle_list_views(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "View list for schema 'test_db.test_schema':" in result[0].text
        assert "- view1" in result[0].text
        assert "- view2" in result[0].text
        assert "- view3" in result[0].text

    @pytest.mark.asyncio
    async def test_empty_views_list(self) -> None:
        """Test when no views are returned."""
        # Arrange
        args = ListViewsArgs(database="empty_db", schema_name="empty_schema")
        effect_handler = MockEffectHandler(views=[])

        # Act
        result = await handle_list_views(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "View list for schema 'empty_db.empty_schema':" in result[0].text
        # Should not contain any view entries
        assert "- " not in result[0].text

    @pytest.mark.asyncio
    async def test_effect_handler_exception(self) -> None:
        """Test exception handling from effect handler."""
        # Arrange
        args = ListViewsArgs(database="error_db", schema_name="error_schema")
        error_message = "Connection failed"
        effect_handler = MockEffectHandler(should_raise=Exception(error_message))

        # Act
        result = await handle_list_views(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Failed to retrieve views:" in result[0].text
        assert error_message in result[0].text

    @pytest.mark.asyncio
    async def test_with_standard_view_names(self) -> None:
        """Test with typical view names."""
        # Arrange
        args = ListViewsArgs(database="production_db", schema_name="public")
        effect_handler = MockEffectHandler(
            views=["user_summary", "order_analytics", "product_info"]
        )

        # Act
        result = await handle_list_views(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "View list for schema 'production_db.public':" in result[0].text
        assert "- user_summary" in result[0].text
        assert "- order_analytics" in result[0].text
        assert "- product_info" in result[0].text

    @pytest.mark.asyncio
    async def test_single_view(self) -> None:
        """Test with single view result."""
        # Arrange
        args = ListViewsArgs(database="single_db", schema_name="single_schema")
        effect_handler = MockEffectHandler(views=["ONLY_VIEW"])

        # Act
        result = await handle_list_views(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "View list for schema 'single_db.single_schema':" in result[0].text
        assert "- ONLY_VIEW" in result[0].text
        # Should only contain one view line
        assert result[0].text.count("- ") == 1

    @pytest.mark.asyncio
    async def test_case_sensitive_view_names(self) -> None:
        """Test with case-sensitive view names."""
        # Arrange
        args = ListViewsArgs(database="case_db", schema_name="case_schema")
        effect_handler = MockEffectHandler(views=["MyView", "my_view", "MY_VIEW"])

        # Act
        result = await handle_list_views(args, effect_handler)

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "View list for schema 'case_db.case_schema':" in result[0].text
        assert "- MyView" in result[0].text
        assert "- my_view" in result[0].text
        assert "- MY_VIEW" in result[0].text
