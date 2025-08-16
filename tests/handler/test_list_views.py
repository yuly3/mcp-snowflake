import pytest
from pydantic import ValidationError

from mcp_snowflake.handler import ListViewsArgs, handle_list_views

from ._utils import assert_list_output, assert_single_text


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
        args = ListViewsArgs(database="test_db", schema="test_schema")
        assert args.database == "test_db"
        assert args.schema_ == "test_schema"

    def test_missing_database(self) -> None:
        """Test missing database argument."""
        with pytest.raises(ValidationError):
            _ = ListViewsArgs.model_validate({"schema": "test_schema"})

    def test_missing_schema(self) -> None:
        """Test missing schema argument."""
        with pytest.raises(ValidationError):
            _ = ListViewsArgs.model_validate({"database": "test_db"})

    def test_missing_both_args(self) -> None:
        """Test missing both arguments."""
        with pytest.raises(ValidationError):
            _ = ListViewsArgs.model_validate({})

    def test_empty_database(self) -> None:
        """Test empty database string."""
        args = ListViewsArgs(database="", schema="test_schema")
        assert args.database == ""
        assert args.schema_ == "test_schema"

    def test_empty_schema(self) -> None:
        """Test empty schema string."""
        args = ListViewsArgs(database="test_db", schema="")
        assert args.database == "test_db"
        assert args.schema_ == ""


class TestHandleListViews:
    """Test handle_list_views function."""

    @pytest.mark.asyncio
    async def test_successful_list_views(self) -> None:
        """Test successful view listing."""
        # Arrange
        args = ListViewsArgs(database="test_db", schema="test_schema")
        mock_views = ["view1", "view2", "view3"]
        effect_handler = MockEffectHandler(views=mock_views)

        # Act
        result = await handle_list_views(args, effect_handler)

        # Assert
        content = assert_single_text(result)
        assert_list_output(
            content.text,
            "View list for schema 'test_db.test_schema':",
            mock_views,
        )

    @pytest.mark.asyncio
    async def test_empty_views_list(self) -> None:
        """Test when no views are returned."""
        # Arrange
        args = ListViewsArgs(database="empty_db", schema="empty_schema")
        effect_handler = MockEffectHandler(views=[])

        # Act
        result = await handle_list_views(args, effect_handler)

        # Assert
        content = assert_single_text(result)
        assert "View list for schema 'empty_db.empty_schema':" in content.text
        # Should not contain any view entries
        assert "- " not in content.text

    @pytest.mark.asyncio
    async def test_effect_handler_exception(self) -> None:
        """Test exception handling from effect handler."""
        # Arrange
        args = ListViewsArgs(database="error_db", schema="error_schema")
        error_message = "Connection failed"
        effect_handler = MockEffectHandler(should_raise=Exception(error_message))

        # Act
        result = await handle_list_views(args, effect_handler)

        # Assert
        content = assert_single_text(result)
        assert "Error: Failed to retrieve views:" in content.text
        assert error_message in content.text

    @pytest.mark.asyncio
    async def test_with_standard_view_names(self) -> None:
        """Test with typical view names."""
        # Arrange
        args = ListViewsArgs(database="production_db", schema="public")
        effect_handler = MockEffectHandler(
            views=["user_summary", "order_analytics", "product_info"],
        )

        # Act
        result = await handle_list_views(args, effect_handler)

        # Assert
        content = assert_single_text(result)
        assert_list_output(
            content.text,
            "View list for schema 'production_db.public':",
            ["user_summary", "order_analytics", "product_info"],
        )

    @pytest.mark.asyncio
    async def test_single_view(self) -> None:
        """Test with single view result."""
        # Arrange
        args = ListViewsArgs(database="single_db", schema="single_schema")
        effect_handler = MockEffectHandler(views=["ONLY_VIEW"])

        # Act
        result = await handle_list_views(args, effect_handler)

        # Assert
        content = assert_single_text(result)
        assert_list_output(
            content.text,
            "View list for schema 'single_db.single_schema':",
            ["ONLY_VIEW"],
        )
        # Should only contain one view line
        assert content.text.count("- ") == 1

    @pytest.mark.asyncio
    async def test_case_sensitive_view_names(self) -> None:
        """Test with case-sensitive view names."""
        # Arrange
        args = ListViewsArgs(database="case_db", schema="case_schema")
        effect_handler = MockEffectHandler(views=["MyView", "my_view", "MY_VIEW"])

        # Act
        result = await handle_list_views(args, effect_handler)

        # Assert
        content = assert_single_text(result)
        assert_list_output(
            content.text,
            "View list for schema 'case_db.case_schema':",
            ["MyView", "my_view", "MY_VIEW"],
        )
