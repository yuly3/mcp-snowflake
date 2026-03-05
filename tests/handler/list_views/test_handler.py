import pytest
from pydantic import ValidationError

from kernel.table_metadata import DataBase, Schema, View
from mcp_snowflake.handler import ListViewsArgs, handle_list_views

from ...mock_effect_handler import MockListViews


class TestListViewsArgs:
    """Test ListViewsArgs validation."""

    def test_valid_args(self) -> None:
        """Test valid arguments."""
        args = ListViewsArgs.model_validate({"database": "test_db", "schema": "test_schema"})
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
        args = ListViewsArgs.model_validate({"database": "", "schema": "test_schema"})
        assert args.database == DataBase("")
        assert args.schema_ == "test_schema"

    def test_empty_schema(self) -> None:
        """Test empty schema string."""
        args = ListViewsArgs.model_validate({"database": "test_db", "schema": ""})
        assert args.database == "test_db"
        assert args.schema_ == Schema("")

    def test_valid_filter_contains(self) -> None:
        """Test valid contains filter."""
        args = ListViewsArgs.model_validate({
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
            _ = ListViewsArgs.model_validate({
                "database": "test_db",
                "schema": "test_schema",
                "filter": {"type": "starts_with", "value": "ord"},
            })

    def test_invalid_filter_empty_value(self) -> None:
        """Test filter with empty value."""
        with pytest.raises(ValidationError):
            _ = ListViewsArgs.model_validate({
                "database": "test_db",
                "schema": "test_schema",
                "filter": {"type": "contains", "value": ""},
            })


class TestHandleListViews:
    """Test handle_list_views function."""

    @pytest.mark.asyncio
    async def test_successful_list_views(self) -> None:
        """Test successful view listing returns ListViewsResult."""
        args = ListViewsArgs.model_validate({"database": "test_db", "schema": "test_schema"})
        mock_views = [View("view1"), View("view2"), View("view3")]
        effect_handler = MockListViews(result_data=mock_views)

        result = await handle_list_views(args, effect_handler)

        assert result.database == "test_db"
        assert result.schema == "test_schema"
        assert result.views == ["view1", "view2", "view3"]

    @pytest.mark.asyncio
    async def test_empty_views_list(self) -> None:
        """Test when no views are returned."""
        args = ListViewsArgs.model_validate({"database": "empty_db", "schema": "empty_schema"})
        effect_handler = MockListViews(result_data=[])

        result = await handle_list_views(args, effect_handler)

        assert result.database == "empty_db"
        assert result.schema == "empty_schema"
        assert result.views == []

    @pytest.mark.asyncio
    async def test_effect_handler_exception(self) -> None:
        """Test exception handling from effect handler."""
        args = ListViewsArgs.model_validate({"database": "error_db", "schema": "error_schema"})
        error_message = "Connection failed"
        effect_handler = MockListViews(should_raise=Exception(error_message))

        with pytest.raises(Exception, match=error_message):
            _ = await handle_list_views(args, effect_handler)

    @pytest.mark.asyncio
    async def test_with_standard_view_names(self) -> None:
        """Test with typical view names."""
        args = ListViewsArgs.model_validate({"database": "production_db", "schema": "public"})
        effect_handler = MockListViews(result_data=[View("user_view"), View("order_summary"), View("product_catalog")])

        result = await handle_list_views(args, effect_handler)

        assert result.database == "production_db"
        assert result.schema == "public"
        assert result.views == ["user_view", "order_summary", "product_catalog"]

    @pytest.mark.asyncio
    async def test_single_view(self) -> None:
        """Test with single view result."""
        args = ListViewsArgs.model_validate({"database": "single_db", "schema": "single_schema"})
        effect_handler = MockListViews(result_data=[View("ONLY_VIEW")])

        result = await handle_list_views(args, effect_handler)

        assert result.database == "single_db"
        assert result.schema == "single_schema"
        assert result.views == ["ONLY_VIEW"]

    @pytest.mark.asyncio
    async def test_case_sensitive_view_names(self) -> None:
        """Test with case-sensitive view names."""
        args = ListViewsArgs.model_validate({"database": "case_db", "schema": "case_schema"})
        effect_handler = MockListViews(result_data=[View("MyView"), View("my_view"), View("MY_VIEW")])

        result = await handle_list_views(args, effect_handler)

        assert result.database == "case_db"
        assert result.schema == "case_schema"
        assert result.views == ["MyView", "my_view", "MY_VIEW"]

    @pytest.mark.asyncio
    async def test_filter_contains_applies_case_insensitive_match(self) -> None:
        """Test that contains filter is applied case-insensitively."""
        args = ListViewsArgs.model_validate({
            "database": "case_db",
            "schema": "case_schema",
            "filter": {"type": "contains", "value": "ord"},
        })
        effect_handler = MockListViews(result_data=[View("OrderSummary"), View("order_items"), View("CUSTOMERS")])

        result = await handle_list_views(args, effect_handler)

        assert result.views == ["OrderSummary", "order_items"]

    @pytest.mark.asyncio
    async def test_filter_contains_with_no_match(self) -> None:
        """Test contains filter when no names match."""
        args = ListViewsArgs.model_validate({
            "database": "case_db",
            "schema": "case_schema",
            "filter": {"type": "contains", "value": "xyz"},
        })
        effect_handler = MockListViews(result_data=[View("OrderSummary"), View("CUSTOMERS")])

        result = await handle_list_views(args, effect_handler)

        assert result.views == []
