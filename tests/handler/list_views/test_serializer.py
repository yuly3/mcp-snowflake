"""Tests for list_views serializers."""

from kernel.table_metadata import DataBase, Schema
from mcp_snowflake.handler.list_views import (
    CompactListViewsResultSerializer,
    ListViewsResult,
    ListViewsResultSerializer,
)

# ---------------------------------------------------------------------------
# ListViewsResult
# ---------------------------------------------------------------------------


class TestListViewsResult:
    """Test ListViewsResult data class."""

    def test_view_count(self) -> None:
        """view_count should reflect the number of views."""
        result = ListViewsResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            views=["A", "B", "C"],
        )
        assert result.view_count == 3

    def test_view_count_empty(self) -> None:
        """view_count should be 0 for empty result."""
        result = ListViewsResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            views=[],
        )
        assert result.view_count == 0

    def test_serialize_with_dispatches_to_serializer(self) -> None:
        """serialize_with should call the visitor methods in order."""
        calls: list[str] = []

        class SpySerializer(ListViewsResultSerializer):
            def visit_metadata(self, database: DataBase, schema: Schema, view_count: int) -> None:
                calls.append(f"metadata({database},{schema},{view_count})")

            def visit_views(self, views: list[str]) -> None:
                calls.append(f"views({len(views)})")

            def finish(self) -> str:
                calls.append("finish")
                return "done"

        result = ListViewsResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            views=["V1", "V2"],
        )
        output = result.serialize_with(SpySerializer())
        assert output == "done"
        assert calls == [
            "metadata(db,sch,2)",
            "views(2)",
            "finish",
        ]


# ---------------------------------------------------------------------------
# CompactListViewsResultSerializer
# ---------------------------------------------------------------------------


class TestCompactListViewsResultSerializer:
    """Test compact text serializer."""

    def test_basic_serialization(self) -> None:
        """Test basic compact output."""
        result = ListViewsResult(
            database=DataBase("MY_DB"),
            schema=Schema("PUBLIC"),
            views=["CUSTOMER_VIEW", "ORDER_SUMMARY", "PRODUCT_CATALOG"],
        )
        serializer = CompactListViewsResultSerializer()
        text = result.serialize_with(serializer)

        expected = (
            "database: MY_DB\nschema: PUBLIC\nview_count: 3\nviews: CUSTOMER_VIEW, ORDER_SUMMARY, PRODUCT_CATALOG"
        )
        assert text == expected

    def test_empty_views(self) -> None:
        """Test compact output for empty views."""
        result = ListViewsResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            views=[],
        )
        serializer = CompactListViewsResultSerializer()
        text = result.serialize_with(serializer)

        expected = "database: db\nschema: sch\nview_count: 0\nviews: (none)"
        assert text == expected

    def test_single_view(self) -> None:
        """Test compact output for single view."""
        result = ListViewsResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            views=["ONLY_VIEW"],
        )
        serializer = CompactListViewsResultSerializer()
        text = result.serialize_with(serializer)

        assert "view_count: 1" in text
        assert "views: ONLY_VIEW" in text
