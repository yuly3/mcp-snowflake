"""Tests for list_tables serializers."""

from kernel.table_metadata import DataBase, Schema
from mcp_snowflake.handler.list_tables import (
    CompactListTablesResultSerializer,
    ListTablesResult,
    ListTablesResultSerializer,
)

# ---------------------------------------------------------------------------
# ListTablesResult
# ---------------------------------------------------------------------------


class TestListTablesResult:
    """Test ListTablesResult data class."""

    def test_object_count(self) -> None:
        """object_count should reflect the total number of tables and views."""
        result = ListTablesResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            tables=["A", "B", "C"],
            views=["V1", "V2"],
        )
        assert result.object_count == 5

    def test_object_count_empty(self) -> None:
        """object_count should be 0 for empty result."""
        result = ListTablesResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            tables=[],
            views=[],
        )
        assert result.object_count == 0

    def test_object_count_tables_only(self) -> None:
        """object_count should work with only tables."""
        result = ListTablesResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            tables=["A", "B"],
            views=[],
        )
        assert result.object_count == 2

    def test_object_count_views_only(self) -> None:
        """object_count should work with only views."""
        result = ListTablesResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            tables=[],
            views=["V1"],
        )
        assert result.object_count == 1

    def test_serialize_with_dispatches_to_serializer(self) -> None:
        """serialize_with should call the visitor methods in order."""
        calls: list[str] = []

        class SpySerializer(ListTablesResultSerializer):
            def visit_metadata(self, database: DataBase, schema: Schema, object_count: int) -> None:
                calls.append(f"metadata({database},{schema},{object_count})")

            def visit_tables(self, tables: list[str]) -> None:
                calls.append(f"tables({len(tables)})")

            def visit_views(self, views: list[str]) -> None:
                calls.append(f"views({len(views)})")

            def finish(self) -> str:
                calls.append("finish")
                return "done"

        result = ListTablesResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            tables=["T1", "T2"],
            views=["V1"],
        )
        output = result.serialize_with(SpySerializer())
        assert output == "done"
        assert calls == [
            "metadata(db,sch,3)",
            "tables(2)",
            "views(1)",
            "finish",
        ]


# ---------------------------------------------------------------------------
# CompactListTablesResultSerializer
# ---------------------------------------------------------------------------


class TestCompactListTablesResultSerializer:
    """Test compact text serializer."""

    def test_basic_serialization(self) -> None:
        """Test basic compact output with both tables and views."""
        result = ListTablesResult(
            database=DataBase("MY_DB"),
            schema=Schema("PUBLIC"),
            tables=["CUSTOMERS", "ORDERS", "PRODUCTS"],
            views=["CUSTOMER_VIEW", "ORDER_SUMMARY"],
        )
        serializer = CompactListTablesResultSerializer()
        text = result.serialize_with(serializer)

        expected = "database: MY_DB\nschema: PUBLIC\nobject_count: 5\ntables: CUSTOMERS, ORDERS, PRODUCTS\nviews: CUSTOMER_VIEW, ORDER_SUMMARY"
        assert text == expected

    def test_empty_all(self) -> None:
        """Test compact output when both tables and views are empty."""
        result = ListTablesResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            tables=[],
            views=[],
        )
        serializer = CompactListTablesResultSerializer()
        text = result.serialize_with(serializer)

        expected = "database: db\nschema: sch\nobject_count: 0\ntables: (none)\nviews: (none)"
        assert text == expected

    def test_tables_only(self) -> None:
        """Test compact output with only tables."""
        result = ListTablesResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            tables=["ONLY_TABLE"],
            views=[],
        )
        serializer = CompactListTablesResultSerializer()
        text = result.serialize_with(serializer)

        assert "object_count: 1" in text
        assert "tables: ONLY_TABLE" in text
        assert "views: (none)" in text

    def test_views_only(self) -> None:
        """Test compact output with only views."""
        result = ListTablesResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            tables=[],
            views=["MY_VIEW"],
        )
        serializer = CompactListTablesResultSerializer()
        text = result.serialize_with(serializer)

        assert "object_count: 1" in text
        assert "tables: (none)" in text
        assert "views: MY_VIEW" in text
