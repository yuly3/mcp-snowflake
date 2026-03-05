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

    def test_table_count(self) -> None:
        """table_count should reflect the number of tables."""
        result = ListTablesResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            tables=["A", "B", "C"],
        )
        assert result.table_count == 3

    def test_table_count_empty(self) -> None:
        """table_count should be 0 for empty result."""
        result = ListTablesResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            tables=[],
        )
        assert result.table_count == 0

    def test_serialize_with_dispatches_to_serializer(self) -> None:
        """serialize_with should call the visitor methods in order."""
        calls: list[str] = []

        class SpySerializer(ListTablesResultSerializer):
            def visit_metadata(self, database: DataBase, schema: Schema, table_count: int) -> None:
                calls.append(f"metadata({database},{schema},{table_count})")

            def visit_tables(self, tables: list[str]) -> None:
                calls.append(f"tables({len(tables)})")

            def finish(self) -> str:
                calls.append("finish")
                return "done"

        result = ListTablesResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            tables=["T1", "T2"],
        )
        output = result.serialize_with(SpySerializer())
        assert output == "done"
        assert calls == [
            "metadata(db,sch,2)",
            "tables(2)",
            "finish",
        ]


# ---------------------------------------------------------------------------
# CompactListTablesResultSerializer
# ---------------------------------------------------------------------------


class TestCompactListTablesResultSerializer:
    """Test compact text serializer."""

    def test_basic_serialization(self) -> None:
        """Test basic compact output."""
        result = ListTablesResult(
            database=DataBase("MY_DB"),
            schema=Schema("PUBLIC"),
            tables=["CUSTOMERS", "ORDERS", "PRODUCTS"],
        )
        serializer = CompactListTablesResultSerializer()
        text = result.serialize_with(serializer)

        expected = "database: MY_DB\nschema: PUBLIC\ntable_count: 3\ntables: CUSTOMERS, ORDERS, PRODUCTS"
        assert text == expected

    def test_empty_tables(self) -> None:
        """Test compact output for empty tables."""
        result = ListTablesResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            tables=[],
        )
        serializer = CompactListTablesResultSerializer()
        text = result.serialize_with(serializer)

        expected = "database: db\nschema: sch\ntable_count: 0\ntables: (none)"
        assert text == expected

    def test_single_table(self) -> None:
        """Test compact output for single table."""
        result = ListTablesResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            tables=["ONLY_TABLE"],
        )
        serializer = CompactListTablesResultSerializer()
        text = result.serialize_with(serializer)

        assert "table_count: 1" in text
        assert "tables: ONLY_TABLE" in text
