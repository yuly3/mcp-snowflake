"""Tests for search_columns serializers."""

from kernel.table_metadata import DataBase
from mcp_snowflake.handler.search_columns import (
    CompactSearchColumnsResultSerializer,
    SearchColumnsResult,
    SearchColumnsResultSerializer,
    SearchColumnsTableEntry,
)


class TestSearchColumnsResult:
    """Test SearchColumnsResult data class."""

    def test_table_count(self) -> None:
        result = SearchColumnsResult(
            database=DataBase("db"),
            tables=[
                SearchColumnsTableEntry(schema="S", table="T1", columns_json="[]"),
                SearchColumnsTableEntry(schema="S", table="T2", columns_json="[]"),
            ],
        )
        assert result.table_count == 2

    def test_table_count_empty(self) -> None:
        result = SearchColumnsResult(database=DataBase("db"), tables=[])
        assert result.table_count == 0

    def test_serialize_with_dispatches_to_serializer(self) -> None:
        calls: list[str] = []

        class SpySerializer(SearchColumnsResultSerializer):
            def visit_metadata(self, database: DataBase, table_count: int) -> None:
                calls.append(f"metadata({database},{table_count})")

            def visit_table(self, entry: SearchColumnsTableEntry) -> None:
                calls.append(f"table({entry.schema},{entry.table})")

            def finish(self) -> str:
                calls.append("finish")
                return "done"

        result = SearchColumnsResult(
            database=DataBase("db"),
            tables=[
                SearchColumnsTableEntry(schema="S", table="T", columns_json="[]"),
            ],
        )
        output = result.serialize_with(SpySerializer())
        assert output == "done"
        assert calls == ["metadata(db,1)", "table(S,T)", "finish"]


class TestCompactSearchColumnsResultSerializer:
    """Test compact text serializer."""

    def test_basic_serialization(self) -> None:
        result = SearchColumnsResult(
            database=DataBase("MY_DB"),
            tables=[
                SearchColumnsTableEntry(
                    schema="PUBLIC",
                    table="ORDERS",
                    columns_json='[{"name":"ORDER_ID","type":"NUMBER"}]',
                ),
                SearchColumnsTableEntry(
                    schema="PUBLIC",
                    table="CUSTOMERS",
                    columns_json='[{"name":"ID","type":"NUMBER"},{"name":"UNIT_ID","type":"NUMBER"}]',
                ),
            ],
        )
        serializer = CompactSearchColumnsResultSerializer()
        text = result.serialize_with(serializer)

        expected = """\
database: MY_DB
table_count: 2

schema: PUBLIC
table: ORDERS
columns: [{"name":"ORDER_ID","type":"NUMBER"}]

schema: PUBLIC
table: CUSTOMERS
columns: [{"name":"ID","type":"NUMBER"},{"name":"UNIT_ID","type":"NUMBER"}]"""
        assert text == expected

    def test_empty_results(self) -> None:
        result = SearchColumnsResult(database=DataBase("db"), tables=[])
        serializer = CompactSearchColumnsResultSerializer()
        text = result.serialize_with(serializer)

        assert text == "database: db\ntable_count: 0"

    def test_with_comment(self) -> None:
        result = SearchColumnsResult(
            database=DataBase("db"),
            tables=[
                SearchColumnsTableEntry(
                    schema="S",
                    table="T",
                    columns_json='[{"name":"COL","type":"TEXT","comment":"desc"}]',
                ),
            ],
        )
        serializer = CompactSearchColumnsResultSerializer()
        text = result.serialize_with(serializer)

        assert '"comment":"desc"' in text
