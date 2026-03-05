"""Tests for list_schemas serializers."""

from kernel.table_metadata import DataBase
from mcp_snowflake.handler.list_schemas import (
    CompactListSchemasResultSerializer,
    ListSchemasResult,
    ListSchemasResultSerializer,
)

# ---------------------------------------------------------------------------
# ListSchemasResult
# ---------------------------------------------------------------------------


class TestListSchemasResult:
    """Test ListSchemasResult data class."""

    def test_schema_count(self) -> None:
        """schema_count should reflect the number of schemas."""
        result = ListSchemasResult(
            database=DataBase("db"),
            schemas=["A", "B", "C"],
        )
        assert result.schema_count == 3

    def test_schema_count_empty(self) -> None:
        """schema_count should be 0 for empty result."""
        result = ListSchemasResult(
            database=DataBase("db"),
            schemas=[],
        )
        assert result.schema_count == 0

    def test_serialize_with_dispatches_to_serializer(self) -> None:
        """serialize_with should call the visitor methods in order."""
        calls: list[str] = []

        class SpySerializer(ListSchemasResultSerializer):
            def visit_metadata(self, database: DataBase, schema_count: int) -> None:
                calls.append(f"metadata({database},{schema_count})")

            def visit_schemas(self, schemas: list[str]) -> None:
                calls.append(f"schemas({len(schemas)})")

            def finish(self) -> str:
                calls.append("finish")
                return "done"

        result = ListSchemasResult(
            database=DataBase("db"),
            schemas=["S1", "S2"],
        )
        output = result.serialize_with(SpySerializer())
        assert output == "done"
        assert calls == [
            "metadata(db,2)",
            "schemas(2)",
            "finish",
        ]


# ---------------------------------------------------------------------------
# CompactListSchemasResultSerializer
# ---------------------------------------------------------------------------


class TestCompactListSchemasResultSerializer:
    """Test compact text serializer."""

    def test_basic_serialization(self) -> None:
        """Test basic compact output."""
        result = ListSchemasResult(
            database=DataBase("MY_DB"),
            schemas=["PUBLIC", "INFORMATION_SCHEMA", "RAW"],
        )
        serializer = CompactListSchemasResultSerializer()
        text = result.serialize_with(serializer)

        expected = "database: MY_DB\nschema_count: 3\nschemas: PUBLIC, INFORMATION_SCHEMA, RAW"
        assert text == expected

    def test_empty_schemas(self) -> None:
        """Test compact output for empty schemas."""
        result = ListSchemasResult(
            database=DataBase("db"),
            schemas=[],
        )
        serializer = CompactListSchemasResultSerializer()
        text = result.serialize_with(serializer)

        expected = "database: db\nschema_count: 0\nschemas: (none)"
        assert text == expected

    def test_single_schema(self) -> None:
        """Test compact output for single schema."""
        result = ListSchemasResult(
            database=DataBase("db"),
            schemas=["ONLY_SCHEMA"],
        )
        serializer = CompactListSchemasResultSerializer()
        text = result.serialize_with(serializer)

        assert "schema_count: 1" in text
        assert "schemas: ONLY_SCHEMA" in text
