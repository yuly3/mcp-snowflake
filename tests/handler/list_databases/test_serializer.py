"""Tests for list_databases serializers."""

from mcp_snowflake.handler.list_databases import (
    CompactListDatabasesResultSerializer,
    ListDatabasesResult,
    ListDatabasesResultSerializer,
)

# ---------------------------------------------------------------------------
# ListDatabasesResult
# ---------------------------------------------------------------------------


class TestListDatabasesResult:
    """Test ListDatabasesResult data class."""

    def test_database_count(self) -> None:
        """database_count should reflect the number of databases."""
        result = ListDatabasesResult(databases=["A", "B", "C"])
        assert result.database_count == 3

    def test_database_count_empty(self) -> None:
        """database_count should be 0 for empty result."""
        result = ListDatabasesResult(databases=[])
        assert result.database_count == 0

    def test_serialize_with_dispatches_to_serializer(self) -> None:
        """serialize_with should call the visitor methods in order."""
        calls: list[str] = []

        class SpySerializer(ListDatabasesResultSerializer):
            def visit_metadata(self, database_count: int) -> None:
                calls.append(f"metadata({database_count})")

            def visit_databases(self, databases: list[str]) -> None:
                calls.append(f"databases({len(databases)})")

            def finish(self) -> str:
                calls.append("finish")
                return "done"

        result = ListDatabasesResult(databases=["D1", "D2"])
        output = result.serialize_with(SpySerializer())
        assert output == "done"
        assert calls == [
            "metadata(2)",
            "databases(2)",
            "finish",
        ]


# ---------------------------------------------------------------------------
# CompactListDatabasesResultSerializer
# ---------------------------------------------------------------------------


class TestCompactListDatabasesResultSerializer:
    """Test compact text serializer."""

    def test_basic_serialization(self) -> None:
        """Test basic compact output."""
        result = ListDatabasesResult(
            databases=["MY_DB", "ANALYTICS", "RAW"],
        )
        serializer = CompactListDatabasesResultSerializer()
        text = result.serialize_with(serializer)

        expected = "database_count: 3\ndatabases: MY_DB, ANALYTICS, RAW"
        assert text == expected

    def test_empty_databases(self) -> None:
        """Test compact output for empty databases."""
        result = ListDatabasesResult(databases=[])
        serializer = CompactListDatabasesResultSerializer()
        text = result.serialize_with(serializer)

        expected = "database_count: 0\ndatabases: (none)"
        assert text == expected

    def test_single_database(self) -> None:
        """Test compact output for single database."""
        result = ListDatabasesResult(databases=["ONLY_DB"])
        serializer = CompactListDatabasesResultSerializer()
        text = result.serialize_with(serializer)

        assert "database_count: 1" in text
        assert "databases: ONLY_DB" in text
