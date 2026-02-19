"""Tests for execute_query serializers (JSON and compact format)."""

import json

import pytest

from mcp_snowflake.handler.execute_query import (
    CompactQueryResultSerializer,
    JsonQueryResultSerializer,
    QueryResult,
    QueryResultSerializer,
)
from mcp_snowflake.handler.execute_query._serializer import _format_compact_value

# ---------------------------------------------------------------------------
# QueryResult
# ---------------------------------------------------------------------------


class TestQueryResult:
    """Test QueryResult data class."""

    def test_row_count(self) -> None:
        """row_count should reflect the number of rows."""
        result = QueryResult(
            execution_time_ms=100,
            columns=["a"],
            rows=[{"a": 1}, {"a": 2}, {"a": 3}],
            warnings=[],
        )
        assert result.row_count == 3

    def test_row_count_empty(self) -> None:
        """row_count should be 0 for empty result."""
        result = QueryResult(
            execution_time_ms=0,
            columns=[],
            rows=[],
            warnings=[],
        )
        assert result.row_count == 0

    def test_serialize_with_dispatches_to_serializer(self) -> None:
        """serialize_with should call the visitor methods in order."""
        calls: list[str] = []

        class SpySerializer(QueryResultSerializer):
            def visit_metadata(self, execution_time_ms: int, row_count: int) -> None:
                calls.append(f"metadata({execution_time_ms},{row_count})")

            def visit_row(self, index: int, _row: dict) -> None:  # type: ignore[override]
                calls.append(f"row({index})")

            def visit_warnings(self, warnings: list[str]) -> None:
                calls.append(f"warnings({len(warnings)})")

            def finish(self) -> str:
                calls.append("finish")
                return "done"

        result = QueryResult(
            execution_time_ms=42,
            columns=["x"],
            rows=[{"x": 1}, {"x": 2}],
            warnings=["w1"],
        )
        output = result.serialize_with(SpySerializer())
        assert output == "done"
        assert calls == [
            "metadata(42,2)",
            "row(0)",
            "row(1)",
            "warnings(1)",
            "finish",
        ]


# ---------------------------------------------------------------------------
# JsonQueryResultSerializer
# ---------------------------------------------------------------------------


class TestJsonQueryResultSerializer:
    """Test JSON serializer."""

    def test_basic_serialization(self) -> None:
        """Test basic JSON output structure."""
        result = QueryResult(
            execution_time_ms=123,
            columns=["id", "name"],
            rows=[
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ],
            warnings=[],
        )
        serializer = JsonQueryResultSerializer(result.columns)
        text = result.serialize_with(serializer)
        parsed = json.loads(text)

        assert "query_result" in parsed
        qr = parsed["query_result"]
        assert qr["execution_time_ms"] == 123
        assert qr["row_count"] == 2
        assert qr["columns"] == ["id", "name"]
        assert len(qr["rows"]) == 2
        assert qr["rows"][0] == {"id": 1, "name": "Alice"}
        assert qr["warnings"] == []

    def test_empty_result(self) -> None:
        """Test JSON output for empty result."""
        result = QueryResult(
            execution_time_ms=0,
            columns=[],
            rows=[],
            warnings=[],
        )
        serializer = JsonQueryResultSerializer(result.columns)
        text = result.serialize_with(serializer)
        parsed = json.loads(text)

        qr = parsed["query_result"]
        assert qr["row_count"] == 0
        assert qr["rows"] == []

    def test_with_warnings(self) -> None:
        """Test JSON output includes warnings."""
        result = QueryResult(
            execution_time_ms=10,
            columns=["a"],
            rows=[{"a": "<unsupported_type: complex>"}],
            warnings=["Column 'a' contains unsupported data type"],
        )
        serializer = JsonQueryResultSerializer(result.columns)
        text = result.serialize_with(serializer)
        parsed = json.loads(text)

        assert parsed["query_result"]["warnings"] == ["Column 'a' contains unsupported data type"]

    def test_with_semi_structured_data(self) -> None:
        """Test JSON output handles dicts/lists in values."""
        result = QueryResult(
            execution_time_ms=5,
            columns=["data"],
            rows=[{"data": {"key": "value", "nested": [1, 2]}}],
            warnings=[],
        )
        serializer = JsonQueryResultSerializer(result.columns)
        text = result.serialize_with(serializer)
        parsed = json.loads(text)

        assert parsed["query_result"]["rows"][0]["data"] == {
            "key": "value",
            "nested": [1, 2],
        }


# ---------------------------------------------------------------------------
# CompactQueryResultSerializer
# ---------------------------------------------------------------------------


class TestCompactQueryResultSerializer:
    """Test compact text serializer."""

    def test_basic_serialization(self) -> None:
        """Test basic compact output."""
        result = QueryResult(
            execution_time_ms=1234,
            columns=["col1", "col2"],
            rows=[
                {"col1": "value1", "col2": "value2"},
                {"col1": "value3", "col2": "value4"},
            ],
            warnings=[],
        )
        serializer = CompactQueryResultSerializer()
        text = result.serialize_with(serializer)

        expected = 'execution_time_ms: 1234\nrow_count: 2\n\nrow1:\ncol1: "value1"\ncol2: "value2"\n\nrow2:\ncol1: "value3"\ncol2: "value4"'
        assert text == expected

    def test_empty_result(self) -> None:
        """Test compact output for empty result."""
        result = QueryResult(
            execution_time_ms=0,
            columns=[],
            rows=[],
            warnings=[],
        )
        serializer = CompactQueryResultSerializer()
        text = result.serialize_with(serializer)

        assert text == "execution_time_ms: 0\nrow_count: 0"

    def test_single_row(self) -> None:
        """Test compact output for single row."""
        result = QueryResult(
            execution_time_ms=50,
            columns=["count"],
            rows=[{"count": 42}],
            warnings=[],
        )
        serializer = CompactQueryResultSerializer()
        text = result.serialize_with(serializer)

        expected = "execution_time_ms: 50\nrow_count: 1\n\nrow1:\ncount: 42"
        assert text == expected

    def test_with_semi_structured_data(self) -> None:
        """Test compact format renders semi-structured data as inline JSON."""
        result = QueryResult(
            execution_time_ms=10,
            columns=["name", "metadata"],
            rows=[
                {"name": "Alice", "metadata": {"role": "admin", "tags": ["a", "b"]}},
            ],
            warnings=[],
        )
        serializer = CompactQueryResultSerializer()
        text = result.serialize_with(serializer)

        lines = text.split("\n")
        # Find the metadata line
        metadata_line = next(line for line in lines if line.startswith("metadata:"))
        # Should be JSON
        json_part = metadata_line[len("metadata: ") :]
        parsed = json.loads(json_part)
        assert parsed == {"role": "admin", "tags": ["a", "b"]}

    def test_with_list_value(self) -> None:
        """Test compact format renders list values as JSON."""
        result = QueryResult(
            execution_time_ms=5,
            columns=["items"],
            rows=[{"items": [1, 2, 3]}],
            warnings=[],
        )
        serializer = CompactQueryResultSerializer()
        text = result.serialize_with(serializer)

        assert "items: [1, 2, 3]" in text

    def test_with_null_value(self) -> None:
        """Test compact format renders None as null."""
        result = QueryResult(
            execution_time_ms=5,
            columns=["val"],
            rows=[{"val": None}],
            warnings=[],
        )
        serializer = CompactQueryResultSerializer()
        text = result.serialize_with(serializer)

        assert "val: null" in text

    def test_with_boolean_values(self) -> None:
        """Test compact format renders booleans as lowercase."""
        result = QueryResult(
            execution_time_ms=5,
            columns=["flag"],
            rows=[{"flag": True}, {"flag": False}],
            warnings=[],
        )
        serializer = CompactQueryResultSerializer()
        text = result.serialize_with(serializer)

        assert "flag: true" in text
        assert "flag: false" in text

    def test_with_warnings(self) -> None:
        """Test compact output includes warnings section."""
        result = QueryResult(
            execution_time_ms=10,
            columns=["a"],
            rows=[{"a": "<unsupported_type: complex>"}],
            warnings=["Column 'a' contains unsupported data type"],
        )
        serializer = CompactQueryResultSerializer()
        text = result.serialize_with(serializer)

        assert "\nwarnings:\n- Column 'a' contains unsupported data type" in text

    def test_with_multiline_string_value(self) -> None:
        """Test compact format escapes multiline string values."""
        result = QueryResult(
            execution_time_ms=7,
            columns=["note"],
            rows=[{"note": "line1\nline2"}],
            warnings=[],
        )
        serializer = CompactQueryResultSerializer()
        text = result.serialize_with(serializer)

        assert 'note: "line1\\nline2"' in text

    def test_numeric_values(self) -> None:
        """Test compact format renders numeric values correctly."""
        result = QueryResult(
            execution_time_ms=5,
            columns=["int_val", "float_val"],
            rows=[{"int_val": 42, "float_val": 3.14}],
            warnings=[],
        )
        serializer = CompactQueryResultSerializer()
        text = result.serialize_with(serializer)

        assert "int_val: 42" in text
        assert "float_val: 3.14" in text


# ---------------------------------------------------------------------------
# _format_compact_value helper
# ---------------------------------------------------------------------------


class TestFormatCompactValue:
    """Test _format_compact_value helper."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (42, "42"),
            (3.14, "3.14"),
            ("hello", '"hello"'),
            (True, "true"),
            (False, "false"),
            (None, "null"),
        ],
    )
    def test_scalar_values(self, value: object, expected: str) -> None:
        """Scalar values should use natural string representation."""
        assert _format_compact_value(value) == expected  # type: ignore[arg-type]

    def test_dict_value(self) -> None:
        """Dict values should be rendered as JSON."""
        assert _format_compact_value({"a": 1}) == '{"a": 1}'

    def test_list_value(self) -> None:
        """List values should be rendered as JSON."""
        assert _format_compact_value([1, 2, 3]) == "[1, 2, 3]"

    def test_nested_structure(self) -> None:
        """Nested structures should be fully JSON-encoded."""
        val: dict[str, object] = {"a": {"b": [1, 2]}}
        result = _format_compact_value(val)  # type: ignore[arg-type]
        parsed = json.loads(result)
        assert parsed == {"a": {"b": [1, 2]}}
