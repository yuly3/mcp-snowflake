"""Tests for sample_table_data serializers."""

from collections.abc import Mapping

from cattrs_converter import Jsonable
from kernel.table_metadata import DataBase, Schema, Table
from mcp_snowflake.handler.sample_table_data import (
    CompactSampleTableDataResultSerializer,
    SampleTableDataResult,
    SampleTableDataResultSerializer,
)

# ---------------------------------------------------------------------------
# SampleTableDataResult
# ---------------------------------------------------------------------------


class TestSampleTableDataResult:
    """Test SampleTableDataResult data class."""

    def test_actual_rows(self) -> None:
        """actual_rows should reflect the number of rows."""
        result = SampleTableDataResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            table=Table("tbl"),
            sample_size=10,
            columns=["a", "b"],
            rows=[{"a": 1, "b": 2}, {"a": 3, "b": 4}],
            warnings=[],
        )
        assert result.actual_rows == 2

    def test_actual_rows_empty(self) -> None:
        """actual_rows should be 0 for empty result."""
        result = SampleTableDataResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            table=Table("tbl"),
            sample_size=10,
            columns=[],
            rows=[],
            warnings=[],
        )
        assert result.actual_rows == 0

    def test_serialize_with_dispatches_to_serializer(self) -> None:
        """serialize_with should call the visitor methods in order."""
        calls: list[str] = []

        class SpySerializer(SampleTableDataResultSerializer):
            def visit_metadata(
                self,
                database: DataBase,
                schema: Schema,
                table: Table,
                sample_size: int,
                actual_rows: int,
            ) -> None:
                calls.append(f"metadata({database},{schema},{table},{sample_size},{actual_rows})")

            def visit_row(self, index: int, row: Mapping[str, Jsonable]) -> None:  # noqa: ARG002
                calls.append(f"row({index})")

            def visit_warnings(self, warnings: list[str]) -> None:
                calls.append(f"warnings({len(warnings)})")

            def finish(self) -> str:
                calls.append("finish")
                return "done"

        result = SampleTableDataResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            table=Table("tbl"),
            sample_size=10,
            columns=["a"],
            rows=[{"a": 1}, {"a": 2}],
            warnings=["w1"],
        )
        output = result.serialize_with(SpySerializer())
        assert output == "done"
        assert calls == [
            "metadata(db,sch,tbl,10,2)",
            "row(0)",
            "row(1)",
            "warnings(1)",
            "finish",
        ]


# ---------------------------------------------------------------------------
# CompactSampleTableDataResultSerializer
# ---------------------------------------------------------------------------


class TestCompactSampleTableDataResultSerializer:
    """Test compact text serializer."""

    def test_basic_serialization(self) -> None:
        """Test basic compact output."""
        result = SampleTableDataResult(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            table=Table("users"),
            sample_size=10,
            columns=["id", "name"],
            rows=[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            warnings=[],
        )
        serializer = CompactSampleTableDataResultSerializer()
        text = result.serialize_with(serializer)

        assert "database: test_db" in text
        assert "schema: test_schema" in text
        assert "table: users" in text
        assert "sample_size: 10" in text
        assert "actual_rows: 2" in text
        assert "row1:" in text
        assert 'id: 1\nname: "Alice"' in text
        assert "row2:" in text
        assert 'id: 2\nname: "Bob"' in text
        assert "warnings" not in text

    def test_empty_rows(self) -> None:
        """Test compact output for empty rows."""
        result = SampleTableDataResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            table=Table("tbl"),
            sample_size=10,
            columns=[],
            rows=[],
            warnings=[],
        )
        serializer = CompactSampleTableDataResultSerializer()
        text = result.serialize_with(serializer)

        expected = "database: db\nschema: sch\ntable: tbl\nsample_size: 10\nactual_rows: 0"
        assert text == expected

    def test_with_warnings(self) -> None:
        """Test compact output with warnings."""
        result = SampleTableDataResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            table=Table("tbl"),
            sample_size=5,
            columns=["a"],
            rows=[{"a": 1}],
            warnings=["Column 'x' contains unsupported data type"],
        )
        serializer = CompactSampleTableDataResultSerializer()
        text = result.serialize_with(serializer)

        assert "warnings:" in text
        assert "- Column 'x' contains unsupported data type" in text

    def test_semi_structured_values(self) -> None:
        """Test compact output with dict/list values rendered as JSON."""
        result = SampleTableDataResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            table=Table("tbl"),
            sample_size=5,
            columns=["data", "tags"],
            rows=[{"data": {"key": "val"}, "tags": [1, 2, 3]}],
            warnings=[],
        )
        serializer = CompactSampleTableDataResultSerializer()
        text = result.serialize_with(serializer)

        assert 'data: {"key": "val"}' in text
        assert "tags: [1, 2, 3]" in text

    def test_null_values(self) -> None:
        """Test compact output with null values."""
        result = SampleTableDataResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            table=Table("tbl"),
            sample_size=5,
            columns=["a"],
            rows=[{"a": None}],
            warnings=[],
        )
        serializer = CompactSampleTableDataResultSerializer()
        text = result.serialize_with(serializer)

        assert "a: null" in text
