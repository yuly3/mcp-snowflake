"""Tests for describe_table serializers."""

from kernel.table_metadata import DataBase, Schema, TableColumn
from mcp_snowflake.handler.describe_table import (
    CompactDescribeTableResultSerializer,
    DescribeTableResult,
    DescribeTableResultSerializer,
)

# ---------------------------------------------------------------------------
# DescribeTableResult
# ---------------------------------------------------------------------------


class TestDescribeTableResult:
    """Test DescribeTableResult data class."""

    def test_serialize_with_dispatches_to_serializer(self) -> None:
        """serialize_with should call the visitor methods in order."""
        calls: list[str] = []

        class SpySerializer(DescribeTableResultSerializer):
            def visit_metadata(
                self,
                database: DataBase,
                schema: Schema,
                name: str,
                column_count: int,
            ) -> None:
                calls.append(f"metadata({database},{schema},{name},{column_count})")

            def visit_column(self, index: int, column: TableColumn) -> None:
                calls.append(f"column({index},{column.name})")

            def finish(self) -> str:
                calls.append("finish")
                return "done"

        result = DescribeTableResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            name="tbl",
            column_count=2,
            columns=[
                TableColumn(name="A", data_type="VARCHAR(10)", nullable=True, ordinal_position=1),
                TableColumn(name="B", data_type="NUMBER(38,0)", nullable=False, ordinal_position=2),
            ],
        )
        output = result.serialize_with(SpySerializer())
        assert output == "done"
        assert calls == [
            "metadata(db,sch,tbl,2)",
            "column(0,A)",
            "column(1,B)",
            "finish",
        ]

    def test_serialize_with_empty_columns(self) -> None:
        """serialize_with should work with no columns."""
        calls: list[str] = []

        class SpySerializer(DescribeTableResultSerializer):
            def visit_metadata(
                self,
                database: DataBase,  # noqa: ARG002
                schema: Schema,  # noqa: ARG002
                name: str,  # noqa: ARG002
                column_count: int,  # noqa: ARG002
            ) -> None:
                calls.append("metadata")

            def visit_column(self, index: int, column: TableColumn) -> None:  # noqa: ARG002
                calls.append("column")

            def finish(self) -> str:
                calls.append("finish")
                return "empty"

        result = DescribeTableResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            name="tbl",
            column_count=0,
            columns=[],
        )
        output = result.serialize_with(SpySerializer())
        assert output == "empty"
        assert calls == ["metadata", "finish"]


# ---------------------------------------------------------------------------
# CompactDescribeTableResultSerializer
# ---------------------------------------------------------------------------


class TestCompactDescribeTableResultSerializer:
    """Test compact text serializer."""

    def test_basic_serialization(self) -> None:
        """Test basic compact output."""
        result = DescribeTableResult(
            database=DataBase("test_db"),
            schema=Schema("test_schema"),
            name="test_table",
            column_count=2,
            columns=[
                TableColumn(
                    name="ID",
                    data_type="NUMBER(38,0)",
                    nullable=False,
                    default_value=None,
                    comment="Primary key",
                    ordinal_position=1,
                ),
                TableColumn(
                    name="NAME",
                    data_type="VARCHAR(100)",
                    nullable=True,
                    default_value=None,
                    comment=None,
                    ordinal_position=2,
                ),
            ],
        )
        serializer = CompactDescribeTableResultSerializer()
        text = result.serialize_with(serializer)

        expected = """\
database: test_db
schema: test_schema
table: test_table
column_count: 2

col1:
name: ID
type: NUMBER(38,0)
nullable: false
comment: Primary key

col2:
name: NAME
type: VARCHAR(100)
nullable: true"""
        assert text == expected

    def test_empty_columns(self) -> None:
        """Test compact output for empty columns."""
        result = DescribeTableResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            name="tbl",
            column_count=0,
            columns=[],
        )
        serializer = CompactDescribeTableResultSerializer()
        text = result.serialize_with(serializer)

        assert text == "database: db\nschema: sch\ntable: tbl\ncolumn_count: 0"

    def test_omits_none_default_value(self) -> None:
        """Test compact format omits default_value when None."""
        result = DescribeTableResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            name="tbl",
            column_count=1,
            columns=[
                TableColumn(
                    name="COL",
                    data_type="INTEGER",
                    nullable=True,
                    default_value=None,
                    comment=None,
                    ordinal_position=1,
                ),
            ],
        )
        serializer = CompactDescribeTableResultSerializer()
        text = result.serialize_with(serializer)

        assert "default:" not in text
        assert "comment:" not in text

    def test_includes_default_value_when_present(self) -> None:
        """Test compact format includes default_value when set."""
        result = DescribeTableResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            name="tbl",
            column_count=1,
            columns=[
                TableColumn(
                    name="DATE_COL",
                    data_type="DATE",
                    nullable=True,
                    default_value="CURRENT_DATE",
                    comment="Date column",
                    ordinal_position=1,
                ),
            ],
        )
        serializer = CompactDescribeTableResultSerializer()
        text = result.serialize_with(serializer)

        assert "default: CURRENT_DATE" in text
        assert "comment: Date column" in text

    def test_includes_comment_when_present(self) -> None:
        """Test compact format includes comment when set."""
        result = DescribeTableResult(
            database=DataBase("db"),
            schema=Schema("sch"),
            name="tbl",
            column_count=1,
            columns=[
                TableColumn(
                    name="ID",
                    data_type="NUMBER(38,0)",
                    nullable=False,
                    default_value=None,
                    comment="Primary key",
                    ordinal_position=1,
                ),
            ],
        )
        serializer = CompactDescribeTableResultSerializer()
        text = result.serialize_with(serializer)

        assert "comment: Primary key" in text
