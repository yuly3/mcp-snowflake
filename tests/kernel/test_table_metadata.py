"""Test table metadata domain models."""

import pytest

from mcp_snowflake.kernel.table_metadata import TableColumn, TableInfo


class TestTableColumn:
    """Test TableColumn attrs model."""

    def test_construction_basic(self) -> None:
        """Test basic column construction."""
        column = TableColumn(
            name="id",
            data_type="NUMBER(10,0)",
            nullable=False,
            default_value=None,
            comment=None,
            ordinal_position=1,
        )
        assert column.name == "id"
        assert column.data_type == "NUMBER(10,0)"
        assert column.nullable is False
        assert column.default_value is None
        assert column.comment is None
        assert column.ordinal_position == 1

    def test_construction_with_defaults(self) -> None:
        """Test column construction with comment and default value."""
        column = TableColumn(
            name="created_at",
            data_type="TIMESTAMP_TZ",
            nullable=True,
            default_value="CURRENT_TIMESTAMP()",
            comment="Record creation time",
            ordinal_position=3,
        )
        assert column.name == "created_at"
        assert column.data_type == "TIMESTAMP_TZ"
        assert column.nullable is True
        assert column.default_value == "CURRENT_TIMESTAMP()"
        assert column.comment == "Record creation time"
        assert column.ordinal_position == 3

    def test_immutable_frozen(self) -> None:
        """Test that TableColumn is immutable (frozen)."""
        column = TableColumn(
            name="test",
            data_type="VARCHAR",
            nullable=True,
            ordinal_position=1,
        )
        with pytest.raises(AttributeError):
            column.name = "modified"  # type: ignore[misc]

    def test_slots_no_new_attributes(self) -> None:
        """Test that slots prevent adding new attributes."""
        column = TableColumn(
            name="test",
            data_type="VARCHAR",
            nullable=True,
            ordinal_position=1,
        )
        with pytest.raises(AttributeError):
            column.new_field = "value"  # type: ignore[attr-defined]


class TestTableInfo:
    """Test TableInfo attrs model."""

    def test_construction_basic(self) -> None:
        """Test basic table construction."""
        columns = [
            TableColumn(
                name="id",
                data_type="NUMBER",
                nullable=False,
                ordinal_position=1,
            ),
            TableColumn(
                name="name",
                data_type="VARCHAR",
                nullable=True,
                ordinal_position=2,
            ),
        ]
        table = TableInfo(
            database="test_db",
            schema="test_schema",
            name="test_table",
            column_count=2,
            columns=columns,
        )
        assert table.database == "test_db"
        assert table.schema == "test_schema"
        assert table.name == "test_table"
        assert table.column_count == 2
        assert len(table.columns) == 2
        assert table.columns[0].name == "id"
        assert table.columns[1].name == "name"

    def test_empty_columns(self) -> None:
        """Test table with no columns."""
        table = TableInfo(
            database="empty_db",
            schema="empty_schema",
            name="empty_table",
            column_count=0,
            columns=[],
        )
        assert table.column_count == 0
        assert table.columns == []

    def test_immutable_frozen(self) -> None:
        """Test that TableInfo is immutable (frozen)."""
        table = TableInfo(
            database="db",
            schema="schema",
            name="table",
            column_count=0,
            columns=[],
        )
        with pytest.raises(AttributeError):
            table.database = "modified"  # type: ignore[misc]

    def test_slots_no_new_attributes(self) -> None:
        """Test that slots prevent adding new attributes."""
        table = TableInfo(
            database="db",
            schema="schema",
            name="table",
            column_count=0,
            columns=[],
        )
        with pytest.raises(AttributeError):
            table.new_field = "value"  # type: ignore[attr-defined]
