"""Test table metadata domain models."""

import pytest

from mcp_snowflake.kernel.data_types import SnowflakeDataType
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
        # After refactor: data_type is SnowflakeDataType
        assert column.data_type.raw_type == "NUMBER(10,0)"
        assert column.data_type.normalized_type == "NUMBER"
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
        # After refactor: data_type is SnowflakeDataType
        assert column.data_type.raw_type == "TIMESTAMP_TZ"
        assert column.data_type.normalized_type == "TIMESTAMP_TZ"
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


class TestTableColumnProperties:
    """Test TableColumn computed properties."""

    def test_data_type_property(self) -> None:
        """Test data_type property is SnowflakeDataType with correct values."""
        column = TableColumn(
            name="price",
            data_type="NUMBER(10,2)",
            nullable=True,
            ordinal_position=1,
        )

        sf_type = column.data_type
        assert sf_type.raw_type == "NUMBER(10,2)"
        assert sf_type.normalized_type == "NUMBER"
        assert sf_type.is_numeric()

    def test_statistics_type_property(self) -> None:
        """Test statistics_type property returns correct StatisticsSupportDataType."""
        column = TableColumn(
            name="name",
            data_type="VARCHAR(100)",
            nullable=True,
            ordinal_position=1,
        )

        stats_type = column.statistics_type
        assert stats_type.type_name == "string"

    @pytest.mark.parametrize(
        (
            "data_type",
            "expected_normalized",
            "expected_stats_type",
            "is_numeric",
            "is_string",
            "is_date",
            "is_boolean",
        ),
        [
            # Numeric types
            ("NUMBER(10,2)", "NUMBER", "numeric", True, False, False, False),
            ("INTEGER", "INT", "numeric", True, False, False, False),
            ("FLOAT", "FLOAT", "numeric", True, False, False, False),
            # String types
            ("VARCHAR(255)", "VARCHAR", "string", False, True, False, False),
            ("CHAR(10)", "CHAR", "string", False, True, False, False),
            ("TEXT", "TEXT", "string", False, True, False, False),
            # Date types
            ("DATE", "DATE", "date", False, False, True, False),
            ("TIMESTAMP_NTZ", "TIMESTAMP_NTZ", "date", False, False, True, False),
            ("DATETIME", "TIMESTAMP_NTZ", "date", False, False, True, False),  # alias
            # Boolean type
            ("BOOLEAN", "BOOLEAN", "boolean", False, False, False, True),
        ],
    )
    def test_property_combinations(
        self,
        data_type: str,
        expected_normalized: str,
        expected_stats_type: str,
        is_numeric: bool,  # noqa: FBT001
        is_string: bool,  # noqa: FBT001
        is_date: bool,  # noqa: FBT001
        is_boolean: bool,  # noqa: FBT001
    ) -> None:
        """Test all property combinations for various data types."""
        column = TableColumn(
            name="test_col",
            data_type=data_type,
            nullable=True,
            ordinal_position=1,
        )

        # Test data_type properties
        sf_type = column.data_type
        assert sf_type.normalized_type == expected_normalized
        assert sf_type.is_numeric() == is_numeric
        assert sf_type.is_string() == is_string
        assert sf_type.is_date() == is_date
        assert sf_type.is_boolean() == is_boolean

        # Test statistics_type property
        stats_type = column.statistics_type
        assert stats_type.type_name == expected_stats_type

    def test_unsupported_data_type_raises_error(self) -> None:
        """Test that unsupported data types raise ValueError when accessing properties."""
        column = TableColumn(
            name="variant_col",
            data_type="VARIANT",
            nullable=True,
            ordinal_position=1,
        )

        # data_type should work (VARIANT is in normalized types)
        sf_type = column.data_type
        assert sf_type.normalized_type == "VARIANT"
        assert not sf_type.is_supported_for_statistics()

        # statistics_type should raise ValueError
        with pytest.raises(
            ValueError, match="Unsupported Snowflake data type for statistics"
        ):
            _ = column.statistics_type

    def test_empty_data_type_raises_error(self) -> None:
        """Test that empty data_type raises ValueError during construction."""
        with pytest.raises(ValueError, match="Unsupported Snowflake data type"):
            _ = TableColumn(
                name="bad_col",
                data_type="",
                nullable=True,
                ordinal_position=1,
            )

    def test_construction_accepts_snowflake_data_type(self) -> None:
        """Test that constructor accepts SnowflakeDataType instances."""
        sf_type = SnowflakeDataType("NUMBER(10,2)")
        column = TableColumn(
            name="price",
            data_type=sf_type,
            nullable=True,
            ordinal_position=1,
        )

        assert column.data_type.raw_type == "NUMBER(10,2)"
        assert column.data_type.normalized_type == "NUMBER"
