"""Tests for StatisticsSupportColumn class."""

import attrs
import pytest

from mcp_snowflake.kernel.statistics_support_column import StatisticsSupportColumn
from mcp_snowflake.kernel.table_metadata import TableColumn


class TestStatisticsSupportColumn:
    """Test StatisticsSupportColumn class."""

    def test_from_table_column_numeric_success(self) -> None:
        """Test successful conversion from numeric TableColumn."""
        col = TableColumn(
            name="price",
            data_type="NUMBER(10,2)",
            nullable=True,
            ordinal_position=1,
        )

        result = StatisticsSupportColumn.from_table_column(col)

        assert result is not None
        assert result.name == "price"
        assert result.nullable is True
        assert result.ordinal_position == 1
        assert result.statistics_type.type_name == "numeric"
        assert result.data_type.raw_type == "NUMBER(10,2)"

    def test_from_table_column_string_success(self) -> None:
        """Test successful conversion from string TableColumn."""
        col = TableColumn(
            name="name",
            data_type="VARCHAR(100)",
            nullable=False,
            ordinal_position=2,
        )

        result = StatisticsSupportColumn.from_table_column(col)

        assert result is not None
        assert result.name == "name"
        assert result.nullable is False
        assert result.ordinal_position == 2
        assert result.statistics_type.type_name == "string"

    def test_from_table_column_date_success(self) -> None:
        """Test successful conversion from date TableColumn."""
        col = TableColumn(
            name="created_at",
            data_type="TIMESTAMP_NTZ",
            nullable=True,
            ordinal_position=3,
        )

        result = StatisticsSupportColumn.from_table_column(col)

        assert result is not None
        assert result.name == "created_at"
        assert result.statistics_type.type_name == "date"

    def test_from_table_column_boolean_success(self) -> None:
        """Test successful conversion from boolean TableColumn."""
        col = TableColumn(
            name="is_active",
            data_type="BOOLEAN",
            nullable=True,
            ordinal_position=4,
        )

        result = StatisticsSupportColumn.from_table_column(col)

        assert result is not None
        assert result.name == "is_active"
        assert result.statistics_type.type_name == "boolean"

    def test_from_table_column_unsupported_returns_none(self) -> None:
        """Test conversion from unsupported TableColumn returns None."""
        col = TableColumn(
            name="metadata",
            data_type="VARIANT",
            nullable=True,
            ordinal_position=5,
        )

        result = StatisticsSupportColumn.from_table_column(col)

        assert result is None

    @pytest.mark.parametrize(
        "unsupported_type",
        ["VARIANT", "OBJECT", "GEOGRAPHY", "BINARY"],
    )
    def test_from_table_column_various_unsupported_types(
        self,
        unsupported_type: str,
    ) -> None:
        """Test conversion from various unsupported types returns None."""
        col = TableColumn(
            name=f"col_{unsupported_type.lower()}",
            data_type=unsupported_type,
            nullable=True,
            ordinal_position=1,
        )

        result = StatisticsSupportColumn.from_table_column(col)

        assert result is None

    def test_property_delegation(self) -> None:
        """Test that properties are correctly delegated to base column."""
        col = TableColumn(
            name="test_col",
            data_type="INTEGER",
            nullable=False,
            ordinal_position=42,
        )

        stats_col = StatisticsSupportColumn.from_table_column(col)

        assert stats_col is not None
        assert stats_col.name == col.name
        assert stats_col.data_type == col.data_type
        assert stats_col.nullable == col.nullable
        assert stats_col.ordinal_position == col.ordinal_position
        assert stats_col.base == col

    def test_statistics_type_is_non_optional(self) -> None:
        """Test that statistics_type is guaranteed to be non-None."""
        col = TableColumn(
            name="revenue",
            data_type="DECIMAL(15,2)",
            nullable=True,
            ordinal_position=1,
        )

        stats_col = StatisticsSupportColumn.from_table_column(col)

        assert stats_col is not None
        # This should not require Optional type checks
        assert stats_col.statistics_type.type_name == "numeric"
        # No need for: if stats_col.statistics_type is not None: ...

    def test_immutable_attrs(self) -> None:
        """Test that StatisticsSupportColumn is immutable."""
        col = TableColumn(
            name="test",
            data_type="VARCHAR(50)",
            nullable=True,
            ordinal_position=1,
        )

        stats_col = StatisticsSupportColumn.from_table_column(col)

        assert stats_col is not None
        # Should be frozen (immutable) - test one attribute assignment
        with pytest.raises(attrs.exceptions.FrozenInstanceError):
            stats_col.base = col  # type: ignore[misc]
