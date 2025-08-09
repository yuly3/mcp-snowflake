"""Tests for _types module - ColumnInfo class."""

from typing import Any

import pytest

from mcp_snowflake.handler.analyze_table_statistics._types import (
    BooleanStatsDict,
    ColumnInfo,
)
from mcp_snowflake.kernel import SnowflakeDataType, StatisticsSupportDataType


class TestColumnInfo:
    """Tests for ColumnInfo class."""

    def test_init_with_valid_data(self) -> None:
        """Test initialization with valid data."""
        sf_type = SnowflakeDataType("VARCHAR(255)")
        stats_type = StatisticsSupportDataType("string")

        col_info = ColumnInfo(
            name="test_column",
            snowflake_type=sf_type,
            statistics_type=stats_type,
        )

        assert col_info.name == "test_column"
        assert col_info.snowflake_type == sf_type
        assert col_info.statistics_type == stats_type

    def test_column_type_property_backward_compatibility(self) -> None:
        """Test column_type property for backward compatibility."""
        sf_type = SnowflakeDataType("NUMBER(10,2)")
        stats_type = StatisticsSupportDataType("numeric")

        col_info = ColumnInfo(
            name="amount",
            snowflake_type=sf_type,
            statistics_type=stats_type,
        )

        assert col_info.column_type == "numeric"
        assert col_info.column_type == col_info.statistics_type.type_name

    @pytest.mark.parametrize(
        ("col_dict", "expected_name", "expected_sf_type", "expected_stats_type"),
        [
            # 数値型
            (
                {"name": "price", "data_type": "NUMBER(10,2)"},
                "price",
                "NUMBER",
                "numeric",
            ),
            ({"name": "count", "data_type": "INTEGER"}, "count", "INT", "numeric"),
            ({"name": "ratio", "data_type": "FLOAT"}, "ratio", "FLOAT", "numeric"),
            # 文字列型
            (
                {"name": "name", "data_type": "VARCHAR(255)"},
                "name",
                "VARCHAR",
                "string",
            ),
            ({"name": "code", "data_type": "CHAR(10)"}, "code", "CHAR", "string"),
            (
                {"name": "description", "data_type": "TEXT"},
                "description",
                "TEXT",
                "string",
            ),
            # 日付型
            (
                {"name": "created_at", "data_type": "TIMESTAMP_NTZ"},
                "created_at",
                "TIMESTAMP_NTZ",
                "date",
            ),
            ({"name": "birth_date", "data_type": "DATE"}, "birth_date", "DATE", "date"),
            (
                {"name": "event_time", "data_type": "DATETIME"},
                "event_time",
                "TIMESTAMP_NTZ",  # DATETIMEはTIMESTAMP_NTZのエイリアス
                "date",
            ),
            # 論理型
            (
                {"name": "is_active", "data_type": "BOOLEAN"},
                "is_active",
                "BOOLEAN",
                "boolean",
            ),
        ],
    )
    def test_from_dict_success(
        self,
        col_dict: dict[str, Any],
        expected_name: str,
        expected_sf_type: str,
        expected_stats_type: str,
    ) -> None:
        """Test successful conversion from dict to ColumnInfo."""
        col_info = ColumnInfo.from_dict(col_dict)

        assert col_info.name == expected_name
        assert col_info.snowflake_type.normalized_type == expected_sf_type
        assert col_info.statistics_type.type_name == expected_stats_type
        assert col_info.column_type == expected_stats_type  # backward compatibility

    @pytest.mark.parametrize(
        "invalid_dict",
        [
            # サポートされていない型
            {"name": "metadata", "data_type": "VARIANT"},
            {"name": "location", "data_type": "GEOGRAPHY"},
            {"name": "binary_data", "data_type": "BINARY"},
        ],
    )
    def test_from_dict_unsupported_type_raises_error(
        self, invalid_dict: dict[str, Any]
    ) -> None:
        """Test from_dict with unsupported data type raises ValueError."""
        with pytest.raises(
            ValueError, match="Unsupported Snowflake data type for statistics"
        ):
            ColumnInfo.from_dict(invalid_dict)

    def test_from_dict_missing_keys_raises_error(self) -> None:
        """Test from_dict with missing required keys raises KeyError."""
        with pytest.raises(KeyError):
            _ = ColumnInfo.from_dict({"name": "test"})  # missing data_type

        with pytest.raises(KeyError):
            _ = ColumnInfo.from_dict({"data_type": "VARCHAR(255)"})  # missing name

    def test_from_dict_empty_name_raises_error(self) -> None:
        """Test from_dict with empty name raises ValueError."""
        with pytest.raises(ValueError, match="raw_type cannot be empty"):
            _ = ColumnInfo.from_dict({"name": "test", "data_type": ""})

    def test_immutability(self) -> None:
        """Test that ColumnInfo instances are immutable."""
        col_info = ColumnInfo.from_dict({"name": "test", "data_type": "VARCHAR(255)"})

        # attrs frozen=True should prevent modification
        with pytest.raises(AttributeError):
            col_info.name = "new_name"  # pyright: ignore[reportAttributeAccessIssue]

    def test_equality(self) -> None:
        """Test ColumnInfo equality comparison."""
        col_info1 = ColumnInfo.from_dict({"name": "test", "data_type": "VARCHAR(255)"})
        col_info2 = ColumnInfo.from_dict({"name": "test", "data_type": "VARCHAR(255)"})
        col_info3 = ColumnInfo.from_dict({"name": "test", "data_type": "NUMBER(10,2)"})

        assert col_info1 == col_info2
        assert col_info1 != col_info3

    def test_string_representation(self) -> None:
        """Test string representation of ColumnInfo."""
        col_info = ColumnInfo.from_dict(
            {"name": "test_col", "data_type": "VARCHAR(100)"}
        )

        str_repr = str(col_info)
        assert "test_col" in str_repr
        assert "VARCHAR" in str_repr
        assert "string" in str_repr

    def test_integration_with_kernel_types(self) -> None:
        """Test integration with kernel module types."""
        # SnowflakeDataTypeの機能が正しく統合されているか確認
        col_info = ColumnInfo.from_dict({"name": "price", "data_type": "NUMERIC(10,2)"})

        # SnowflakeDataTypeのメソッドが使用可能か確認
        assert col_info.snowflake_type.is_numeric()
        assert not col_info.snowflake_type.is_string()
        assert not col_info.snowflake_type.is_date()
        assert col_info.snowflake_type.is_supported_for_statistics()

        # StatisticsSupportDataTypeの機能が正しく統合されているか確認
        assert col_info.statistics_type.type_name == "numeric"


class TestBooleanStatsDict:
    """Tests for BooleanStatsDict TypedDict."""

    def test_boolean_stats_dict_structure(self) -> None:
        """Test BooleanStatsDict structure and required fields."""
        # This test will fail until BooleanStatsDict is implemented
        boolean_stats: BooleanStatsDict = {
            "column_type": "boolean",
            "data_type": "BOOLEAN",
            "count": 950,
            "null_count": 50,
            "true_count": 720,
            "false_count": 230,
            "true_percentage": 75.79,
            "false_percentage": 24.21,
            "true_percentage_with_nulls": 72.0,
            "false_percentage_with_nulls": 23.0,
        }

        # Verify all required fields exist and have correct types
        assert boolean_stats["column_type"] == "boolean"
        assert boolean_stats["data_type"] == "BOOLEAN"
        assert isinstance(boolean_stats["count"], int)
        assert isinstance(boolean_stats["null_count"], int)
        assert isinstance(boolean_stats["true_count"], int)
        assert isinstance(boolean_stats["false_count"], int)
        assert isinstance(boolean_stats["true_percentage"], float)
        assert isinstance(boolean_stats["false_percentage"], float)
        assert isinstance(boolean_stats["true_percentage_with_nulls"], float)
        assert isinstance(boolean_stats["false_percentage_with_nulls"], float)
