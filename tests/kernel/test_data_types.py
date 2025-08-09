"""Tests for data types in kernel module."""

from typing import get_args

import pytest

from mcp_snowflake.kernel.data_types import (
    NormalizedSnowflakeDataType,
    SnowflakeDataType,
    StatisticsSupportDataType,
)


class TestSnowflakeDataType:
    """Tests for SnowflakeDataType class."""

    def test_init_with_valid_raw_type(self) -> None:
        """Test initialization with valid raw type."""
        sf_type = SnowflakeDataType("VARCHAR(255)")
        assert sf_type.raw_type == "VARCHAR(255)"

    def test_init_with_empty_raw_type_raises_error(self) -> None:
        """Test initialization with empty raw type raises ValueError."""
        with pytest.raises(ValueError, match="raw_type cannot be empty"):
            SnowflakeDataType("")

    def test_init_with_whitespace_only_raw_type_raises_error(self) -> None:
        """Test initialization with whitespace-only raw type raises ValueError."""
        with pytest.raises(ValueError, match="raw_type cannot be empty"):
            SnowflakeDataType("   ")

    @pytest.mark.parametrize(
        ("raw_type", "expected"),
        [
            # 基本型の正規化
            ("VARCHAR(255)", "VARCHAR"),
            ("NUMBER(10,2)", "NUMBER"),
            ("CHAR(10)", "CHAR"),
            ("FLOAT", "FLOAT"),
            # エイリアス変換
            ("NUMERIC", "DECIMAL"),
            ("INTEGER", "INT"),
            ("DOUBLE PRECISION", "DOUBLE"),
            ("FLOAT4", "FLOAT"),
            ("FLOAT8", "FLOAT"),
            ("CHARACTER", "CHAR"),
            ("DATETIME", "TIMESTAMP_NTZ"),
            ("VARBINARY", "BINARY"),
            # 大文字小文字の処理
            ("varchar(255)", "VARCHAR"),
            ("Number(10,2)", "NUMBER"),
            # タイムスタンプ型（サフィックス保持）
            ("TIMESTAMP_NTZ", "TIMESTAMP_NTZ"),
            ("TIMESTAMP_LTZ", "TIMESTAMP_LTZ"),
            ("TIMESTAMP_TZ", "TIMESTAMP_TZ"),
            # 半構造化データ型
            ("VARIANT", "VARIANT"),
            ("OBJECT", "OBJECT"),
            ("ARRAY", "ARRAY"),
            # 地理空間データ型
            ("GEOGRAPHY", "GEOGRAPHY"),
            ("GEOMETRY", "GEOMETRY"),
            # ベクトルデータ型
            ("VECTOR", "VECTOR"),
        ],
    )
    def test_normalized_type(self, raw_type: str, expected: str) -> None:
        """Test normalized_type property with various input types."""
        sf_type = SnowflakeDataType(raw_type)
        assert sf_type.normalized_type == expected

    def test_normalized_type_with_unsupported_type_raises_error(self) -> None:
        """Test normalized_type with unsupported type raises ValueError."""
        sf_type = SnowflakeDataType("UNSUPPORTED_TYPE")
        with pytest.raises(ValueError, match="Unsupported Snowflake data type"):
            _ = sf_type.normalized_type

    @pytest.mark.parametrize(
        ("raw_type", "expected"),
        [
            # 数値型
            ("NUMBER(10,2)", True),
            ("INTEGER", True),
            ("FLOAT", True),
            ("DECIMAL", True),
            ("BIGINT", True),
            ("SMALLINT", True),
            ("TINYINT", True),
            ("BYTEINT", True),
            ("DOUBLE", True),
            ("REAL", True),
            # 非数値型
            ("VARCHAR(255)", False),
            ("DATE", False),
            ("BOOLEAN", False),
        ],
    )
    def test_is_numeric(self, raw_type: str, expected: bool) -> None:  # noqa: FBT001
        """Test is_numeric method."""
        sf_type = SnowflakeDataType(raw_type)
        assert sf_type.is_numeric() == expected

    @pytest.mark.parametrize(
        ("raw_type", "expected"),
        [
            # 文字列型
            ("VARCHAR(255)", True),
            ("CHAR(10)", True),
            ("STRING", True),
            ("TEXT", True),
            # 非文字列型
            ("NUMBER(10,2)", False),
            ("DATE", False),
            ("BOOLEAN", False),
            ("BINARY", False),  # バイナリ型は文字列型ではない
        ],
    )
    def test_is_string(self, raw_type: str, expected: bool) -> None:  # noqa: FBT001
        """Test is_string method."""
        sf_type = SnowflakeDataType(raw_type)
        assert sf_type.is_string() == expected

    @pytest.mark.parametrize(
        ("raw_type", "expected"),
        [
            # 日付時刻型
            ("DATE", True),
            ("TIME", True),
            ("TIMESTAMP", True),
            ("TIMESTAMP_NTZ", True),
            ("TIMESTAMP_LTZ", True),
            ("TIMESTAMP_TZ", True),
            ("DATETIME", True),  # エイリアス
            # 非日付時刻型
            ("VARCHAR(255)", False),
            ("NUMBER(10,2)", False),
            ("BOOLEAN", False),
        ],
    )
    def test_is_date(self, raw_type: str, expected: bool) -> None:  # noqa: FBT001
        """Test is_date method."""
        sf_type = SnowflakeDataType(raw_type)
        assert sf_type.is_date() == expected

    @pytest.mark.parametrize(
        ("raw_type", "expected"),
        [
            # サポートされる型
            ("NUMBER(10,2)", True),
            ("VARCHAR(255)", True),
            ("DATE", True),
            ("TIMESTAMP_NTZ", True),
            # サポートされない型
            ("BOOLEAN", False),
            ("VARIANT", False),
            ("GEOGRAPHY", False),
            ("BINARY", False),
        ],
    )
    def test_is_supported_for_statistics(self, raw_type: str, expected: bool) -> None:  # noqa: FBT001
        """Test is_supported_for_statistics method."""
        sf_type = SnowflakeDataType(raw_type)
        assert sf_type.is_supported_for_statistics() == expected


class TestStatisticsSupportDataType:
    """Tests for StatisticsSupportDataType class."""

    def test_init_with_valid_type_name(self) -> None:
        """Test initialization with valid type name."""
        stats_type = StatisticsSupportDataType("numeric")
        assert stats_type.type_name == "numeric"

    @pytest.mark.parametrize(
        ("snowflake_raw_type", "expected_stats_type"),
        [
            # 数値型 → numeric
            ("NUMBER(10,2)", "numeric"),
            ("INTEGER", "numeric"),
            ("FLOAT", "numeric"),
            ("DECIMAL", "numeric"),
            # 文字列型 → string
            ("VARCHAR(255)", "string"),
            ("CHAR(10)", "string"),
            ("STRING", "string"),
            ("TEXT", "string"),
            # 日付時刻型 → date
            ("DATE", "date"),
            ("TIMESTAMP_NTZ", "date"),
            ("TIMESTAMP_LTZ", "date"),
            ("TIME", "date"),
            ("DATETIME", "date"),
        ],
    )
    def test_from_snowflake_type_success(
        self, snowflake_raw_type: str, expected_stats_type: str
    ) -> None:
        """Test successful conversion from SnowflakeDataType."""
        sf_type = SnowflakeDataType(snowflake_raw_type)
        stats_type = StatisticsSupportDataType.from_snowflake_type(sf_type)
        assert stats_type.type_name == expected_stats_type

    @pytest.mark.parametrize(
        "unsupported_raw_type",
        [
            "BOOLEAN",
            "VARIANT",
            "GEOGRAPHY",
            "BINARY",
            "VECTOR",
        ],
    )
    def test_from_snowflake_type_unsupported_raises_error(
        self, unsupported_raw_type: str
    ) -> None:
        """Test conversion from unsupported SnowflakeDataType raises ValueError."""
        sf_type = SnowflakeDataType(unsupported_raw_type)
        with pytest.raises(
            ValueError, match="Unsupported Snowflake data type for statistics"
        ):
            StatisticsSupportDataType.from_snowflake_type(sf_type)


class TestNormalizedSnowflakeDataTypeLiteral:
    """Tests for NormalizedSnowflakeDataType Literal definition."""

    def test_literal_contains_all_expected_types(self) -> None:
        """Test that NormalizedSnowflakeDataType Literal contains all expected types."""
        expected_types = {
            # 数値データ型
            "NUMBER",
            "DECIMAL",
            "INT",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
            "BYTEINT",
            "FLOAT",
            "DOUBLE",
            "REAL",
            # 文字列およびバイナリデータ型
            "VARCHAR",
            "CHAR",
            "STRING",
            "TEXT",
            "BINARY",
            # 論理データ型
            "BOOLEAN",
            # 日付と時刻のデータ型
            "DATE",
            "TIME",
            "TIMESTAMP",
            "TIMESTAMP_LTZ",
            "TIMESTAMP_NTZ",
            "TIMESTAMP_TZ",
            # 半構造化データ型
            "VARIANT",
            "OBJECT",
            "ARRAY",
            # 地理空間データ型
            "GEOGRAPHY",
            "GEOMETRY",
            # ベクトルデータ型
            "VECTOR",
            # 構造化データ型（Iceberg用）
            "MAP",
        }

        actual_types = set(get_args(NormalizedSnowflakeDataType))
        assert actual_types == expected_types
