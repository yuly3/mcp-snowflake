"""Tests for data types in kernel module."""

from typing import get_args

import pytest

from kernel.data_types import (
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
        with pytest.raises(ValueError, match="Unsupported Snowflake data type"):
            _ = SnowflakeDataType("")

    def test_init_with_whitespace_only_raw_type_raises_error(self) -> None:
        """Test initialization with whitespace-only raw type raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported Snowflake data type"):
            _ = SnowflakeDataType("   ")

    @pytest.mark.parametrize(
        ("raw_type", "expected"),
        [
            # Basic type normalization
            ("VARCHAR(255)", "VARCHAR"),
            ("NUMBER(10,2)", "NUMBER"),
            ("CHAR(10)", "CHAR"),
            ("FLOAT", "FLOAT"),
            # Alias conversion
            ("NUMERIC", "DECIMAL"),
            ("INTEGER", "INT"),
            ("DOUBLE PRECISION", "DOUBLE"),
            ("FLOAT4", "FLOAT"),
            ("FLOAT8", "FLOAT"),
            ("CHARACTER", "CHAR"),
            ("DATETIME", "TIMESTAMP_NTZ"),
            ("VARBINARY", "BINARY"),
            # Case handling
            ("varchar(255)", "VARCHAR"),
            ("Number(10,2)", "NUMBER"),
            # Timestamp types (suffix preservation)
            ("TIMESTAMP_NTZ", "TIMESTAMP_NTZ"),
            ("TIMESTAMP_LTZ", "TIMESTAMP_LTZ"),
            ("TIMESTAMP_TZ", "TIMESTAMP_TZ"),
            # Semi-structured data types
            ("VARIANT", "VARIANT"),
            ("OBJECT", "OBJECT"),
            ("ARRAY", "ARRAY"),
            # Geospatial data types
            ("GEOGRAPHY", "GEOGRAPHY"),
            ("GEOMETRY", "GEOMETRY"),
            # Vector data types
            ("VECTOR", "VECTOR"),
        ],
    )
    def test_normalized_type(self, raw_type: str, expected: str) -> None:
        """Test normalized_type property with various input types."""
        sf_type = SnowflakeDataType(raw_type)
        assert sf_type.normalized_type == expected

    def test_normalized_type_with_unsupported_type_raises_error(self) -> None:
        """Test unsupported type raises ValueError at construction time."""
        with pytest.raises(ValueError, match="Unsupported Snowflake data type"):
            _ = SnowflakeDataType("UNSUPPORTED_TYPE")

    @pytest.mark.parametrize(
        ("raw_type", "expected"),
        [
            # Numeric types
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
            # Non-numeric types
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
            # String types
            ("VARCHAR(255)", True),
            ("CHAR(10)", True),
            ("STRING", True),
            ("TEXT", True),
            # Non-string types
            ("NUMBER(10,2)", False),
            ("DATE", False),
            ("BOOLEAN", False),
            ("BINARY", False),  # Binary type is not a string type
        ],
    )
    def test_is_string(self, raw_type: str, expected: bool) -> None:  # noqa: FBT001
        """Test is_string method."""
        sf_type = SnowflakeDataType(raw_type)
        assert sf_type.is_string() == expected

    @pytest.mark.parametrize(
        ("raw_type", "expected"),
        [
            # Date/time types
            ("DATE", True),
            ("TIME", True),
            ("TIMESTAMP", True),
            ("TIMESTAMP_NTZ", True),
            ("TIMESTAMP_LTZ", True),
            ("TIMESTAMP_TZ", True),
            ("DATETIME", True),  # Alias
            # Non-date/time types
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
            # Boolean type
            ("BOOLEAN", True),
            # Non-boolean types
            ("VARCHAR(255)", False),
            ("NUMBER(10,2)", False),
            ("DATE", False),
            ("TIMESTAMP_NTZ", False),
            ("VARIANT", False),
            ("BINARY", False),
        ],
    )
    def test_is_boolean(self, raw_type: str, expected: bool) -> None:  # noqa: FBT001
        """Test is_boolean method."""
        sf_type = SnowflakeDataType(raw_type)
        assert sf_type.is_boolean() == expected

    @pytest.mark.parametrize(
        ("raw_type", "expected"),
        [
            # Supported types
            ("NUMBER(10,2)", True),
            ("VARCHAR(255)", True),
            ("DATE", True),
            ("TIMESTAMP_NTZ", True),
            ("BOOLEAN", True),
            # Unsupported types
            ("VARIANT", False),
            ("GEOGRAPHY", False),
            ("BINARY", False),
        ],
    )
    def test_is_supported_for_statistics(self, raw_type: str, expected: bool) -> None:  # noqa: FBT001
        """Test is_supported_for_statistics method."""
        sf_type = SnowflakeDataType(raw_type)
        assert sf_type.is_supported_for_statistics() == expected

    def test_from_raw_str_success(self) -> None:
        """Test from_raw_str with supported types returns instance."""
        sf_type = SnowflakeDataType.from_raw_str("VARCHAR(10)")
        assert sf_type is not None
        assert sf_type.normalized_type == "VARCHAR"

    def test_from_raw_str_with_unsupported_type_returns_none(self) -> None:
        """Test from_raw_str with unsupported type returns None."""
        result = SnowflakeDataType.from_raw_str("UNSUPPORTED")
        assert result is None

    def test_from_raw_str_with_empty_string_returns_none(self) -> None:
        """Test from_raw_str with empty string returns None."""
        result = SnowflakeDataType.from_raw_str("   ")
        assert result is None


class TestStatisticsSupportDataType:
    """Tests for StatisticsSupportDataType class."""

    def test_constructor_with_supported_snowflake_type(self) -> None:
        """Test constructor with supported SnowflakeDataType."""
        sf_type = SnowflakeDataType("NUMBER(10,2)")
        stats_type = StatisticsSupportDataType(sf_type)
        assert stats_type.type_name == "numeric"
        assert stats_type.snowflake_type == sf_type

    def test_constructor_with_unsupported_snowflake_type_raises_value_error(
        self,
    ) -> None:
        """Test constructor with unsupported SnowflakeDataType raises ValueError."""
        sf_type = SnowflakeDataType("VARIANT")
        with pytest.raises(
            ValueError, match="Unsupported Snowflake data type for statistics"
        ):
            _ = StatisticsSupportDataType(sf_type)

    @pytest.mark.parametrize(
        ("snowflake_raw_type", "expected_stats_type"),
        [
            # Numeric type → numeric
            ("NUMBER(10,2)", "numeric"),
            ("INTEGER", "numeric"),
            ("FLOAT", "numeric"),
            ("DECIMAL", "numeric"),
            # String type → string
            ("VARCHAR(255)", "string"),
            ("CHAR(10)", "string"),
            ("STRING", "string"),
            ("TEXT", "string"),
            # Date/time type → date
            ("DATE", "date"),
            ("TIMESTAMP_NTZ", "date"),
            ("TIMESTAMP_LTZ", "date"),
            ("TIME", "date"),
            ("DATETIME", "date"),
            # Boolean type → boolean
            ("BOOLEAN", "boolean"),
        ],
    )
    def test_from_snowflake_type_success(
        self,
        snowflake_raw_type: str,
        expected_stats_type: str,
    ) -> None:
        """Test successful conversion from SnowflakeDataType."""
        sf_type = SnowflakeDataType(snowflake_raw_type)
        stats_type = StatisticsSupportDataType.from_snowflake_type(sf_type)
        assert stats_type is not None
        assert stats_type.type_name == expected_stats_type

    @pytest.mark.parametrize(
        "unsupported_raw_type",
        [
            "VARIANT",
            "GEOGRAPHY",
            "BINARY",
            "VECTOR",
        ],
    )
    def test_from_snowflake_type_unsupported_returns_none(
        self,
        unsupported_raw_type: str,
    ) -> None:
        """Test conversion from unsupported SnowflakeDataType returns None."""
        sf_type = SnowflakeDataType(unsupported_raw_type)
        result = StatisticsSupportDataType.from_snowflake_type(sf_type)
        assert result is None


class TestNormalizedSnowflakeDataTypeLiteral:
    """Tests for NormalizedSnowflakeDataType Literal definition."""

    def test_literal_contains_all_expected_types(self) -> None:
        """Test that NormalizedSnowflakeDataType Literal contains all expected types."""
        expected_types = {
            # Numeric data types
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
            # String and binary data types
            "VARCHAR",
            "CHAR",
            "STRING",
            "TEXT",
            "BINARY",
            # Boolean data type
            "BOOLEAN",
            # Date and time data types
            "DATE",
            "TIME",
            "TIMESTAMP",
            "TIMESTAMP_LTZ",
            "TIMESTAMP_NTZ",
            "TIMESTAMP_TZ",
            # Semi-structured data types
            "VARIANT",
            "OBJECT",
            "ARRAY",
            # Geospatial data types
            "GEOGRAPHY",
            "GEOMETRY",
            # Vector data types
            "VECTOR",
            # Structured data types (for Iceberg)
            "MAP",
        }

        actual_types = set(get_args(NormalizedSnowflakeDataType))
        assert actual_types == expected_types
