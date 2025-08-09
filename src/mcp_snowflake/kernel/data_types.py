"""Data types for Snowflake domain layer."""

from typing import Literal, cast

import attrs

# Normalized Snowflake data type definitions based on official Snowflake data types
NormalizedSnowflakeDataType = Literal[
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
]


@attrs.define(frozen=True)
class SnowflakeDataType:
    """Snowflake data type representation."""

    raw_type: str

    def __attrs_post_init__(self) -> None:
        if not self.raw_type.strip():
            raise ValueError("raw_type cannot be empty")

    @property
    def normalized_type(self) -> NormalizedSnowflakeDataType:
        """
        Convert raw Snowflake data type to normalized type name

        Examples
        --------
        - "VARCHAR(255)" -> "VARCHAR"
        - "NUMBER(10,2)" -> "NUMBER"
        - "TIMESTAMP_NTZ" -> "TIMESTAMP_NTZ"  # Suffix is preserved
        - "DECIMAL" -> "DECIMAL"
        """
        upper_type = self.raw_type.upper().strip()

        # Remove parentheses and their contents (e.g., VARCHAR(255) -> VARCHAR)
        if "(" in upper_type:
            upper_type = upper_type.split("(")[0]

        # Alias normalization
        alias_mapping = {
            "NUMERIC": "DECIMAL",
            "INTEGER": "INT",
            "DOUBLE PRECISION": "DOUBLE",
            "FLOAT4": "FLOAT",
            "FLOAT8": "FLOAT",
            "CHARACTER": "CHAR",
            "DATETIME": "TIMESTAMP_NTZ",  # DATETIME is an alias for TIMESTAMP_NTZ
            "VARBINARY": "BINARY",
        }

        # Apply alias conversion
        normalized = alias_mapping.get(upper_type, upper_type)

        # Type safety check: raise exception if not in Literal
        if normalized not in NormalizedSnowflakeDataType.__args__:
            raise ValueError(f"Unsupported Snowflake data type: {self.raw_type}")

        return cast("NormalizedSnowflakeDataType", normalized)

    def is_numeric(self) -> bool:
        """Check if the data type is numeric"""
        return self.normalized_type in {
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
        }

    def is_string(self) -> bool:
        """Check if the data type is string"""
        return self.normalized_type in {"VARCHAR", "CHAR", "STRING", "TEXT"}

    def is_date(self) -> bool:
        """Check if the data type is date/time"""
        return self.normalized_type in {
            "DATE",
            "TIME",
            "TIMESTAMP",
            "TIMESTAMP_LTZ",
            "TIMESTAMP_NTZ",
            "TIMESTAMP_TZ",
        }

    def is_boolean(self) -> bool:
        """Check if the data type is boolean"""
        return self.normalized_type == "BOOLEAN"

    def is_supported_for_statistics(self) -> bool:
        """Check if the data type is supported for statistical analysis"""
        return (
            self.is_numeric() or self.is_string() or self.is_date() or self.is_boolean()
        )


@attrs.define(frozen=True)
class StatisticsSupportDataType:
    """Statistics-specific data type classification."""

    type_name: Literal["numeric", "string", "date", "boolean"]

    @classmethod
    def from_snowflake_type(
        cls,
        sf_type: SnowflakeDataType,
    ) -> "StatisticsSupportDataType":
        """Convert SnowflakeDataType to StatisticsSupportDataType."""
        if sf_type.is_numeric():
            return cls("numeric")
        if sf_type.is_string():
            return cls("string")
        if sf_type.is_date():
            return cls("date")
        if sf_type.is_boolean():
            return cls("boolean")
        raise ValueError(
            f"Unsupported Snowflake data type for statistics: {sf_type.raw_type}"
        )
