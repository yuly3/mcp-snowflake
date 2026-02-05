"""Data types for Snowflake domain layer."""

from typing import Literal, Self, TypeGuard

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
NORMALIZED_SNOWFLAKE_DATA_TYPES: frozenset[str] = frozenset(NormalizedSnowflakeDataType.__args__)
ALIAS_MAPPING = {
    "NUMERIC": "DECIMAL",
    "INTEGER": "INT",
    "DOUBLE PRECISION": "DOUBLE",
    "FLOAT4": "FLOAT",
    "FLOAT8": "FLOAT",
    "CHARACTER": "CHAR",
    "DATETIME": "TIMESTAMP_NTZ",  # DATETIME is an alias for TIMESTAMP_NTZ
    "VARBINARY": "BINARY",
}


def is_normalized_snowflake_data_type(s: str) -> TypeGuard[NormalizedSnowflakeDataType]:
    """Check if a string is a normalized Snowflake data type."""
    return s in NORMALIZED_SNOWFLAKE_DATA_TYPES


@attrs.define(frozen=True)
class SnowflakeDataType:
    """Snowflake data type representation."""

    raw_type: str
    normalized_type: NormalizedSnowflakeDataType = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        normalized = self._normalize_raw_type(self.raw_type)
        if normalized is None:
            raise ValueError(f"Unsupported Snowflake data type: {self.raw_type}")
        object.__setattr__(self, "normalized_type", normalized)

    @classmethod
    def from_raw_str(cls, s: str) -> Self | None:
        """Create SnowflakeDataType from raw string, returning None for unsupported types."""
        normalized = cls._normalize_raw_type(s)
        if normalized is None:
            return None
        return cls(s)

    @staticmethod
    def _normalize_raw_type(s: str) -> NormalizedSnowflakeDataType | None:
        """
        Normalize raw Snowflake data type to NormalizedSnowflakeDataType.

        Returns None if the type is not supported.
        """
        upper_type = s.upper().strip()
        if not upper_type:
            return None

        # Remove parentheses and their contents (e.g., VARCHAR(255) -> VARCHAR)
        if "(" in upper_type:
            upper_type = upper_type.split("(")[0]

        # Apply alias conversion
        normalized = ALIAS_MAPPING.get(upper_type, upper_type)

        # Type safety check: return None if not in Literal
        if not is_normalized_snowflake_data_type(normalized):
            return None

        return normalized

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
        return self.is_numeric() or self.is_string() or self.is_date() or self.is_boolean()


@attrs.define(frozen=True)
class StatisticsSupportDataType:
    """Statistics-specific data type classification."""

    snowflake_type: SnowflakeDataType
    _classification: Literal["numeric", "string", "date", "boolean"] = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        """Initialize classification based on snowflake_type."""
        if self.snowflake_type.is_numeric():
            classification = "numeric"
        elif self.snowflake_type.is_string():
            classification = "string"
        elif self.snowflake_type.is_date():
            classification = "date"
        elif self.snowflake_type.is_boolean():
            classification = "boolean"
        else:
            raise ValueError(f"Unsupported Snowflake data type for statistics: {self.snowflake_type.raw_type}")
        object.__setattr__(self, "_classification", classification)

    @property
    def type_name(self) -> Literal["numeric", "string", "date", "boolean"]:
        """Get the classification type name."""
        return self._classification

    @classmethod
    def from_snowflake_type(
        cls,
        sf_type: SnowflakeDataType,
    ) -> Self | None:
        """Convert SnowflakeDataType to StatisticsSupportDataType, returning None for unsupported types."""
        try:
            return cls(sf_type)
        except ValueError:
            return None
