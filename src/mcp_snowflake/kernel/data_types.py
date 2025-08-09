"""Data types for Snowflake domain layer."""

from typing import Literal

import attrs

# Snowflake公式データ型に基づく正規化型定義
NormalizedSnowflakeDataType = Literal[
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
]


@attrs.define(frozen=True)
class SnowflakeDataType:
    """Snowflake data type representation."""

    raw_type: str

    def __attrs_post_init__(self) -> None:
        if not self.raw_type or not self.raw_type.strip():
            raise ValueError("raw_type cannot be empty")

    @property
    def normalized_type(self) -> NormalizedSnowflakeDataType:
        """
        Snowflake生データ型を正規化された型名に変換

        Examples
        --------
        - "VARCHAR(255)" -> "VARCHAR"
        - "NUMBER(10,2)" -> "NUMBER"
        - "TIMESTAMP_NTZ" -> "TIMESTAMP_NTZ"  # サフィックスは保持
        - "DECIMAL" -> "DECIMAL"
        """
        upper_type = self.raw_type.upper().strip()

        # 括弧とその内容を削除 (例: VARCHAR(255) -> VARCHAR)
        if "(" in upper_type:
            upper_type = upper_type.split("(")[0]

        # エイリアス正規化
        alias_mapping = {
            "NUMERIC": "DECIMAL",
            "INTEGER": "INT",
            "DOUBLE PRECISION": "DOUBLE",
            "FLOAT4": "FLOAT",
            "FLOAT8": "FLOAT",
            "CHARACTER": "CHAR",
            "DATETIME": "TIMESTAMP_NTZ",  # DATETIMEはTIMESTAMP_NTZのエイリアス
            "VARBINARY": "BINARY",
        }

        # エイリアス変換
        normalized = alias_mapping.get(upper_type, upper_type)

        # 型安全性チェック: Literalに含まれない場合は例外
        if normalized not in NormalizedSnowflakeDataType.__args__:
            raise ValueError(f"Unsupported Snowflake data type: {self.raw_type}")

        return normalized  # type: ignore[return-value]

    def is_numeric(self) -> bool:
        """数値型かどうかを判定"""
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
        """文字列型かどうかを判定"""
        return self.normalized_type in {"VARCHAR", "CHAR", "STRING", "TEXT"}

    def is_date(self) -> bool:
        """日付時刻型かどうかを判定"""
        return self.normalized_type in {
            "DATE",
            "TIME",
            "TIMESTAMP",
            "TIMESTAMP_LTZ",
            "TIMESTAMP_NTZ",
            "TIMESTAMP_TZ",
        }

    def is_boolean(self) -> bool:
        """論理型かどうかを判定"""
        return self.normalized_type == "BOOLEAN"

    def is_supported_for_statistics(self) -> bool:
        """統計分析でサポートされる型かどうかを判定"""
        return (
            self.is_numeric() or self.is_string() or self.is_date() or self.is_boolean()
        )


@attrs.define(frozen=True)
class StatisticsSupportDataType:
    """Statistics-specific data type classification."""

    type_name: Literal["numeric", "string", "date", "boolean"]

    @classmethod
    def from_snowflake_type(
        cls, sf_type: SnowflakeDataType
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
