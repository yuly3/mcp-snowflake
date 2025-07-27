"""
Snowflakeのraw dataに対する共通データ処理モジュール.

このモジュールはSnowflakeから取得したraw dataを
JSON互換形式に変換する共通処理を提供します。
"""

from typing import Any, TypedDict

from ...json_converter import _is_json_compatible_type, converter


class DataProcessingResult(TypedDict):
    """データ処理結果の型定義."""

    processed_rows: list[dict[str, Any]]
    warnings: list[str]


class RowProcessingResult(TypedDict):
    """単一行処理結果の型定義."""

    processed_row: dict[str, Any]
    warnings: list[str]


def process_row_data(raw_row: dict[str, Any]) -> RowProcessingResult:
    """
    Snowflakeの単一行データをJSON安全な形式に変換する.

    Parameters
    ----------
    raw_row : dict[str, Any]
        Snowflakeからの生データ行

    Returns
    -------
    RowProcessingResult
        処理済み行データと警告リスト
    """
    processed_row: dict[str, Any] = {}
    warnings: list[str] = []

    for column, value in raw_row.items():
        try:
            processed_value = converter.unstructure(value)
            # Check if the unstructured result is JSON-compatible
            if _is_json_compatible_type(processed_value):
                processed_row[column] = processed_value
            else:
                processed_row[column] = f"<unsupported_type: {type(value).__name__}>"
                warnings.append(f"Column '{column}' contains unsupported data type")
        except Exception:
            processed_row[column] = f"<unsupported_type: {type(value).__name__}>"
            warnings.append(f"Column '{column}' contains unsupported data type")

    return RowProcessingResult(
        processed_row=processed_row,
        warnings=warnings,
    )


def process_multiple_rows_data(
    raw_rows: list[dict[str, Any]],
) -> DataProcessingResult:
    """
    Snowflakeの複数行データをJSON安全な形式に変換する.

    Parameters
    ----------
    raw_rows : list[dict[str, Any]]
        Snowflakeからの生データ行リスト

    Returns
    -------
    DataProcessingResult
        処理済み行データリストと警告リスト
    """
    if not raw_rows:
        return DataProcessingResult(
            processed_rows=[],
            warnings=[],
        )

    processed_rows: list[dict[str, Any]] = []
    warnings_set: set[str] = set()

    for row in raw_rows:
        result = process_row_data(row)
        processed_rows.append(result["processed_row"])
        warnings_set.update(result["warnings"])

    return DataProcessingResult(
        processed_rows=processed_rows,
        warnings=list(warnings_set),
    )
