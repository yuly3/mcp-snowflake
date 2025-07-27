"""
データ処理モジュール.

Snowflakeから取得したraw dataを処理するための共通機能を提供します。
"""

from .common import (
    DataProcessingResult,
    RowProcessingResult,
    process_multiple_rows_data,
    process_row_data,
)

__all__ = [
    "DataProcessingResult",
    "RowProcessingResult",
    "process_multiple_rows_data",
    "process_row_data",
]
