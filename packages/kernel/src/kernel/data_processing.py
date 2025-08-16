"""
Common data processing for Snowflake raw data (kernel layer)

This module provides common processing to convert raw data retrieved from Snowflake
into JSON-compatible formats.
"""

from typing import Any

import attrs

from cattrs_converter import Jsonable, is_json_compatible_type, json_converter


@attrs.define(frozen=True)
class RowProcessingResult:
    """Result of processing a single row of Snowflake data.

    This class encapsulates the result of converting a single raw row from Snowflake
    into a JSON-compatible format, along with any warnings generated during processing.

    Attributes
    ----------
    processed_row : dict[str, Jsonable]
        The row data converted to JSON-compatible types
    warnings : list[str]
        List of warning messages for unsupported data types or processing issues
    """

    processed_row: dict[str, Jsonable]
    warnings: list[str]

    @classmethod
    def from_raw_row(cls, raw_row: dict[str, Any]) -> "RowProcessingResult":
        processed_row: dict[str, Jsonable] = {}
        warnings: list[str] = []

        for column, value in raw_row.items():
            processed_value = json_converter.unstructure(value)
            if is_json_compatible_type(processed_value):
                processed_row[column] = processed_value
            else:
                processed_row[column] = f"<unsupported_type: {type(value).__name__}>"
                warnings.append(f"Column '{column}' contains unsupported data type")

        return cls(processed_row=processed_row, warnings=warnings)


@attrs.define(frozen=True)
class DataProcessingResult:
    """Result of processing multiple rows of Snowflake data.

    This class encapsulates the result of converting multiple raw rows from Snowflake
    into JSON-compatible formats, along with any warnings generated during processing.
    Warnings are deduplicated across all processed rows.

    Attributes
    ----------
    processed_rows : list[dict[str, Jsonable]]
        List of row data converted to JSON-compatible types
    warnings : list[str]
        Deduplicated list of warning messages for unsupported data types or processing issues
    """

    processed_rows: list[dict[str, Jsonable]]
    warnings: list[str]

    @classmethod
    def from_raw_rows(cls, raw_rows: list[dict[str, Any]]) -> "DataProcessingResult":
        if not raw_rows:
            return cls(processed_rows=[], warnings=[])

        processed_rows: list[dict[str, Jsonable]] = []
        warnings_set: set[str] = set()

        for row in raw_rows:
            result = RowProcessingResult.from_raw_row(row)
            processed_rows.append(result.processed_row)
            warnings_set.update(result.warnings)

        return cls(processed_rows=processed_rows, warnings=list(warnings_set))
