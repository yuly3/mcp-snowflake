"""
Common data processing for Snowflake raw data (kernel layer)

This module provides common processing to convert raw data retrieved from Snowflake
into JSON-compatible formats.
"""

from typing import Any

import attrs

from cattrs_converter import Jsonable, JsonImmutableConverter


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
    def from_raw_row(cls, converter: JsonImmutableConverter, raw_row: dict[str, Any]) -> "RowProcessingResult":
        processed_row: dict[str, Jsonable] = {}
        warnings: list[str] = []

        for column, value in raw_row.items():
            try:
                processed_value = converter.unstructure(value)
            except ValueError:
                processed_row[column] = f"<unsupported_type: {type(value).__name__}>"
                warnings.append(f"Column '{column}' contains unsupported data type")
            else:
                processed_row[column] = processed_value

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
    def from_raw_rows(
        cls,
        converter: JsonImmutableConverter,
        raw_rows: list[dict[str, Any]],
    ) -> "DataProcessingResult":
        if not raw_rows:
            return cls(processed_rows=[], warnings=[])

        processed_rows: list[dict[str, Jsonable]] = []
        warnings_set: set[str] = set()

        for row in raw_rows:
            result = RowProcessingResult.from_raw_row(converter, row)
            processed_rows.append(result.processed_row)
            warnings_set.update(result.warnings)

        return cls(processed_rows=processed_rows, warnings=list(warnings_set))
