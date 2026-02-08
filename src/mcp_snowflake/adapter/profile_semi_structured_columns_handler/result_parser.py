"""Result parsing utilities for semi-structured profiling."""

import json
import logging
from collections.abc import Mapping, Sequence
from typing import Any, Literal

from ...handler.profile_semi_structured_columns.models import (
    ColumnProfileDict,
    PathProfileDict,
    SemiStructuredProfileResultParseError,
    TopLevelTypeDistributionDict,
    TopValue,
)

logger = logging.getLogger(__name__)


def get_value_case_insensitive(row: Mapping[str, Any], key: str) -> Any:
    """Get row value by key with case-insensitive fallback."""
    direct = row.get(key)
    if direct is not None:
        return direct

    key_upper = key.upper()
    for row_key, row_value in row.items():
        if row_key.upper() == key_upper:
            return row_value
    return None


def parse_count_value(row: Mapping[str, Any], key: str) -> int:
    """Parse an integer count value from a row key."""
    raw = get_value_case_insensitive(row, key)
    if raw is None:
        raise SemiStructuredProfileResultParseError(f"{key} missing from profiling result")
    return int(raw)


def parse_float_value(row: Mapping[str, Any], key: str, default: float = 0.0) -> float:
    """Parse a floating-point value from a row key."""
    raw = get_value_case_insensitive(row, key)
    if raw is None:
        return default
    return float(raw)


def parse_variant_json(raw_value: Any, field_name: str) -> Any:
    """Parse Snowflake VARIANT-like values that may arrive as JSON strings."""
    if raw_value is None:
        return None
    if isinstance(raw_value, (list, dict)):
        return raw_value
    if isinstance(raw_value, str):
        if not raw_value:
            return None
        try:
            return json.loads(raw_value)
        except json.JSONDecodeError as e:
            logger.error(
                "failed to parse JSON field from Snowflake",
                extra={"field_name": field_name, "raw_value": raw_value},
            )
            raise SemiStructuredProfileResultParseError(f"Failed to parse JSON for {field_name}") from e

    return raw_value


def parse_top_level_type_distribution(raw_value: Any) -> TopLevelTypeDistributionDict:
    """Parse top-level type distribution object."""
    parsed = parse_variant_json(raw_value, "TOP_LEVEL_TYPE_DISTRIBUTION")
    if not isinstance(parsed, Mapping):
        raise SemiStructuredProfileResultParseError("TOP_LEVEL_TYPE_DISTRIBUTION has unexpected shape")

    def _read(key: str) -> int:
        for raw_key, item_value in parsed.items():
            if str(raw_key).upper() == key:
                return int(item_value or 0)
        return 0

    return TopLevelTypeDistributionDict(
        OBJECT=_read("OBJECT"),
        ARRAY=_read("ARRAY"),
        STRING=_read("STRING"),
        NUMBER=_read("NUMBER"),
        BOOLEAN=_read("BOOLEAN"),
        NULL=_read("NULL"),
    )


def parse_top_values(raw_value: Any, field_name: str) -> list[TopValue[str]]:
    """Parse top values payload from APPROX_TOP_K or ARRAY_AGG objects."""
    parsed = parse_variant_json(raw_value, field_name)
    if parsed is None:
        return []
    if not isinstance(parsed, Sequence) or isinstance(parsed, str):
        raise SemiStructuredProfileResultParseError(f"{field_name} has unexpected shape")

    top_values: list[TopValue[str]] = []
    for item in parsed:
        value_raw: Any
        count_raw: Any
        if isinstance(item, Sequence) and not isinstance(item, (str, bytes)) and len(item) == 2:
            value_raw, count_raw = item
        elif isinstance(item, Mapping):
            value_raw = item.get("value", item.get("VALUE"))
            count_raw = item.get("count", item.get("COUNT"))
        else:
            raise SemiStructuredProfileResultParseError(f"Invalid top value entry in {field_name}")

        value = None if value_raw is None else str(value_raw)
        try:
            count = int(count_raw)
        except (TypeError, ValueError) as e:
            raise SemiStructuredProfileResultParseError(f"Invalid top value count in {field_name}") from e

        top_values.append(TopValue(value, count))

    return top_values


def parse_column_profile_row(
    row: Mapping[str, Any],
    column_type: Literal["VARIANT", "ARRAY", "OBJECT"],
) -> ColumnProfileDict:
    """Parse one row of column-level aggregate profile."""
    profile = ColumnProfileDict(
        column_type=column_type,
        null_count=parse_count_value(row, "NULL_COUNT"),
        non_null_count=parse_count_value(row, "NON_NULL_COUNT"),
        null_ratio=parse_float_value(row, "NULL_RATIO"),
        top_level_type_distribution=parse_top_level_type_distribution(
            get_value_case_insensitive(row, "TOP_LEVEL_TYPE_DISTRIBUTION")
        ),
    )

    has_array_stats = any(
        get_value_case_insensitive(row, key) is not None
        for key in ("ARRAY_LENGTH_MIN", "ARRAY_LENGTH_MAX", "ARRAY_LENGTH_P25", "ARRAY_LENGTH_P50", "ARRAY_LENGTH_P75")
    )
    if has_array_stats:
        profile["array_length_stats"] = {
            "min": int(get_value_case_insensitive(row, "ARRAY_LENGTH_MIN") or 0),
            "max": int(get_value_case_insensitive(row, "ARRAY_LENGTH_MAX") or 0),
            "p25": parse_float_value(row, "ARRAY_LENGTH_P25"),
            "p50": parse_float_value(row, "ARRAY_LENGTH_P50"),
            "p75": parse_float_value(row, "ARRAY_LENGTH_P75"),
        }

    return profile


def parse_path_profile_rows(
    rows: Sequence[Mapping[str, Any]],
    column_name: str,
    include_value_samples: bool,  # noqa: FBT001
) -> list[PathProfileDict]:
    """Parse grouped path-profile rows."""
    grouped: dict[str, PathProfileDict] = {}
    for row in rows:
        path = str(get_value_case_insensitive(row, "PATH") or "$")
        path_depth = int(get_value_case_insensitive(row, "PATH_DEPTH") or 1)

        if path not in grouped:
            grouped[path] = PathProfileDict(
                column=column_name,
                path=path,
                path_depth=path_depth,
                value_type_distribution={},
                distinct_count_approx=int(get_value_case_insensitive(row, "DISTINCT_COUNT_APPROX") or 0),
                null_ratio=float(get_value_case_insensitive(row, "NULL_RATIO") or 0.0),
            )

        value_type = str(get_value_case_insensitive(row, "VALUE_TYPE") or "UNKNOWN")
        value_count = int(get_value_case_insensitive(row, "VALUE_COUNT") or 0)
        grouped[path]["value_type_distribution"][value_type] = value_count

        if include_value_samples and "top_values" not in grouped[path]:
            parsed_top_values = parse_top_values(get_value_case_insensitive(row, "TOP_VALUES"), "TOP_VALUES")
            if parsed_top_values:
                grouped[path]["top_values"] = parsed_top_values

    return sorted(grouped.values(), key=lambda item: (item["path_depth"], item["path"]))
