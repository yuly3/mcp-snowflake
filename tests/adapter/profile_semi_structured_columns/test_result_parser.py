"""Tests for semi-structured profiling result parser."""

from mcp_snowflake.adapter.profile_semi_structured_columns_handler.result_parser import (
    parse_column_profile_row,
    parse_path_profile_rows,
    parse_top_values,
)


def test_parse_column_profile_row() -> None:
    """Should parse basic column profile row into typed structure."""
    row = {
        "NULL_COUNT": 10,
        "NON_NULL_COUNT": 90,
        "NULL_RATIO": 0.1,
        "TOP_LEVEL_TYPE_DISTRIBUTION": {
            "OBJECT": 50,
            "ARRAY": 30,
            "STRING": 5,
            "NUMBER": 4,
            "BOOLEAN": 1,
            "NULL": 10,
        },
        "ARRAY_LENGTH_MIN": 1,
        "ARRAY_LENGTH_MAX": 8,
        "ARRAY_LENGTH_P25": 2,
        "ARRAY_LENGTH_P50": 3,
        "ARRAY_LENGTH_P75": 5,
    }

    parsed = parse_column_profile_row(row, "VARIANT")
    assert parsed["column_type"] == "VARIANT"
    assert parsed["null_count"] == 10
    assert parsed["non_null_count"] == 90
    assert parsed["top_level_type_distribution"]["OBJECT"] == 50
    assert "array_length_stats" in parsed
    assert parsed["array_length_stats"]["p50"] == 3.0


def test_parse_top_values_from_pair_list() -> None:
    """Should parse APPROX_TOP_K style list payload."""
    parsed = parse_top_values([["a", 3], ["b", 2]], "TOP_VALUES")
    assert [x.value for x in parsed] == ["a", "b"]
    assert [x.count for x in parsed] == [3, 2]


def test_parse_path_profile_rows() -> None:
    """Should group path rows into value_type_distribution maps."""
    rows = [
        {
            "PATH": "$.a",
            "PATH_DEPTH": 2,
            "VALUE_TYPE": "OBJECT",
            "VALUE_COUNT": 6,
            "DISTINCT_COUNT_APPROX": 2,
            "NULL_RATIO": 0.0,
            "TOP_VALUES": [],
        },
        {
            "PATH": "$.a",
            "PATH_DEPTH": 2,
            "VALUE_TYPE": "ARRAY",
            "VALUE_COUNT": 4,
            "DISTINCT_COUNT_APPROX": 2,
            "NULL_RATIO": 0.0,
            "TOP_VALUES": [],
        },
    ]

    parsed = parse_path_profile_rows(rows, "payload", include_value_samples=True)
    assert len(parsed) == 1
    assert parsed[0]["column"] == "payload"
    assert parsed[0]["path"] == "$.a"
    assert parsed[0]["value_type_distribution"]["OBJECT"] == 6
    assert parsed[0]["value_type_distribution"]["ARRAY"] == 4
