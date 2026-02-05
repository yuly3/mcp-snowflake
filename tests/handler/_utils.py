"""Shared test utilities for handler tests.

This module provides lightweight helpers to reduce code duplication in handler tests.
These utilities are for test code only and should not be used in production code.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import mcp.types as types

from kernel.table_metadata import TableColumn

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


def assert_single_text(result: Sequence[Any]) -> types.TextContent:
    """Assert result contains exactly one TextContent item and return it."""
    assert len(result) == 1, f"Expected single content, got {len(result)}"
    content = result[0]
    assert isinstance(content, types.TextContent)
    assert content.type == "text"
    return content


def parse_json_text(content: types.TextContent) -> dict[str, Any]:
    """Parse TextContent as JSON and return the dict."""
    return json.loads(content.text)


def assert_keys_exact(obj: dict[str, Any], expected: set[str]) -> None:
    """Assert object has exactly the expected keys."""
    actual = set(obj.keys())
    assert actual == expected, f"Keys mismatch: {actual} != {expected}"


def assert_tabular_json(
    root: dict[str, Any],
    key: str,
    expected_cols: list[str],
    expected_row_count: int,
) -> dict[str, Any]:
    """Assert tabular data structure in JSON and return the data dict.

    Works with both execute_query format (row_count) and sample_table_data format (actual_rows).
    """
    assert key in root, f"Key '{key}' not found in response"
    data = root[key]
    assert "columns" in data, f"'columns' field missing in {key}"
    assert "rows" in data, f"'rows' field missing in {key}"

    assert data["columns"] == expected_cols, f"Column mismatch in {key}"

    # Handle both row_count and actual_rows fields
    actual_count = data.get("row_count", data.get("actual_rows", -1))
    assert actual_count == expected_row_count, f"Row count mismatch in {key}: {actual_count} != {expected_row_count}"

    return data


def assert_list_output(text: str, header: str, items: Iterable[str]) -> None:
    """Assert text contains header and all items in list format."""
    assert header in text, f"Header '{header}' not found in output"
    for item in items:
        expected_line = f"- {item}"
        assert expected_line in text, f"List item '{expected_line}' not found in output"


def col(
    name: str,
    data_type: str,
    *,
    nullable: bool,
    pos: int,
    comment: str | None = None,
    default: str | None = None,
) -> TableColumn:
    """Create TableColumn instances for tests."""
    return TableColumn(
        name=name,
        data_type=data_type,
        nullable=nullable,
        ordinal_position=pos,
        comment=comment,
        default_value=default,
    )
