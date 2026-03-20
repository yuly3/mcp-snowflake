"""Tests for search_columns result parsing in adapter."""

import json
from typing import Any

from mcp_snowflake.adapter.search_columns_handler import _REQUIRED_COLUMNS
from mcp_snowflake.handler.search_columns import SearchColumnsTableEntry


def _parse_rows(rows: list[dict[str, Any]]) -> list[SearchColumnsTableEntry]:
    """Simulate the result parsing logic from SearchColumnsEffectHandler."""
    entries: list[SearchColumnsTableEntry] = []
    for row in rows:
        columns_raw = row["COLUMNS"]
        columns_parsed = json.loads(columns_raw) if isinstance(columns_raw, str) else columns_raw
        columns_json = json.dumps(columns_parsed, ensure_ascii=False, separators=(",", ":"))
        entries.append(
            SearchColumnsTableEntry(
                schema=row["TABLE_SCHEMA"],
                table=row["TABLE_NAME"],
                columns_json=columns_json,
            )
        )
    return entries


class TestResultParsing:
    """Test result parsing behavior."""

    def test_columns_as_string_is_minified(self) -> None:
        """When Snowflake returns COLUMNS as a pretty-printed JSON string."""
        rows = [
            {
                "TABLE_SCHEMA": "PUBLIC",
                "TABLE_NAME": "ORDERS",
                "COLUMNS": '[\n  {\n    "name": "ID",\n    "type": "NUMBER"\n  }\n]',
            }
        ]
        entries = _parse_rows(rows)

        assert len(entries) == 1
        assert entries[0].columns_json == '[{"name":"ID","type":"NUMBER"}]'

    def test_columns_as_native_list(self) -> None:
        """When Snowflake connector returns COLUMNS as a native Python list."""
        rows = [
            {
                "TABLE_SCHEMA": "PUBLIC",
                "TABLE_NAME": "ORDERS",
                "COLUMNS": [{"name": "ID", "type": "NUMBER"}],
            }
        ]
        entries = _parse_rows(rows)

        assert entries[0].columns_json == '[{"name":"ID","type":"NUMBER"}]'

    def test_comment_omitted_when_null(self) -> None:
        """OBJECT_CONSTRUCT omits NULL values, so comment key should be absent."""
        rows = [
            {
                "TABLE_SCHEMA": "S",
                "TABLE_NAME": "T",
                "COLUMNS": '[{"name": "COL", "type": "TEXT"}]',
            }
        ]
        entries = _parse_rows(rows)

        assert "comment" not in entries[0].columns_json

    def test_comment_included_when_present(self) -> None:
        rows = [
            {
                "TABLE_SCHEMA": "S",
                "TABLE_NAME": "T",
                "COLUMNS": '[{"name": "COL", "type": "TEXT", "comment": "description"}]',
            }
        ]
        entries = _parse_rows(rows)

        assert '"comment":"description"' in entries[0].columns_json

    def test_multiple_columns_per_table(self) -> None:
        rows = [
            {
                "TABLE_SCHEMA": "S",
                "TABLE_NAME": "T",
                "COLUMNS": '[{"name": "A", "type": "NUMBER"}, {"name": "B", "type": "TEXT"}]',
            }
        ]
        entries = _parse_rows(rows)

        parsed = json.loads(entries[0].columns_json)
        assert len(parsed) == 2
        assert parsed[0]["name"] == "A"
        assert parsed[1]["name"] == "B"

    def test_unicode_comment_preserved(self) -> None:
        rows = [
            {
                "TABLE_SCHEMA": "S",
                "TABLE_NAME": "T",
                "COLUMNS": '[{"name": "COL", "type": "TEXT", "comment": "日本語コメント"}]',
            }
        ]
        entries = _parse_rows(rows)

        assert "日本語コメント" in entries[0].columns_json

    def test_missing_required_column_raises_error(self) -> None:
        """Missing required columns should be detected via _REQUIRED_COLUMNS check."""
        row = {"TABLE_SCHEMA": "S", "TABLE_NAME": "T"}  # No COLUMNS key
        missing = _REQUIRED_COLUMNS - row.keys()
        assert missing == {"COLUMNS"}

    def test_all_required_columns_present(self) -> None:
        row = {"TABLE_SCHEMA": "S", "TABLE_NAME": "T", "COLUMNS": "[]"}
        missing = _REQUIRED_COLUMNS - row.keys()
        assert missing == set()
