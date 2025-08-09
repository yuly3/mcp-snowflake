"""Tests for column analysis functionality."""

from typing import Any

import mcp.types as types

from mcp_snowflake.handler.analyze_table_statistics._column_analysis import (
    validate_and_select_columns,
)


class TestValidateAndSelectColumns:
    """Test validate_and_select_columns function."""

    def test_valid_columns_selection(self) -> None:
        """Test successful column selection."""
        all_columns = [
            {"name": "id", "data_type": "NUMBER(10,0)"},
            {"name": "name", "data_type": "VARCHAR(50)"},
            {"name": "date", "data_type": "DATE"},
        ]
        requested_columns = ["id", "name"]

        columns = validate_and_select_columns(all_columns, requested_columns)

        assert not isinstance(columns, types.TextContent)
        assert len(columns) == 2
        assert columns[0].name == "id"
        assert columns[1].name == "name"

    def test_no_columns_requested(self) -> None:
        """Test when no specific columns are requested (all columns)."""
        all_columns = [
            {"name": "id", "data_type": "NUMBER(10,0)"},
            {"name": "name", "data_type": "VARCHAR(50)"},
        ]
        requested_columns: list[str] = []

        columns = validate_and_select_columns(all_columns, requested_columns)

        assert not isinstance(columns, types.TextContent)
        assert len(columns) == 2

    def test_missing_columns(self) -> None:
        """Test error when requested columns don't exist."""
        all_columns = [
            {"name": "id", "data_type": "NUMBER(10,0)"},
            {"name": "name", "data_type": "VARCHAR(50)"},
        ]
        requested_columns = ["id", "nonexistent"]

        error = validate_and_select_columns(all_columns, requested_columns)

        assert isinstance(error, types.TextContent)
        assert "nonexistent" in error.text

    def test_empty_columns_list(self) -> None:
        """Test error when no columns are available."""
        all_columns: list[dict[str, Any]] = []
        requested_columns: list[str] = []

        error = validate_and_select_columns(all_columns, requested_columns)

        assert isinstance(error, types.TextContent)
        assert "No columns to analyze" in error.text

    def test_unsupported_column_types(self) -> None:
        """Test error when unsupported column types are found."""
        all_columns = [
            {"name": "id", "data_type": "NUMBER(10,0)"},
            {"name": "metadata", "data_type": "VARIANT"},
        ]
        requested_columns: list[str] = []

        error = validate_and_select_columns(all_columns, requested_columns)

        assert isinstance(error, types.TextContent)
        assert "metadata (VARIANT)" in error.text

    def test_partial_unsupported_types(self) -> None:
        """Test when some columns are supported and some are not."""
        all_columns = [
            {"name": "id", "data_type": "NUMBER(10,0)"},
            {"name": "metadata", "data_type": "VARIANT"},
            {"name": "config", "data_type": "OBJECT"},
        ]
        requested_columns: list[str] = []

        error = validate_and_select_columns(all_columns, requested_columns)

        assert isinstance(error, types.TextContent)
        assert "metadata (VARIANT)" in error.text
        assert "config (OBJECT)" in error.text
