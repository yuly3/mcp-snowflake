"""Tests for column analysis functionality."""

from typing import TYPE_CHECKING, Any, cast

import pytest

from src.mcp_snowflake.handler.analyze_table_statistics._column_analysis import (
    classify_column_type,
    validate_and_select_columns,
)

if TYPE_CHECKING:
    import mcp.types as types


class TestClassifyColumnType:
    """Test classify_column_type function."""

    def test_numeric_types(self) -> None:
        """Test numeric type classification."""
        assert classify_column_type("NUMBER(10,2)") == "numeric"
        assert classify_column_type("INT") == "numeric"
        assert classify_column_type("FLOAT") == "numeric"
        assert classify_column_type("DOUBLE") == "numeric"
        assert classify_column_type("DECIMAL(10,2)") == "numeric"

    def test_date_types(self) -> None:
        """Test date type classification."""
        assert classify_column_type("DATE") == "date"
        assert classify_column_type("TIMESTAMP") == "date"
        assert classify_column_type("TIMESTAMP_NTZ") == "date"
        assert classify_column_type("TIME") == "date"

    def test_string_types(self) -> None:
        """Test string type classification."""
        assert classify_column_type("VARCHAR(100)") == "string"
        assert classify_column_type("CHAR(10)") == "string"
        assert classify_column_type("TEXT") == "string"
        assert classify_column_type("STRING") == "string"

    def test_case_insensitive(self) -> None:
        """Test that classification is case insensitive."""
        assert classify_column_type("varchar(100)") == "string"
        assert classify_column_type("Number(10,2)") == "numeric"
        assert classify_column_type("date") == "date"

    def test_unsupported_types(self) -> None:
        """Test that unsupported types raise ValueError."""
        with pytest.raises(ValueError, match="Unsupported column data type"):
            classify_column_type("ARRAY")
        with pytest.raises(ValueError, match="Unsupported column data type"):
            classify_column_type("OBJECT")
        with pytest.raises(ValueError, match="Unsupported column data type"):
            classify_column_type("VARIANT")


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

        columns, errors = validate_and_select_columns(all_columns, requested_columns)

        assert columns is not None
        assert errors is None
        assert len(columns) == 2
        assert columns[0]["name"] == "id"
        assert columns[1]["name"] == "name"

    def test_no_columns_requested(self) -> None:
        """Test when no specific columns are requested (all columns)."""
        all_columns = [
            {"name": "id", "data_type": "NUMBER(10,0)"},
            {"name": "name", "data_type": "VARCHAR(50)"},
        ]
        requested_columns: list[str] = []

        columns, errors = validate_and_select_columns(all_columns, requested_columns)

        assert columns is not None
        assert errors is None
        assert len(columns) == 2

    def test_missing_columns(self) -> None:
        """Test error when requested columns don't exist."""
        all_columns = [
            {"name": "id", "data_type": "NUMBER(10,0)"},
            {"name": "name", "data_type": "VARCHAR(50)"},
        ]
        requested_columns = ["id", "nonexistent"]

        columns, errors = validate_and_select_columns(all_columns, requested_columns)

        assert columns is None
        assert errors is not None
        assert len(errors) == 1
        error_content = cast("types.TextContent", errors[0])
        assert "nonexistent" in error_content.text

    def test_empty_columns_list(self) -> None:
        """Test error when no columns are available."""
        all_columns: list[dict[str, Any]] = []
        requested_columns: list[str] = []

        columns, errors = validate_and_select_columns(all_columns, requested_columns)

        assert columns is None
        assert errors is not None
        assert len(errors) == 1
        error_content = cast("types.TextContent", errors[0])
        assert "No columns to analyze" in error_content.text

    def test_unsupported_column_types(self) -> None:
        """Test error when unsupported column types are found."""
        all_columns = [
            {"name": "id", "data_type": "NUMBER(10,0)"},
            {"name": "metadata", "data_type": "VARIANT"},
        ]
        requested_columns: list[str] = []

        columns, errors = validate_and_select_columns(all_columns, requested_columns)

        assert columns is None
        assert errors is not None
        assert len(errors) == 1
        error_content = cast("types.TextContent", errors[0])
        assert "metadata (VARIANT)" in error_content.text

    def test_partial_unsupported_types(self) -> None:
        """Test when some columns are supported and some are not."""
        all_columns = [
            {"name": "id", "data_type": "NUMBER(10,0)"},
            {"name": "metadata", "data_type": "VARIANT"},
            {"name": "config", "data_type": "OBJECT"},
        ]
        requested_columns: list[str] = []

        columns, errors = validate_and_select_columns(all_columns, requested_columns)

        assert columns is None
        assert errors is not None
        assert len(errors) == 1
        error_content = cast("types.TextContent", errors[0])
        assert "metadata (VARIANT)" in error_content.text
        assert "config (OBJECT)" in error_content.text
