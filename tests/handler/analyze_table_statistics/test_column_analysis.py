"""Tests for column analysis functionality."""

import mcp.types as types

from mcp_snowflake.handler.analyze_table_statistics._column_analysis import (
    validate_and_select_columns,
)
from mcp_snowflake.kernel.table_metadata import TableColumn


class TestValidateAndSelectColumns:
    """Test validate_and_select_columns function."""

    def test_valid_columns_selection(self) -> None:
        """Test successful column selection."""
        all_columns = [
            TableColumn(
                name="id",
                data_type="NUMBER(10,0)",
                nullable=False,
                default_value=None,
                comment=None,
                ordinal_position=1,
            ),
            TableColumn(
                name="name",
                data_type="VARCHAR(50)",
                nullable=True,
                default_value=None,
                comment=None,
                ordinal_position=2,
            ),
            TableColumn(
                name="date",
                data_type="DATE",
                nullable=True,
                default_value=None,
                comment=None,
                ordinal_position=3,
            ),
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
            TableColumn(
                name="id",
                data_type="NUMBER(10,0)",
                nullable=False,
                default_value=None,
                comment=None,
                ordinal_position=1,
            ),
            TableColumn(
                name="name",
                data_type="VARCHAR(50)",
                nullable=True,
                default_value=None,
                comment=None,
                ordinal_position=2,
            ),
        ]
        requested_columns: list[str] = []

        columns = validate_and_select_columns(all_columns, requested_columns)

        assert not isinstance(columns, types.TextContent)
        assert len(columns) == 2

    def test_missing_columns(self) -> None:
        """Test error when requested columns don't exist."""
        all_columns = [
            TableColumn(
                name="id",
                data_type="NUMBER(10,0)",
                nullable=False,
                default_value=None,
                comment=None,
                ordinal_position=1,
            ),
            TableColumn(
                name="name",
                data_type="VARCHAR(50)",
                nullable=True,
                default_value=None,
                comment=None,
                ordinal_position=2,
            ),
        ]
        requested_columns = ["id", "nonexistent"]

        error = validate_and_select_columns(all_columns, requested_columns)

        assert isinstance(error, types.TextContent)
        assert "nonexistent" in error.text

    def test_empty_columns_list(self) -> None:
        """Test error when no columns are available."""
        all_columns: list[TableColumn] = []
        requested_columns: list[str] = []

        error = validate_and_select_columns(all_columns, requested_columns)

        assert isinstance(error, types.TextContent)
        assert "No columns to analyze" in error.text

    def test_unsupported_column_types(self) -> None:
        """Test error when unsupported column types are found."""
        all_columns = [
            TableColumn(
                name="id",
                data_type="NUMBER(10,0)",
                nullable=False,
                default_value=None,
                comment=None,
                ordinal_position=1,
            ),
            TableColumn(
                name="metadata",
                data_type="VARIANT",
                nullable=True,
                default_value=None,
                comment=None,
                ordinal_position=2,
            ),
        ]
        requested_columns: list[str] = []

        error = validate_and_select_columns(all_columns, requested_columns)

        assert isinstance(error, types.TextContent)
        assert "metadata (VARIANT)" in error.text

    def test_partial_unsupported_types(self) -> None:
        """Test when some columns are supported and some are not."""
        all_columns = [
            TableColumn(
                name="id",
                data_type="NUMBER(10,0)",
                nullable=False,
                default_value=None,
                comment=None,
                ordinal_position=1,
            ),
            TableColumn(
                name="metadata",
                data_type="VARIANT",
                nullable=True,
                default_value=None,
                comment=None,
                ordinal_position=2,
            ),
            TableColumn(
                name="config",
                data_type="OBJECT",
                nullable=True,
                default_value=None,
                comment=None,
                ordinal_position=3,
            ),
        ]
        requested_columns: list[str] = []

        error = validate_and_select_columns(all_columns, requested_columns)

        assert isinstance(error, types.TextContent)
        assert "metadata (VARIANT)" in error.text
        assert "config (OBJECT)" in error.text
