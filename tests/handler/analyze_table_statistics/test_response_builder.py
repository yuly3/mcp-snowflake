"""Tests for response building functionality."""

import json
from typing import TYPE_CHECKING, cast

from mcp_snowflake.handler.analyze_table_statistics._response_builder import (
    build_response,
)
from mcp_snowflake.handler.analyze_table_statistics.models import (
    AnalyzeTableStatisticsArgs,
)
from mcp_snowflake.kernel.table_metadata import TableColumn

if TYPE_CHECKING:
    from mcp import types


class TestBuildResponse:
    """Test build_response function."""

    def test_build_response_single_numeric_column(self) -> None:
        """Test response building with a single numeric column."""
        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )

        result_row = {
            "TOTAL_ROWS": 100,
            "NUMERIC_ID_COUNT": 100,
            "NUMERIC_ID_NULL_COUNT": 0,
            "NUMERIC_ID_MIN": 1.0,
            "NUMERIC_ID_MAX": 100.0,
            "NUMERIC_ID_AVG": 50.5,
            "NUMERIC_ID_Q1": 25.0,
            "NUMERIC_ID_MEDIAN": 50.0,
            "NUMERIC_ID_Q3": 75.0,
            "NUMERIC_ID_DISTINCT": 100,
        }

        columns_to_analyze = [
            TableColumn(
                name="id",
                data_type="NUMBER(10,0)",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        response = build_response(args, result_row, columns_to_analyze)

        assert len(response) == 2
        assert response[0].type == "text"
        assert response[1].type == "text"

        # Check summary text
        summary_text = response[0].text
        assert "test_db.test_schema.test_table" in summary_text
        assert "100 total rows" in summary_text
        assert "Numeric: 1 columns" in summary_text
        assert "String: 0 columns" in summary_text
        assert "Date: 0 columns" in summary_text

        # Parse and check JSON response
        text_content = response[1]
        json_response = json.loads(text_content.text)

        assert "table_statistics" in json_response
        table_stats = json_response["table_statistics"]

        assert table_stats["table_info"]["database"] == "test_db"
        assert table_stats["table_info"]["schema"] == "test_schema"
        assert table_stats["table_info"]["table"] == "test_table"
        assert table_stats["table_info"]["total_rows"] == 100
        assert table_stats["table_info"]["analyzed_columns"] == 1

        assert "id" in table_stats["column_statistics"]
        id_stats = table_stats["column_statistics"]["id"]
        assert id_stats["column_type"] == "numeric"
        assert id_stats["count"] == 100
        assert id_stats["avg"] == 50.5

    def test_build_response_mixed_columns(self) -> None:
        """Test response building with mixed column types."""
        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )

        result_row = {
            "TOTAL_ROWS": 1000,
            # Numeric column
            "NUMERIC_PRICE_COUNT": 1000,
            "NUMERIC_PRICE_NULL_COUNT": 0,
            "NUMERIC_PRICE_MIN": 10.0,
            "NUMERIC_PRICE_MAX": 100.0,
            "NUMERIC_PRICE_AVG": 55.0,
            "NUMERIC_PRICE_Q1": 25.0,
            "NUMERIC_PRICE_MEDIAN": 50.0,
            "NUMERIC_PRICE_Q3": 75.0,
            "NUMERIC_PRICE_DISTINCT": 90,
            # String column
            "STRING_STATUS_COUNT": 1000,
            "STRING_STATUS_NULL_COUNT": 0,
            "STRING_STATUS_MIN_LENGTH": 6,
            "STRING_STATUS_MAX_LENGTH": 8,
            "STRING_STATUS_DISTINCT": 2,
            "STRING_STATUS_TOP_VALUES": '[["active", 600], ["pending", 400]]',
            # Date column
            "DATE_CREATED_COUNT": 1000,
            "DATE_CREATED_NULL_COUNT": 0,
            "DATE_CREATED_MIN": "2023-01-01",
            "DATE_CREATED_MAX": "2023-12-31",
            "DATE_CREATED_RANGE_DAYS": 364,
            "DATE_CREATED_DISTINCT": 365,
        }

        columns_to_analyze = [
            TableColumn(
                name="price",
                data_type="NUMBER(10,2)",
                nullable=False,
                ordinal_position=2,
            ),
            TableColumn(
                name="status",
                data_type="VARCHAR(10)",
                nullable=False,
                ordinal_position=3,
            ),
            TableColumn(
                name="created",
                data_type="DATE",
                nullable=False,
                ordinal_position=4,
            ),
        ]

        response = build_response(args, result_row, columns_to_analyze)

        # Check summary text
        summary_text = cast("types.TextContent", response[0]).text
        assert "1,000 total rows" in summary_text
        assert "Numeric: 1 columns" in summary_text
        assert "String: 1 columns" in summary_text
        assert "Date: 1 columns" in summary_text

        # Parse and check JSON response
        text_content = cast("types.TextContent", response[1])
        json_response = json.loads(text_content.text)

        table_stats = json_response["table_statistics"]
        assert table_stats["table_info"]["analyzed_columns"] == 3

        # Check all columns are present
        assert "price" in table_stats["column_statistics"]
        assert "status" in table_stats["column_statistics"]
        assert "created" in table_stats["column_statistics"]

    def test_build_response_with_custom_args(self) -> None:
        """Test response building with custom arguments."""
        args = AnalyzeTableStatisticsArgs(
            database="custom_db",
            schema_name="custom_schema",
            table_name="custom_table",
            columns=["specific_column"],
            top_k_limit=5,
        )

        result_row = {
            "TOTAL_ROWS": 50,
            "STRING_SPECIFIC_COLUMN_COUNT": 50,
            "STRING_SPECIFIC_COLUMN_NULL_COUNT": 0,
            "STRING_SPECIFIC_COLUMN_MIN_LENGTH": 1,
            "STRING_SPECIFIC_COLUMN_MAX_LENGTH": 10,
            "STRING_SPECIFIC_COLUMN_DISTINCT": 5,
            "STRING_SPECIFIC_COLUMN_TOP_VALUES": '[["A", 10], ["B", 10], ["C", 10], ["D", 10], ["E", 10]]',
        }

        columns_to_analyze = [
            TableColumn(
                name="specific_column",
                data_type="VARCHAR(10)",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        response = build_response(args, result_row, columns_to_analyze)

        # Check summary reflects custom database/schema/table
        summary_text = cast("types.TextContent", response[0]).text
        assert "custom_db.custom_schema.custom_table" in summary_text

        # Parse JSON and check structure
        text_content = cast("types.TextContent", response[1])
        json_response = json.loads(text_content.text)

        table_info = json_response["table_statistics"]["table_info"]
        assert table_info["database"] == "custom_db"
        assert table_info["schema"] == "custom_schema"
        assert table_info["table"] == "custom_table"

    def test_build_response_large_numbers(self) -> None:
        """Test response building with large numbers (formatting)."""
        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )

        result_row = {
            "TOTAL_ROWS": 1234567,
            "NUMERIC_ID_COUNT": 1234567,
            "NUMERIC_ID_NULL_COUNT": 0,
            "NUMERIC_ID_MIN": 1.0,
            "NUMERIC_ID_MAX": 1234567.0,
            "NUMERIC_ID_AVG": 617284.0,
            "NUMERIC_ID_Q1": 308642.0,
            "NUMERIC_ID_MEDIAN": 617284.0,
            "NUMERIC_ID_Q3": 925926.0,
            "NUMERIC_ID_DISTINCT": 1234567,
        }

        columns_to_analyze = [
            TableColumn(
                name="id",
                data_type="NUMBER(10,0)",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        response = build_response(args, result_row, columns_to_analyze)

        # Check that large numbers are formatted with commas
        summary_text = cast("types.TextContent", response[0]).text
        assert "1,234,567 total rows" in summary_text

    def test_build_response_json_formatting(self) -> None:
        """Test that JSON response is properly formatted."""
        args = AnalyzeTableStatisticsArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )

        result_row = {
            "TOTAL_ROWS": 100,
            "NUMERIC_ID_COUNT": 100,
            "NUMERIC_ID_NULL_COUNT": 0,
            "NUMERIC_ID_MIN": 1.0,
            "NUMERIC_ID_MAX": 100.0,
            "NUMERIC_ID_AVG": 50.5,
            "NUMERIC_ID_Q1": 25.0,
            "NUMERIC_ID_MEDIAN": 50.0,
            "NUMERIC_ID_Q3": 75.0,
            "NUMERIC_ID_DISTINCT": 100,
        }

        columns_to_analyze = [
            TableColumn(
                name="id",
                data_type="NUMBER(10,0)",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        response = build_response(args, result_row, columns_to_analyze)

        # Check JSON is properly formatted (indented)
        text_content = cast("types.TextContent", response[1])
        json_text = text_content.text

        # Should be indented (contains newlines and spaces)
        assert "\n" in json_text
        assert "  " in json_text  # 2-space indentation

        # Should be valid JSON
        parsed = json.loads(json_text)
        assert isinstance(parsed, dict)
