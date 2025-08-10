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


class TestBuildResponseWithUnsupportedColumns:
    """Test build_response function with unsupported columns."""

    def test_build_response_with_unsupported_columns(self) -> None:
        """Test response building with unsupported columns."""
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

        supported_columns = [
            TableColumn(
                name="id",
                data_type="NUMBER(10,0)",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        unsupported_columns = [
            TableColumn(
                name="metadata",
                data_type="VARIANT",
                nullable=True,
                ordinal_position=2,
            ),
            TableColumn(
                name="config",
                data_type="OBJECT",
                nullable=True,
                ordinal_position=3,
            ),
        ]

        response = build_response(
            args, result_row, supported_columns, unsupported_columns
        )

        assert len(response) == 2
        text_content = cast("types.TextContent", response[0])
        json_content = cast("types.TextContent", response[1])

        # Check summary text includes unsupported note
        assert (
            "Note: Some columns were not analyzed due to unsupported data types"
            in text_content.text
        )
        assert "2 column(s) skipped" in text_content.text

        # Parse and validate JSON structure
        parsed = json.loads(json_content.text)
        table_stats = parsed["table_statistics"]

        # Check unsupported_columns field
        assert "unsupported_columns" in table_stats
        unsupported = table_stats["unsupported_columns"]
        assert len(unsupported) == 2
        assert unsupported[0]["name"] == "metadata"
        assert unsupported[0]["data_type"] == "VARIANT"
        assert unsupported[1]["name"] == "config"
        assert unsupported[1]["data_type"] == "OBJECT"

        # Ensure no reason field is included
        for col in unsupported:
            assert "reason" not in col
            assert len(col) == 2  # Only name and data_type

    def test_build_response_without_unsupported_columns(self) -> None:
        """Test response building without unsupported columns (default behavior)."""
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

        supported_columns = [
            TableColumn(
                name="id",
                data_type="NUMBER(10,0)",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        # Test with default empty unsupported_columns
        response = build_response(args, result_row, supported_columns)

        assert len(response) == 2
        text_content = cast("types.TextContent", response[0])
        json_content = cast("types.TextContent", response[1])

        # Check summary text does NOT include unsupported note
        assert "Note: Some columns were not analyzed" not in text_content.text
        assert "skipped" not in text_content.text

        # Parse and validate JSON structure
        parsed = json.loads(json_content.text)
        table_stats = parsed["table_statistics"]

        # Check unsupported_columns field is absent
        assert "unsupported_columns" not in table_stats

    def test_build_response_empty_unsupported_columns(self) -> None:
        """Test response building with explicitly empty unsupported columns."""
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

        supported_columns = [
            TableColumn(
                name="id",
                data_type="NUMBER(10,0)",
                nullable=False,
                ordinal_position=1,
            ),
        ]

        # Test with explicitly empty list
        response = build_response(args, result_row, supported_columns, [])

        assert len(response) == 2
        text_content = cast("types.TextContent", response[0])
        json_content = cast("types.TextContent", response[1])

        # Check summary text does NOT include unsupported note
        assert "Note: Some columns were not analyzed" not in text_content.text

        # Parse and validate JSON structure
        parsed = json.loads(json_content.text)
        table_stats = parsed["table_statistics"]

        # Check unsupported_columns field is absent
        assert "unsupported_columns" not in table_stats
