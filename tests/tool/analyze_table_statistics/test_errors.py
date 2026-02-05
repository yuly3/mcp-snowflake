"""Test for AnalyzeTableStatisticsTool - Error Cases."""

from types import SimpleNamespace
from typing import Any

import mcp.types as types
import pytest
from snowflake.connector import (
    DataError,
    IntegrityError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)

from cattrs_converter import JsonImmutableConverter
from expression.contract import ContractViolationError
from kernel.table_metadata import TableColumn
from mcp_snowflake.adapter.analyze_table_statistics_handler.result_parser import (
    parse_statistics_result,
)
from mcp_snowflake.handler.analyze_table_statistics import TableStatisticsParseResult
from mcp_snowflake.tool.analyze_table_statistics import AnalyzeTableStatisticsTool

from ...mock_effect_handler import MockAnalyzeTableStatistics


class TestAnalyzeTableStatisticsToolErrors:
    """Tests for error conditions in analyze_table_statistics tool."""

    @pytest.mark.asyncio
    async def test_perform_with_empty_arguments(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test with empty arguments."""
        mock_effect = MockAnalyzeTableStatistics()
        tool = AnalyzeTableStatisticsTool(json_converter, mock_effect)

        result = await tool.perform(None)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Invalid arguments for analyze_table_statistics:" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_invalid_arguments(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test with invalid arguments."""
        mock_effect = MockAnalyzeTableStatistics()
        tool = AnalyzeTableStatisticsTool(json_converter, mock_effect)

        # Missing required fields
        arguments = {"database": "test_db"}  # Missing schema and table
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Invalid arguments for analyze_table_statistics:" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_empty_dict_arguments(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test with empty dictionary arguments."""
        mock_effect = MockAnalyzeTableStatistics()
        tool = AnalyzeTableStatisticsTool(json_converter, mock_effect)

        arguments = {}  # Empty dict, missing required fields
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Invalid arguments for analyze_table_statistics:" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_invalid_top_k_limit(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test with invalid top_k_limit values."""
        mock_effect = MockAnalyzeTableStatistics()
        tool = AnalyzeTableStatisticsTool(json_converter, mock_effect)

        # Test with top_k_limit exceeding maximum
        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "top_k_limit": 150,  # Exceeds maximum of 100
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Invalid arguments for analyze_table_statistics:" in result[0].text

        # Test with top_k_limit below minimum
        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "top_k_limit": 0,  # Below minimum of 1
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Invalid arguments for analyze_table_statistics:" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_invalid_columns_type(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test with invalid columns argument type."""
        mock_effect = MockAnalyzeTableStatistics()
        tool = AnalyzeTableStatisticsTool(json_converter, mock_effect)

        # Test with columns as string instead of list
        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "columns": "ID,NAME",  # Should be a list, not string
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Invalid arguments for analyze_table_statistics:" in result[0].text

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("exception", "expected_message_prefix"),
        [
            (TimeoutError("Connection timeout"), "Error: Query timed out:"),
            (
                ProgrammingError("SQL syntax error"),
                "Error: SQL syntax error or other programming error:",
            ),
            (
                OperationalError("Database connection error"),
                "Error: Database operation related error:",
            ),
            (DataError("Invalid data"), "Error: Data processing related error:"),
            (
                IntegrityError("Constraint violation"),
                "Error: Referential integrity constraint violation:",
            ),
            (
                NotSupportedError("Feature not supported"),
                "Error: Unsupported database feature used:",
            ),
            (
                ContractViolationError("Contract violation"),
                "Error: Unexpected error:",
            ),
        ],
    )
    async def test_perform_with_exceptions(
        self,
        json_converter: JsonImmutableConverter,
        exception: Exception,
        expected_message_prefix: str,
    ) -> None:
        """Test exception handling in perform method."""
        mock_effect = MockAnalyzeTableStatistics(should_raise=exception)
        tool = AnalyzeTableStatisticsTool(json_converter, mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert result[0].text.startswith(expected_message_prefix)
        assert str(exception) in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_describe_table_exception(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test exception during describe_table operation."""
        # Test that exceptions from describe_table are also handled properly
        mock_effect = MockAnalyzeTableStatistics(should_raise=ProgrammingError("Table does not exist"))
        tool = AnalyzeTableStatisticsTool(json_converter, mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "nonexistent_table",
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: SQL syntax error or other programming error:" in result[0].text
        assert "Table does not exist" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_analyze_statistics_exception(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test exception during analyze_table_statistics operation."""
        # Test that exceptions from analyze_table_statistics are handled properly
        mock_effect = MockAnalyzeTableStatistics(should_raise=OperationalError("Statistics analysis failed"))
        tool = AnalyzeTableStatisticsTool(json_converter, mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Database operation related error:" in result[0].text
        assert "Statistics analysis failed" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_timeout_error(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test timeout error handling."""
        mock_effect = MockAnalyzeTableStatistics(should_raise=TimeoutError("Query execution timeout after 30 seconds"))
        tool = AnalyzeTableStatisticsTool(json_converter, mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "large_table",
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Query timed out:" in result[0].text
        assert "Query execution timeout after 30 seconds" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_data_error(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test data error handling."""
        mock_effect = MockAnalyzeTableStatistics(should_raise=DataError("Invalid data format in column"))
        tool = AnalyzeTableStatisticsTool(json_converter, mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Data processing related error:" in result[0].text
        assert "Invalid data format in column" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_contract_violation_error(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test contract violation error handling."""
        mock_effect = MockAnalyzeTableStatistics(should_raise=ContractViolationError("Unexpected contract violation"))
        tool = AnalyzeTableStatisticsTool(json_converter, mock_effect)

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
        }
        result = await tool.perform(arguments)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Unexpected error:" in result[0].text
        assert "Unexpected contract violation" in result[0].text

    @pytest.mark.asyncio
    async def test_perform_with_statistics_result_parse_error(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test that StatisticsResultParseError is properly mapped to user-friendly message."""

        # Mock effect that returns result causing parse error
        class MockEffectWithBadResult:
            async def describe_table(
                self,
                database: Any,  # noqa: ARG002
                schema: Any,  # noqa: ARG002
                table: Any,  # noqa: ARG002
            ) -> Any:
                return SimpleNamespace(
                    columns=[
                        TableColumn(
                            name="status",
                            data_type="VARCHAR(10)",
                            nullable=False,
                            ordinal_position=1,
                        )
                    ]
                )

            async def analyze_table_statistics(
                self,
                database: Any,  # noqa: ARG002
                schema: Any,  # noqa: ARG002
                table: Any,  # noqa: ARG002
                columns_to_analyze: Any,
                top_k_limit: Any,  # noqa: ARG002
            ) -> TableStatisticsParseResult:
                # Return result with invalid TOP_VALUES JSON
                result_row = {
                    "TOTAL_ROWS": 1000,
                    "STRING_STATUS_COUNT": 1000,
                    "STRING_STATUS_NULL_COUNT": 0,
                    "STRING_STATUS_MIN_LENGTH": 1,
                    "STRING_STATUS_MAX_LENGTH": 10,
                    "STRING_STATUS_DISTINCT": 3,
                    "STRING_STATUS_TOP_VALUES": "invalid_json",  # This will cause parse error
                }
                return parse_statistics_result(result_row, columns_to_analyze)

        tool = AnalyzeTableStatisticsTool(
            json_converter=json_converter,
            effect_handler=MockEffectWithBadResult(),
        )

        arguments = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "columns": ["status"],
        }

        result = await tool.perform(arguments)
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].type == "text"
        assert "Error: Snowflake returned unexpected result format:" in result[0].text, (
            f"Expected parse error message but got: {result[0].text!r}"
        )
        assert "Failed to parse STRING_STATUS_TOP_VALUES JSON" in result[0].text
