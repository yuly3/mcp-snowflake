from datetime import timedelta
from typing import ClassVar

import pytest
from pydantic import ValidationError

from cattrs_converter import JsonImmutableConverter
from kernel import DataProcessingResult
from mcp_snowflake.handler.execute_query import (
    ExecuteQueryArgs,
    handle_execute_query,
)

from ..mock_effect_handler import MockExecuteQuery


class TestExecuteQueryArgs:
    """Test ExecuteQueryArgs validation."""

    def test_valid_args(self) -> None:
        """Test valid arguments."""
        args = ExecuteQueryArgs(sql="SELECT 1")
        assert args.sql == "SELECT 1"
        assert args.timeout_seconds == 30  # default value

    def test_valid_args_with_timeout(self) -> None:
        """Test valid arguments with custom timeout."""
        args = ExecuteQueryArgs(sql="SELECT 1", timeout_seconds=60)
        assert args.sql == "SELECT 1"
        assert args.timeout_seconds == 60

    def test_missing_sql(self) -> None:
        """Test missing sql argument."""
        with pytest.raises(ValidationError):
            _ = ExecuteQueryArgs.model_validate({})

    def test_empty_sql(self) -> None:
        """Test empty sql string."""
        args = ExecuteQueryArgs(sql="")
        assert args.sql == ""


class TestExecuteQueryHandler:
    """Test execute_query handler functionality."""

    # Expected keys in execute_query response
    EXPECTED_RESPONSE_KEYS: ClassVar[set[str]] = {
        "execution_time_ms",
        "row_count",
        "columns",
        "rows",
        "warnings",
    }

    @pytest.mark.asyncio
    async def test_handle_execute_query_success(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test successful query execution."""
        # Mock effect handler
        mock_data = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
        ]
        effect_handler = MockExecuteQuery(result_data=mock_data)

        # Test args
        args = ExecuteQueryArgs(sql="SELECT id, name, age FROM users LIMIT 2")

        # Execute handler
        result = await handle_execute_query(json_converter, args, effect_handler)

        # Verify result - should be ExecuteQueryJsonResponse directly
        assert isinstance(result, dict)
        assert "query_result" in result

        query_result = result["query_result"]
        assert query_result["row_count"] == 2
        assert query_result["columns"] == ["id", "name", "age"]
        assert len(query_result["rows"]) == 2
        assert query_result["rows"][0] == {"id": 1, "name": "Alice", "age": 30}
        assert query_result["rows"][1] == {"id": 2, "name": "Bob", "age": 25}
        assert isinstance(query_result["execution_time_ms"], int)
        assert query_result["execution_time_ms"] >= 0

    @pytest.mark.asyncio
    async def test_handle_execute_query_write_sql_blocked(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test that write SQL is blocked."""
        # Mock effect handler (should not be called for write operations)
        effect_handler = MockExecuteQuery()

        # Test args with write SQL
        args = ExecuteQueryArgs(sql="INSERT INTO users (name) VALUES ('Charlie')")

        # Execute handler - should raise ValueError for write operations
        with pytest.raises(ValueError, match="Write operations are not allowed"):
            _ = await handle_execute_query(json_converter, args, effect_handler)

        # Verify effect handler was not called
        assert effect_handler.called_with_sql is None
        assert effect_handler.called_with_timeout is None

    @pytest.mark.asyncio
    async def test_handle_execute_query_empty_result(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test query execution with empty result."""
        # Mock effect handler with empty result
        effect_handler = MockExecuteQuery(result_data=[])

        # Test args
        args = ExecuteQueryArgs(sql="SELECT * FROM empty_table")

        # Execute handler
        result = await handle_execute_query(json_converter, args, effect_handler)

        # Verify result - should be ExecuteQueryJsonResponse directly
        assert isinstance(result, dict)
        assert "query_result" in result
        query_result = result["query_result"]
        assert query_result["row_count"] == 0
        assert query_result["columns"] == []
        assert query_result["rows"] == []

    @pytest.mark.asyncio
    async def test_handle_execute_query_with_timeout(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test query execution with custom timeout."""
        # Mock effect handler
        effect_handler = MockExecuteQuery(result_data=[{"result": "success"}])

        # Test args with custom timeout
        args = ExecuteQueryArgs(sql="SELECT 1", timeout_seconds=60)

        # Execute handler
        result = await handle_execute_query(json_converter, args, effect_handler)

        # Verify result
        assert len(result) == 1

        # Verify effect handler was called with correct timeout
        assert effect_handler.called_with_sql == "SELECT 1"
        assert effect_handler.called_with_timeout == timedelta(seconds=60)

    @pytest.mark.asyncio
    async def test_handle_execute_query_execution_error(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test error handling during query execution."""
        # Mock effect handler to raise exception
        error_message = "Database connection failed"
        effect_handler = MockExecuteQuery(should_raise=Exception(error_message))

        # Test args
        args = ExecuteQueryArgs(sql="SELECT 1")

        # Execute handler - should raise exception directly
        with pytest.raises(Exception, match="Database connection failed"):
            _ = await handle_execute_query(json_converter, args, effect_handler)

    def test_process_multiple_rows_data_success(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test processing of query result data."""
        raw_data = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.0},
        ]

        result = DataProcessingResult.from_raw_rows(json_converter, raw_data)

        assert len(result.processed_rows) == 2
        assert result.processed_rows[0] == {"id": 1, "name": "Alice", "score": 95.5}
        assert result.processed_rows[1] == {"id": 2, "name": "Bob", "score": 87.0}
        assert result.warnings == []

    def test_process_multiple_rows_data_empty(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test processing of empty query result."""
        result = DataProcessingResult.from_raw_rows(json_converter, [])

        assert result.processed_rows == []
        assert result.warnings == []
