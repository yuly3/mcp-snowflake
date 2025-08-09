import json
from datetime import timedelta
from typing import Any, ClassVar

import pytest
from pydantic import ValidationError

from mcp_snowflake.handler.execute_query import (
    ExecuteQueryArgs,
    _format_query_response,
    handle_execute_query,
)
from mcp_snowflake.kernel import DataProcessingResult


class MockEffectHandler:
    """Mock implementation of EffectExecuteQuery protocol."""

    def __init__(
        self,
        query_result: list[dict[str, Any]] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.query_result = query_result or []
        self.should_raise = should_raise
        self.called_with_sql: str | None = None
        self.called_with_timeout: timedelta | None = None

    async def execute_query(
        self,
        query: str,
        query_timeout: timedelta | None = None,
    ) -> list[dict[str, Any]]:
        self.called_with_sql = query
        self.called_with_timeout = query_timeout
        if self.should_raise:
            raise self.should_raise
        return self.query_result


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
    async def test_handle_execute_query_success(self) -> None:
        """Test successful query execution."""
        # Mock effect handler
        mock_data = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
        ]
        effect_handler = MockEffectHandler(query_result=mock_data)

        # Test args
        args = ExecuteQueryArgs(sql="SELECT id, name, age FROM users LIMIT 2")

        # Execute handler
        result = await handle_execute_query(args, effect_handler)

        # Verify result
        assert len(result) == 1
        content = result[0]
        assert content.type == "text"

        # Parse JSON response
        response_data = json.loads(content.text)
        assert "query_result" in response_data

        query_result = response_data["query_result"]
        assert set(query_result.keys()) == self.EXPECTED_RESPONSE_KEYS
        assert query_result["row_count"] == 2
        assert query_result["columns"] == ["id", "name", "age"]
        assert len(query_result["rows"]) == 2
        assert query_result["rows"][0] == {"id": 1, "name": "Alice", "age": 30}
        assert query_result["rows"][1] == {"id": 2, "name": "Bob", "age": 25}
        assert isinstance(query_result["execution_time_ms"], int)
        assert query_result["execution_time_ms"] >= 0

    @pytest.mark.asyncio
    async def test_handle_execute_query_write_sql_blocked(self) -> None:
        """Test that write SQL is blocked."""
        # Mock effect handler (should not be called for write operations)
        effect_handler = MockEffectHandler()

        # Test args with write SQL
        args = ExecuteQueryArgs(sql="INSERT INTO users (name) VALUES ('Charlie')")

        # Execute handler
        result = await handle_execute_query(args, effect_handler)

        # Verify write operation is blocked
        assert len(result) == 1
        content = result[0]
        assert content.type == "text"
        assert "Write operations are not allowed" in content.text

        # Verify effect handler was not called
        assert effect_handler.called_with_sql is None
        assert effect_handler.called_with_timeout is None

    @pytest.mark.asyncio
    async def test_handle_execute_query_empty_result(self) -> None:
        """Test query execution with empty result."""
        # Mock effect handler with empty result
        effect_handler = MockEffectHandler(query_result=[])

        # Test args
        args = ExecuteQueryArgs(sql="SELECT * FROM empty_table")

        # Execute handler
        result = await handle_execute_query(args, effect_handler)

        # Verify result
        assert len(result) == 1
        content = result[0]
        assert content.type == "text"

        # Parse JSON response
        response_data = json.loads(content.text)
        query_result = response_data["query_result"]
        assert set(query_result.keys()) == self.EXPECTED_RESPONSE_KEYS
        assert query_result["row_count"] == 0
        assert query_result["columns"] == []
        assert query_result["rows"] == []

    @pytest.mark.asyncio
    async def test_handle_execute_query_with_timeout(self) -> None:
        """Test query execution with custom timeout."""
        # Mock effect handler
        effect_handler = MockEffectHandler(query_result=[{"result": "success"}])

        # Test args with custom timeout
        args = ExecuteQueryArgs(sql="SELECT 1", timeout_seconds=60)

        # Execute handler
        result = await handle_execute_query(args, effect_handler)

        # Verify result
        assert len(result) == 1

        # Verify effect handler was called with correct timeout
        assert effect_handler.called_with_sql == "SELECT 1"
        assert effect_handler.called_with_timeout == timedelta(seconds=60)

    @pytest.mark.asyncio
    async def test_handle_execute_query_execution_error(self) -> None:
        """Test error handling during query execution."""
        # Mock effect handler to raise exception
        error_message = "Database connection failed"
        effect_handler = MockEffectHandler(should_raise=Exception(error_message))

        # Test args
        args = ExecuteQueryArgs(sql="SELECT 1")

        # Execute handler
        result = await handle_execute_query(args, effect_handler)

        # Verify error handling
        assert len(result) == 1
        content = result[0]
        assert content.type == "text"
        assert "Failed to execute query" in content.text
        assert "Database connection failed" in content.text

    def test_process_multiple_rows_data_success(self) -> None:
        """Test processing of query result data."""
        raw_data = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.0},
        ]

        result = DataProcessingResult.from_raw_rows(raw_data)

        assert len(result.processed_rows) == 2
        assert result.processed_rows[0] == {"id": 1, "name": "Alice", "score": 95.5}
        assert result.processed_rows[1] == {"id": 2, "name": "Bob", "score": 87.0}
        assert result.warnings == []

    def test_process_multiple_rows_data_empty(self) -> None:
        """Test processing of empty query result."""
        result = DataProcessingResult.from_raw_rows([])

        assert result.processed_rows == []
        assert result.warnings == []

    def test_format_query_response(self) -> None:
        """Test query response formatting."""
        processed_rows = [
            {"col1": "value1", "col2": "value2"},
            {"col1": "value3", "col2": "value4"},
        ]
        warnings = ["Warning message"]
        execution_time_ms = 150

        response = _format_query_response(
            processed_rows,
            warnings,
            execution_time_ms,
        )

        assert "query_result" in response
        query_result = response["query_result"]
        assert set(query_result.keys()) == self.EXPECTED_RESPONSE_KEYS
        assert query_result["execution_time_ms"] == execution_time_ms
        assert query_result["row_count"] == 2
        assert query_result["columns"] == ["col1", "col2"]
        assert query_result["rows"] == processed_rows
        assert query_result["warnings"] == warnings
