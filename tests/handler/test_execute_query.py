import json
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from mcp_snowflake.handler.data_processing import process_multiple_rows_data
from mcp_snowflake.handler.execute_query import (
    ExecuteQueryArgs,
    _format_query_response,
    handle_execute_query,
)


class TestExecuteQueryHandler:
    """Test execute_query handler functionality."""

    @pytest.mark.asyncio
    async def test_handle_execute_query_success(self) -> None:
        """Test successful query execution."""
        # Mock effect handler
        effect_handler = AsyncMock()
        effect_handler.execute_query.return_value = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
        ]

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
        assert query_result["sql"] == "SELECT id, name, age FROM users LIMIT 2"
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
        # Mock effect handler
        effect_handler = AsyncMock()

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
        effect_handler.execute_query.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_execute_query_empty_result(self) -> None:
        """Test query execution with empty result."""
        # Mock effect handler
        effect_handler = AsyncMock()
        effect_handler.execute_query.return_value = []

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
        assert query_result["row_count"] == 0
        assert query_result["columns"] == []
        assert query_result["rows"] == []

    @pytest.mark.asyncio
    async def test_handle_execute_query_with_timeout(self) -> None:
        """Test query execution with custom timeout."""
        # Mock effect handler
        effect_handler = AsyncMock()
        effect_handler.execute_query.return_value = [{"result": "success"}]

        # Test args with custom timeout
        args = ExecuteQueryArgs(sql="SELECT 1", timeout_seconds=60)

        # Execute handler
        result = await handle_execute_query(args, effect_handler)

        # Verify result
        assert len(result) == 1

        # Verify effect handler was called with correct timeout
        effect_handler.execute_query.assert_called_once()
        call_args = effect_handler.execute_query.call_args
        assert call_args[0][0] == "SELECT 1"  # SQL
        assert call_args[0][1] == timedelta(seconds=60)  # timeout

    @pytest.mark.asyncio
    async def test_handle_execute_query_execution_error(self) -> None:
        """Test error handling during query execution."""
        # Mock effect handler to raise exception
        effect_handler = AsyncMock()
        effect_handler.execute_query.side_effect = Exception(
            "Database connection failed"
        )

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

        result = process_multiple_rows_data(raw_data)

        assert len(result["processed_rows"]) == 2
        assert result["processed_rows"][0] == {"id": 1, "name": "Alice", "score": 95.5}
        assert result["processed_rows"][1] == {"id": 2, "name": "Bob", "score": 87.0}
        assert result["warnings"] == []

    def test_process_multiple_rows_data_empty(self) -> None:
        """Test processing of empty query result."""
        result = process_multiple_rows_data([])

        assert result["processed_rows"] == []
        assert result["warnings"] == []

    def test_format_query_response(self) -> None:
        """Test query response formatting."""
        processed_rows = [
            {"col1": "value1", "col2": "value2"},
            {"col1": "value3", "col2": "value4"},
        ]
        warnings = ["Warning message"]
        sql = "SELECT col1, col2 FROM test_table"
        execution_time_ms = 150

        response = _format_query_response(
            processed_rows, warnings, sql, execution_time_ms
        )

        assert "query_result" in response
        query_result = response["query_result"]
        assert query_result["sql"] == sql
        assert query_result["execution_time_ms"] == execution_time_ms
        assert query_result["row_count"] == 2
        assert query_result["columns"] == ["col1", "col2"]
        assert query_result["rows"] == processed_rows
        assert query_result["warnings"] == warnings
