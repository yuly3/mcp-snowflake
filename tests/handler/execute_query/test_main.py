from datetime import timedelta

import pytest
from pydantic import ValidationError

from cattrs_converter import JsonImmutableConverter
from kernel import DataProcessingResult
from mcp_snowflake.handler.errors import SQLAnalysisFailedError, SQLBlockedError
from mcp_snowflake.handler.execute_query import (
    ExecuteQueryArgs,
    QueryResult,
    handle_execute_query,
)
from snowflake_sql_parser import DiagnosticCode

from ...mock_effect_handler import MockExecuteQuery


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

    def test_valid_args_with_timeout_max_context(self) -> None:
        """Test timeout validation with custom max timeout from context."""
        args = ExecuteQueryArgs.model_validate(
            {"sql": "SELECT 1", "timeout_seconds": 120},
            context={"timeout_seconds_max": 120},
        )
        assert args.timeout_seconds == 120

    def test_timeout_exceeds_timeout_max_context(self) -> None:
        """Test validation error when timeout exceeds custom max timeout."""
        with pytest.raises(ValidationError, match="less than or equal to 60"):
            _ = ExecuteQueryArgs.model_validate(
                {"sql": "SELECT 1", "timeout_seconds": 61},
                context={"timeout_seconds_max": 60},
            )

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

        # Verify result - should be QueryResult
        assert isinstance(result, QueryResult)
        assert result.row_count == 2
        assert result.columns == ["id", "name", "age"]
        assert len(result.rows) == 2
        assert result.rows[0] == {"id": 1, "name": "Alice", "age": 30}
        assert result.rows[1] == {"id": 2, "name": "Bob", "age": 25}
        assert isinstance(result.execution_time_ms, int)
        assert result.execution_time_ms >= 0

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

        with pytest.raises(SQLBlockedError, match="DML statements are not allowed"):
            _ = await handle_execute_query(json_converter, args, effect_handler)

        # Verify effect handler was not called
        assert effect_handler.called_with_sql is None
        assert effect_handler.called_with_timeout is None

    @pytest.mark.asyncio
    async def test_handle_execute_query_with_call_blocked(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test that WITH ... CALL is blocked."""
        effect_handler = MockExecuteQuery()
        args = ExecuteQueryArgs(
            sql="WITH p AS PROCEDURE () RETURNS VARCHAR LANGUAGE SQL AS $$BEGIN RETURN 'ok'; END;$$ CALL p()"
        )

        with pytest.raises(SQLBlockedError, match="CALL statements are not allowed"):
            _ = await handle_execute_query(json_converter, args, effect_handler)

        assert effect_handler.called_with_sql is None
        assert effect_handler.called_with_timeout is None

    @pytest.mark.asyncio
    async def test_handle_execute_query_with_call_blocked_after_additional_cte_bindings(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test that WITH ... AS PROCEDURE ..., cte AS (...) CALL ... is blocked."""
        effect_handler = MockExecuteQuery()
        args = ExecuteQueryArgs(
            sql="WITH p AS PROCEDURE () RETURNS VARCHAR LANGUAGE SQL AS $$BEGIN RETURN 'ok'; END;$$, cte AS (SELECT 1) CALL p()"
        )

        with pytest.raises(SQLBlockedError, match="CALL statements are not allowed"):
            _ = await handle_execute_query(json_converter, args, effect_handler)

        assert effect_handler.called_with_sql is None
        assert effect_handler.called_with_timeout is None

    @pytest.mark.asyncio
    async def test_handle_execute_query_propagates_sql_analysis_error(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test that SQL analysis failures are surfaced to callers."""
        effect_handler = MockExecuteQuery()
        args = ExecuteQueryArgs(sql="SELECT 'unterminated")

        with pytest.raises(SQLAnalysisFailedError) as exc_info:
            _ = await handle_execute_query(json_converter, args, effect_handler)

        diagnostic = exc_info.value.diagnostic
        assert diagnostic is not None
        assert diagnostic.code is DiagnosticCode.UNTERMINATED_STRING
        assert effect_handler.called_with_sql is None
        assert effect_handler.called_with_timeout is None

    @pytest.mark.asyncio
    async def test_handle_execute_query_explain_insert_allowed(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test that EXPLAIN retains read-only behavior."""
        effect_handler = MockExecuteQuery(result_data=[{"plan": "ok"}])
        args = ExecuteQueryArgs(sql="EXPLAIN INSERT INTO users VALUES (1)")

        result = await handle_execute_query(json_converter, args, effect_handler)

        assert isinstance(result, QueryResult)
        assert result.row_count == 1
        assert effect_handler.called_with_sql == "EXPLAIN INSERT INTO users VALUES (1)"

    @pytest.mark.asyncio
    async def test_handle_execute_query_explain_using_insert_allowed(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test that EXPLAIN USING retains read-only behavior."""
        effect_handler = MockExecuteQuery(result_data=[{"plan": "ok"}])
        args = ExecuteQueryArgs(sql="EXPLAIN USING JSON INSERT INTO users VALUES (1)")

        result = await handle_execute_query(json_converter, args, effect_handler)

        assert isinstance(result, QueryResult)
        assert result.row_count == 1
        assert effect_handler.called_with_sql == "EXPLAIN USING JSON INSERT INTO users VALUES (1)"

    @pytest.mark.asyncio
    async def test_handle_execute_query_list_allowed(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test that LIST is allowed as read-only metadata."""
        effect_handler = MockExecuteQuery(result_data=[{"name": "path/file.csv"}])
        args = ExecuteQueryArgs(sql="LIST @mystage")

        result = await handle_execute_query(json_converter, args, effect_handler)

        assert isinstance(result, QueryResult)
        assert result.row_count == 1
        assert effect_handler.called_with_sql == "LIST @mystage"

    @pytest.mark.asyncio
    async def test_handle_execute_query_allows_variant_path_key_named_into(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test that semi-structured path keys do not trigger SELECT ... INTO blocking."""
        effect_handler = MockExecuteQuery(result_data=[{"value": "ok"}])
        args = ExecuteQueryArgs(sql="SELECT src:into FROM car_sales")

        result = await handle_execute_query(json_converter, args, effect_handler)

        assert isinstance(result, QueryResult)
        assert result.row_count == 1
        assert effect_handler.called_with_sql == "SELECT src:into FROM car_sales"

    @pytest.mark.asyncio
    async def test_handle_execute_query_allows_recursive_named_cte(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test that a CTE named recursive is treated as a valid identifier."""
        effect_handler = MockExecuteQuery(result_data=[{"value": 1}])
        sql = "WITH recursive AS (SELECT 1 AS value) SELECT * FROM recursive"
        args = ExecuteQueryArgs(sql=sql)

        result = await handle_execute_query(json_converter, args, effect_handler)

        assert isinstance(result, QueryResult)
        assert result.row_count == 1
        assert effect_handler.called_with_sql == sql

    @pytest.mark.asyncio
    async def test_handle_execute_query_rejects_unsupported_with_body_keyword(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test that unsupported WITH bodies surface as invalid SQL input."""
        effect_handler = MockExecuteQuery()
        args = ExecuteQueryArgs(sql="WITH cte AS (SELECT 1) INSERT INTO users SELECT * FROM cte")

        with pytest.raises(SQLAnalysisFailedError) as exc_info:
            _ = await handle_execute_query(json_converter, args, effect_handler)

        diagnostic = exc_info.value.diagnostic
        assert diagnostic is not None
        assert diagnostic.code is DiagnosticCode.UNPARSABLE_WITH_BODY
        assert effect_handler.called_with_sql is None
        assert effect_handler.called_with_timeout is None

    @pytest.mark.asyncio
    async def test_handle_execute_query_blocks_write_cte_definition(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test that CTE definitions must remain read-only."""
        effect_handler = MockExecuteQuery()
        args = ExecuteQueryArgs(sql="WITH cte AS (DELETE FROM users WHERE 1 = 1) SELECT 1")

        with pytest.raises(SQLBlockedError, match="CTE definitions must be read-only: DML statements are not allowed"):
            _ = await handle_execute_query(json_converter, args, effect_handler)

        assert effect_handler.called_with_sql is None
        assert effect_handler.called_with_timeout is None

    @pytest.mark.asyncio
    async def test_handle_execute_query_blocks_select_for_update_in_cte_definition(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test that non-read-only SELECT clauses are blocked inside CTE definitions."""
        effect_handler = MockExecuteQuery()
        args = ExecuteQueryArgs(sql="WITH cte AS (SELECT * FROM users FOR UPDATE) SELECT 1")

        with pytest.raises(
            SQLBlockedError,
            match=r"CTE definitions must be read-only: SELECT \.\.\. FOR UPDATE is not read-only",
        ):
            _ = await handle_execute_query(json_converter, args, effect_handler)

        assert effect_handler.called_with_sql is None
        assert effect_handler.called_with_timeout is None

    @pytest.mark.asyncio
    async def test_handle_execute_query_blocks_explain_with_unparsable_subject(
        self,
        json_converter: JsonImmutableConverter,
    ) -> None:
        """Test that EXPLAIN still requires a parseable nested statement."""
        effect_handler = MockExecuteQuery()
        args = ExecuteQueryArgs(sql="EXPLAIN WITH cte AS (SELECT 1)")

        with pytest.raises(SQLBlockedError, match="WITH statement body could not be determined"):
            _ = await handle_execute_query(json_converter, args, effect_handler)

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

        # Verify result - should be QueryResult
        assert isinstance(result, QueryResult)
        assert result.row_count == 0
        assert result.columns == []
        assert result.rows == []

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
        assert isinstance(result, QueryResult)
        assert result.row_count == 1

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
