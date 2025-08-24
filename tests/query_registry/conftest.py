"""
Shared fixtures and mocks for QueryRegistry tests.
"""

import asyncio
import uuid
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from snowflake.connector.cursor import ResultMetadata

from query_registry import (
    QueryRegistry,
    SnowflakeConnectionProvider,
)


class MockSnowflakeConnectionProvider(SnowflakeConnectionProvider):
    """Test mock for SnowflakeConnectionProvider."""

    def __init__(self) -> None:
        # Don't call parent constructor
        self.mock_connections: dict[str, Any] = {}
        self.query_states: dict[str, tuple[bool, list[dict[str, Any]] | None]] = {}
        """sfqid -> (is_running, result)"""
        self.cancel_calls: list[str] = []  # Cancel call records
        self.created_connections: list[MockConnection] = []  # Track created connections

    def get_connection(self) -> "MockConnection":  # pyright: ignore[reportIncompatibleMethodOverride]
        """Return mock connection."""
        connection = MockConnection(self)
        self.created_connections.append(connection)  # Track connection
        return connection

    def get_total_close_calls(self) -> int:
        """Get total close calls across all connections."""
        return sum(conn.close_call_count for conn in self.created_connections)

    def reset_all_close_counts(self) -> None:
        """Reset close call counts for all connections."""
        for conn in self.created_connections:
            conn.reset_close_count()

    def simulate_query_completion(
        self,
        sfqid: str,
        result: list[dict[str, Any]],
    ) -> None:
        """Simulate query completion for tests."""
        self.query_states[sfqid] = (False, result)


class MockConnection:
    """Mock for Snowflake connection."""

    def __init__(self, provider: MockSnowflakeConnectionProvider) -> None:
        self.provider = provider
        self._cursor: MockCursor | None = None
        self.close_call_count = 0  # Track close() calls

    def cursor(self) -> "MockCursor":
        self._cursor = MockCursor(self.provider)
        return self._cursor

    def close(self) -> None:
        self.close_call_count += 1  # Increment counter

    def reset_close_count(self) -> None:
        """Reset close call count for testing."""
        self.close_call_count = 0

    def get_query_status_throw_if_error(self, sfqid: str) -> str:
        """Mock implementation for status checking."""
        return (
            "RUNNING"
            if self.provider.query_states.get(sfqid, (True, None))[0]
            else "SUCCEEDED"
        )

    def is_still_running(self, status: str) -> bool:
        """Mock implementation for running check."""
        return status == "RUNNING"


class MockCursor:
    """Mock for Snowflake cursor."""

    def __init__(self, provider: MockSnowflakeConnectionProvider) -> None:
        self.provider = provider
        self.sfqid: str | None = None
        self.description: list[ResultMetadata] | None = None
        self._rows: list[dict[str, Any]] = []
        self._row_index = 0

    def execute_async(self, sql: str, timeout: int | None = None) -> None:  # noqa: ARG002
        """Mock execute_async."""
        self.sfqid = str(uuid.uuid4())
        # Default to running state
        self.provider.query_states[self.sfqid] = (True, None)

    def execute(self, sql: str) -> None:
        """Mock regular execute (for cancellation)."""
        if "SYSTEM$CANCEL_QUERY" in sql:
            self.provider.cancel_calls.append(sql)

    def get_results_from_sfqid(self, sfqid: str) -> None:
        """Mock getting results by sfqid."""
        if sfqid in self.provider.query_states:
            is_running, result = self.provider.query_states[sfqid]
            if not is_running and result:
                # Setup mock data
                first_row = result[0]
                # Create description based on first row keys
                # Use proper type_code: 0=FIXED, 2=TEXT for common types
                self.description = [
                    ResultMetadata(
                        name=key,
                        type_code=0
                        if isinstance(list(first_row.values())[i], int | float)
                        else 2,
                        display_size=None,
                        internal_size=None,
                        precision=None,
                        scale=None,
                        is_nullable=True,  # Default to True for tests
                    )
                    for i, key in enumerate(first_row)
                ]
                self._rows = result
                self._row_index = 0

    def fetchone(self) -> dict[str, Any]:
        return {"result": "success"}

    def __iter__(self) -> Generator[tuple[Any, ...]]:
        """Iterate over rows as tuples (like Snowflake cursor)."""
        for row_dict in self._rows:
            # Convert dict to tuple in the order of columns
            if self.description:
                column_names = [desc[0] for desc in self.description]
                yield tuple(row_dict.get(col, None) for col in column_names)
            else:
                yield tuple(row_dict.values())

    def __enter__(self) -> "MockCursor":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass


@pytest.fixture
def event_loop() -> Generator[asyncio.AbstractEventLoop]:
    """Create event loop for tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def executor() -> Generator[ThreadPoolExecutor]:
    """Create thread pool executor for tests."""
    with ThreadPoolExecutor(max_workers=2) as pool:
        yield pool


@pytest.fixture
def mock_provider() -> MockSnowflakeConnectionProvider:
    """Create mock connection provider."""
    return MockSnowflakeConnectionProvider()


@pytest.fixture
def registry(
    mock_provider: MockSnowflakeConnectionProvider,
    executor: ThreadPoolExecutor,
) -> QueryRegistry:
    """Create QueryRegistry for tests."""
    return QueryRegistry(mock_provider, executor)
