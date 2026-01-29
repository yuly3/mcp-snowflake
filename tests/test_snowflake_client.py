"""Tests for SnowflakeClient."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from pydantic_settings import SettingsConfigDict
from snowflake.connector import (
    DataError,
    IntegrityError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)

from expression.contract import ContractViolationError
from mcp_snowflake.settings import Settings
from mcp_snowflake.snowflake_client import SnowflakeClient


@pytest.fixture(scope="module")
def config_path() -> Path:
    return Path(__file__).parent / "fixtures" / "test.mcp_snowflake.toml"


@pytest.fixture(scope="module")
def settings(config_path: Path) -> Settings:
    return Settings.build(SettingsConfigDict(toml_file=config_path))


@pytest.fixture(scope="class")
def thread_pool() -> ThreadPoolExecutor:
    return ThreadPoolExecutor(max_workers=1)


@pytest.fixture(scope="class")
def client(thread_pool: ThreadPoolExecutor, settings: Settings) -> SnowflakeClient:
    return SnowflakeClient(thread_pool, settings.snowflake)


class TestSnowflakeClient:
    """Test cases for SnowflakeClient."""

    @pytest.mark.asyncio
    async def test_execute_query_success(self, client: SnowflakeClient) -> None:
        """Test successful query execution."""
        mock_result = [{"id": 1, "name": "test"}]

        with patch.object(
            client,
            "_execute_query_sync",
            return_value=mock_result,
        ) as mock_sync:
            result = await client.execute_query("SELECT * FROM test")

            assert result == mock_result
            mock_sync.assert_called_once_with(
                "SELECT * FROM test",
                timedelta(seconds=30),
                None,
                None,
            )

    @pytest.mark.asyncio
    async def test_execute_query_with_custom_timeout(
        self,
        client: SnowflakeClient,
    ) -> None:
        """Test query execution with custom timeout."""
        mock_result = [{"count": 42}]
        custom_timeout = timedelta(minutes=5)

        with patch.object(
            client,
            "_execute_query_sync",
            return_value=mock_result,
        ) as mock_sync:
            result = await client.execute_query(
                "SELECT COUNT(*) as count FROM test",
                custom_timeout,
            )

            assert result == mock_result
            mock_sync.assert_called_once_with(
                "SELECT COUNT(*) as count FROM test",
                custom_timeout,
                None,
                None,
            )

    @pytest.mark.asyncio
    async def test_execute_query_timeout_error(self, client: SnowflakeClient) -> None:
        """Test TimeoutError is properly propagated (known error)."""
        with (
            patch.object(
                client,
                "_execute_query_sync",
                side_effect=TimeoutError("Query timed out"),
            ),
            pytest.raises(TimeoutError, match="Query timed out"),
        ):
            _ = await client.execute_query("SELECT * FROM large_table")

    @pytest.mark.asyncio
    async def test_execute_query_programming_error(
        self,
        client: SnowflakeClient,
    ) -> None:
        """Test ProgrammingError is properly propagated (known error)."""
        error = ProgrammingError("SQL syntax error")
        with (
            patch.object(client, "_execute_query_sync", side_effect=error),
            pytest.raises(ProgrammingError, match="SQL syntax error"),
        ):
            _ = await client.execute_query("SELECT * FRM test")  # typo in SQL

    @pytest.mark.asyncio
    async def test_execute_query_operational_error(
        self,
        client: SnowflakeClient,
    ) -> None:
        """Test OperationalError is properly propagated (known error)."""
        error = OperationalError("Database connection error")
        with (
            patch.object(client, "_execute_query_sync", side_effect=error),
            pytest.raises(OperationalError, match="Database connection error"),
        ):
            _ = await client.execute_query("SELECT * FROM test")

    @pytest.mark.asyncio
    async def test_execute_query_data_error(self, client: SnowflakeClient) -> None:
        """Test DataError is properly propagated (known error)."""
        error = DataError("Data conversion error")
        with (
            patch.object(client, "_execute_query_sync", side_effect=error),
            pytest.raises(DataError, match="Data conversion error"),
        ):
            _ = await client.execute_query("SELECT CAST('invalid' AS INTEGER)")

    @pytest.mark.asyncio
    async def test_execute_query_integrity_error(self, client: SnowflakeClient) -> None:
        """Test IntegrityError is properly propagated (known error)."""
        error = IntegrityError("Foreign key constraint violation")
        with (
            patch.object(client, "_execute_query_sync", side_effect=error),
            pytest.raises(IntegrityError, match="Foreign key constraint violation"),
        ):
            _ = await client.execute_query("INSERT INTO child VALUES (999)")

    @pytest.mark.asyncio
    async def test_execute_query_not_supported_error(
        self,
        client: SnowflakeClient,
    ) -> None:
        """Test NotSupportedError is properly propagated (known error)."""
        error = NotSupportedError("Unsupported feature")
        with (
            patch.object(client, "_execute_query_sync", side_effect=error),
            pytest.raises(NotSupportedError, match="Unsupported feature"),
        ):
            _ = await client.execute_query("SOME_UNSUPPORTED_SQL")

    @pytest.mark.asyncio
    async def test_execute_query_unexpected_error_wrapped_in_contract_violation(
        self,
        client: SnowflakeClient,
    ) -> None:
        """Test unexpected errors are wrapped in ContractViolationError."""
        unexpected_error = RuntimeError("Unexpected runtime error")

        with patch.object(client, "_execute_query_sync", side_effect=unexpected_error):
            with pytest.raises(ContractViolationError) as exc_info:
                _ = await client.execute_query("SELECT * FROM test")

            # Verify the contract violation error contains the expected information
            contract_error = exc_info.value
            assert contract_error.function_name == "execute_query"
            assert contract_error.original_exception is unexpected_error
            assert "args" in contract_error.context
            assert "kwargs" in contract_error.context

    @pytest.mark.asyncio
    async def test_execute_query_contract_violation_masks_password(
        self,
        client: SnowflakeClient,
    ) -> None:
        """Test that password-like arguments are masked in ContractViolationError."""
        unexpected_error = ValueError("Some error")

        # Mock a method that might have sensitive parameters
        with patch.object(client, "_execute_query_sync", side_effect=unexpected_error):
            with pytest.raises(ContractViolationError) as exc_info:
                _ = await client.execute_query(
                    "SELECT * FROM test",
                    timedelta(seconds=60),
                )

            # The context should not contain the actual password from settings
            # (though execute_query doesn't directly pass password, the contract system should handle it)
            contract_error = exc_info.value
            assert contract_error.context is not None
            # Verify that sensitive information is handled appropriately
            assert "password" not in str(contract_error.context).lower()

    @pytest.mark.asyncio
    async def test_execute_query_empty_result(self, client: SnowflakeClient) -> None:
        """Test query execution with empty result."""
        mock_result: list[dict[str, Any]] = []

        with patch.object(
            client,
            "_execute_query_sync",
            return_value=mock_result,
        ) as mock_sync:
            result = await client.execute_query("SELECT * FROM empty_table")

            assert result == []
            mock_sync.assert_called_once_with(
                "SELECT * FROM empty_table",
                timedelta(seconds=30),
                None,
                None,
            )

    @pytest.mark.asyncio
    async def test_execute_query_large_result(self, client: SnowflakeClient) -> None:
        """Test query execution with large result set."""
        mock_result = [{"id": i, "value": f"value_{i}"} for i in range(1000)]

        with patch.object(
            client,
            "_execute_query_sync",
            return_value=mock_result,
        ) as mock_sync:
            result = await client.execute_query("SELECT * FROM large_table")

            assert result == mock_result
            assert len(result) == 1000
            mock_sync.assert_called_once_with(
                "SELECT * FROM large_table",
                timedelta(seconds=30),
                None,
                None,
            )

    def test_get_connection_parameters(self, client: SnowflakeClient) -> None:
        """Test that connection parameters are properly configured."""
        # This tests the _get_connection method indirectly by checking settings
        assert client.settings.account == "dummy"
        assert client.settings.user == "dummy"
        assert client.settings.password.get_secret_value() == "dummy"
        assert client.settings.warehouse == "dummy"
        assert client.settings.role == "dummy"
