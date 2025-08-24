"""
Tests for logging utilities.
"""

import logging
from unittest.mock import MagicMock

import pytest

from query_registry.logging_utils import (
    log_performance,
    log_query_event,
    log_registry_event,
    sanitize_sql_for_logging,
)


class TestLoggingUtils:
    """Test cases for logging utility functions."""

    def test_log_query_event_basic(self) -> None:
        """Test basic query event logging."""
        mock_logger = MagicMock()

        log_query_event(mock_logger, "test_action", "test-query-123")

        mock_logger.log.assert_called_once_with(
            logging.INFO,
            "Query test_action: test-query-123",
            extra={
                "action": "test_action",
                "query_id": "test-query-123",
            },
        )

    def test_log_query_event_with_extra_data(self) -> None:
        """Test query event logging with extra data."""
        mock_logger = MagicMock()

        log_query_event(
            mock_logger,
            "execution_started",
            "query-456",
            level=logging.DEBUG,
            timeout_seconds=300,
            sql_preview="SELECT * FROM table",
        )

        mock_logger.log.assert_called_once_with(
            logging.DEBUG,
            "Query execution_started: query-456",
            extra={
                "action": "execution_started",
                "query_id": "query-456",
                "timeout_seconds": 300,
                "sql_preview": "SELECT * FROM table",
            },
        )

    def test_log_performance_basic(self) -> None:
        """Test basic performance logging."""
        mock_logger = MagicMock()

        log_performance(mock_logger, "execute_query", 150.5)

        mock_logger.info.assert_called_once_with(
            "Performance: execute_query took 150.50ms",
            extra={
                "operation": "execute_query",
                "duration_ms": 150.5,
            },
        )

    def test_log_performance_with_query_id(self) -> None:
        """Test performance logging with query ID."""
        mock_logger = MagicMock()

        log_performance(mock_logger, "cancel", 75.2, query_id="test-query-789")

        mock_logger.info.assert_called_once_with(
            "Performance: cancel took 75.20ms",
            extra={
                "operation": "cancel",
                "duration_ms": 75.2,
                "query_id": "test-query-789",
            },
        )

    def test_log_performance_with_extra_data(self) -> None:
        """Test performance logging with extra data."""
        mock_logger = MagicMock()

        log_performance(
            mock_logger, "prune_expired", 200.0, removed_count=5, remaining_count=10
        )

        mock_logger.info.assert_called_once_with(
            "Performance: prune_expired took 200.00ms",
            extra={
                "operation": "prune_expired",
                "duration_ms": 200.0,
                "removed_count": 5,
                "remaining_count": 10,
            },
        )

    def test_log_registry_event_basic(self) -> None:
        """Test basic registry event logging."""
        mock_logger = MagicMock()

        log_registry_event(mock_logger, "initialized")

        mock_logger.log.assert_called_once_with(
            logging.INFO,
            "Registry initialized",
            extra={"action": "initialized"},
        )

    def test_log_registry_event_with_extra_data(self) -> None:
        """Test registry event logging with extra data."""
        mock_logger = MagicMock()

        log_registry_event(
            mock_logger,
            "shutdown_completed",
            level=logging.WARNING,
            cleaned_queries=15,
            cleaned_connections=3,
        )

        mock_logger.log.assert_called_once_with(
            logging.WARNING,
            "Registry shutdown_completed",
            extra={
                "action": "shutdown_completed",
                "cleaned_queries": 15,
                "cleaned_connections": 3,
            },
        )

    def test_logging_functions_with_real_logger(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test logging functions with real logger."""
        logger = logging.getLogger("test_logger")

        with caplog.at_level(logging.INFO):
            log_query_event(logger, "test_action", "query-123", test_data="value")
            log_performance(logger, "test_operation", 100.5, query_id="query-123")
            log_registry_event(logger, "test_event", extra_field="extra_value")

        records = caplog.records
        assert len(records) == 3

        # Check query event log
        assert records[0].getMessage() == "Query test_action: query-123"
        assert getattr(records[0], "action", None) == "test_action"
        assert getattr(records[0], "query_id", None) == "query-123"
        assert getattr(records[0], "test_data", None) == "value"

        # Check performance log
        assert records[1].getMessage() == "Performance: test_operation took 100.50ms"
        assert getattr(records[1], "operation", None) == "test_operation"
        assert getattr(records[1], "duration_ms", None) == 100.5
        assert getattr(records[1], "query_id", None) == "query-123"

        # Check registry event log
        assert records[2].getMessage() == "Registry test_event"
        assert getattr(records[2], "action", None) == "test_event"
        assert getattr(records[2], "extra_field", None) == "extra_value"


class TestSqlSanitization:
    """Test cases for SQL sanitization utility."""

    def test_sanitize_sql_basic(self) -> None:
        """Test basic SQL sanitization."""
        sql = "SELECT * FROM users WHERE id = 1"
        result = sanitize_sql_for_logging(sql)
        assert result == sql

    def test_sanitize_sql_with_whitespace(self) -> None:
        """Test SQL sanitization with leading/trailing whitespace."""
        sql = "   SELECT * FROM users   "
        result = sanitize_sql_for_logging(sql)
        assert result == "SELECT * FROM users"

    def test_sanitize_sql_truncation(self) -> None:
        """Test SQL sanitization with truncation."""
        long_sql = "SELECT " + "column, " * 50 + "FROM table"  # Very long SQL
        result = sanitize_sql_for_logging(long_sql, max_length=50)
        assert len(result) == 53  # 50 + "..."
        assert result.endswith("...")
        assert result.startswith("SELECT")

    def test_sanitize_sql_custom_max_length(self) -> None:
        """Test SQL sanitization with custom max length."""
        sql = "SELECT * FROM users WHERE name = 'very_long_name_here'"
        result = sanitize_sql_for_logging(sql, max_length=20)
        assert result == "SELECT * FROM users ..."
        assert len(result) == 23  # 20 + "..."

    def test_sanitize_sql_exactly_max_length(self) -> None:
        """Test SQL sanitization when exactly max length."""
        sql = "SELECT * FROM users"  # 19 characters
        result = sanitize_sql_for_logging(sql, max_length=19)
        assert result == sql
        assert not result.endswith("...")

    def test_sanitize_sql_empty_string(self) -> None:
        """Test SQL sanitization with empty string."""
        result = sanitize_sql_for_logging("")
        assert result == ""

    def test_sanitize_sql_none_or_whitespace_only(self) -> None:
        """Test SQL sanitization with whitespace-only string."""
        result = sanitize_sql_for_logging("   ")
        assert result == ""

    def test_sanitize_sql_multiline(self) -> None:
        """Test SQL sanitization with multiline SQL."""
        sql = """
        SELECT user_id, name, email
        FROM users
        WHERE active = true
        ORDER BY name
        """
        result = sanitize_sql_for_logging(sql, max_length=50)
        assert "SELECT user_id, name, email" in result
        assert result.endswith("...")
        assert len(result) <= 53  # 50 + "..."
