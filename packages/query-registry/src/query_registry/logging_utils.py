"""
Logging utilities for QueryRegistry.
"""

import logging
from typing import Any


def sanitize_sql_for_logging(sql: str, max_length: int = 100) -> str:
    """
    Sanitize SQL query for safe logging.

    Parameters
    ----------
    sql : str
        The SQL query to sanitize
    max_length : int, default=100
        Maximum length of the sanitized SQL preview

    Returns
    -------
    str
        Sanitized SQL query safe for logging

    Notes
    -----
    This function:
    - Strips whitespace
    - Truncates to max_length characters
    - Adds "..." if truncated
    - Removes sensitive information (future enhancement)
    """
    if not sql:
        return ""

    sanitized = sql.strip()
    if len(sanitized) > max_length:
        return sanitized[:max_length] + "..."
    return sanitized


def log_query_event(
    logger: logging.Logger,
    action: str,
    query_id: str,
    level: int = logging.INFO,
    **extra_data: Any,
) -> None:
    """
    Log structured query events.

    Parameters
    ----------
    logger : logging.Logger
        The logger instance to use
    action : str
        The action being performed
    query_id : str
        The query identifier
    level : int
        The logging level
    **extra_data : Any
        Additional data to include in the log
    """
    log_data = {
        "action": action,
        "query_id": query_id,
        **extra_data,
    }
    logger.log(level, f"Query {action}: {query_id}", extra=log_data)


def log_performance(
    logger: logging.Logger,
    operation: str,
    duration_ms: float,
    query_id: str | None = None,
    **extra_data: Any,
) -> None:
    """
    Log performance metrics.

    Parameters
    ----------
    logger : logging.Logger
        The logger instance to use
    operation : str
        The operation name
    duration_ms : float
        Duration in milliseconds
    query_id : str | None
        Optional query identifier
    **extra_data : Any
        Additional performance data
    """
    log_data = {
        "operation": operation,
        "duration_ms": duration_ms,
        **extra_data,
    }
    if query_id:
        log_data["query_id"] = query_id

    logger.info(f"Performance: {operation} took {duration_ms:.2f}ms", extra=log_data)


def log_registry_event(
    logger: logging.Logger,
    action: str,
    level: int = logging.INFO,
    **extra_data: Any,
) -> None:
    """
    Log general registry events (not query-specific).

    Parameters
    ----------
    logger : logging.Logger
        The logger instance to use
    action : str
        The action being performed
    level : int
        The logging level
    **extra_data : Any
        Additional data to include in the log
    """
    log_data = {
        "action": action,
        **extra_data,
    }
    logger.log(level, f"Registry {action}", extra=log_data)
