"""Shared message helpers for read-only policy decisions."""

from .models import StatementFamily


def blocked_statement_reason(keyword: str | None, family: StatementFamily) -> str:
    """Return the user-facing reason for blocking a statement family."""

    if keyword == "ALTER":
        return "ALTER statements are not allowed"
    if keyword == "CALL":
        return "CALL statements are not allowed"
    if family == "dml":
        return "DML statements are not allowed"
    if family == "copy":
        return "COPY statements are not allowed"
    if family == "file_transfer":
        return "File transfer statements are not allowed"
    if family == "access_control":
        return "Access-control statements are not allowed"
    if family == "session":
        return "Session statements are not allowed"
    if family == "transaction":
        return "Transaction statements are not allowed"
    if family == "scripting":
        return "Snowflake Scripting blocks are not allowed"
    if family == "dynamic_sql":
        return "EXECUTE IMMEDIATE is not allowed"
    if family == "ddl":
        return "DDL statements are not allowed"
    return "Statement type is not proven read-only"
