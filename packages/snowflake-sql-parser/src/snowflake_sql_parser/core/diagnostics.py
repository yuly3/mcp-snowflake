"""Diagnostic models for Snowflake SQL analysis."""

from enum import StrEnum

import attrs

from .text import TextSpan


class DiagnosticSeverity(StrEnum):
    """Diagnostic severity level."""

    ERROR = "error"
    WARNING = "warning"


class DiagnosticCode(StrEnum):
    """Machine-readable diagnostic code."""

    EMPTY_SQL = "empty_sql"
    UNTERMINATED_STRING = "unterminated_string"
    UNTERMINATED_COMMENT = "unterminated_comment"
    UNKNOWN_STATEMENT = "unknown_statement"
    BLOCKED_STATEMENT = "blocked_statement"
    UNPARSABLE_WITH_BODY = "unparsable_with_body"
    INVALID_PIPE_CHAIN = "invalid_pipe_chain"
    UNEXPECTED_INPUT = "unexpected_input"


@attrs.define(frozen=True, slots=True)
class Diagnostic:
    """A diagnostic tied to a span in the SQL text."""

    code: DiagnosticCode
    message: str
    span: TextSpan | None
    severity: DiagnosticSeverity = DiagnosticSeverity.ERROR
