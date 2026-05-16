"""Handler-layer exceptions."""

from snowflake_sql_parser import AnalysisDenial, Diagnostic, SQLAnalysisError


class MissingResponseColumnError(Exception):
    """Required columns are missing from a Snowflake query response row."""


class SQLNotExecutableError(Exception):
    """Base for SQL inputs that the handler refuses before execution."""


class SQLAnalysisFailedError(SQLNotExecutableError):
    """The SQL analyzer could not classify the input."""

    diagnostic: Diagnostic | None

    def __init__(self, cause: SQLAnalysisError) -> None:
        super().__init__(str(cause))
        self.diagnostic = cause.diagnostic
        self.__cause__ = cause


class SQLBlockedError(SQLNotExecutableError):
    """The SQL analyzer classified the input as not read-only."""

    denial: AnalysisDenial

    def __init__(self, denial: AnalysisDenial) -> None:
        super().__init__(f"Write operations are not allowed: {denial.reason}")
        self.denial = denial
