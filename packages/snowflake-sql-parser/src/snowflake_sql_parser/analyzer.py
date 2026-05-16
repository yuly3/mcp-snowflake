"""Facade API for Snowflake SQL analysis."""

from .core import (
    AllowedAnalysis,
    AnalysisReport,
    BlockedAnalysis,
    DiagnosticCode,
    SQLAnalysisError,
)
from .core.contracts import analysis_contract
from .core.models import build_analysis_report
from .parsing import parse_script
from .policy import ReadOnlySafetyPolicy

_DEFAULT_POLICY = ReadOnlySafetyPolicy()


class SQLAnalyzer:
    """Analyze Snowflake SQL for read-only safety gating."""

    @analysis_contract
    def analyze(self, sql: str) -> AnalysisReport:
        """Analyze SQL and return an aggregated safety report."""

        script = parse_script(sql)
        if not script.statements:
            raise SQLAnalysisError(
                "Could not parse any SQL statements",
                code=DiagnosticCode.UNEXPECTED_INPUT,
            )

        analyses = tuple(_DEFAULT_POLICY.analyze(statement) for statement in script.statements)
        return build_analysis_report(analyses)

    @analysis_contract
    def is_read_only_sql(self, sql: str) -> bool:
        """Return whether the input SQL is read-only."""

        return isinstance(self.analyze(sql), AllowedAnalysis)

    @analysis_contract
    def is_write_sql(self, sql: str) -> bool:
        """Return whether the input SQL is not read-only."""

        return isinstance(self.analyze(sql), BlockedAnalysis)
