"""Snowflake-aware SQL parser package."""

from .analyzer import SQLAnalyzer
from .core.diagnostics import Diagnostic, DiagnosticCode, DiagnosticSeverity
from .core.errors import SQLAnalysisError
from .core.models import (
    AllowedAnalysis,
    AnalysisDenial,
    AnalysisReport,
    BlockedAnalysis,
    QueryConstruct,
    SplitStatement,
    StatementAnalysis,
    StatementFamily,
    TextSpan,
)

__all__ = [
    "AllowedAnalysis",
    "AnalysisDenial",
    "AnalysisReport",
    "BlockedAnalysis",
    "Diagnostic",
    "DiagnosticCode",
    "DiagnosticSeverity",
    "QueryConstruct",
    "SQLAnalysisError",
    "SQLAnalyzer",
    "SplitStatement",
    "StatementAnalysis",
    "StatementFamily",
    "TextSpan",
]
