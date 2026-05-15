"""Parser and analysis errors."""

from .diagnostics import Diagnostic, DiagnosticCode
from .text import TextSpan


class SQLAnalysisError(Exception):
    """Raised when SQL analysis fails before classification completes."""

    def __init__(
        self,
        message: str,
        *,
        diagnostic: Diagnostic | None = None,
        code: DiagnosticCode | None = None,
        span: TextSpan | None = None,
    ) -> None:
        if diagnostic is None and code is not None:
            diagnostic = Diagnostic(code=code, message=message, span=span)
        self.diagnostic = diagnostic
        super().__init__(message)

    @classmethod
    def from_diagnostic(cls, diagnostic: Diagnostic) -> "SQLAnalysisError":
        """Create an analysis error from a diagnostic."""

        return cls(diagnostic.message, diagnostic=diagnostic)
