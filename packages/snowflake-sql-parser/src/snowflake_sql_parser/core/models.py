"""Domain models for Snowflake SQL parsing."""

from typing import TYPE_CHECKING, Literal, Self

import attrs

from .diagnostics import Diagnostic, DiagnosticCode
from .text import TextSpan

if TYPE_CHECKING:
    from collections.abc import Iterator

type StatementFamily = Literal[
    "query",
    "metadata",
    "dml",
    "ddl",
    "session",
    "transaction",
    "access_control",
    "scripting",
    "dynamic_sql",
    "file_transfer",
    "copy",
    "unknown",
]

type QueryConstruct = Literal[
    "AT",
    "BEFORE",
    "CHANGES",
    "QUALIFY",
    "SAMPLE",
    "LATERAL",
    "FLATTEN",
    "FOR_UPDATE",
    "INTO",
]

type AnalysisStatus = Literal["allowed", "blocked"]


@attrs.define(frozen=True, slots=True)
class TextPiece:
    """A span-aware SQL text fragment."""

    text: str
    span: TextSpan


@attrs.define(frozen=True, slots=True)
class SplitStatement:
    """A top-level SQL statement after splitting."""

    text: str
    span: TextSpan
    pipe_segment_pieces: tuple[TextPiece, ...]

    @property
    def pipe_segments(self) -> tuple[str, ...]:
        """Backward-compatible string-only pipe segments."""

        return tuple(piece.text for piece in self.pipe_segment_pieces)


@attrs.define(frozen=True, slots=True)
class StatementAnalysis:
    """Analysis result for a single statement."""

    text: str
    span: TextSpan
    family: StatementFamily
    top_level_keyword: str | None
    is_read_only: bool
    constructs: frozenset[QueryConstruct]
    nested: tuple[Self, ...] = ()
    diagnostic: Diagnostic | None = None

    @property
    def block_reason(self) -> str | None:
        if self.diagnostic is None:
            return None
        return self.diagnostic.message

    def iter_diagnostics(self) -> "Iterator[Diagnostic]":
        if self.diagnostic is not None:
            yield self.diagnostic
        for nested in self.nested:
            yield from nested.iter_diagnostics()


@attrs.define(frozen=True, slots=True)
class AnalysisDenial:
    """Primary reason an analyzed SQL input is not executable."""

    diagnostic: Diagnostic
    statement: StatementAnalysis
    statement_index: int
    path: tuple[int, ...] = ()

    @property
    def reason(self) -> str:
        return self.diagnostic.message


@attrs.define(frozen=True, slots=True)
class AnalysisReport:
    """Aggregated analysis result for a SQL input."""

    statements: tuple[StatementAnalysis, ...]
    status: AnalysisStatus
    denial: AnalysisDenial | None = None
    diagnostics: tuple[Diagnostic, ...] = ()

    @classmethod
    def from_statements(cls, statements: tuple[StatementAnalysis, ...]) -> Self:
        denial = select_primary_denial(statements)
        diagnostic_list: list[Diagnostic] = []
        for statement in statements:
            if statement.is_read_only:
                continue
            for diagnostic in statement.iter_diagnostics():
                if diagnostic not in diagnostic_list:
                    diagnostic_list.append(diagnostic)
        if denial is not None and denial.diagnostic not in diagnostic_list:
            diagnostic_list.append(denial.diagnostic)

        return cls(
            statements=statements,
            status="allowed" if denial is None else "blocked",
            denial=denial,
            diagnostics=tuple(diagnostic_list),
        )

    @property
    def is_allowed(self) -> bool:
        return self.status == "allowed"

    @property
    def is_read_only(self) -> bool:
        return self.is_allowed

    @property
    def is_blocked(self) -> bool:
        return self.status == "blocked"

    @property
    def block_reason(self) -> str | None:
        if self.denial is None:
            return None
        return self.denial.reason

    @property
    def user_message(self) -> str:
        if self.denial is None:
            return "SQL is allowed."
        return f"Write operations are not allowed: {self.denial.reason}"


def select_primary_denial(
    statements: tuple[StatementAnalysis, ...],
) -> AnalysisDenial | None:
    for statement_index, statement in enumerate(statements):
        denial = _find_denial(statement, path=(statement_index,))
        if denial is not None:
            return denial
    return None


def _find_denial(
    statement: StatementAnalysis,
    *,
    path: tuple[int, ...],
) -> AnalysisDenial | None:
    if statement.is_read_only:
        return None

    if statement.diagnostic is not None:
        return AnalysisDenial(
            diagnostic=statement.diagnostic,
            statement=statement,
            statement_index=path[0],
            path=path,
        )

    for index, nested in enumerate(statement.nested):
        denial = _find_denial(nested, path=(*path, index))
        if denial is not None:
            return denial

    diagnostic = _fallback_denial_diagnostic(statement)
    return AnalysisDenial(
        diagnostic=diagnostic,
        statement=statement,
        statement_index=path[0],
        path=path,
    )


def _fallback_denial_diagnostic(statement: StatementAnalysis) -> Diagnostic:
    return Diagnostic(
        code=DiagnosticCode.BLOCKED_STATEMENT,
        message="Statement is not proven read-only",
        span=statement.span,
    )
