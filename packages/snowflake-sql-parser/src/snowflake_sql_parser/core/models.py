"""Domain models for Snowflake SQL parsing."""

from typing import TYPE_CHECKING, Literal, Self

import attrs

from .diagnostics import Diagnostic, DiagnosticCode
from .invariants import ParserInvariantError
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
class AllowedAnalysis:
    """Analysis result for an SQL input that is fully read-only."""

    statements: tuple[StatementAnalysis, ...]


@attrs.define(frozen=True, slots=True)
class BlockedAnalysis:
    """Analysis result for an SQL input that contains a non-read-only statement."""

    statements: tuple[StatementAnalysis, ...]
    denial: AnalysisDenial
    diagnostics: tuple[Diagnostic, ...]


type AnalysisReport = AllowedAnalysis | BlockedAnalysis


def build_analysis_report(statements: tuple[StatementAnalysis, ...]) -> AnalysisReport:
    """Build the appropriate report variant from per-statement analyses."""

    denial = select_primary_denial(statements)
    if denial is None:
        for statement in statements:
            if not statement.is_read_only:
                raise ParserInvariantError("No primary denial found but a statement is not read-only")
        return AllowedAnalysis(statements=statements)

    diagnostic_list: list[Diagnostic] = []
    for statement in statements:
        if statement.is_read_only:
            continue
        for diagnostic in statement.iter_diagnostics():
            if diagnostic not in diagnostic_list:
                diagnostic_list.append(diagnostic)
    if denial.diagnostic not in diagnostic_list:
        diagnostic_list.append(denial.diagnostic)

    return BlockedAnalysis(
        statements=statements,
        denial=denial,
        diagnostics=tuple(diagnostic_list),
    )


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
