"""Facade API for Snowflake SQL analysis."""

from typing import TYPE_CHECKING

from expression import option

from .core import (
    AnalysisReport,
    DiagnosticCode,
    ExplainNode,
    PipeChainNode,
    SQLAnalysisError,
    StatementAnalysis,
    StatementNode,
    WithNode,
)
from .core.contracts import analysis_contract, internal_contract
from .core.invariants import ParserInvariantError
from .parsing import parse_script
from .policy import ReadOnlySafetyPolicy, SafetyDecision

if TYPE_CHECKING:
    from .core import Diagnostic

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

        analyses = tuple(_analyze_node(statement, policy=_DEFAULT_POLICY) for statement in script.statements)
        return AnalysisReport.from_statements(analyses)

    @analysis_contract
    def is_read_only_sql(self, sql: str) -> bool:
        """Return whether the input SQL is read-only."""

        return self.analyze(sql).is_allowed

    @analysis_contract
    def is_write_sql(self, sql: str) -> bool:
        """Return whether the input SQL is not read-only."""

        return self.analyze(sql).is_blocked


def _analyze_node(
    node: StatementNode,
    *,
    policy: ReadOnlySafetyPolicy,
) -> StatementAnalysis:
    decision = policy.evaluate(node)
    return _to_statement_analysis(node, decision)


@internal_contract
def _to_statement_analysis(node: StatementNode, decision: SafetyDecision) -> StatementAnalysis:
    children = _node_children(node)
    if len(children) != len(decision.nested):
        raise ParserInvariantError("Statement node children must align with nested safety decisions")

    nested = tuple(
        _to_statement_analysis(child, child_decision)
        for child, child_decision in zip(children, decision.nested, strict=False)
    )
    return StatementAnalysis(
        text=node.text,
        span=node.span,
        family=decision.family,
        top_level_keyword=decision.top_level_keyword,
        is_read_only=decision.is_read_only,
        constructs=decision.constructs,
        nested=nested,
        diagnostic=_statement_diagnostic(node, decision),
    )


def _statement_diagnostic(
    node: StatementNode,
    decision: SafetyDecision,
) -> "Diagnostic | None":
    match node:
        case PipeChainNode():
            return None
        case WithNode() if node.body is not None and not node.diagnostics:
            return None
        case _:
            return decision.diagnostic


def _node_children(node: StatementNode) -> tuple[StatementNode, ...]:
    match node:
        case PipeChainNode():
            return node.segments
        case WithNode():
            return option.map_or(node.body, (), lambda body: (body,))
        case ExplainNode():
            return option.map_or(node.subject, (), lambda subject: (subject,))
        case _:
            return ()
