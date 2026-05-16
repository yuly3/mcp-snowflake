"""Read-only safety policy evaluation for statement nodes."""

from collections.abc import Iterable

from more_itertools import first

from expression import option

from ..core import (
    Diagnostic,
    DiagnosticCode,
    ExplainNode,
    PipeChainNode,
    PolicyKind,
    QueryConstruct,
    QueryNode,
    StatementAnalysis,
    StatementFamily,
    StatementFamilyNode,
    StatementNode,
    UnknownStatementNode,
    WithNode,
)
from ..core.invariants import ParserInvariantError
from ..core.reasons import blocked_statement_reason


class ReadOnlySafetyPolicy:
    """Evaluate statement nodes under the read-only safety policy."""

    def analyze(self, node: StatementNode) -> StatementAnalysis:
        match node:
            case PipeChainNode():
                return self._analyze_pipe_chain(node)
            case WithNode():
                return self._analyze_with(node)
            case ExplainNode():
                return self._analyze_explain(node)
            case QueryNode():
                return _analyze_query(node)
            case StatementFamilyNode():
                return _analyze_family(node)
            case UnknownStatementNode():
                return _analyze_unknown(node)

    def _analyze_pipe_chain(self, node: PipeChainNode) -> StatementAnalysis:
        nested = tuple(map(self.analyze, node.segments))
        first_nested = first(nested, None)
        return StatementAnalysis(
            text=node.text,
            span=node.span,
            family=option.map_or(first_nested, "unknown", lambda a: a.family),
            top_level_keyword=option.map_(first_nested, lambda a: a.top_level_keyword),
            is_read_only=all(a.is_read_only for a in nested),
            constructs=_merge_constructs(a.constructs for a in nested),
            nested=nested,
            diagnostic=None,
        )

    def _analyze_with(self, node: WithNode) -> StatementAnalysis:
        if node.body is None:
            diagnostic = _node_diagnostic(
                node,
                code=DiagnosticCode.UNPARSABLE_WITH_BODY,
                message="WITH statement body could not be determined",
            )
            return StatementAnalysis(
                text=node.text,
                span=node.span,
                family="unknown",
                top_level_keyword="WITH",
                is_read_only=False,
                constructs=frozenset(),
                nested=(),
                diagnostic=diagnostic,
            )

        nested = self.analyze(node.body)
        if node.diagnostics:
            return StatementAnalysis(
                text=node.text,
                span=node.span,
                family=nested.family,
                top_level_keyword="WITH",
                is_read_only=False,
                constructs=nested.constructs,
                nested=(nested,),
                diagnostic=node.diagnostics[0],
            )

        return StatementAnalysis(
            text=node.text,
            span=node.span,
            family=nested.family,
            top_level_keyword="WITH",
            is_read_only=nested.is_read_only,
            constructs=nested.constructs,
            nested=(nested,),
            diagnostic=None,
        )

    def _analyze_explain(self, node: ExplainNode) -> StatementAnalysis:
        if node.subject is None:
            diagnostic = _node_diagnostic(
                node,
                code=DiagnosticCode.BLOCKED_STATEMENT,
                message="EXPLAIN must wrap another statement",
            )
            return StatementAnalysis(
                text=node.text,
                span=node.span,
                family="metadata",
                top_level_keyword="EXPLAIN",
                is_read_only=False,
                constructs=frozenset(),
                nested=(),
                diagnostic=diagnostic,
            )

        nested = self.analyze(node.subject)
        if nested.family == "unknown":
            diagnostic = nested.diagnostic or Diagnostic(
                code=DiagnosticCode.UNKNOWN_STATEMENT,
                message="Statement type is not proven read-only",
                span=node.subject.span,
            )
            return StatementAnalysis(
                text=node.text,
                span=node.span,
                family="metadata",
                top_level_keyword="EXPLAIN",
                is_read_only=False,
                constructs=nested.constructs,
                nested=(nested,),
                diagnostic=diagnostic,
            )

        return StatementAnalysis(
            text=node.text,
            span=node.span,
            family="metadata",
            top_level_keyword="EXPLAIN",
            is_read_only=True,
            constructs=nested.constructs,
            nested=(nested,),
            diagnostic=None,
        )


def _analyze_query(node: QueryNode) -> StatementAnalysis:
    if "INTO" in node.constructs:
        return _block(
            node=node,
            family="query",
            keyword=node.keyword,
            constructs=node.constructs,
            reason="SELECT ... INTO is not read-only",
        )

    if "FOR_UPDATE" in node.constructs:
        return _block(
            node=node,
            family="query",
            keyword=node.keyword,
            constructs=node.constructs,
            reason="SELECT ... FOR UPDATE is not read-only",
        )

    return StatementAnalysis(
        text=node.text,
        span=node.span,
        family="query",
        top_level_keyword=node.keyword,
        is_read_only=True,
        constructs=node.constructs,
        nested=(),
        diagnostic=None,
    )


def _analyze_family(node: StatementFamilyNode) -> StatementAnalysis:
    if node.policy_kind is PolicyKind.ALLOW:
        return StatementAnalysis(
            text=node.text,
            span=node.span,
            family=node.family,
            top_level_keyword=node.keyword,
            is_read_only=True,
            constructs=frozenset(),
            nested=(),
            diagnostic=None,
        )

    if node.policy_kind is not PolicyKind.BLOCK:
        raise ParserInvariantError(f"Unhandled family policy: {node.policy_kind!r}")

    return _block(
        node=node,
        family=node.family,
        keyword=node.keyword,
        reason=blocked_statement_reason(node.keyword, node.family),
    )


def _analyze_unknown(node: UnknownStatementNode) -> StatementAnalysis:
    diagnostic = _node_diagnostic(
        node,
        code=DiagnosticCode.UNKNOWN_STATEMENT,
        message="Statement type is not proven read-only",
    )
    return StatementAnalysis(
        text=node.text,
        span=node.span,
        family="unknown",
        top_level_keyword=node.keyword,
        is_read_only=False,
        constructs=frozenset(),
        nested=(),
        diagnostic=diagnostic,
    )


def _block(
    *,
    node: StatementNode,
    family: StatementFamily,
    keyword: str | None,
    reason: str,
    constructs: frozenset[QueryConstruct] = frozenset(),
    nested: tuple[StatementAnalysis, ...] = (),
) -> StatementAnalysis:
    return StatementAnalysis(
        text=node.text,
        span=node.span,
        family=family,
        top_level_keyword=keyword,
        is_read_only=False,
        constructs=constructs,
        nested=nested,
        diagnostic=Diagnostic(
            code=DiagnosticCode.BLOCKED_STATEMENT,
            message=reason,
            span=None,
        ),
    )


def _node_diagnostic(
    node: StatementNode,
    *,
    code: DiagnosticCode,
    message: str,
) -> Diagnostic:
    if node.diagnostics:
        return node.diagnostics[0]
    return Diagnostic(code=code, message=message, span=node.span)


def _merge_constructs(
    construct_groups: Iterable[frozenset[QueryConstruct]],
) -> frozenset[QueryConstruct]:
    constructs: set[QueryConstruct] = set()
    for group in construct_groups:
        constructs.update(group)
    return frozenset(constructs)
