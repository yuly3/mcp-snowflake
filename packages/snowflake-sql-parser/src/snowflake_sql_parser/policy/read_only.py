"""Read-only safety policy evaluation for statement nodes."""

from collections.abc import Iterable

import attrs
from more_itertools import first

from expression import option

from ..core.contracts import internal_contract
from ..core.diagnostics import Diagnostic, DiagnosticCode
from ..core.invariants import ParserInvariantError
from ..core.models import QueryConstruct, StatementFamily
from ..core.reasons import blocked_statement_reason
from ..core.syntax import (
    ExplainNode,
    PipeChainNode,
    PolicyKind,
    QueryNode,
    StatementFamilyNode,
    StatementNode,
    UnknownStatementNode,
    WithNode,
)


@attrs.define(frozen=True, slots=True)
class SafetyDecision:
    """Policy result for a parsed statement."""

    family: StatementFamily
    top_level_keyword: str | None
    is_read_only: bool
    constructs: frozenset[QueryConstruct] = frozenset()
    nested: tuple["SafetyDecision", ...] = ()
    diagnostic: Diagnostic | None = None


class ReadOnlySafetyPolicy:
    """Evaluate statement nodes under the read-only safety policy."""

    @internal_contract
    def evaluate(self, node: StatementNode) -> SafetyDecision:
        if isinstance(node, PipeChainNode):
            return self._evaluate_pipe_chain(node)
        if isinstance(node, WithNode):
            return self._evaluate_with(node)
        if isinstance(node, ExplainNode):
            return self._evaluate_explain(node)
        if isinstance(node, QueryNode):
            return self._evaluate_query(node)
        if isinstance(node, StatementFamilyNode):
            return self._evaluate_family(node)
        if isinstance(node, UnknownStatementNode):
            return self._evaluate_unknown(node)

        raise ParserInvariantError(f"Unhandled statement node: {type(node)!r}")

    def _evaluate_pipe_chain(self, node: PipeChainNode) -> SafetyDecision:
        decisions = tuple(map(self.evaluate, node.segments))
        first_decision = first(decisions, None)
        return SafetyDecision(
            family=option.map_or(first_decision, "unknown", lambda d: d.family),
            top_level_keyword=option.map_or(first_decision, None, lambda d: d.top_level_keyword),
            is_read_only=all(decision.is_read_only for decision in decisions),
            constructs=_merge_constructs(decision.constructs for decision in decisions),
            nested=decisions,
            diagnostic=next(
                (decision.diagnostic for decision in decisions if decision.diagnostic is not None),
                None,
            ),
        )

    def _evaluate_with(self, node: WithNode) -> SafetyDecision:
        if node.body is None:
            diagnostic = _node_diagnostic(
                node,
                code=DiagnosticCode.UNPARSABLE_WITH_BODY,
                message="WITH statement body could not be determined",
            )
            return SafetyDecision(
                family="unknown",
                top_level_keyword="WITH",
                is_read_only=False,
                diagnostic=diagnostic,
            )

        nested = self.evaluate(node.body)
        if node.diagnostics:
            return SafetyDecision(
                family=nested.family,
                top_level_keyword="WITH",
                is_read_only=False,
                constructs=nested.constructs,
                nested=(nested,),
                diagnostic=node.diagnostics[0],
            )
        return SafetyDecision(
            family=nested.family,
            top_level_keyword="WITH",
            is_read_only=nested.is_read_only,
            constructs=nested.constructs,
            nested=(nested,),
            diagnostic=nested.diagnostic,
        )

    def _evaluate_explain(self, node: ExplainNode) -> SafetyDecision:
        if node.subject is None:
            diagnostic = _node_diagnostic(
                node,
                code=DiagnosticCode.BLOCKED_STATEMENT,
                message="EXPLAIN must wrap another statement",
            )
            return SafetyDecision(
                family="metadata",
                top_level_keyword="EXPLAIN",
                is_read_only=False,
                diagnostic=diagnostic,
            )

        nested = self.evaluate(node.subject)
        if nested.family == "unknown":
            diagnostic = nested.diagnostic or Diagnostic(
                code=DiagnosticCode.UNKNOWN_STATEMENT,
                message="Statement type is not proven read-only",
                span=node.subject.span,
            )
            return SafetyDecision(
                family="metadata",
                top_level_keyword="EXPLAIN",
                is_read_only=False,
                constructs=nested.constructs,
                nested=(nested,),
                diagnostic=diagnostic,
            )
        return SafetyDecision(
            family="metadata",
            top_level_keyword="EXPLAIN",
            is_read_only=True,
            constructs=nested.constructs,
            nested=(nested,),
        )

    def _evaluate_query(self, node: QueryNode) -> SafetyDecision:
        if "INTO" in node.constructs:
            return self._block(
                family="query",
                keyword=node.keyword,
                constructs=node.constructs,
                reason="SELECT ... INTO is not read-only",
            )
        if "FOR_UPDATE" in node.constructs:
            return self._block(
                family="query",
                keyword=node.keyword,
                constructs=node.constructs,
                reason="SELECT ... FOR UPDATE is not read-only",
            )
        return SafetyDecision(
            family="query",
            top_level_keyword=node.keyword,
            is_read_only=True,
            constructs=node.constructs,
        )

    def _evaluate_family(self, node: StatementFamilyNode) -> SafetyDecision:
        if node.policy_kind is PolicyKind.ALLOW:
            return SafetyDecision(
                family=node.family,
                top_level_keyword=node.keyword,
                is_read_only=True,
            )
        if node.policy_kind is not PolicyKind.BLOCK:
            raise ParserInvariantError(f"Unhandled family policy: {node.policy_kind!r}")
        return self._block(
            family=node.family,
            keyword=node.keyword,
            reason=blocked_statement_reason(node.keyword, node.family),
        )

    def _evaluate_unknown(self, node: UnknownStatementNode) -> SafetyDecision:
        diagnostic = _node_diagnostic(
            node,
            code=DiagnosticCode.UNKNOWN_STATEMENT,
            message="Statement type is not proven read-only",
        )
        return SafetyDecision(
            family="unknown",
            top_level_keyword=node.keyword,
            is_read_only=False,
            diagnostic=diagnostic,
        )

    def _block(
        self,
        *,
        family: StatementFamily,
        keyword: str | None,
        reason: str,
        constructs: frozenset[QueryConstruct] = frozenset(),
        nested: tuple[SafetyDecision, ...] = (),
    ) -> SafetyDecision:
        return SafetyDecision(
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
