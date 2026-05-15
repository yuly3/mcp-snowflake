"""Statement-level syntax nodes."""

from enum import StrEnum

import attrs

from .diagnostics import Diagnostic
from .models import QueryConstruct, StatementFamily, TextSpan


class PolicyKind(StrEnum):
    """Policy evaluation hint for family-classified statements."""

    ALLOW = "allow"
    BLOCK = "block"


@attrs.define(frozen=True, slots=True)
class SqlScript:
    """A parsed SQL script."""

    statements: tuple["StatementNode", ...]
    diagnostics: tuple[Diagnostic, ...] = ()


@attrs.define(frozen=True, slots=True)
class _StatementNode:
    """Base class for statement-level syntax nodes."""

    span: TextSpan
    text: str
    diagnostics: tuple[Diagnostic, ...] = attrs.field(factory=tuple, kw_only=True)


@attrs.define(frozen=True, slots=True)
class PipeChainNode(_StatementNode):
    """A statement composed of Snowflake pipe-chain segments."""

    segments: tuple["StatementNode", ...]


@attrs.define(frozen=True, slots=True)
class WithNode(_StatementNode):
    """A WITH statement that delegates policy to its body."""

    body: "StatementNode | None"


@attrs.define(frozen=True, slots=True)
class ExplainNode(_StatementNode):
    """An EXPLAIN statement wrapping another statement."""

    subject: "StatementNode | None"


@attrs.define(frozen=True, slots=True)
class QueryNode(_StatementNode):
    """A query statement with top-level Snowflake constructs."""

    keyword: str
    constructs: frozenset[QueryConstruct]


@attrs.define(frozen=True, slots=True)
class StatementFamilyNode(_StatementNode):
    """A statement classified only by family and default policy."""

    keyword: str
    family: StatementFamily
    policy_kind: PolicyKind


@attrs.define(frozen=True, slots=True)
class UnknownStatementNode(_StatementNode):
    """A statement that could not be proven read-only."""

    keyword: str | None = None


type StatementNode = PipeChainNode | WithNode | ExplainNode | QueryNode | StatementFamilyNode | UnknownStatementNode
