"""Registry for statement-level parser dispatch."""

import attrs

from ..core import PolicyKind, StatementFamily


@attrs.define(frozen=True, slots=True)
class FamilyParserSpec:
    """A spec for statements classified by a static family and policy."""

    keywords: frozenset[str]
    default_family: StatementFamily
    family_policy: PolicyKind


@attrs.define(frozen=True, slots=True)
class QueryParserSpec:
    """A spec for query statements."""

    keywords: frozenset[str]


@attrs.define(frozen=True, slots=True)
class WithParserSpec:
    """A spec for WITH statements."""

    keywords: frozenset[str]


@attrs.define(frozen=True, slots=True)
class ExplainParserSpec:
    """A spec for EXPLAIN statements."""

    keywords: frozenset[str]


@attrs.define(frozen=True, slots=True)
class BeginParserSpec:
    """A spec for BEGIN statements with dynamic family resolution."""

    keywords: frozenset[str]
    family_policy: PolicyKind


@attrs.define(frozen=True, slots=True)
class AlterParserSpec:
    """A spec for ALTER statements with dynamic family resolution."""

    keywords: frozenset[str]
    family_policy: PolicyKind


@attrs.define(frozen=True, slots=True)
class ExecuteParserSpec:
    """A spec for EXECUTE statements."""

    keywords: frozenset[str]
    family_policy: PolicyKind


@attrs.define(frozen=True, slots=True)
class StartParserSpec:
    """A spec for START statements."""

    keywords: frozenset[str]
    family_policy: PolicyKind


type StatementParserSpec = (
    FamilyParserSpec
    | QueryParserSpec
    | WithParserSpec
    | ExplainParserSpec
    | BeginParserSpec
    | AlterParserSpec
    | ExecuteParserSpec
    | StartParserSpec
)


@attrs.define(frozen=True, slots=True)
class StatementRegistry:
    """Lookup table from a leading keyword to its parser spec."""

    specs: tuple[StatementParserSpec, ...]
    _by_keyword: dict[str, StatementParserSpec] = attrs.field(init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        lookup: dict[str, StatementParserSpec] = {}
        for spec in self.specs:
            for keyword in spec.keywords:
                lookup[keyword] = spec
        object.__setattr__(self, "_by_keyword", lookup)

    @property
    def keywords(self) -> frozenset[str]:
        """All registered first keywords."""

        return frozenset(self._by_keyword)

    def lookup(self, keyword: str) -> StatementParserSpec | None:
        """Return the parser spec for the given keyword."""

        return self._by_keyword.get(keyword)
