"""Registry for statement-level parser dispatch."""

from enum import StrEnum

import attrs

from ..core.models import StatementFamily
from ..core.syntax import PolicyKind


class StatementParserKind(StrEnum):
    """Named parser implementation used for statement dispatch."""

    FAMILY = "family"
    METADATA = "metadata"
    QUERY = "query"
    WITH = "with"
    EXPLAIN = "explain"
    BEGIN = "begin"
    ALTER = "alter"
    EXECUTE = "execute"
    START = "start"


@attrs.define(frozen=True, slots=True)
class StatementParserSpec:
    """A statement parser registration entry."""

    keywords: frozenset[str]
    parser_kind: StatementParserKind
    default_family: StatementFamily
    family_policy: PolicyKind | None = None


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
