"""Base protocol for SQL dialect definitions."""

from collections.abc import Mapping
from typing import Protocol

from ..core.models import QueryConstruct
from ..parsing.registry import StatementRegistry


class Dialect(Protocol):
    """Protocol for statement parser dialects."""

    @property
    def keywords(self) -> frozenset[str]:
        """Known dialect keywords."""
        ...

    @property
    def registry(self) -> StatementRegistry:
        """Registry used for statement dispatch."""
        ...

    @property
    def with_body_start_keywords(self) -> frozenset[str]:
        """Keywords that can start a WITH body."""
        ...

    @property
    def explain_output_formats(self) -> frozenset[str]:
        """Supported EXPLAIN USING output formats."""
        ...

    @property
    def begin_transaction_followers(self) -> frozenset[str]:
        """Tokens that make BEGIN a transaction statement."""
        ...

    @property
    def block_end_followers(self) -> frozenset[str]:
        """Tokens that can legally follow END in scripting blocks."""
        ...

    @property
    def query_construct_by_keyword(self) -> Mapping[str, QueryConstruct]:
        """Top-level query constructs keyed by keyword."""
        ...
