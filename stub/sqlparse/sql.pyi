"""
sqlparse SQL module type stubs.

This module contains classes representing syntactical elements of SQL.
"""

from collections.abc import Generator, Iterator
from typing import Any

class NameAliasMixin:
    """Implements get_real_name and get_alias."""
    def get_real_name(self) -> str | None:
        """Return the real name (object name) of this identifier."""

    def get_alias(self) -> str | None:
        """Return the alias for this identifier or ``None``."""

class Token:
    """Base class for all other classes in this module.

    It represents a single token and has two instance attributes:
    ``value`` is the unchanged value of the token and ``ttype`` is
    the type of the token.
    """

    __slots__: tuple[str, ...]
    value: str
    ttype: Any  # Token type from sqlparse.tokens

    def __init__(self, ttype: Any, value: str) -> None: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...
    def flatten(self) -> Generator[Token]: ...
    def match(self, ttype: Any, values: Any, regex: bool = ...) -> bool: ...
    def within(self, group_cls: type[TokenList]) -> bool: ...
    def is_child_of(self, other: TokenList) -> bool: ...
    def has_ancestor(self, other: TokenList) -> bool: ...

class TokenList(Token):
    """A group of tokens.

    It has an additional instance attribute ``tokens`` which holds a
    list of child-tokens.
    """

    __slots__: tuple[str, ...]
    tokens: list[Token]

    def __init__(self, tokens: list[Token] | None = ...) -> None: ...
    def __str__(self) -> str: ...
    def __iter__(self) -> Iterator[Token]: ...
    def __getitem__(self, item: int) -> Token: ...
    def get_token_at_offset(self, offset: int) -> Token | None: ...
    def flatten(self) -> Generator[Token]: ...
    def get_sublists(self) -> Generator[TokenList]: ...
    def token_first(self, skip_ws: bool = True, skip_cm: bool = False) -> Token | None:
        """Return the first child token.

        If *skip_ws* is ``True`` (the default), whitespace
        tokens are ignored.

        if *skip_cm* is ``True`` (default: ``False``), comments are
        ignored too.
        """

    def token_next_by(
        self, i: Any = ..., m: Any = ..., t: Any = ..., idx: int = ..., end: int | None = ...
    ) -> tuple[int, Token] | tuple[None, None] | None: ...
    def token_not_matching(self, funcs: Any, idx: int) -> tuple[int, Token] | tuple[None, None] | None: ...
    def token_matching(self, funcs: Any, idx: int) -> Token | None: ...
    def token_prev(
        self, idx: int, skip_ws: bool = ..., skip_cm: bool = ...
    ) -> tuple[None, None] | tuple[int, Token] | None:
        """Returns the previous token relative to *idx*.

        If *skip_ws* is ``True`` (the default) whitespace tokens are ignored.
        If *skip_cm* is ``True`` comments are ignored.
        ``None`` is returned if there's no previous token.
        """

    def token_next(
        self, idx, skip_ws=..., skip_cm=..., _reverse=...
    ):  # -> tuple[None, None] | tuple[int, Any] | None:
        """Returns the next token relative to *idx*.

        If *skip_ws* is ``True`` (the default) whitespace tokens are ignored.
        If *skip_cm* is ``True`` comments are ignored.
        ``None`` is returned if there's no next token.
        """

    def token_index(self, token, start=...):  # -> int:
        """Return list index of token."""

    def group_tokens(self, grp_cls, start, end, include_end=..., extend=...):
        """Replace tokens by an instance of *grp_cls*."""

    def insert_before(self, where, token):  # -> None:
        """Inserts *token* before *where*."""

    def insert_after(self, where, token, skip_ws=...):  # -> None:
        """Inserts *token* after *where*."""

    def has_alias(self):  # -> bool:
        """Returns ``True`` if an alias is present."""

    def get_alias(self):  # -> None:
        """Returns the alias for this identifier or ``None``."""

    def get_name(self):  # -> None:
        """Returns the name of this identifier.

        This is either it's alias or it's real name. The returned valued can
        be considered as the name under which the object corresponding to
        this identifier is known within the current statement.
        """

    def get_real_name(self):  # -> None:
        """Returns the real name (object name) of this identifier."""

    def get_parent_name(self):  # -> None:
        """Return name of the parent object if any.

        A parent object is identified by the first occurring dot.
        """

class Statement(TokenList):
    """Represent a SQL statement."""

    def get_type(self) -> str:
        """Return the type of a statement.

        The returned value is a string holding an upper-cased reprint of
        the first DML or DDL keyword. If the first token in this group
        isn't a DML or DDL keyword "UNKNOWN" is returned.

        Whitespaces and comments at the beginning of the statement
        are ignored.
        """

class Identifier(NameAliasMixin, TokenList):
    """Represents an identifier.

    Identifiers may have aliases or typecasts.
    """
    def is_wildcard(self):  # -> bool:
        """Return ``True`` if this identifier contains a wildcard."""

    def get_typecast(self):  # -> None:
        """Returns the typecast or ``None`` of this object as a string."""

    def get_ordering(self):  # -> None:
        """Returns the ordering or ``None`` as uppercase string."""

    def get_array_indices(self):  # -> Generator[Any | list[Any], Any, None]:
        """Returns an iterator of index token lists"""

class IdentifierList(TokenList):
    """A list of :class:`~sqlparse.sql.Identifier`\'s."""
    def get_identifiers(self):  # -> Generator[Any, Any, None]:
        """Returns the identifiers.

        Whitespaces and punctuations are not included in this generator.
        """

class TypedLiteral(TokenList):
    """A typed literal, such as "date '2001-09-28'" or "interval '2 hours'"."""

    M_OPEN = ...
    M_CLOSE = ...
    M_EXTEND = ...

class Parenthesis(TokenList):
    """Tokens between parenthesis."""

    M_OPEN = ...
    M_CLOSE = ...

class SquareBrackets(TokenList):
    """Tokens between square brackets"""

    M_OPEN = ...
    M_CLOSE = ...

class Assignment(TokenList):
    """An assignment like 'var := val;'"""


class If(TokenList):
    """An 'if' clause with possible 'else if' or 'else' parts."""

    M_OPEN = ...
    M_CLOSE = ...

class For(TokenList):
    """A 'FOR' loop."""

    M_OPEN = ...
    M_CLOSE = ...

class Comparison(TokenList):
    """A comparison used for example in WHERE clauses."""
    @property
    def left(self): ...
    @property
    def right(self): ...

class Comment(TokenList):
    """A comment."""
    def is_multiline(self):  # -> list[Any]:
        ...

class Where(TokenList):
    """A WHERE clause."""

    M_OPEN = ...
    M_CLOSE = ...

class Over(TokenList):
    """An OVER clause."""

    M_OPEN = ...

class Having(TokenList):
    """A HAVING clause."""

    M_OPEN = ...
    M_CLOSE = ...

class Case(TokenList):
    """A CASE statement with one or more WHEN and possibly an ELSE part."""

    M_OPEN = ...
    M_CLOSE = ...
    def get_cases(self, skip_ws=...):  # -> list[Any]:
        """Returns a list of 2-tuples (condition, value).

        If an ELSE exists condition is None.
        """

class Function(NameAliasMixin, TokenList):
    """A function or procedure call."""
    def get_parameters(self):  # -> Generator[Any, Any, None] | list[Any]:
        """Return a list of parameters."""

    def get_window(self):  # -> None:
        """Return the window if it exists."""

class Begin(TokenList):
    """A BEGIN/END block."""

    M_OPEN = ...
    M_CLOSE = ...

class Operation(TokenList):
    """Grouping of operations"""


class Values(TokenList):
    """Grouping of values"""


class Command(TokenList):
    """Grouping of CLI commands."""

