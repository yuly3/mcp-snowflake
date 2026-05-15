"""Internal invariant helpers for parser implementation."""


class ParserInvariantError(Exception):
    """Raised when the parser reaches an impossible internal state."""
