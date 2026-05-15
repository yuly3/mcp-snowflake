"""Statement splitting and parsing."""

from .parser import ParserContext, parse_script, parse_statement
from .registry import StatementParserKind, StatementParserSpec, StatementRegistry
from .splitter import build_split_statement, split_pipe_segments, split_statements

__all__ = [
    "ParserContext",
    "StatementParserKind",
    "StatementParserSpec",
    "StatementRegistry",
    "build_split_statement",
    "parse_script",
    "parse_statement",
    "split_pipe_segments",
    "split_statements",
]
