"""Statement splitting and parsing."""

from .parser import ParserContext, parse_script, parse_statement
from .registry import (
    AlterParserSpec,
    BeginParserSpec,
    ExecuteParserSpec,
    ExplainParserSpec,
    FamilyParserSpec,
    QueryParserSpec,
    StartParserSpec,
    StatementParserSpec,
    StatementRegistry,
    WithParserSpec,
)
from .splitter import build_split_statement, split_pipe_segments, split_statements

__all__ = [
    "AlterParserSpec",
    "BeginParserSpec",
    "ExecuteParserSpec",
    "ExplainParserSpec",
    "FamilyParserSpec",
    "ParserContext",
    "QueryParserSpec",
    "StartParserSpec",
    "StatementParserSpec",
    "StatementRegistry",
    "WithParserSpec",
    "build_split_statement",
    "parse_script",
    "parse_statement",
    "split_pipe_segments",
    "split_statements",
]
