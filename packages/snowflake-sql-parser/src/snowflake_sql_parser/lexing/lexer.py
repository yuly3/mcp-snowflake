"""Lexical scanning for SQL text."""

from enum import StrEnum
from typing import Literal

import attrs

from ..core import DiagnosticCode, SQLAnalysisError, TextSpan
from ..core.contracts import analysis_contract

type TokenKind = Literal[
    "word",
    "symbol",
    "string",
    "quoted_identifier",
    "comment",
    "whitespace",
    "eof",
]


class TokenType(StrEnum):
    """Closed token categories used by the parser."""

    KEYWORD = "keyword"
    IDENTIFIER = "identifier"
    QUOTED_IDENTIFIER = "quoted_identifier"
    STRING = "string"
    NUMBER = "number"
    OPERATOR = "operator"
    SYMBOL = "symbol"
    COMMENT = "comment"
    WHITESPACE = "whitespace"
    EOF = "eof"


@attrs.define(frozen=True, slots=True)
class Token:
    """A lexical token extracted from SQL text."""

    type: TokenType
    text: str
    normalized: str
    span: TextSpan

    @property
    def kind(self) -> TokenKind:
        """Backward-compatible token kind."""

        if self.type in {TokenType.KEYWORD, TokenType.IDENTIFIER, TokenType.NUMBER}:
            return "word"
        if self.type is TokenType.QUOTED_IDENTIFIER:
            return "quoted_identifier"
        if self.type is TokenType.STRING:
            return "string"
        if self.type is TokenType.COMMENT:
            return "comment"
        if self.type is TokenType.WHITESPACE:
            return "whitespace"
        if self.type is TokenType.EOF:
            return "eof"
        return "symbol"

    @property
    def upper_text(self) -> str:
        """Upper-cased token text for keyword matching."""

        return self.normalized


@analysis_contract
def tokenize(
    sql: str,
    *,
    preserve_trivia: bool = False,
    offset: int = 0,
    keywords: frozenset[str] | None = None,
) -> tuple[Token, ...]:
    """Tokenize SQL while respecting strings and comments."""

    tokens: list[Token] = []
    index = 0
    text_length = len(sql)
    keyword_set: frozenset[str] = frozenset() if keywords is None else keywords

    while index < text_length:
        char = sql[index]

        if char.isspace():
            end = _consume_whitespace(sql, index)
            if preserve_trivia:
                tokens.append(
                    _make_token(
                        TokenType.WHITESPACE,
                        sql[index:end],
                        index,
                        end,
                        offset,
                    )
                )
            index = end
            continue

        if sql.startswith("--", index):
            end = _consume_line_comment(sql, index)
            if preserve_trivia:
                tokens.append(
                    _make_token(
                        TokenType.COMMENT,
                        sql[index:end],
                        index,
                        end,
                        offset,
                    )
                )
            index = end
            continue

        if sql.startswith("/*", index):
            end = _consume_block_comment(sql, index, offset=offset)
            if preserve_trivia:
                tokens.append(
                    _make_token(
                        TokenType.COMMENT,
                        sql[index:end],
                        index,
                        end,
                        offset,
                    )
                )
            index = end
            continue

        if sql.startswith("$$", index):
            end = _consume_dollar_quoted_string(sql, index, offset=offset)
            tokens.append(
                _make_token(
                    TokenType.STRING,
                    sql[index:end],
                    index,
                    end,
                    offset,
                )
            )
            index = end
            continue

        if char == "'":
            end = _consume_single_quoted_string(sql, index, offset=offset)
            tokens.append(
                _make_token(
                    TokenType.STRING,
                    sql[index:end],
                    index,
                    end,
                    offset,
                )
            )
            index = end
            continue

        if char == '"':
            end = _consume_double_quoted_identifier(sql, index, offset=offset)
            tokens.append(
                _make_token(
                    TokenType.QUOTED_IDENTIFIER,
                    sql[index:end],
                    index,
                    end,
                    offset,
                )
            )
            index = end
            continue

        if sql.startswith("->>", index):
            tokens.append(
                _make_token(
                    TokenType.OPERATOR,
                    "->>",
                    index,
                    index + 3,
                    offset,
                )
            )
            index += 3
            continue

        if sql.startswith("::", index):
            tokens.append(
                _make_token(
                    TokenType.OPERATOR,
                    "::",
                    index,
                    index + 2,
                    offset,
                )
            )
            index += 2
            continue

        if _is_word_start(sql, index):
            end = _consume_word(sql, index)
            word = sql[index:end]
            token_type = _classify_word(word, keyword_set)
            normalized = word.upper()
            tokens.append(
                Token(
                    type=token_type,
                    text=word,
                    normalized=normalized,
                    span=TextSpan(offset + index, offset + end),
                )
            )
            index = end
            continue

        tokens.append(
            _make_token(
                TokenType.SYMBOL,
                char,
                index,
                index + 1,
                offset,
            )
        )
        index += 1

    return tuple(tokens)


def _make_token(
    token_type: TokenType,
    text: str,
    start: int,
    end: int,
    offset: int,
) -> Token:
    normalized = text.upper() if token_type in {TokenType.KEYWORD, TokenType.IDENTIFIER, TokenType.NUMBER} else text
    return Token(
        type=token_type,
        text=text,
        normalized=normalized,
        span=TextSpan(offset + start, offset + end),
    )


def _classify_word(word: str, keywords: frozenset[str]) -> TokenType:
    if word.upper() in keywords:
        return TokenType.KEYWORD
    if word[0].isdigit() and word.replace("_", "").isdigit():
        return TokenType.NUMBER
    return TokenType.IDENTIFIER


def _is_word_start(sql: str, index: int) -> bool:
    char = sql[index]
    if char.isalpha() or char == "_" or char.isdigit():
        return True

    if char != "$":
        return False

    next_index = index + 1
    return next_index < len(sql) and (sql[next_index].isalnum() or sql[next_index] == "_")


def _consume_word(sql: str, index: int) -> int:
    while index < len(sql) and (sql[index].isalnum() or sql[index] == "_" or sql[index] == "$"):
        index += 1
    return index


def _consume_whitespace(sql: str, index: int) -> int:
    while index < len(sql) and sql[index].isspace():
        index += 1
    return index


def _consume_line_comment(sql: str, index: int) -> int:
    comment_end = index + 2
    while comment_end < len(sql):
        if sql[comment_end] == "\n":
            return comment_end + 1
        if sql[comment_end] == "\r":
            if comment_end + 1 < len(sql) and sql[comment_end + 1] == "\n":
                return comment_end + 2
            return comment_end + 1
        comment_end += 1
    return len(sql)


def _consume_block_comment(sql: str, index: int, *, offset: int) -> int:
    comment_end = sql.find("*/", index + 2)
    if comment_end == -1:
        raise SQLAnalysisError(
            "Unterminated block comment",
            code=DiagnosticCode.UNTERMINATED_COMMENT,
            span=TextSpan(offset + index, offset + len(sql)),
        )
    return comment_end + 2


def _consume_dollar_quoted_string(sql: str, index: int, *, offset: int) -> int:
    string_end = sql.find("$$", index + 2)
    if string_end == -1:
        raise SQLAnalysisError(
            "Unterminated dollar-quoted string",
            code=DiagnosticCode.UNTERMINATED_STRING,
            span=TextSpan(offset + index, offset + len(sql)),
        )
    return string_end + 2


def _consume_single_quoted_string(sql: str, index: int, *, offset: int) -> int:
    start = index
    index += 1
    while index < len(sql):
        if sql[index] == "\\" and index + 1 < len(sql):
            index += 2
            continue
        if sql[index] == "'":
            if index + 1 < len(sql) and sql[index + 1] == "'":
                index += 2
                continue
            return index + 1
        index += 1

    raise SQLAnalysisError(
        "Unterminated single-quoted string",
        code=DiagnosticCode.UNTERMINATED_STRING,
        span=TextSpan(offset + start, offset + len(sql)),
    )


def _consume_double_quoted_identifier(sql: str, index: int, *, offset: int) -> int:
    start = index
    index += 1
    while index < len(sql):
        if sql[index] == '"':
            if index + 1 < len(sql) and sql[index + 1] == '"':
                index += 2
                continue
            return index + 1
        index += 1

    raise SQLAnalysisError(
        "Unterminated double-quoted identifier",
        code=DiagnosticCode.UNTERMINATED_STRING,
        span=TextSpan(offset + start, offset + len(sql)),
    )
