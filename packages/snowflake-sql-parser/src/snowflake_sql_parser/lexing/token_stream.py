"""Cursor helpers for statement-level SQL parsing."""

from collections.abc import Callable

import attrs

from ..core.contracts import analysis_contract
from ..core.diagnostics import DiagnosticCode
from ..core.errors import SQLAnalysisError
from ..core.models import TextSpan
from .lexer import Token, TokenType, tokenize


@attrs.define(slots=True)
class TokenStream:
    """A mutable cursor over lexical tokens."""

    tokens: tuple[Token, ...]
    text: str = ""
    span: TextSpan | None = None
    _index: int = 0

    @classmethod
    @analysis_contract
    def from_sql(
        cls,
        sql: str,
        *,
        offset: int = 0,
        keywords: frozenset[str] | None = None,
    ) -> "TokenStream":
        return cls(
            tokens=tokenize(sql, offset=offset, keywords=keywords),
            text=sql,
            span=TextSpan(offset, offset + len(sql)),
        )

    @property
    def index(self) -> int:
        """Current token index."""

        return self._index

    def checkpoint(self) -> int:
        """Return a checkpoint that can be rewound to later."""

        return self._index

    def rewind(self, checkpoint: int) -> None:
        """Rewind the cursor to a previous checkpoint."""

        self._index = checkpoint

    def at_end(self) -> bool:
        """Return whether the cursor is at EOF."""

        return self.peek().type is TokenType.EOF

    def peek(self, offset: int = 0) -> Token:
        """Return the token at the current cursor plus offset."""

        position = self._index + offset
        if 0 <= position < len(self.tokens):
            return self.tokens[position]
        return self._eof_token()

    def advance(self) -> Token:
        """Consume and return the current token."""

        token = self.peek()
        if token.type is not TokenType.EOF:
            self._index += 1
        return token

    def match_keyword(self, keyword: str) -> Token | None:
        """Consume the current token if it matches the keyword."""

        token = self.peek()
        if token.kind == "word" and token.normalized == keyword.upper():
            return self.advance()
        return None

    def match_keywords(self, *keywords: str) -> tuple[Token, ...] | None:
        """Consume a sequence of keywords or rewind on mismatch."""

        checkpoint = self.checkpoint()
        matched: list[Token] = []
        for keyword in keywords:
            token = self.match_keyword(keyword)
            if token is None:
                self.rewind(checkpoint)
                return None
            matched.append(token)
        return tuple(matched)

    def match_sequence(self, *parts: str) -> tuple[Token, ...] | None:
        """Consume a sequence of token texts/keywords or rewind on mismatch."""

        checkpoint = self.checkpoint()
        matched: list[Token] = []
        for part in parts:
            token = self.peek()
            expected = part.upper() if token.kind == "word" else part
            actual = token.normalized if token.kind == "word" else token.text
            if actual != expected:
                self.rewind(checkpoint)
                return None
            matched.append(self.advance())
        return tuple(matched)

    def consume_symbol(self, text: str) -> Token | None:
        """Consume the current symbol token if it matches."""

        token = self.peek()
        if token.kind == "symbol" and token.text == text:
            return self.advance()
        return None

    def consume_operator(self, text: str) -> Token | None:
        """Consume the current operator token if it matches."""

        token = self.peek()
        if token.type is TokenType.OPERATOR and token.text == text:
            return self.advance()
        return None

    @analysis_contract
    def expect(
        self,
        predicate: Callable[[Token], bool],
        *,
        message: str,
        code: DiagnosticCode,
    ) -> Token:
        """Consume the current token if it matches, otherwise raise."""

        token = self.peek()
        if predicate(token):
            return self.advance()
        raise SQLAnalysisError(message, code=code, span=token.span)

    def _eof_token(self) -> Token:
        end = 0
        if self.tokens:
            end = self.tokens[-1].span.end
        elif self.span is not None:
            end = self.span.end
        span = TextSpan(end, end)
        return Token(type=TokenType.EOF, text="", normalized="", span=span)
