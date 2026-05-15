import pytest

from snowflake_sql_parser import DiagnosticCode, SQLAnalysisError, TextSpan
from snowflake_sql_parser.dialects import SNOWFLAKE_DIALECT
from snowflake_sql_parser.lexing import TokenType, tokenize


def test_tokenize_preserves_strings_and_skips_comments() -> None:
    tokens = tokenize("SELECT 'a;--b', $$c;->>$$, \"quoted;name\" /* block; */ FROM sample -- tail\n->> SELECT $1")

    assert [token.text for token in tokens] == [
        "SELECT",
        "'a;--b'",
        ",",
        "$$c;->>$$",
        ",",
        '"quoted;name"',
        "FROM",
        "sample",
        "->>",
        "SELECT",
        "$1",
    ]


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        (
            'SELECT "quote"";--andunquote""" FROM demo',
            ["SELECT", '"quote"";--andunquote"""', "FROM", "demo"],
        ),
        (
            "SELECT 'Today'';--s sales' FROM demo",
            ["SELECT", "'Today'';--s sales'", "FROM", "demo"],
        ),
        (
            "SELECT 'it\\'s;--still string' FROM demo",
            ["SELECT", "'it\\'s;--still string'", "FROM", "demo"],
        ),
    ],
)
def test_tokenize_preserves_escaped_delimiters_inside_quoted_tokens(sql: str, expected: list[str]) -> None:
    tokens = tokenize(sql)

    assert [token.text for token in tokens] == expected


def test_tokenize_can_preserve_trivia_with_typed_tokens() -> None:
    tokens = tokenize(
        "SELECT 1 -- tail\nFROM demo_table",
        preserve_trivia=True,
        keywords=SNOWFLAKE_DIALECT.keywords,
    )

    assert [token.type for token in tokens] == [
        TokenType.KEYWORD,
        TokenType.WHITESPACE,
        TokenType.NUMBER,
        TokenType.WHITESPACE,
        TokenType.COMMENT,
        TokenType.KEYWORD,
        TokenType.WHITESPACE,
        TokenType.IDENTIFIER,
    ]


def test_tokenize_treats_carriage_return_as_line_comment_terminator() -> None:
    tokens = tokenize("SELECT 1 -- tail\rFROM demo")

    assert [token.text for token in tokens] == [
        "SELECT",
        "1",
        "FROM",
        "demo",
    ]


def test_tokenize_reports_unterminated_string_diagnostic_with_span() -> None:
    sql = "SELECT 'unterminated"

    with pytest.raises(SQLAnalysisError) as exc_info:
        _ = tokenize(sql)

    diagnostic = exc_info.value.diagnostic
    assert diagnostic is not None
    assert diagnostic.code is DiagnosticCode.UNTERMINATED_STRING
    assert diagnostic.span == TextSpan(7, len(sql))


def test_tokenize_reports_unterminated_comment_diagnostic_with_span() -> None:
    sql = "SELECT /* unterminated"

    with pytest.raises(SQLAnalysisError) as exc_info:
        _ = tokenize(sql)

    diagnostic = exc_info.value.diagnostic
    assert diagnostic is not None
    assert diagnostic.code is DiagnosticCode.UNTERMINATED_COMMENT
    assert diagnostic.span == TextSpan(7, len(sql))
