import pytest

from snowflake_sql_parser import DiagnosticCode, SQLAnalysisError, TextSpan
from snowflake_sql_parser.dialects import SNOWFLAKE_DIALECT
from snowflake_sql_parser.lexing import TokenStream


def test_token_stream_supports_checkpoints_and_sequence_matching() -> None:
    stream = TokenStream.from_sql(
        "SELECT 1 ->> SELECT 2",
        keywords=SNOWFLAKE_DIALECT.keywords,
    )

    assert stream.match_keyword("SELECT") is not None
    assert stream.advance().text == "1"

    checkpoint = stream.checkpoint()
    assert stream.match_sequence("->>", "SELECT") is not None

    stream.rewind(checkpoint)
    assert stream.consume_operator("->>") is not None
    assert stream.match_keywords("SELECT") is not None
    assert stream.advance().text == "2"
    assert stream.at_end()


def test_token_stream_expect_raises_sql_analysis_error() -> None:
    stream = TokenStream.from_sql(
        "SELECT 1",
        keywords=SNOWFLAKE_DIALECT.keywords,
    )

    with pytest.raises(SQLAnalysisError) as exc_info:
        _ = stream.expect(
            lambda token: token.normalized == "FROM",
            message="Expected FROM",
            code=DiagnosticCode.UNEXPECTED_INPUT,
        )

    diagnostic = exc_info.value.diagnostic
    assert diagnostic is not None
    assert diagnostic.code is DiagnosticCode.UNEXPECTED_INPUT
    assert diagnostic.span == TextSpan(0, 6)
