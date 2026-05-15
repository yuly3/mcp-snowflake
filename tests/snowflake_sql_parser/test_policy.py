import pytest

from snowflake_sql_parser.parsing import build_split_statement, parse_statement
from snowflake_sql_parser.policy import ReadOnlySafetyPolicy, SafetyDecision


def _evaluate(sql: str) -> SafetyDecision:
    return ReadOnlySafetyPolicy().evaluate(parse_statement(build_split_statement(sql)))


def test_policy_blocks_with_call_body() -> None:
    decision = _evaluate("WITH p AS PROCEDURE () RETURNS VARCHAR LANGUAGE SQL AS $$BEGIN RETURN 'ok'; END;$$ CALL p()")

    assert not decision.is_read_only
    assert decision.top_level_keyword == "WITH"
    assert decision.nested[0].top_level_keyword == "CALL"
    assert decision.diagnostic is not None
    assert decision.diagnostic.message == "CALL statements are not allowed"


def test_policy_blocks_write_cte_definition() -> None:
    decision = _evaluate("WITH cte AS (DELETE FROM users WHERE 1 = 1) SELECT 1")

    assert not decision.is_read_only
    assert decision.family == "query"
    assert decision.nested[0].top_level_keyword == "SELECT"
    assert decision.diagnostic is not None
    assert decision.diagnostic.message == "CTE definitions must be read-only: DML statements are not allowed"


@pytest.mark.parametrize(
    "sql",
    [
        "WITH RECURSIVE recursive(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM recursive) SELECT * FROM recursive",
        "WITH RECURSIVE base(n) AS (SELECT 1), walk(n) AS (SELECT n FROM base UNION ALL SELECT n + 1 FROM walk) SELECT * FROM walk",
    ],
)
def test_policy_allows_recursive_with_modifier_cases(sql: str) -> None:
    decision = _evaluate(sql)

    assert decision.is_read_only
    assert decision.top_level_keyword == "WITH"
    assert decision.nested[0].top_level_keyword == "SELECT"


@pytest.mark.parametrize(
    "sql",
    [
        "WITH RECURSIVE base AS (SELECT 1) SELECT * FROM base",
        "WITH RECURSIVE seed AS (SELECT 1), walk(n) AS (SELECT n FROM seed UNION ALL SELECT n + 1 FROM walk) SELECT * FROM walk",
    ],
)
def test_policy_allows_recursive_keyword_without_leading_recursive_cte(sql: str) -> None:
    decision = _evaluate(sql)

    assert decision.is_read_only
    assert decision.top_level_keyword == "WITH"
    assert decision.nested[0].top_level_keyword == "SELECT"


@pytest.mark.parametrize(
    "sql",
    [
        'WITH "use" AS (SELECT 1) SELECT * FROM "use"',
        'WITH "copy" AS (SELECT 1) SELECT * FROM "copy"',
        'WITH "rollback" AS (SELECT 1) SELECT * FROM "rollback"',
        'WITH "begin" AS (SELECT 1) SELECT * FROM "begin"',
    ],
)
def test_policy_allows_quoted_keyword_like_cte_aliases(sql: str) -> None:
    decision = _evaluate(sql)

    assert decision.is_read_only
    assert decision.top_level_keyword == "WITH"
    assert decision.nested[0].top_level_keyword == "SELECT"


@pytest.mark.parametrize(
    "sql",
    [
        "START TRANSACTION",
        "START TRANSACTION NAME tx2",
    ],
)
def test_policy_blocks_start_transaction_as_transaction(sql: str) -> None:
    decision = _evaluate(sql)

    assert not decision.is_read_only
    assert decision.top_level_keyword == "START"
    assert decision.family == "transaction"
    assert decision.diagnostic is not None
    assert decision.diagnostic.message == "Transaction statements are not allowed"


@pytest.mark.parametrize("sql", ["UNSET V1", "UNSET (V1, V2)"])
def test_policy_blocks_unset_as_session(sql: str) -> None:
    decision = _evaluate(sql)

    assert not decision.is_read_only
    assert decision.top_level_keyword == "UNSET"
    assert decision.family == "session"
    assert decision.diagnostic is not None
    assert decision.diagnostic.message == "Session statements are not allowed"


@pytest.mark.parametrize(
    ("sql", "keyword"),
    [
        ("REMOVE @mystage/path1/subpath2", "REMOVE"),
        ("RM @~ pattern='.*jun.*'", "RM"),
    ],
)
def test_policy_blocks_remove_commands_as_file_transfer(sql: str, keyword: str) -> None:
    decision = _evaluate(sql)

    assert not decision.is_read_only
    assert decision.top_level_keyword == keyword
    assert decision.family == "file_transfer"
    assert decision.diagnostic is not None
    assert decision.diagnostic.message == "File transfer statements are not allowed"


def test_policy_allows_explain_insert() -> None:
    decision = _evaluate("EXPLAIN INSERT INTO t VALUES (1)")

    assert decision.is_read_only
    assert decision.family == "metadata"
    assert decision.nested[0].family == "dml"


def test_policy_allows_explain_with_using_clause() -> None:
    decision = _evaluate("EXPLAIN USING TEXT INSERT INTO t VALUES (1)")

    assert decision.is_read_only
    assert decision.family == "metadata"
    assert decision.nested[0].family == "dml"


def test_policy_allows_explain_with_with_subject() -> None:
    decision = _evaluate("EXPLAIN USING JSON WITH cte AS (SELECT 1) SELECT * FROM cte")

    assert decision.is_read_only
    assert decision.family == "metadata"
    assert decision.nested[0].top_level_keyword == "WITH"
    assert decision.nested[0].family == "query"


def test_policy_blocks_explain_unknown_subject() -> None:
    decision = _evaluate("EXPLAIN MYSTERY 1")

    assert not decision.is_read_only
    assert decision.family == "metadata"
    assert decision.nested[0].family == "unknown"
    assert decision.diagnostic is not None
    assert decision.diagnostic.message == "Statement type is not proven read-only"


def test_policy_blocks_explain_unparsable_with_subject() -> None:
    decision = _evaluate("EXPLAIN WITH cte AS (SELECT 1)")

    assert not decision.is_read_only
    assert decision.family == "metadata"
    assert decision.nested[0].family == "unknown"
    assert decision.diagnostic is not None
    assert decision.diagnostic.message == "WITH statement body could not be determined"


def test_policy_blocks_pipe_chain_when_any_segment_is_blocked() -> None:
    decision = _evaluate("SHOW TABLES ->> CREATE TABLE t (id INT)")

    assert not decision.is_read_only
    assert decision.nested[1].family == "ddl"
    assert decision.diagnostic is not None
    assert decision.diagnostic.message == "DDL statements are not allowed"


def test_policy_allows_pipe_chain_with_middle_with_segment() -> None:
    decision = _evaluate(
        'SHOW TABLES ->> WITH named AS (SELECT "name" FROM $1) SELECT * FROM named ->> SELECT * FROM $1'
    )

    assert decision.is_read_only
    assert [segment.top_level_keyword for segment in decision.nested] == ["SHOW", "WITH", "SELECT"]
    assert decision.diagnostic is None


def test_policy_allows_pipe_chain_with_relative_pipe_references() -> None:
    decision = _evaluate('SHOW TABLES ->> SELECT "name" FROM $1 ->> SELECT COUNT(*) FROM $2')

    assert decision.is_read_only
    assert [segment.top_level_keyword for segment in decision.nested] == ["SHOW", "SELECT", "SELECT"]
    assert decision.diagnostic is None


@pytest.mark.parametrize(
    ("sql", "construct"),
    [
        ("SELECT 'FOR UPDATE' AS note FROM demo", "FOR_UPDATE"),
        ("SELECT 'INTO' AS note FROM demo", "INTO"),
        ("SELECT 'it\\'s FOR UPDATE' AS note FROM demo", "FOR_UPDATE"),
        ("SELECT 'it\\'s INTO' AS note FROM demo", "INTO"),
    ],
)
def test_policy_ignores_string_literals_for_write_detection(sql: str, construct: str) -> None:
    decision = _evaluate(sql)

    assert decision.is_read_only
    assert construct not in decision.constructs
    assert decision.diagnostic is None


def test_policy_blocks_with_call_into_body_after_additional_cte_bindings() -> None:
    decision = _evaluate(
        "WITH p AS PROCEDURE () RETURNS VARCHAR LANGUAGE SQL AS $$BEGIN RETURN 'ok'; END;$$, cte AS (SELECT 1) CALL p() INTO :ret1"
    )

    assert not decision.is_read_only
    assert decision.top_level_keyword == "WITH"
    assert decision.family == "scripting"
    assert decision.nested[0].top_level_keyword == "CALL"
    assert decision.diagnostic is not None
    assert decision.diagnostic.message == "CALL statements are not allowed"


@pytest.mark.parametrize(
    "sql",
    [
        "WITH p AS PROCEDURE (n NUMBER) RETURNS NUMBER LANGUAGE SQL RETURNS NULL ON NULL INPUT AS $$BEGIN RETURN n; END;$$ CALL p(1)",
        "WITH p AS PROCEDURE (n NUMBER) RETURNS NUMBER LANGUAGE SQL CALLED ON NULL INPUT AS $$BEGIN RETURN n; END;$$ CALL p(1)",
        "WITH p AS PROCEDURE () RETURNS VARCHAR LANGUAGE JAVASCRIPT STRICT AS $$return 'ok';$$ CALL p()",
        "WITH p AS PROCEDURE (n NUMBER) RETURNS NUMBER LANGUAGE PYTHON RUNTIME_VERSION = '3.11' PACKAGES = ('snowflake-snowpark-python') HANDLER = 'run' STRICT AS $$def run(session, n):\n  return n$$, cte AS (SELECT 1) CALL p(n => 1) INTO :ret1",
        "WITH p AS PROCEDURE () RETURNS TABLE () LANGUAGE PYTHON RUNTIME_VERSION = '3.11' PACKAGES = ('snowflake-snowpark-python') HANDLER = 'run' AS $$def run(session):\n  return session.sql('select 1')$$ CALL p()",
    ],
)
def test_policy_blocks_documented_anonymous_procedure_variants(sql: str) -> None:
    decision = _evaluate(sql)

    assert not decision.is_read_only
    assert decision.top_level_keyword == "WITH"
    assert decision.family == "scripting"
    assert decision.nested[0].top_level_keyword == "CALL"
    assert decision.diagnostic is not None
    assert decision.diagnostic.message == "CALL statements are not allowed"


def test_policy_blocks_call_with_named_arguments_and_into_clause() -> None:
    decision = _evaluate("CALL sv_proc1(province => 'Manitoba', amount => 127.4) INTO :ret1")

    assert not decision.is_read_only
    assert decision.top_level_keyword == "CALL"
    assert decision.family == "scripting"
    assert decision.diagnostic is not None
    assert decision.diagnostic.message == "CALL statements are not allowed"
