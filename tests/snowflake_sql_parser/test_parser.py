import pytest

from snowflake_sql_parser import DiagnosticCode, SQLAnalysisError
from snowflake_sql_parser.core import ExplainNode, PipeChainNode, QueryNode, StatementFamilyNode, WithNode
from snowflake_sql_parser.parsing import build_split_statement, parse_statement


def test_parser_builds_with_body_tree() -> None:
    node = parse_statement(build_split_statement("WITH cte AS (SELECT 1) SELECT * FROM cte"))

    assert isinstance(node, WithNode)
    assert isinstance(node.body, QueryNode)
    assert node.body.keyword == "SELECT"


@pytest.mark.parametrize(
    "sql",
    [
        "WITH use AS (SELECT 1) SELECT * FROM use",
        "WITH copy AS (SELECT 1) SELECT * FROM copy",
        "WITH rollback AS (SELECT 1) SELECT * FROM rollback",
        "WITH begin AS (SELECT 1) SELECT * FROM begin",
    ],
)
def test_parser_keeps_keyword_like_cte_aliases_out_of_with_body_detection(sql: str) -> None:
    node = parse_statement(build_split_statement(sql))

    assert isinstance(node, WithNode)
    assert isinstance(node.body, QueryNode)
    assert node.body.keyword == "SELECT"


@pytest.mark.parametrize(
    "sql",
    [
        'WITH "use" AS (SELECT 1) SELECT * FROM "use"',
        'WITH "copy" AS (SELECT 1) SELECT * FROM "copy"',
        'WITH "rollback" AS (SELECT 1) SELECT * FROM "rollback"',
        'WITH "begin" AS (SELECT 1) SELECT * FROM "begin"',
    ],
)
def test_parser_keeps_quoted_keyword_like_cte_aliases_out_of_with_body_detection(sql: str) -> None:
    node = parse_statement(build_split_statement(sql))

    assert isinstance(node, WithNode)
    assert isinstance(node.body, QueryNode)
    assert node.body.keyword == "SELECT"


@pytest.mark.parametrize(
    "sql",
    [
        "WITH recursive AS (SELECT 1) SELECT * FROM recursive",
        "WITH recursive(value) AS (SELECT 1) SELECT * FROM recursive",
    ],
)
def test_parser_keeps_recursive_named_cte_out_of_modifier_detection(sql: str) -> None:
    node = parse_statement(build_split_statement(sql))

    assert isinstance(node, WithNode)
    assert isinstance(node.body, QueryNode)
    assert node.body.keyword == "SELECT"
    assert node.diagnostics == ()


@pytest.mark.parametrize(
    "sql",
    [
        "WITH RECURSIVE recursive(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM recursive) SELECT * FROM recursive",
        "WITH RECURSIVE base(n) AS (SELECT 1), walk(n) AS (SELECT n FROM base UNION ALL SELECT n + 1 FROM walk) SELECT * FROM walk",
    ],
)
def test_parser_supports_recursive_with_modifier_cases(sql: str) -> None:
    node = parse_statement(build_split_statement(sql))

    assert isinstance(node, WithNode)
    assert isinstance(node.body, QueryNode)
    assert node.body.keyword == "SELECT"
    assert node.diagnostics == ()


@pytest.mark.parametrize(
    "sql",
    [
        "WITH RECURSIVE base AS (SELECT 1) SELECT * FROM base",
        "WITH RECURSIVE seed AS (SELECT 1), walk(n) AS (SELECT n FROM seed UNION ALL SELECT n + 1 FROM walk) SELECT * FROM walk",
    ],
)
def test_parser_supports_recursive_keyword_without_leading_recursive_cte(sql: str) -> None:
    node = parse_statement(build_split_statement(sql))

    assert isinstance(node, WithNode)
    assert isinstance(node.body, QueryNode)
    assert node.body.keyword == "SELECT"
    assert node.diagnostics == ()


def test_parser_supports_anonymous_procedure_with_additional_cte_bindings() -> None:
    node = parse_statement(
        build_split_statement(
            "WITH p AS PROCEDURE () RETURNS VARCHAR LANGUAGE SQL AS $$BEGIN RETURN 'ok'; END;$$, cte AS (SELECT 1) CALL p()"
        )
    )

    assert isinstance(node, WithNode)
    assert isinstance(node.body, StatementFamilyNode)
    assert node.body.keyword == "CALL"
    assert node.body.family == "scripting"
    assert node.diagnostics == ()


def test_parser_supports_anonymous_javascript_procedure_with_call_into() -> None:
    node = parse_statement(
        build_split_statement(
            "WITH p AS PROCEDURE () RETURNS VARCHAR LANGUAGE JAVASCRIPT AS $$return 'ok';$$ CALL p() INTO :ret1"
        )
    )

    assert isinstance(node, WithNode)
    assert isinstance(node.body, StatementFamilyNode)
    assert node.body.keyword == "CALL"
    assert node.body.family == "scripting"
    assert node.diagnostics == ()


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
def test_parser_supports_documented_anonymous_procedure_variants(sql: str) -> None:
    node = parse_statement(build_split_statement(sql))

    assert isinstance(node, WithNode)
    assert isinstance(node.body, StatementFamilyNode)
    assert node.body.keyword == "CALL"
    assert node.body.family == "scripting"
    assert node.diagnostics == ()


@pytest.mark.xfail(
    reason="Snowflake documents staged anonymous procedures without AS, but parsing does not support them yet",
    strict=True,
)
@pytest.mark.parametrize(
    "sql",
    [
        "WITH p AS PROCEDURE (n NUMBER) RETURNS NUMBER LANGUAGE PYTHON RUNTIME_VERSION = '3.11' PACKAGES = ('snowflake-snowpark-python') IMPORTS = ('@mystage/handler.py') HANDLER = 'handler.run' CALL p(1)",
        "WITH p AS PROCEDURE () RETURNS VARCHAR LANGUAGE JAVA RUNTIME_VERSION = '11' PACKAGES = ('com.snowflake:snowpark:1.0.0') IMPORTS = ('@mystage/handler.jar') HANDLER = 'com.example.Run' CALL p()",
        "WITH p AS PROCEDURE () RETURNS VARCHAR LANGUAGE SCALA RUNTIME_VERSION = '2.12' PACKAGES = ('com.snowflake:snowpark_2.12:1.0.0') IMPORTS = ('@mystage/handler.jar') HANDLER = 'com.example.Run' CALL p()",
    ],
)
def test_parser_supports_documented_staged_anonymous_procedure_forms(sql: str) -> None:
    node = parse_statement(build_split_statement(sql))

    assert isinstance(node, WithNode)
    assert isinstance(node.body, StatementFamilyNode)
    assert node.body.keyword == "CALL"
    assert node.body.family == "scripting"
    assert node.diagnostics == ()


def test_parser_rejects_unsupported_with_body_keyword() -> None:
    with pytest.raises(SQLAnalysisError) as exc_info:
        _ = parse_statement(build_split_statement("WITH cte AS (SELECT 1) INSERT INTO t SELECT * FROM cte"))

    diagnostic = exc_info.value.diagnostic
    assert diagnostic is not None
    assert diagnostic.code is DiagnosticCode.UNPARSABLE_WITH_BODY


@pytest.mark.parametrize(
    "sql",
    [
        "WITH p AS PROCEDURE () LANGUAGE SQL AS $$BEGIN RETURN 'ok'; END;$$ CALL p()",
        "WITH p AS PROCEDURE () RETURNS VARCHAR AS $$BEGIN RETURN 'ok'; END;$$ CALL p()",
        "WITH p AS PROCEDURE () RETURNS VARCHAR LANGUAGE SQL CALL p()",
    ],
)
def test_parser_rejects_invalid_anonymous_procedure_forms_documented_by_snowflake(sql: str) -> None:
    with pytest.raises(SQLAnalysisError):
        _ = parse_statement(build_split_statement(sql))


@pytest.mark.parametrize(
    "sql",
    [
        "WITH RECURSIVE seed AS (SELECT 1), RECURSIVE walk(n) AS (SELECT n FROM seed UNION ALL SELECT n + 1 FROM walk) SELECT * FROM walk",
        "WITH RECURSIVE walk(n) AS (SELECT 1 UNION SELECT n + 1 FROM walk) SELECT * FROM walk",
    ],
)
def test_parser_rejects_invalid_recursive_cte_forms_documented_by_snowflake(sql: str) -> None:
    with pytest.raises(SQLAnalysisError) as exc_info:
        _ = parse_statement(build_split_statement(sql))

    diagnostic = exc_info.value.diagnostic
    assert diagnostic is not None
    assert diagnostic.code is DiagnosticCode.UNEXPECTED_INPUT


def test_parser_does_not_treat_variant_path_key_as_select_into() -> None:
    node = parse_statement(build_split_statement("SELECT src:into FROM car_sales"))

    assert isinstance(node, QueryNode)
    assert "INTO" not in node.constructs


@pytest.mark.parametrize(
    ("sql", "construct"),
    [
        ("SELECT 'FOR UPDATE' AS note FROM demo", "FOR_UPDATE"),
        ("SELECT 'INTO' AS note FROM demo", "INTO"),
        ("SELECT 'it\\'s FOR UPDATE' AS note FROM demo", "FOR_UPDATE"),
        ("SELECT 'it\\'s INTO' AS note FROM demo", "INTO"),
    ],
)
def test_parser_does_not_treat_string_literal_as_select_side_effect_construct(
    sql: str,
    construct: str,
) -> None:
    node = parse_statement(build_split_statement(sql))

    assert isinstance(node, QueryNode)
    assert construct not in node.constructs
    assert node.diagnostics == ()


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT emp_id, name, dept, FROM employees",
        "SELECT * ILIKE '%id%' FROM employee_table",
        "SELECT * EXCLUDE (department_id, employee_id) FROM employee_table",
        "SELECT * RENAME (department_id AS department, employee_id AS id) FROM employee_table",
        "SELECT * ILIKE '%id%' RENAME department_id AS department FROM employee_table",
        "SELECT * REPLACE ('DEPT-' || department_id AS department_id) FROM employee_table",
        "SELECT * REPLACE ('DEPT-' || department_id AS department_id) RENAME department_id AS department FROM employee_table",
        "SELECT * EXCLUDE id REPLACE (42 AS answer) RENAME answer AS renamed FROM demo",
        "SELECT table_a.* EXCLUDE column_in_table_a, table_b.* EXCLUDE column_in_table_b FROM table_a JOIN table_b ON table_a.id = table_b.id",
    ],
)
def test_parser_supports_select_syntax_edges(sql: str) -> None:
    node = parse_statement(build_split_statement(sql))

    assert isinstance(node, QueryNode)
    assert node.keyword == "SELECT"
    assert node.diagnostics == ()


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        (
            "WITH cte AS (DELETE FROM t) SELECT 1",
            "CTE definitions must be read-only: DML statements are not allowed",
        ),
        (
            "WITH cte AS (SELECT * FROM t FOR UPDATE) SELECT 1",
            "CTE definitions must be read-only: SELECT ... FOR UPDATE is not read-only",
        ),
        (
            "WITH cte AS (SELECT col INTO :v FROM t) SELECT 1",
            "CTE definitions must be read-only: SELECT ... INTO is not read-only",
        ),
    ],
)
def test_parser_records_blocking_diagnostic_for_non_read_only_cte_definition(
    sql: str,
    message: str,
) -> None:
    node = parse_statement(build_split_statement(sql))

    assert isinstance(node, WithNode)
    assert isinstance(node.body, QueryNode)
    assert node.diagnostics
    assert node.diagnostics[0].message == message


def test_parser_builds_pipe_chain_tree() -> None:
    node = parse_statement(build_split_statement('SHOW TABLES ->> SELECT "name" FROM $1'))

    assert isinstance(node, PipeChainNode)
    assert isinstance(node.segments[0], StatementFamilyNode)
    assert isinstance(node.segments[1], QueryNode)
    assert node.segments[0].keyword == "SHOW"


def test_parser_builds_pipe_chain_tree_after_carriage_return_line_comment() -> None:
    node = parse_statement(build_split_statement('SHOW TABLES -- tail\r->> SELECT "name" FROM $1'))

    assert isinstance(node, PipeChainNode)
    assert isinstance(node.segments[0], StatementFamilyNode)
    assert isinstance(node.segments[1], QueryNode)
    assert node.segments[0].keyword == "SHOW"
    assert node.segments[1].keyword == "SELECT"


def test_parser_builds_pipe_chain_tree_with_middle_with_segment() -> None:
    node = parse_statement(
        build_split_statement(
            'SHOW TABLES ->> WITH named AS (SELECT "name" FROM $1) SELECT * FROM named ->> SELECT * FROM $1'
        )
    )

    assert isinstance(node, PipeChainNode)
    assert isinstance(node.segments[0], StatementFamilyNode)
    assert isinstance(node.segments[1], WithNode)
    assert isinstance(node.segments[1].body, QueryNode)
    assert isinstance(node.segments[2], QueryNode)
    assert node.segments[0].keyword == "SHOW"
    assert node.segments[1].body.keyword == "SELECT"
    assert node.segments[2].keyword == "SELECT"


def test_parser_builds_pipe_chain_tree_with_relative_pipe_references() -> None:
    node = parse_statement(build_split_statement('SHOW TABLES ->> SELECT "name" FROM $1 ->> SELECT COUNT(*) FROM $2'))

    assert isinstance(node, PipeChainNode)
    assert isinstance(node.segments[0], StatementFamilyNode)
    assert isinstance(node.segments[1], QueryNode)
    assert isinstance(node.segments[2], QueryNode)
    assert node.segments[0].keyword == "SHOW"
    assert node.segments[1].keyword == "SELECT"
    assert node.segments[2].keyword == "SELECT"


def test_parser_builds_explain_tree() -> None:
    node = parse_statement(build_split_statement("EXPLAIN INSERT INTO t VALUES (1)"))

    assert isinstance(node, ExplainNode)
    assert isinstance(node.subject, StatementFamilyNode)
    assert node.subject.family == "dml"


def test_parser_builds_explain_tree_with_using_clause() -> None:
    node = parse_statement(build_split_statement("EXPLAIN USING TEXT INSERT INTO t VALUES (1)"))

    assert isinstance(node, ExplainNode)
    assert isinstance(node.subject, StatementFamilyNode)
    assert node.subject.family == "dml"


def test_parser_builds_explain_tree_with_with_subject() -> None:
    node = parse_statement(build_split_statement("EXPLAIN USING JSON WITH cte AS (SELECT 1) SELECT * FROM cte"))

    assert isinstance(node, ExplainNode)
    assert isinstance(node.subject, WithNode)
    assert isinstance(node.subject.body, QueryNode)
    assert node.subject.body.keyword == "SELECT"


def test_parser_classifies_list_as_metadata() -> None:
    node = parse_statement(build_split_statement("LIST @mystage"))

    assert isinstance(node, StatementFamilyNode)
    assert node.keyword == "LIST"
    assert node.family == "metadata"


@pytest.mark.parametrize(
    "sql",
    [
        "START TRANSACTION",
        "START TRANSACTION NAME tx2",
    ],
)
def test_parser_classifies_start_transaction_as_transaction(sql: str) -> None:
    node = parse_statement(build_split_statement(sql))

    assert isinstance(node, StatementFamilyNode)
    assert node.keyword == "START"
    assert node.family == "transaction"
    assert node.diagnostics == ()


@pytest.mark.parametrize("sql", ["UNSET V1", "UNSET (V1, V2)"])
def test_parser_classifies_unset_as_session(sql: str) -> None:
    node = parse_statement(build_split_statement(sql))

    assert isinstance(node, StatementFamilyNode)
    assert node.keyword == "UNSET"
    assert node.family == "session"
    assert node.diagnostics == ()


@pytest.mark.parametrize(
    ("sql", "keyword"),
    [
        ("REMOVE @mystage/path1/subpath2", "REMOVE"),
        ("RM @~ pattern='.*jun.*'", "RM"),
    ],
)
def test_parser_classifies_remove_commands_as_file_transfer(sql: str, keyword: str) -> None:
    node = parse_statement(build_split_statement(sql))

    assert isinstance(node, StatementFamilyNode)
    assert node.keyword == keyword
    assert node.family == "file_transfer"
    assert node.diagnostics == ()


def test_parser_classifies_call_with_named_arguments_and_into_clause_as_scripting() -> None:
    node = parse_statement(build_split_statement("CALL sv_proc1(province => 'Manitoba', amount => 127.4) INTO :ret1"))

    assert isinstance(node, StatementFamilyNode)
    assert node.keyword == "CALL"
    assert node.family == "scripting"
    assert node.diagnostics == ()


@pytest.mark.parametrize(
    ("sql", "keyword"),
    [
        ("IF (TRUE) THEN SELECT 1; END IF", "IF"),
        ("FOR i IN 1 TO 3 DO SELECT 1; END FOR", "FOR"),
        ("WHILE (TRUE) DO SELECT 1; END WHILE", "WHILE"),
        ("WHILE (TRUE) LOOP SELECT 1; END LOOP", "WHILE"),
        ("REPEAT SELECT 1; UNTIL (TRUE) END REPEAT", "REPEAT"),
        ("LOOP BREAK; END LOOP", "LOOP"),
        ("LOOP CONTINUE; END LOOP", "LOOP"),
        ("LOOP ITERATE; END LOOP", "LOOP"),
        ("LOOP EXIT; END LOOP", "LOOP"),
        ("CASE WHEN TRUE THEN SELECT 1; END CASE", "CASE"),
    ],
)
def test_parser_classifies_top_level_control_flow_as_scripting(
    sql: str,
    keyword: str,
) -> None:
    node = parse_statement(build_split_statement(sql))

    assert isinstance(node, StatementFamilyNode)
    assert node.keyword == keyword
    assert node.family == "scripting"


@pytest.mark.parametrize(
    ("sql", "keyword"),
    [
        (
            "IF (TRUE) THEN BEGIN SELECT 1; SELECT 2; END; ELSEIF (FALSE) THEN SELECT 3; ELSE SELECT 4; END IF",
            "IF",
        ),
        ("FOR i IN REVERSE 1 TO 3 DO SELECT 1; END FOR", "FOR"),
        ("FOR rec IN c1 DO SELECT rec.price; END FOR", "FOR"),
        ("FOR i IN 1 TO 3 LOOP SELECT 1; END LOOP", "FOR"),
    ],
)
def test_parser_classifies_documented_control_flow_variants_as_scripting(
    sql: str,
    keyword: str,
) -> None:
    node = parse_statement(build_split_statement(sql))

    assert isinstance(node, StatementFamilyNode)
    assert node.keyword == keyword
    assert node.family == "scripting"
