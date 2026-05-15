import pytest

import snowflake_sql_parser.analyzer as analyzer_module
from expression.contract import ContractViolationError
from snowflake_sql_parser import (
    AnalysisReport,
    DiagnosticCode,
    SQLAnalysisError,
    SQLAnalyzer,
    StatementAnalysis,
    TextSpan,
)
from snowflake_sql_parser.core import SqlScript, WithNode


def _analyze_statement(sql: str) -> StatementAnalysis:
    return SQLAnalyzer().analyze(sql).statements[0]


def test_analyzer_aggregates_multi_statement_sql() -> None:
    report = SQLAnalyzer().analyze("SELECT 1; INSERT INTO t VALUES (1)")

    assert report.status == "blocked"
    assert report.is_blocked
    assert not report.is_allowed
    assert not report.is_read_only
    assert len(report.statements) == 2
    assert report.statements[0].is_read_only
    assert not report.statements[1].is_read_only
    assert report.block_reason == "DML statements are not allowed"
    assert report.user_message == "Write operations are not allowed: DML statements are not allowed"
    assert report.denial is not None
    assert report.denial.statement_index == 1
    assert report.denial.path == (1,)
    assert report.diagnostics == (report.denial.diagnostic,)


@pytest.mark.parametrize(
    ("sql", "first_keyword", "second_keyword"),
    [
        ("BEGIN SELECT 1; END my_label; SELECT 2;", "BEGIN", "SELECT"),
        (
            "BEGIN SELECT 1; EXCEPTION WHEN OTHER THEN SELECT 2; END my_label; SELECT 3;",
            "BEGIN",
            "SELECT",
        ),
    ],
)
def test_analyzer_counts_statement_after_labeled_begin_end_block(
    sql: str,
    first_keyword: str,
    second_keyword: str,
) -> None:
    report = SQLAnalyzer().analyze(sql)

    assert len(report.statements) == 2
    assert report.statements[0].top_level_keyword == first_keyword
    assert report.statements[1].top_level_keyword == second_keyword


def test_analyzer_counts_statement_after_labeled_repeat_block() -> None:
    report = SQLAnalyzer().analyze("REPEAT SELECT 1; UNTIL (TRUE) END REPEAT my_label; SELECT 2;")

    assert len(report.statements) == 2
    assert report.statements[0].top_level_keyword == "REPEAT"
    assert report.statements[1].top_level_keyword == "SELECT"


def test_analyzer_reports_allowed_status_for_read_only_sql() -> None:
    report = SQLAnalyzer().analyze("SELECT 1")

    assert report.status == "allowed"
    assert report.is_allowed
    assert not report.is_blocked
    assert report.is_read_only
    assert report.denial is None
    assert report.block_reason is None
    assert report.user_message == "SQL is allowed."
    assert report.diagnostics == ()


def test_analyzer_rejects_comment_only_input() -> None:
    with pytest.raises(SQLAnalysisError, match="Empty SQL statement") as exc_info:
        _ = SQLAnalyzer().analyze("  -- only a comment\n  /* and another */")

    diagnostic = exc_info.value.diagnostic
    assert diagnostic is not None
    assert diagnostic.code is DiagnosticCode.EMPTY_SQL


@pytest.mark.parametrize("sql", ["", "   "])
def test_analyzer_helpers_reject_empty_sql(sql: str) -> None:
    with pytest.raises(SQLAnalysisError, match="Empty SQL statement") as exc_info:
        _ = SQLAnalyzer().is_write_sql(sql)

    diagnostic = exc_info.value.diagnostic
    assert diagnostic is not None
    assert diagnostic.code is DiagnosticCode.EMPTY_SQL


def test_analyzer_helpers_detect_write_and_read_sql() -> None:
    analyzer = SQLAnalyzer()

    assert analyzer.is_read_only_sql('SHOW TABLES ->> SELECT "name" FROM $1')
    assert analyzer.is_read_only_sql("LIST @mystage")
    assert analyzer.is_read_only_sql("LS @mystage")
    assert analyzer.is_write_sql("EXECUTE IMMEDIATE $$ SELECT 1 $$")


@pytest.mark.parametrize(
    "sql",
    [
        "START TRANSACTION",
        "START TRANSACTION NAME tx2",
    ],
)
def test_analyzer_blocks_start_transaction_as_transaction(sql: str) -> None:
    analysis = _analyze_statement(sql)

    assert not analysis.is_read_only
    assert analysis.top_level_keyword == "START"
    assert analysis.family == "transaction"
    assert analysis.block_reason == "Transaction statements are not allowed"


@pytest.mark.parametrize("sql", ["UNSET V1", "UNSET (V1, V2)"])
def test_analyzer_blocks_unset_as_session(sql: str) -> None:
    analysis = _analyze_statement(sql)

    assert not analysis.is_read_only
    assert analysis.top_level_keyword == "UNSET"
    assert analysis.family == "session"
    assert analysis.block_reason == "Session statements are not allowed"


@pytest.mark.parametrize(
    ("sql", "keyword"),
    [
        ("REMOVE @mystage/path1/subpath2", "REMOVE"),
        ("RM @~ pattern='.*jun.*'", "RM"),
    ],
)
def test_analyzer_blocks_remove_commands_as_file_transfer(sql: str, keyword: str) -> None:
    analysis = _analyze_statement(sql)

    assert not analysis.is_read_only
    assert analysis.top_level_keyword == keyword
    assert analysis.family == "file_transfer"
    assert analysis.block_reason == "File transfer statements are not allowed"


def test_analyzer_projects_with_body_into_nested_statement_analysis() -> None:
    analysis = _analyze_statement("WITH cte AS (SELECT 1) SELECT * FROM cte")

    assert analysis.is_read_only
    assert analysis.top_level_keyword == "WITH"
    assert analysis.family == "query"
    assert analysis.nested[0].top_level_keyword == "SELECT"


@pytest.mark.parametrize(
    "sql",
    [
        "WITH use AS (SELECT 1) SELECT * FROM use",
        "WITH copy AS (SELECT 1) SELECT * FROM copy",
        "WITH rollback AS (SELECT 1) SELECT * FROM rollback",
        "WITH begin AS (SELECT 1) SELECT * FROM begin",
    ],
)
def test_analyzer_allows_keyword_like_cte_aliases(sql: str) -> None:
    analysis = _analyze_statement(sql)

    assert analysis.is_read_only
    assert analysis.top_level_keyword == "WITH"
    assert analysis.nested[0].top_level_keyword == "SELECT"


@pytest.mark.parametrize(
    "sql",
    [
        'WITH "use" AS (SELECT 1) SELECT * FROM "use"',
        'WITH "copy" AS (SELECT 1) SELECT * FROM "copy"',
        'WITH "rollback" AS (SELECT 1) SELECT * FROM "rollback"',
        'WITH "begin" AS (SELECT 1) SELECT * FROM "begin"',
    ],
)
def test_analyzer_allows_quoted_keyword_like_cte_aliases(sql: str) -> None:
    analysis = _analyze_statement(sql)

    assert analysis.is_read_only
    assert analysis.top_level_keyword == "WITH"
    assert analysis.nested[0].top_level_keyword == "SELECT"


def test_analyzer_allows_with_body_after_carriage_return_line_comment() -> None:
    analysis = _analyze_statement("WITH cte AS (SELECT 1) -- tail\rSELECT * FROM cte")

    assert analysis.is_read_only
    assert analysis.top_level_keyword == "WITH"
    assert analysis.nested[0].top_level_keyword == "SELECT"
    assert analysis.block_reason is None


@pytest.mark.parametrize(
    "sql",
    [
        "WITH recursive AS (SELECT 1) SELECT * FROM recursive",
        "WITH recursive(value) AS (SELECT 1) SELECT * FROM recursive",
    ],
)
def test_analyzer_allows_recursive_named_cte(sql: str) -> None:
    analysis = _analyze_statement(sql)

    assert analysis.is_read_only
    assert analysis.top_level_keyword == "WITH"
    assert analysis.nested[0].top_level_keyword == "SELECT"


@pytest.mark.parametrize(
    "sql",
    [
        "WITH RECURSIVE recursive(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM recursive) SELECT * FROM recursive",
        "WITH RECURSIVE base(n) AS (SELECT 1), walk(n) AS (SELECT n FROM base UNION ALL SELECT n + 1 FROM walk) SELECT * FROM walk",
    ],
)
def test_analyzer_allows_recursive_with_modifier_cases(sql: str) -> None:
    analysis = _analyze_statement(sql)

    assert analysis.is_read_only
    assert analysis.top_level_keyword == "WITH"
    assert analysis.nested[0].top_level_keyword == "SELECT"


@pytest.mark.parametrize(
    "sql",
    [
        "WITH RECURSIVE base AS (SELECT 1) SELECT * FROM base",
        "WITH RECURSIVE seed AS (SELECT 1), walk(n) AS (SELECT n FROM seed UNION ALL SELECT n + 1 FROM walk) SELECT * FROM walk",
    ],
)
def test_analyzer_allows_recursive_keyword_without_leading_recursive_cte(sql: str) -> None:
    analysis = _analyze_statement(sql)

    assert analysis.is_read_only
    assert analysis.top_level_keyword == "WITH"
    assert analysis.nested[0].top_level_keyword == "SELECT"


def test_analyzer_blocks_with_call_body() -> None:
    analysis = _analyze_statement(
        "WITH p AS PROCEDURE () RETURNS VARCHAR LANGUAGE SQL AS $$BEGIN RETURN 'ok'; END;$$ CALL p()"
    )

    assert not analysis.is_read_only
    assert analysis.top_level_keyword == "WITH"
    assert analysis.nested[0].top_level_keyword == "CALL"
    assert analysis.nested[0].family == "scripting"


def test_analyzer_blocks_with_call_body_after_additional_cte_bindings() -> None:
    report = SQLAnalyzer().analyze(
        "WITH p AS PROCEDURE () RETURNS VARCHAR LANGUAGE SQL AS $$BEGIN RETURN 'ok'; END;$$, cte AS (SELECT 1) CALL p()"
    )
    analysis = report.statements[0]

    assert not analysis.is_read_only
    assert report.block_reason == "CALL statements are not allowed"
    assert analysis.top_level_keyword == "WITH"
    assert analysis.family == "scripting"
    assert analysis.nested[0].top_level_keyword == "CALL"


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
def test_analyzer_blocks_documented_anonymous_procedure_variants(sql: str) -> None:
    report = SQLAnalyzer().analyze(sql)
    analysis = report.statements[0]

    assert report.is_blocked
    assert report.block_reason == "CALL statements are not allowed"
    assert not analysis.is_read_only
    assert analysis.top_level_keyword == "WITH"
    assert analysis.family == "scripting"
    assert analysis.nested[0].top_level_keyword == "CALL"


@pytest.mark.xfail(
    reason="Snowflake documents staged anonymous procedures without AS, but analysis does not support them yet",
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
def test_analyzer_blocks_documented_staged_anonymous_procedure_forms(sql: str) -> None:
    report = SQLAnalyzer().analyze(sql)
    analysis = report.statements[0]

    assert report.is_blocked
    assert report.block_reason == "CALL statements are not allowed"
    assert not analysis.is_read_only
    assert analysis.top_level_keyword == "WITH"
    assert analysis.family == "scripting"
    assert analysis.nested[0].top_level_keyword == "CALL"


def test_analyzer_blocks_with_call_into_body_after_additional_cte_bindings() -> None:
    report = SQLAnalyzer().analyze(
        "WITH p AS PROCEDURE () RETURNS VARCHAR LANGUAGE SQL AS $$BEGIN RETURN 'ok'; END;$$, cte AS (SELECT 1) CALL p() INTO :ret1"
    )
    analysis = report.statements[0]

    assert not analysis.is_read_only
    assert report.block_reason == "CALL statements are not allowed"
    assert analysis.top_level_keyword == "WITH"
    assert analysis.family == "scripting"
    assert analysis.nested[0].top_level_keyword == "CALL"


def test_analyzer_rejects_unsupported_with_body_keyword() -> None:
    with pytest.raises(SQLAnalysisError) as exc_info:
        _ = SQLAnalyzer().analyze("WITH cte AS (SELECT 1) INSERT INTO t SELECT * FROM cte")

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
def test_analyzer_rejects_invalid_anonymous_procedure_forms_documented_by_snowflake(sql: str) -> None:
    with pytest.raises(SQLAnalysisError):
        _ = SQLAnalyzer().analyze(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "WITH RECURSIVE seed AS (SELECT 1), RECURSIVE walk(n) AS (SELECT n FROM seed UNION ALL SELECT n + 1 FROM walk) SELECT * FROM walk",
        "WITH RECURSIVE walk(n) AS (SELECT 1 UNION SELECT n + 1 FROM walk) SELECT * FROM walk",
    ],
)
def test_analyzer_rejects_invalid_recursive_cte_forms_documented_by_snowflake(sql: str) -> None:
    with pytest.raises(SQLAnalysisError):
        _ = SQLAnalyzer().analyze(sql)


def test_analyzer_blocks_write_cte_definition() -> None:
    analysis = _analyze_statement("WITH cte AS (DELETE FROM users WHERE 1 = 1) SELECT 1")

    assert not analysis.is_read_only
    assert analysis.top_level_keyword == "WITH"
    assert analysis.family == "query"
    assert analysis.block_reason == "CTE definitions must be read-only: DML statements are not allowed"
    assert analysis.nested[0].top_level_keyword == "SELECT"


def test_analyzer_blocks_select_for_update_in_cte_definition() -> None:
    analysis = _analyze_statement("WITH cte AS (SELECT * FROM users FOR UPDATE) SELECT 1")

    assert not analysis.is_read_only
    assert analysis.block_reason == "CTE definitions must be read-only: SELECT ... FOR UPDATE is not read-only"


def test_analyzer_blocks_select_into_in_cte_definition() -> None:
    analysis = _analyze_statement("WITH cte AS (SELECT id INTO :target FROM users) SELECT 1")

    assert not analysis.is_read_only
    assert analysis.block_reason == "CTE definitions must be read-only: SELECT ... INTO is not read-only"


def test_analyzer_requires_all_pipe_segments_to_be_read_only() -> None:
    allowed = _analyze_statement('SHOW TABLES ->> SELECT "name" FROM $1')
    blocked = _analyze_statement("SHOW TABLES ->> CREATE TABLE t (id INT)")

    assert allowed.is_read_only
    assert not blocked.is_read_only
    assert blocked.nested[1].family == "ddl"


def test_analyzer_allows_pipe_chain_with_middle_with_segment() -> None:
    analysis = _analyze_statement(
        'SHOW TABLES ->> WITH named AS (SELECT "name" FROM $1) SELECT * FROM named ->> SELECT * FROM $1'
    )

    assert analysis.is_read_only
    assert analysis.top_level_keyword == "SHOW"
    assert [segment.top_level_keyword for segment in analysis.nested] == ["SHOW", "WITH", "SELECT"]


def test_analyzer_allows_pipe_chain_with_relative_pipe_references() -> None:
    analysis = _analyze_statement('SHOW TABLES ->> SELECT "name" FROM $1 ->> SELECT COUNT(*) FROM $2')

    assert analysis.is_read_only
    assert analysis.top_level_keyword == "SHOW"
    assert [segment.top_level_keyword for segment in analysis.nested] == ["SHOW", "SELECT", "SELECT"]


def test_analyzer_allows_pipe_chain_with_positional_select_dollar_reference() -> None:
    analysis = _analyze_statement('SHOW TABLES ->> SELECT $1, "name" FROM $1')

    assert analysis.is_read_only
    assert analysis.top_level_keyword == "SHOW"
    assert analysis.nested[1].top_level_keyword == "SELECT"


def test_analyzer_reports_nested_denial_path_for_pipe_chain() -> None:
    report = SQLAnalyzer().analyze("SHOW TABLES ->> CREATE TABLE t (id INT)")

    assert report.is_blocked
    assert report.block_reason == "DDL statements are not allowed"
    assert report.denial is not None
    assert report.denial.statement_index == 0
    assert report.denial.path == (0, 1)
    assert report.denial.statement.family == "ddl"
    assert report.statements[0].diagnostic is None
    assert report.diagnostics == (report.denial.diagnostic,)


def test_analyzer_aggregates_nested_diagnostics_from_all_statements() -> None:
    report = SQLAnalyzer().analyze("SHOW TABLES ->> CREATE TABLE t (id INT); SHOW TABLES ->> INSERT INTO u VALUES (1)")

    assert report.is_blocked
    assert report.block_reason == "DDL statements are not allowed"
    assert [diagnostic.message for diagnostic in report.diagnostics] == [
        "DDL statements are not allowed",
        "DML statements are not allowed",
    ]


def test_analyzer_allows_explain_insert() -> None:
    report = SQLAnalyzer().analyze("EXPLAIN INSERT INTO t VALUES (1)")
    analysis = report.statements[0]

    assert report.is_allowed
    assert report.denial is None
    assert report.diagnostics == ()
    assert analysis.is_read_only
    assert analysis.family == "metadata"
    assert analysis.nested[0].family == "dml"


def test_analyzer_allows_explain_with_using_clause() -> None:
    report = SQLAnalyzer().analyze("EXPLAIN USING JSON INSERT INTO t VALUES (1)")

    assert report.is_allowed
    assert report.denial is None
    assert report.diagnostics == ()
    assert report.statements[0].nested[0].family == "dml"


def test_analyzer_allows_explain_with_with_subject() -> None:
    report = SQLAnalyzer().analyze("EXPLAIN USING JSON WITH cte AS (SELECT 1) SELECT * FROM cte")

    assert report.is_allowed
    assert report.denial is None
    assert report.diagnostics == ()
    assert report.statements[0].nested[0].top_level_keyword == "WITH"
    assert report.statements[0].nested[0].family == "query"


def test_analyzer_blocks_explain_unknown_subject() -> None:
    report = SQLAnalyzer().analyze("EXPLAIN MYSTERY 1")

    assert report.is_blocked
    assert report.block_reason == "Statement type is not proven read-only"
    assert report.denial is not None
    assert report.denial.diagnostic.code is DiagnosticCode.UNKNOWN_STATEMENT
    assert report.statements[0].family == "metadata"
    assert report.statements[0].nested[0].family == "unknown"


def test_analyzer_blocks_explain_with_unparsable_with_subject() -> None:
    report = SQLAnalyzer().analyze("EXPLAIN WITH cte AS (SELECT 1)")

    assert report.is_blocked
    assert report.block_reason == "WITH statement body could not be determined"
    assert report.denial is not None
    assert report.denial.diagnostic.code is DiagnosticCode.UNPARSABLE_WITH_BODY
    assert report.statements[0].family == "metadata"
    assert report.statements[0].nested[0].family == "unknown"


def test_analyzer_blocks_select_for_update() -> None:
    analysis = _analyze_statement("SELECT * FROM table1 FOR UPDATE")

    assert not analysis.is_read_only
    assert "FOR_UPDATE" in analysis.constructs
    assert analysis.block_reason == "SELECT ... FOR UPDATE is not read-only"


def test_analyzer_blocks_select_into() -> None:
    analysis = _analyze_statement("SELECT col INTO :var FROM table1")

    assert not analysis.is_read_only
    assert "INTO" in analysis.constructs
    assert analysis.block_reason == "SELECT ... INTO is not read-only"


@pytest.mark.parametrize(
    ("sql", "construct"),
    [
        ("SELECT 'FOR UPDATE' AS note FROM demo", "FOR_UPDATE"),
        ("SELECT 'INTO' AS note FROM demo", "INTO"),
        ("SELECT 'it\\'s FOR UPDATE' AS note FROM demo", "FOR_UPDATE"),
        ("SELECT 'it\\'s INTO' AS note FROM demo", "INTO"),
    ],
)
def test_analyzer_ignores_string_literals_for_write_detection(sql: str, construct: str) -> None:
    analysis = _analyze_statement(sql)

    assert analysis.is_read_only
    assert construct not in analysis.constructs
    assert analysis.block_reason is None


def test_analyzer_blocks_execute_immediate() -> None:
    analysis = _analyze_statement("EXECUTE IMMEDIATE $$ SELECT 1 $$")

    assert not analysis.is_read_only
    assert analysis.family == "dynamic_sql"
    assert analysis.block_reason == "EXECUTE IMMEDIATE is not allowed"


def test_analyzer_blocks_call_with_into_clause() -> None:
    analysis = _analyze_statement("CALL sv_proc1('Manitoba', 127.4) INTO :ret1")

    assert not analysis.is_read_only
    assert analysis.top_level_keyword == "CALL"
    assert analysis.family == "scripting"
    assert analysis.block_reason == "CALL statements are not allowed"


def test_analyzer_blocks_call_with_named_arguments_and_into_clause() -> None:
    analysis = _analyze_statement("CALL sv_proc1(province => 'Manitoba', amount => 127.4) INTO :ret1")

    assert not analysis.is_read_only
    assert analysis.top_level_keyword == "CALL"
    assert analysis.family == "scripting"
    assert analysis.block_reason == "CALL statements are not allowed"


def test_analyzer_blocks_execute_immediate_with_variable_and_using_clause() -> None:
    analysis = _analyze_statement("EXECUTE IMMEDIATE query USING (minimum_price, maximum_price)")

    assert not analysis.is_read_only
    assert analysis.top_level_keyword == "EXECUTE"
    assert analysis.family == "dynamic_sql"
    assert analysis.block_reason == "EXECUTE IMMEDIATE is not allowed"


def test_analyzer_allows_snowflake_query_constructs() -> None:
    analysis = _analyze_statement("SELECT src:vehicle[0].price::NUMBER FROM TABLE(IDENTIFIER('CAR_SALES'))")

    assert analysis.is_read_only


def test_analyzer_allows_variant_path_key_named_into() -> None:
    analysis = _analyze_statement("SELECT src:into FROM car_sales")

    assert analysis.is_read_only
    assert "INTO" not in analysis.constructs


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
def test_analyzer_allows_select_syntax_edges(sql: str) -> None:
    analysis = _analyze_statement(sql)

    assert analysis.is_read_only
    assert analysis.top_level_keyword == "SELECT"
    assert analysis.block_reason is None


def test_analyzer_reports_invalid_pipe_chain_as_sql_analysis_error() -> None:
    with pytest.raises(SQLAnalysisError) as exc_info:
        _ = SQLAnalyzer().analyze("SELECT 1 ->>")

    diagnostic = exc_info.value.diagnostic
    assert diagnostic is not None
    assert diagnostic.code is DiagnosticCode.INVALID_PIPE_CHAIN


@pytest.mark.parametrize(
    "sql",
    [
        "WHILE (TRUE) DO SELECT 1; END LOOP",
        "WHILE (TRUE) LOOP SELECT 1; END WHILE",
        "FOR i IN 1 TO 3 DO SELECT 1; END LOOP",
        "FOR i IN 1 TO 3 LOOP SELECT 1; END FOR",
        "FOR rec IN c1 DO SELECT rec.price; END LOOP",
    ],
)
def test_analyzer_rejects_mismatched_loop_terminators(sql: str) -> None:
    with pytest.raises(SQLAnalysisError) as exc_info:
        _ = SQLAnalyzer().analyze(sql)

    diagnostic = exc_info.value.diagnostic
    assert diagnostic is not None
    assert diagnostic.code is DiagnosticCode.UNEXPECTED_INPUT


@pytest.mark.parametrize(
    "sql",
    [
        "BEGIN; SELECT 1; END",
        "IF TRUE THEN SELECT 1; END IF",
        "IF (TRUE) SELECT 1; END IF",
        "WHILE TRUE DO SELECT 1; END WHILE",
        "REPEAT SELECT 1; UNTIL TRUE END REPEAT",
        "REPEAT SELECT 1; UNTIL (TRUE); END REPEAT",
        "CASE WHEN TRUE SELECT 1; END CASE",
        "FOR rec IN c1 LOOP SELECT rec.price; END LOOP",
        "FOR rec IN REVERSE c1 DO SELECT rec.price; END FOR",
    ],
)
def test_analyzer_rejects_invalid_scripting_forms_documented_by_snowflake(sql: str) -> None:
    with pytest.raises(SQLAnalysisError) as exc_info:
        _ = SQLAnalyzer().analyze(sql)

    diagnostic = exc_info.value.diagnostic
    assert diagnostic is not None
    assert diagnostic.code is DiagnosticCode.UNEXPECTED_INPUT


def test_analyzer_blocks_unparsable_with_body_without_exception() -> None:
    analysis = _analyze_statement("WITH cte AS (SELECT 1)")

    assert not analysis.is_read_only
    assert analysis.top_level_keyword == "WITH"
    assert analysis.block_reason == "WITH statement body could not be determined"


def test_analyzer_keeps_nested_control_flow_inside_begin_statement() -> None:
    report = SQLAnalyzer().analyze("BEGIN IF (TRUE) THEN SELECT 1; END IF; END;")

    assert len(report.statements) == 1
    assert report.statements[0].family == "scripting"
    assert report.block_reason == "Snowflake Scripting blocks are not allowed"


def test_analyzer_classifies_if_with_elseif_and_nested_begin_as_scripting() -> None:
    analysis = _analyze_statement(
        "IF (TRUE) THEN BEGIN SELECT 1; SELECT 2; END; ELSEIF (FALSE) THEN SELECT 3; ELSE SELECT 4; END IF"
    )

    assert not analysis.is_read_only
    assert analysis.top_level_keyword == "IF"
    assert analysis.family == "scripting"
    assert analysis.block_reason == "Snowflake Scripting blocks are not allowed"


@pytest.mark.parametrize(
    ("sql", "keyword"),
    [
        ("IF (TRUE) THEN SELECT 1; END IF;", "IF"),
        ("FOR i IN 1 TO 3 DO SELECT 1; END FOR;", "FOR"),
        ("WHILE (TRUE) DO SELECT 1; END WHILE;", "WHILE"),
        ("WHILE (TRUE) LOOP SELECT 1; END LOOP;", "WHILE"),
        ("REPEAT SELECT 1; UNTIL (TRUE) END REPEAT;", "REPEAT"),
        ("LOOP BREAK; END LOOP;", "LOOP"),
        ("LOOP CONTINUE; END LOOP;", "LOOP"),
        ("LOOP ITERATE; END LOOP;", "LOOP"),
        ("LOOP EXIT; END LOOP;", "LOOP"),
        ("CASE WHEN TRUE THEN SELECT 1; END CASE;", "CASE"),
    ],
)
def test_analyzer_classifies_top_level_control_flow_as_scripting(
    sql: str,
    keyword: str,
) -> None:
    analysis = _analyze_statement(sql)

    assert not analysis.is_read_only
    assert analysis.top_level_keyword == keyword
    assert analysis.family == "scripting"
    assert analysis.block_reason == "Snowflake Scripting blocks are not allowed"


@pytest.mark.parametrize(
    ("sql", "keyword"),
    [
        ("FOR i IN REVERSE 1 TO 3 DO SELECT 1; END FOR;", "FOR"),
        ("FOR rec IN c1 DO SELECT rec.price; END FOR;", "FOR"),
        ("FOR i IN 1 TO 3 LOOP SELECT 1; END LOOP;", "FOR"),
    ],
)
def test_analyzer_classifies_documented_for_loop_variants_as_scripting(
    sql: str,
    keyword: str,
) -> None:
    analysis = _analyze_statement(sql)

    assert not analysis.is_read_only
    assert analysis.top_level_keyword == keyword
    assert analysis.family == "scripting"
    assert analysis.block_reason == "Snowflake Scripting blocks are not allowed"


@pytest.mark.parametrize(
    ("sql", "first_keyword", "second_keyword"),
    [
        ("FOR i IN REVERSE 1 TO 3 DO SELECT 1; END FOR outer_loop; SELECT 2;", "FOR", "SELECT"),
        ("FOR i IN 1 TO 3 LOOP SELECT 1; END LOOP outer_loop; SELECT 2;", "FOR", "SELECT"),
        ("WHILE (TRUE) DO SELECT 1; END WHILE outer_loop; SELECT 2;", "WHILE", "SELECT"),
        ("WHILE (TRUE) LOOP SELECT 1; END LOOP outer_loop; SELECT 2;", "WHILE", "SELECT"),
        ("LOOP CONTINUE outer_loop; END LOOP outer_loop; SELECT 2;", "LOOP", "SELECT"),
        ("LOOP ITERATE outer_loop; END LOOP outer_loop; SELECT 2;", "LOOP", "SELECT"),
        ("LOOP EXIT outer_loop; END LOOP outer_loop; SELECT 2;", "LOOP", "SELECT"),
    ],
)
def test_analyzer_counts_statement_after_labeled_loop_blocks(
    sql: str,
    first_keyword: str,
    second_keyword: str,
) -> None:
    report = SQLAnalyzer().analyze(sql)

    assert len(report.statements) == 2
    assert report.statements[0].top_level_keyword == first_keyword
    assert report.statements[1].top_level_keyword == second_keyword


def test_analysis_report_uses_fallback_denial_when_diagnostic_is_missing() -> None:
    statement = StatementAnalysis(
        text="mystery",
        span=TextSpan(0, 7),
        family="unknown",
        top_level_keyword=None,
        is_read_only=False,
        constructs=frozenset(),
    )

    report = AnalysisReport.from_statements((statement,))

    assert report.is_blocked
    assert report.block_reason == "Statement is not proven read-only"
    assert report.user_message == "Write operations are not allowed: Statement is not proven read-only"
    assert report.denial is not None
    assert report.denial.statement_index == 0
    assert report.denial.path == (0,)
    assert report.denial.diagnostic.code is DiagnosticCode.BLOCKED_STATEMENT
    assert report.diagnostics == (report.denial.diagnostic,)


def test_analyzer_maps_empty_script_to_sql_analysis_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def parse_script_stub(sql: str) -> SqlScript:
        _ = sql
        return SqlScript(statements=())

    monkeypatch.setattr(analyzer_module, "parse_script", parse_script_stub)

    with pytest.raises(SQLAnalysisError, match="Could not parse any SQL statements") as exc_info:
        _ = SQLAnalyzer().analyze("SELECT 1")

    diagnostic = exc_info.value.diagnostic
    assert diagnostic is not None
    assert diagnostic.code is DiagnosticCode.UNEXPECTED_INPUT


def test_analyzer_wraps_statement_analysis_invariant_violations(monkeypatch: pytest.MonkeyPatch) -> None:
    private_name = "_node_children"
    original_node_children = getattr(analyzer_module, private_name)

    def broken_node_children(node: object) -> tuple[object, ...]:
        if isinstance(node, WithNode):
            return ()
        return original_node_children(node)

    monkeypatch.setattr(analyzer_module, private_name, broken_node_children)

    with pytest.raises(ContractViolationError) as exc_info:
        _ = SQLAnalyzer().analyze("WITH cte AS (SELECT 1) SELECT * FROM cte")

    error = exc_info.value
    assert error.function_name == "_to_statement_analysis"
    assert error.original_exception is not None
    assert type(error.original_exception).__name__ == "ParserInvariantError"
