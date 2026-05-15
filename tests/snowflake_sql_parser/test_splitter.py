import pytest

from snowflake_sql_parser import DiagnosticCode, SQLAnalysisError
from snowflake_sql_parser.parsing.splitter import build_split_statement, split_statements


def test_splitter_respects_string_boundaries() -> None:
    statements = split_statements("SELECT ';' AS semi; SELECT $$a;b$$ AS dollar;")

    assert [statement.text for statement in statements] == [
        "SELECT ';' AS semi",
        "SELECT $$a;b$$ AS dollar",
    ]


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        (
            'SELECT "quote"";--andunquote""" FROM demo; SELECT 2;',
            ['SELECT "quote"";--andunquote""" FROM demo', "SELECT 2"],
        ),
        (
            "SELECT 'Today'';--s sales' FROM demo; SELECT 2;",
            ["SELECT 'Today'';--s sales' FROM demo", "SELECT 2"],
        ),
        (
            "SELECT 'it\\'s;--still string' FROM demo; SELECT 2;",
            ["SELECT 'it\\'s;--still string' FROM demo", "SELECT 2"],
        ),
    ],
)
def test_splitter_respects_escaped_delimiters_inside_quoted_tokens(sql: str, expected: list[str]) -> None:
    statements = split_statements(sql)

    assert [statement.text for statement in statements] == expected


def test_splitter_keeps_pipe_chain_in_one_statement() -> None:
    statements = split_statements('SHOW TABLES ->> SELECT "name" FROM $1;')

    assert len(statements) == 1
    assert statements[0].pipe_segments == ("SHOW TABLES", 'SELECT "name" FROM $1')


def test_splitter_keeps_pipe_chain_after_carriage_return_line_comment() -> None:
    statements = split_statements('SHOW TABLES -- tail\r->> SELECT "name" FROM $1; SELECT 2;')

    assert [statement.text for statement in statements] == [
        'SHOW TABLES -- tail\r->> SELECT "name" FROM $1',
        "SELECT 2",
    ]
    assert statements[0].pipe_segments == ("SHOW TABLES -- tail", 'SELECT "name" FROM $1')


def test_splitter_keeps_three_segment_pipe_chain_with_middle_with_query_in_one_statement() -> None:
    statements = split_statements(
        'SHOW TABLES ->> WITH named AS (SELECT "name" FROM $1) SELECT * FROM named ->> SELECT * FROM $1;'
    )

    assert len(statements) == 1
    assert statements[0].pipe_segments == (
        "SHOW TABLES",
        'WITH named AS (SELECT "name" FROM $1) SELECT * FROM named',
        "SELECT * FROM $1",
    )


def test_splitter_keeps_block_semicolons_inside_statement() -> None:
    statements = split_statements("BEGIN INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); END;")

    assert len(statements) == 1
    assert statements[0].text == "BEGIN INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); END"


def test_splitter_keeps_nested_if_inside_begin_block() -> None:
    statements = split_statements("BEGIN IF (TRUE) THEN SELECT 1; END IF; END;")

    assert len(statements) == 1
    assert statements[0].text == "BEGIN IF (TRUE) THEN SELECT 1; END IF; END"


def test_splitter_keeps_nested_if_else_inside_begin_block() -> None:
    statements = split_statements("BEGIN IF (TRUE) THEN SELECT 1; ELSE SELECT 2; END IF; END;")

    assert len(statements) == 1
    assert statements[0].text == "BEGIN IF (TRUE) THEN SELECT 1; ELSE SELECT 2; END IF; END"


def test_splitter_keeps_if_with_elseif_and_nested_begin_block_in_one_statement() -> None:
    sql = "IF (TRUE) THEN BEGIN SELECT 1; SELECT 2; END; ELSEIF (FALSE) THEN SELECT 3; ELSE SELECT 4; END IF;"

    statements = split_statements(sql)

    assert len(statements) == 1
    assert statements[0].text == sql[:-1]


def test_splitter_keeps_nested_for_inside_declare_block() -> None:
    statements = split_statements("DECLARE x INT; BEGIN FOR i IN 1 TO 3 DO SELECT 1; END FOR; END;")

    assert len(statements) == 1
    assert statements[0].text == "DECLARE x INT; BEGIN FOR i IN 1 TO 3 DO SELECT 1; END FOR; END"


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        (
            "BEGIN SELECT 1; END my_label; SELECT 2;",
            ["BEGIN SELECT 1; END my_label", "SELECT 2"],
        ),
        (
            "BEGIN SELECT 1; EXCEPTION WHEN OTHER THEN SELECT 2; END my_label; SELECT 3;",
            ["BEGIN SELECT 1; EXCEPTION WHEN OTHER THEN SELECT 2; END my_label", "SELECT 3"],
        ),
    ],
)
def test_splitter_splits_after_labeled_begin_end_block(sql: str, expected: list[str]) -> None:
    statements = split_statements(sql)

    assert [statement.text for statement in statements] == expected


def test_splitter_splits_after_labeled_repeat_block() -> None:
    statements = split_statements("REPEAT SELECT 1; UNTIL (TRUE) END REPEAT my_label; SELECT 2;")

    assert [statement.text for statement in statements] == [
        "REPEAT SELECT 1; UNTIL (TRUE) END REPEAT my_label",
        "SELECT 2",
    ]


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        (
            "FOR i IN REVERSE 1 TO 3 DO SELECT 1; END FOR outer_loop; SELECT 2;",
            ["FOR i IN REVERSE 1 TO 3 DO SELECT 1; END FOR outer_loop", "SELECT 2"],
        ),
        (
            "FOR i IN 1 TO 3 LOOP SELECT 1; END LOOP outer_loop; SELECT 2;",
            ["FOR i IN 1 TO 3 LOOP SELECT 1; END LOOP outer_loop", "SELECT 2"],
        ),
        (
            "WHILE (TRUE) DO SELECT 1; END WHILE outer_loop; SELECT 2;",
            ["WHILE (TRUE) DO SELECT 1; END WHILE outer_loop", "SELECT 2"],
        ),
        (
            "WHILE (TRUE) LOOP SELECT 1; END LOOP outer_loop; SELECT 2;",
            ["WHILE (TRUE) LOOP SELECT 1; END LOOP outer_loop", "SELECT 2"],
        ),
        (
            "LOOP CONTINUE outer_loop; END LOOP outer_loop; SELECT 2;",
            ["LOOP CONTINUE outer_loop; END LOOP outer_loop", "SELECT 2"],
        ),
        (
            "LOOP ITERATE outer_loop; END LOOP outer_loop; SELECT 2;",
            ["LOOP ITERATE outer_loop; END LOOP outer_loop", "SELECT 2"],
        ),
        (
            "LOOP EXIT outer_loop; END LOOP outer_loop; SELECT 2;",
            ["LOOP EXIT outer_loop; END LOOP outer_loop", "SELECT 2"],
        ),
    ],
)
def test_splitter_splits_after_labeled_loop_blocks(sql: str, expected: list[str]) -> None:
    statements = split_statements(sql)

    assert [statement.text for statement in statements] == expected


@pytest.mark.parametrize(
    "sql",
    [
        "IF (TRUE) THEN SELECT 1; END IF;",
        "FOR i IN 1 TO 3 DO SELECT 1; END FOR;",
        "WHILE (TRUE) DO SELECT 1; END WHILE;",
        "WHILE (TRUE) LOOP SELECT 1; END LOOP;",
        "REPEAT SELECT 1; UNTIL (TRUE) END REPEAT;",
        "LOOP BREAK; END LOOP;",
        "LOOP CONTINUE; END LOOP;",
        "LOOP ITERATE; END LOOP;",
        "LOOP EXIT; END LOOP;",
        "CASE WHEN TRUE THEN SELECT 1; END CASE;",
    ],
)
def test_splitter_keeps_top_level_control_flow_in_one_statement(sql: str) -> None:
    statements = split_statements(sql)

    assert len(statements) == 1
    assert statements[0].text == sql[:-1]


@pytest.mark.parametrize(
    "sql",
    [
        "FOR i IN REVERSE 1 TO 3 DO SELECT 1; END FOR;",
        "FOR rec IN c1 DO SELECT rec.price; END FOR;",
        "FOR i IN 1 TO 3 LOOP SELECT 1; END LOOP;",
    ],
)
def test_splitter_keeps_documented_for_loop_variants_in_one_statement(sql: str) -> None:
    statements = split_statements(sql)

    assert len(statements) == 1
    assert statements[0].text == sql[:-1]


def test_splitter_keeps_pipe_chain_with_relative_pipe_references_in_one_statement() -> None:
    statements = split_statements('SHOW TABLES ->> SELECT "name" FROM $1 ->> SELECT COUNT(*) FROM $2;')

    assert len(statements) == 1
    assert statements[0].pipe_segments == (
        "SHOW TABLES",
        'SELECT "name" FROM $1',
        "SELECT COUNT(*) FROM $2",
    )


def test_splitter_splits_after_with_anonymous_javascript_procedure() -> None:
    statements = split_statements(
        "WITH p AS PROCEDURE () RETURNS VARCHAR LANGUAGE JAVASCRIPT AS $$return 'ok';$$ CALL p(); SELECT 2;"
    )

    assert [statement.text for statement in statements] == [
        "WITH p AS PROCEDURE () RETURNS VARCHAR LANGUAGE JAVASCRIPT AS $$return 'ok';$$ CALL p()",
        "SELECT 2",
    ]


def test_splitter_splits_after_documented_python_anonymous_procedure_variant() -> None:
    statements = split_statements(
        "WITH p AS PROCEDURE (n NUMBER) RETURNS NUMBER LANGUAGE PYTHON RUNTIME_VERSION = '3.11' PACKAGES = ('snowflake-snowpark-python') HANDLER = 'run' STRICT AS $$def run(session, n):\n  return n$$, cte AS (SELECT 1) CALL p(n => 1) INTO :ret1; SELECT 2;"
    )

    assert [statement.text for statement in statements] == [
        "WITH p AS PROCEDURE (n NUMBER) RETURNS NUMBER LANGUAGE PYTHON RUNTIME_VERSION = '3.11' PACKAGES = ('snowflake-snowpark-python') HANDLER = 'run' STRICT AS $$def run(session, n):\n  return n$$, cte AS (SELECT 1) CALL p(n => 1) INTO :ret1",
        "SELECT 2",
    ]


def test_build_split_statement_preserves_pipe_segment_spans() -> None:
    sql = 'SHOW TABLES ->> SELECT "name" FROM $1'
    statement = build_split_statement(sql, offset=10)

    first, second = statement.pipe_segment_pieces
    assert first.span.start == 10
    assert first.text == sql[first.span.start - 10 : first.span.end - 10]
    assert second.text == sql[second.span.start - 10 : second.span.end - 10]


@pytest.mark.parametrize("sql", ["", "   ", " -- comment only"])
def test_build_split_statement_rejects_empty_sql(sql: str) -> None:
    with pytest.raises(SQLAnalysisError) as exc_info:
        _ = build_split_statement(sql)

    diagnostic = exc_info.value.diagnostic
    assert diagnostic is not None
    assert diagnostic.code is DiagnosticCode.EMPTY_SQL


def test_splitter_rejects_invalid_pipe_chain() -> None:
    with pytest.raises(SQLAnalysisError) as exc_info:
        _ = build_split_statement("SELECT 1 ->>")

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
def test_splitter_rejects_mismatched_loop_terminators(sql: str) -> None:
    with pytest.raises(SQLAnalysisError) as exc_info:
        _ = build_split_statement(sql)

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
def test_splitter_rejects_invalid_scripting_forms_documented_by_snowflake(sql: str) -> None:
    with pytest.raises(SQLAnalysisError) as exc_info:
        _ = build_split_statement(sql)

    diagnostic = exc_info.value.diagnostic
    assert diagnostic is not None
    assert diagnostic.code is DiagnosticCode.UNEXPECTED_INPUT
