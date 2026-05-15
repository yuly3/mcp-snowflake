"""Snowflake dialect registry and keyword definitions."""

from collections.abc import Mapping

import attrs

from ..core.models import QueryConstruct
from ..core.syntax import PolicyKind
from ..parsing.registry import StatementParserKind, StatementParserSpec, StatementRegistry

METADATA_KEYWORDS = frozenset({"SHOW", "DESCRIBE", "DESC", "LIST", "LS"})
QUERY_KEYWORDS = frozenset({"SELECT"})

DML_KEYWORDS = frozenset({"INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE"})
DDL_KEYWORDS = frozenset({"CREATE", "ALTER", "DROP", "UNDROP", "RENAME"})
FILE_TRANSFER_KEYWORDS = frozenset({"PUT", "GET", "REMOVE", "RM"})
ACCESS_CONTROL_KEYWORDS = frozenset({"GRANT", "REVOKE"})
SESSION_KEYWORDS = frozenset({"USE", "SET", "UNSET"})
TRANSACTION_KEYWORDS = frozenset({"COMMIT", "ROLLBACK"})
SCRIPTING_CONTROL_FLOW_KEYWORDS = frozenset({"IF", "FOR", "WHILE", "LOOP", "REPEAT", "CASE"})
SCRIPTING_KEYWORDS = frozenset({"DECLARE", "CALL", *SCRIPTING_CONTROL_FLOW_KEYWORDS})

BEGIN_TRANSACTION_FOLLOWERS = frozenset({"TRANSACTION", "WORK", "NAME", ";"})
BLOCK_END_FOLLOWERS = frozenset({";", "IF", "FOR", "WHILE", "LOOP", "CASE", "REPEAT"})

EXPLAIN_OUTPUT_FORMATS = frozenset({"TABULAR", "JSON", "TEXT"})

WITH_BODY_START_KEYWORDS = frozenset({
    "SELECT",
    "CALL",
})

QUERY_CONSTRUCT_BY_KEYWORD: dict[str, QueryConstruct] = {
    "AT": "AT",
    "BEFORE": "BEFORE",
    "CHANGES": "CHANGES",
    "QUALIFY": "QUALIFY",
    "SAMPLE": "SAMPLE",
    "TABLESAMPLE": "SAMPLE",
    "LATERAL": "LATERAL",
    "FLATTEN": "FLATTEN",
}

SNOWFLAKE_KEYWORDS = frozenset({
    *METADATA_KEYWORDS,
    *QUERY_KEYWORDS,
    *DML_KEYWORDS,
    *DDL_KEYWORDS,
    *FILE_TRANSFER_KEYWORDS,
    *ACCESS_CONTROL_KEYWORDS,
    *SESSION_KEYWORDS,
    *TRANSACTION_KEYWORDS,
    *SCRIPTING_KEYWORDS,
    "AT",
    "BEFORE",
    "BEGIN",
    "CALL",
    "CASE",
    "CHANGES",
    "COMMIT",
    "COPY",
    "DECLARE",
    "DESC",
    "DESCRIBE",
    "END",
    "EXECUTE",
    "EXPLAIN",
    "FLATTEN",
    "FOR",
    "FROM",
    "GET",
    "IF",
    "IMMEDIATE",
    "INTO",
    "LATERAL",
    "LOOP",
    "NAME",
    "PUT",
    "QUALIFY",
    "REPEAT",
    "ROLLBACK",
    "SAMPLE",
    "SELECT",
    "SESSION",
    "SHOW",
    "SQL",
    "START",
    "TABLE",
    "TABLESAMPLE",
    "TRANSACTION",
    "USING",
    "UPDATE",
    "VALUES",
    "WHILE",
    "WITH",
    "WORK",
    *EXPLAIN_OUTPUT_FORMATS,
})


@attrs.define(frozen=True, slots=True)
class SnowflakeDialect:
    """Snowflake-specific parsing configuration."""

    keywords: frozenset[str]
    registry: StatementRegistry
    with_body_start_keywords: frozenset[str]
    explain_output_formats: frozenset[str]
    begin_transaction_followers: frozenset[str]
    block_end_followers: frozenset[str]
    query_construct_by_keyword: Mapping[str, QueryConstruct]


def _build_registry() -> StatementRegistry:
    return StatementRegistry(
        specs=(
            StatementParserSpec(
                keywords=frozenset({"SELECT"}),
                parser_kind=StatementParserKind.QUERY,
                default_family="query",
            ),
            StatementParserSpec(
                keywords=frozenset({"SHOW", "DESCRIBE", "DESC", "LIST", "LS"}),
                parser_kind=StatementParserKind.METADATA,
                default_family="metadata",
                family_policy=PolicyKind.ALLOW,
            ),
            StatementParserSpec(
                keywords=frozenset({"EXPLAIN"}),
                parser_kind=StatementParserKind.EXPLAIN,
                default_family="metadata",
            ),
            StatementParserSpec(
                keywords=frozenset({"WITH"}),
                parser_kind=StatementParserKind.WITH,
                default_family="unknown",
            ),
            StatementParserSpec(
                keywords=DML_KEYWORDS,
                parser_kind=StatementParserKind.FAMILY,
                default_family="dml",
                family_policy=PolicyKind.BLOCK,
            ),
            StatementParserSpec(
                keywords=frozenset({"CREATE", "DROP", "UNDROP", "RENAME"}),
                parser_kind=StatementParserKind.FAMILY,
                default_family="ddl",
                family_policy=PolicyKind.BLOCK,
            ),
            StatementParserSpec(
                keywords=frozenset({"ALTER"}),
                parser_kind=StatementParserKind.ALTER,
                default_family="ddl",
                family_policy=PolicyKind.BLOCK,
            ),
            StatementParserSpec(
                keywords=frozenset({"COPY"}),
                parser_kind=StatementParserKind.FAMILY,
                default_family="copy",
                family_policy=PolicyKind.BLOCK,
            ),
            StatementParserSpec(
                keywords=FILE_TRANSFER_KEYWORDS,
                parser_kind=StatementParserKind.FAMILY,
                default_family="file_transfer",
                family_policy=PolicyKind.BLOCK,
            ),
            StatementParserSpec(
                keywords=ACCESS_CONTROL_KEYWORDS,
                parser_kind=StatementParserKind.FAMILY,
                default_family="access_control",
                family_policy=PolicyKind.BLOCK,
            ),
            StatementParserSpec(
                keywords=SESSION_KEYWORDS,
                parser_kind=StatementParserKind.FAMILY,
                default_family="session",
                family_policy=PolicyKind.BLOCK,
            ),
            StatementParserSpec(
                keywords=TRANSACTION_KEYWORDS,
                parser_kind=StatementParserKind.FAMILY,
                default_family="transaction",
                family_policy=PolicyKind.BLOCK,
            ),
            StatementParserSpec(
                keywords=frozenset({"BEGIN"}),
                parser_kind=StatementParserKind.BEGIN,
                default_family="transaction",
                family_policy=PolicyKind.BLOCK,
            ),
            StatementParserSpec(
                keywords=SCRIPTING_KEYWORDS,
                parser_kind=StatementParserKind.FAMILY,
                default_family="scripting",
                family_policy=PolicyKind.BLOCK,
            ),
            StatementParserSpec(
                keywords=frozenset({"EXECUTE"}),
                parser_kind=StatementParserKind.EXECUTE,
                default_family="dynamic_sql",
                family_policy=PolicyKind.BLOCK,
            ),
            StatementParserSpec(
                keywords=frozenset({"START"}),
                parser_kind=StatementParserKind.START,
                default_family="transaction",
                family_policy=PolicyKind.BLOCK,
            ),
        )
    )


SNOWFLAKE_DIALECT = SnowflakeDialect(
    keywords=SNOWFLAKE_KEYWORDS,
    registry=_build_registry(),
    with_body_start_keywords=WITH_BODY_START_KEYWORDS,
    explain_output_formats=EXPLAIN_OUTPUT_FORMATS,
    begin_transaction_followers=BEGIN_TRANSACTION_FOLLOWERS,
    block_end_followers=BLOCK_END_FOLLOWERS,
    query_construct_by_keyword=QUERY_CONSTRUCT_BY_KEYWORD,
)
