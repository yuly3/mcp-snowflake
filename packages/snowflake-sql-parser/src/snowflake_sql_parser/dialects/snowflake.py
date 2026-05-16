"""Snowflake dialect registry and keyword definitions."""

from collections.abc import Mapping

import attrs

from ..core import PolicyKind, QueryConstruct
from ..parsing.registry import (
    AlterParserSpec,
    BeginParserSpec,
    ExecuteParserSpec,
    ExplainParserSpec,
    FamilyParserSpec,
    QueryParserSpec,
    StartParserSpec,
    StatementRegistry,
    WithParserSpec,
)

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
            QueryParserSpec(keywords=frozenset({"SELECT"})),
            FamilyParserSpec(
                keywords=frozenset({"SHOW", "DESCRIBE", "DESC", "LIST", "LS"}),
                default_family="metadata",
                family_policy=PolicyKind.ALLOW,
            ),
            ExplainParserSpec(keywords=frozenset({"EXPLAIN"})),
            WithParserSpec(keywords=frozenset({"WITH"})),
            FamilyParserSpec(
                keywords=DML_KEYWORDS,
                default_family="dml",
                family_policy=PolicyKind.BLOCK,
            ),
            FamilyParserSpec(
                keywords=frozenset({"CREATE", "DROP", "UNDROP", "RENAME"}),
                default_family="ddl",
                family_policy=PolicyKind.BLOCK,
            ),
            AlterParserSpec(
                keywords=frozenset({"ALTER"}),
                family_policy=PolicyKind.BLOCK,
            ),
            FamilyParserSpec(
                keywords=frozenset({"COPY"}),
                default_family="copy",
                family_policy=PolicyKind.BLOCK,
            ),
            FamilyParserSpec(
                keywords=FILE_TRANSFER_KEYWORDS,
                default_family="file_transfer",
                family_policy=PolicyKind.BLOCK,
            ),
            FamilyParserSpec(
                keywords=ACCESS_CONTROL_KEYWORDS,
                default_family="access_control",
                family_policy=PolicyKind.BLOCK,
            ),
            FamilyParserSpec(
                keywords=SESSION_KEYWORDS,
                default_family="session",
                family_policy=PolicyKind.BLOCK,
            ),
            FamilyParserSpec(
                keywords=TRANSACTION_KEYWORDS,
                default_family="transaction",
                family_policy=PolicyKind.BLOCK,
            ),
            BeginParserSpec(
                keywords=frozenset({"BEGIN"}),
                family_policy=PolicyKind.BLOCK,
            ),
            FamilyParserSpec(
                keywords=SCRIPTING_KEYWORDS,
                default_family="scripting",
                family_policy=PolicyKind.BLOCK,
            ),
            ExecuteParserSpec(
                keywords=frozenset({"EXECUTE"}),
                family_policy=PolicyKind.BLOCK,
            ),
            StartParserSpec(
                keywords=frozenset({"START"}),
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
