"""Statement-level parser for Snowflake SQL."""

from collections.abc import Mapping

import attrs

from ..core import (
    Diagnostic,
    DiagnosticCode,
    ExplainNode,
    PipeChainNode,
    PolicyKind,
    QueryConstruct,
    QueryNode,
    SplitStatement,
    SQLAnalysisError,
    SqlScript,
    StatementFamily,
    StatementFamilyNode,
    StatementNode,
    TextSpan,
    UnknownStatementNode,
    WithNode,
)
from ..core.contracts import analysis_contract, internal_contract
from ..core.invariants import ParserInvariantError
from ..dialects import SNOWFLAKE_DIALECT
from ..dialects.base import Dialect
from ..lexing import Token, TokenStream
from ..policy import ReadOnlySafetyPolicy, SafetyDecision
from .registry import StatementParserKind, StatementParserSpec
from .splitter import build_split_statement, split_statements

_READ_ONLY_POLICY = ReadOnlySafetyPolicy()


@attrs.define(frozen=True, slots=True)
class ParserContext:
    """Context object passed to statement parser functions."""

    text: str
    span: TextSpan
    stream: TokenStream
    dialect: Dialect
    spec: StatementParserSpec

    def first_keyword(self) -> str | None:
        """Return the leading word token, if any."""

        return _statement_keyword(self.stream.peek())

    @internal_contract
    def require_family_policy(self) -> PolicyKind:
        """Return the policy for family-classified statements."""

        family_policy = self.spec.family_policy
        if family_policy is None:
            raise ParserInvariantError("Statement parser spec must define a family policy")
        return family_policy

    @internal_contract
    def parse_nested_from_token_index(self, index: int) -> StatementNode:
        """Parse a nested statement starting at the given token index."""

        absolute_start = self.stream.tokens[index].span.start
        local_start = absolute_start - self.span.start
        nested_statement = build_split_statement(
            self.text[local_start:],
            offset=absolute_start,
        )
        return parse_statement(nested_statement, dialect=self.dialect)


@analysis_contract
def parse_script(sql: str, *, dialect: Dialect | None = None) -> SqlScript:
    """Parse a SQL script into statement nodes."""

    active_dialect = dialect or SNOWFLAKE_DIALECT
    statements = split_statements(sql)
    if not statements:
        raise SQLAnalysisError.from_diagnostic(
            Diagnostic(
                code=DiagnosticCode.EMPTY_SQL,
                message="Empty SQL statement",
                span=None,
            )
        )

    parsed = tuple(parse_statement(statement, dialect=active_dialect) for statement in statements)
    diagnostics = tuple(diagnostic for statement in parsed for diagnostic in statement.diagnostics)
    return SqlScript(statements=parsed, diagnostics=diagnostics)


@analysis_contract
def parse_statement(
    statement: SplitStatement,
    *,
    dialect: Dialect | None = None,
) -> StatementNode:
    """Parse a split statement into a statement node."""

    active_dialect = dialect or SNOWFLAKE_DIALECT
    if len(statement.pipe_segment_pieces) > 1:
        segments = tuple(
            parse_statement(
                build_split_statement(piece.text, offset=piece.span.start),
                dialect=active_dialect,
            )
            for piece in statement.pipe_segment_pieces
        )
        return PipeChainNode(
            span=statement.span,
            text=statement.text,
            segments=segments,
        )

    stream = TokenStream.from_sql(
        statement.text,
        offset=statement.span.start,
        keywords=active_dialect.keywords,
    )
    if not stream.tokens:
        raise SQLAnalysisError.from_diagnostic(
            Diagnostic(
                code=DiagnosticCode.EMPTY_SQL,
                message="Empty SQL statement",
                span=statement.span,
            )
        )

    keyword = _statement_keyword(stream.peek())
    if keyword is None:
        return UnknownStatementNode(
            span=statement.span,
            text=statement.text,
            diagnostics=(
                Diagnostic(
                    code=DiagnosticCode.UNKNOWN_STATEMENT,
                    message="Could not determine the statement keyword",
                    span=statement.span,
                ),
            ),
        )

    spec = active_dialect.registry.lookup(keyword)
    if spec is None:
        return UnknownStatementNode(
            span=statement.span,
            text=statement.text,
            keyword=keyword,
            diagnostics=(
                Diagnostic(
                    code=DiagnosticCode.UNKNOWN_STATEMENT,
                    message="Statement type is not proven read-only",
                    span=statement.span,
                ),
            ),
        )

    context = ParserContext(
        text=statement.text,
        span=statement.span,
        stream=stream,
        dialect=active_dialect,
        spec=spec,
    )
    return _parse_by_kind(context)


@analysis_contract
def _parse_by_kind(context: ParserContext) -> StatementNode:
    match context.spec.parser_kind:
        case StatementParserKind.FAMILY:
            return parse_family(context)
        case StatementParserKind.METADATA:
            return parse_metadata(context)
        case StatementParserKind.QUERY:
            return parse_query(context)
        case StatementParserKind.WITH:
            return parse_with(context)
        case StatementParserKind.EXPLAIN:
            return parse_explain(context)
        case StatementParserKind.BEGIN:
            return parse_begin(context)
        case StatementParserKind.ALTER:
            return parse_alter(context)
        case StatementParserKind.EXECUTE:
            return parse_execute(context)
        case StatementParserKind.START:
            return parse_start(context)


def parse_family(context: ParserContext) -> StatementNode:
    """Parse a family-classified statement."""

    spec = context.spec
    keyword = context.first_keyword()
    if keyword is None:
        return UnknownStatementNode(
            span=context.span,
            text=context.text,
            diagnostics=(
                Diagnostic(
                    code=DiagnosticCode.UNKNOWN_STATEMENT,
                    message="Could not determine the statement keyword",
                    span=context.span,
                ),
            ),
        )

    return StatementFamilyNode(
        span=context.span,
        text=context.text,
        keyword=keyword,
        family=spec.default_family,
        policy_kind=context.require_family_policy(),
    )


def parse_metadata(context: ParserContext) -> StatementNode:
    """Parse a read-only metadata statement."""

    return parse_family(context)


def parse_query(context: ParserContext) -> StatementNode:
    """Parse a query statement and collect top-level constructs."""

    keyword = context.first_keyword() or "SELECT"
    constructs = _parse_query_constructs(
        context.stream.tokens,
        context.dialect.query_construct_by_keyword,
    )
    return QueryNode(
        span=context.span,
        text=context.text,
        keyword=keyword,
        constructs=frozenset(constructs),
    )


def parse_with(context: ParserContext) -> StatementNode:
    """Parse a WITH statement that delegates to its body."""

    cte_diagnostic = _find_blocked_with_cte_diagnostic(
        context.text,
        context.span,
        context.stream.tokens,
        dialect=context.dialect,
    )
    diagnostics = () if cte_diagnostic is None else (cte_diagnostic,)
    body_index = _find_with_body_candidate_index(context.stream.tokens)
    if body_index is None:
        diagnostics = diagnostics or (
            Diagnostic(
                code=DiagnosticCode.UNPARSABLE_WITH_BODY,
                message="WITH statement body could not be determined",
                span=context.span,
            ),
        )
        return WithNode(
            span=context.span,
            text=context.text,
            body=None,
            diagnostics=diagnostics,
        )

    body_token = context.stream.tokens[body_index]
    if body_token.kind != "word" or body_token.normalized not in context.dialect.with_body_start_keywords:
        raise SQLAnalysisError.from_diagnostic(
            Diagnostic(
                code=DiagnosticCode.UNPARSABLE_WITH_BODY,
                message="WITH statement body could not be determined",
                span=body_token.span,
            )
        )

    return WithNode(
        span=context.span,
        text=context.text,
        body=context.parse_nested_from_token_index(body_index),
        diagnostics=diagnostics,
    )


def parse_explain(context: ParserContext) -> StatementNode:
    """Parse an EXPLAIN statement."""

    subject_index = _find_explain_subject_index(
        context.stream.tokens,
        context.dialect.explain_output_formats,
    )
    if subject_index is None:
        return ExplainNode(
            span=context.span,
            text=context.text,
            subject=None,
            diagnostics=(
                Diagnostic(
                    code=DiagnosticCode.BLOCKED_STATEMENT,
                    message="EXPLAIN must wrap another statement",
                    span=context.span,
                ),
            ),
        )

    return ExplainNode(
        span=context.span,
        text=context.text,
        subject=context.parse_nested_from_token_index(subject_index),
    )


def parse_begin(context: ParserContext) -> StatementNode:
    """Parse BEGIN as transaction or scripting."""

    family: StatementFamily = "transaction"
    second_value = _next_word(context.stream.tokens, 0)
    if second_value is not None and second_value not in context.dialect.begin_transaction_followers:
        family = "scripting"

    return StatementFamilyNode(
        span=context.span,
        text=context.text,
        keyword="BEGIN",
        family=family,
        policy_kind=context.require_family_policy(),
    )


def parse_alter(context: ParserContext) -> StatementNode:
    """Parse ALTER with session special-casing."""

    family: StatementFamily = "session" if _next_word(context.stream.tokens, 0) == "SESSION" else "ddl"
    return StatementFamilyNode(
        span=context.span,
        text=context.text,
        keyword="ALTER",
        family=family,
        policy_kind=context.require_family_policy(),
    )


def parse_execute(context: ParserContext) -> StatementNode:
    """Parse EXECUTE statements with EXECUTE IMMEDIATE special-casing."""

    if _next_word(context.stream.tokens, 0) == "IMMEDIATE":
        return StatementFamilyNode(
            span=context.span,
            text=context.text,
            keyword="EXECUTE",
            family="dynamic_sql",
            policy_kind=context.require_family_policy(),
        )

    return UnknownStatementNode(
        span=context.span,
        text=context.text,
        keyword="EXECUTE",
        diagnostics=(
            Diagnostic(
                code=DiagnosticCode.UNKNOWN_STATEMENT,
                message="EXECUTE statements are not proven read-only",
                span=context.span,
            ),
        ),
    )


def parse_start(context: ParserContext) -> StatementNode:
    """Parse START statements with START TRANSACTION special-casing."""

    if _next_word(context.stream.tokens, 0) == "TRANSACTION":
        return StatementFamilyNode(
            span=context.span,
            text=context.text,
            keyword="START",
            family="transaction",
            policy_kind=context.require_family_policy(),
        )

    return UnknownStatementNode(
        span=context.span,
        text=context.text,
        keyword="START",
        diagnostics=(
            Diagnostic(
                code=DiagnosticCode.UNKNOWN_STATEMENT,
                message="START statements are not proven read-only",
                span=context.span,
            ),
        ),
    )


def _statement_keyword(token: Token) -> str | None:
    return token.normalized if token.kind == "word" else None


def _parse_query_constructs(
    tokens: tuple[Token, ...],
    constructs_by_keyword: dict[str, QueryConstruct] | Mapping[str, QueryConstruct],
) -> set[QueryConstruct]:
    constructs: set[QueryConstruct] = set()
    parenthesis_depth = 0
    seen_from = False

    for index, token in enumerate(tokens):
        if token.text == "(":
            parenthesis_depth += 1
            continue

        if token.text == ")" and parenthesis_depth > 0:
            parenthesis_depth -= 1
            continue

        if parenthesis_depth != 0 or token.kind != "word" or _is_path_element(tokens, index):
            continue

        upper = token.normalized
        if upper == "FROM":
            seen_from = True

        construct = constructs_by_keyword.get(upper)
        if construct is not None:
            constructs.add(construct)

        if upper == "INTO" and not seen_from:
            constructs.add("INTO")

        if upper == "FOR" and seen_from and _next_word(tokens, index) == "UPDATE":
            constructs.add("FOR_UPDATE")

    return constructs


def _find_with_body_candidate_index(tokens: tuple[Token, ...]) -> int | None:
    index = 1
    has_recursive_modifier = _is_with_recursive_modifier(tokens, index)
    if has_recursive_modifier:
        index += 1

    while index < len(tokens):
        binding_name = _with_binding_name(tokens, index)
        if binding_name is None:
            return None

        declaration_start = _skip_with_binding_target(tokens, index)
        if declaration_start is None or declaration_start >= len(tokens):
            return None

        if tokens[declaration_start].kind != "word" or tokens[declaration_start].normalized != "AS":
            raise SQLAnalysisError(
                "Unexpected input in WITH clause",
                code=DiagnosticCode.UNEXPECTED_INPUT,
                span=tokens[declaration_start].span,
            )

        index = declaration_start + 1
        if index >= len(tokens):
            return None

        if tokens[index].kind == "word" and tokens[index].normalized == "PROCEDURE":
            procedure_end = _find_with_procedure_tail_index(tokens, index + 1)
            if procedure_end is None:
                return None
            index = procedure_end
        else:
            if tokens[index].text != "(":
                raise SQLAnalysisError(
                    "Unexpected input in WITH clause",
                    code=DiagnosticCode.UNEXPECTED_INPUT,
                    span=tokens[index].span,
                )

            definition_end = _find_matching_right_parenthesis(tokens, index)
            if definition_end is None:
                return None
            if has_recursive_modifier:
                _validate_recursive_cte_definition(tokens, binding_name, index + 1, definition_end)
            index = definition_end + 1

        if index >= len(tokens):
            return None
        if tokens[index].text == ",":
            index += 1
            continue
        return index

    return None


def _is_with_recursive_modifier(tokens: tuple[Token, ...], index: int) -> bool:
    if index >= len(tokens):
        return False

    token = tokens[index]
    if token.kind != "word" or token.normalized != "RECURSIVE":
        return False

    next_index = _next_token_index(tokens, index)
    if next_index is None:
        return False

    next_token = tokens[next_index]
    if next_token.kind == "quoted_identifier":
        return True
    return next_token.kind == "word" and next_token.normalized != "AS"


def _skip_with_binding_target(tokens: tuple[Token, ...], index: int) -> int | None:
    if index >= len(tokens) or tokens[index].kind not in {"word", "quoted_identifier"}:
        return None

    next_index = index + 1
    if next_index < len(tokens) and tokens[next_index].text == "(":
        right_paren_index = _find_matching_right_parenthesis(tokens, next_index)
        if right_paren_index is None:
            return None
        next_index = right_paren_index + 1
    return next_index


def _find_with_procedure_tail_index(
    tokens: tuple[Token, ...],
    index: int,
) -> int | None:
    if index >= len(tokens):
        return None
    if tokens[index].text != "(":
        raise SQLAnalysisError(
            "Unexpected input in WITH clause",
            code=DiagnosticCode.UNEXPECTED_INPUT,
            span=tokens[index].span,
        )

    right_paren_index = _find_matching_right_parenthesis(tokens, index)
    if right_paren_index is None:
        return None

    returns_index = right_paren_index + 1
    if returns_index >= len(tokens):
        return None
    if tokens[returns_index].kind != "word" or tokens[returns_index].normalized != "RETURNS":
        raise SQLAnalysisError(
            "Unexpected input in WITH clause",
            code=DiagnosticCode.UNEXPECTED_INPUT,
            span=tokens[returns_index].span,
        )

    language_index = _find_required_top_level_keyword(
        tokens,
        returns_index + 1,
        required="LANGUAGE",
        stop_keywords=frozenset({"AS", "CALL", "SELECT"}),
    )
    if language_index is None:
        return None
    if language_index == returns_index + 1:
        raise SQLAnalysisError(
            "Unexpected input in WITH clause",
            code=DiagnosticCode.UNEXPECTED_INPUT,
            span=tokens[language_index].span,
        )

    language_value_index = language_index + 1
    if language_value_index >= len(tokens):
        return None
    if tokens[language_value_index].kind not in {"word", "quoted_identifier"}:
        raise SQLAnalysisError(
            "Unexpected input in WITH clause",
            code=DiagnosticCode.UNEXPECTED_INPUT,
            span=tokens[language_value_index].span,
        )

    as_index = _find_required_top_level_keyword(
        tokens,
        language_value_index + 1,
        required="AS",
        stop_keywords=frozenset({"CALL", "SELECT"}),
    )
    if as_index is None:
        return None

    body_index = as_index + 1
    if body_index >= len(tokens):
        return None
    if tokens[body_index].kind != "string":
        raise SQLAnalysisError(
            "Unexpected input in WITH clause",
            code=DiagnosticCode.UNEXPECTED_INPUT,
            span=tokens[body_index].span,
        )

    return body_index + 1


def _find_required_top_level_keyword(
    tokens: tuple[Token, ...],
    start_index: int,
    *,
    required: str,
    stop_keywords: frozenset[str],
) -> int | None:
    parenthesis_depth = 0
    for index, token in enumerate(tokens[start_index:], start=start_index):
        if token.text == "(":
            parenthesis_depth += 1
            continue
        if token.text == ")" and parenthesis_depth > 0:
            parenthesis_depth -= 1
            continue
        if parenthesis_depth != 0:
            continue
        if token.text == ",":
            raise SQLAnalysisError(
                "Unexpected input in WITH clause",
                code=DiagnosticCode.UNEXPECTED_INPUT,
                span=token.span,
            )
        if token.kind != "word":
            continue
        if token.normalized == required:
            return index
        if token.normalized in stop_keywords:
            raise SQLAnalysisError(
                "Unexpected input in WITH clause",
                code=DiagnosticCode.UNEXPECTED_INPUT,
                span=token.span,
            )
    return None


def _with_binding_name(tokens: tuple[Token, ...], index: int) -> str | None:
    if index >= len(tokens):
        return None

    token = tokens[index]
    if token.kind not in {"word", "quoted_identifier"}:
        return None
    return token.text if token.kind == "quoted_identifier" else token.normalized


def _validate_recursive_cte_definition(
    tokens: tuple[Token, ...],
    binding_name: str,
    definition_start_index: int,
    definition_end_index: int,
) -> None:
    if not _cte_definition_references_binding(tokens, binding_name, definition_start_index, definition_end_index):
        return
    if _cte_definition_contains_union_all(tokens, definition_start_index, definition_end_index):
        return

    raise SQLAnalysisError(
        "Unexpected input in WITH clause",
        code=DiagnosticCode.UNEXPECTED_INPUT,
        span=tokens[definition_start_index].span,
    )


def _cte_definition_references_binding(
    tokens: tuple[Token, ...],
    binding_name: str,
    definition_start_index: int,
    definition_end_index: int,
) -> bool:
    for token in tokens[definition_start_index:definition_end_index]:
        if token.kind == "quoted_identifier" and token.text == binding_name:
            return True
        if token.kind == "word" and token.normalized == binding_name:
            return True
    return False


def _cte_definition_contains_union_all(
    tokens: tuple[Token, ...],
    definition_start_index: int,
    definition_end_index: int,
) -> bool:
    parenthesis_depth = 0
    for index, token in enumerate(tokens[definition_start_index:definition_end_index], start=definition_start_index):
        if token.text == "(":
            parenthesis_depth += 1
            continue
        if token.text == ")" and parenthesis_depth > 0:
            parenthesis_depth -= 1
            continue
        if parenthesis_depth != 0:
            continue
        if token.kind == "word" and token.normalized == "UNION" and _next_word(tokens, index) == "ALL":
            return True
    return False


def _find_blocked_with_cte_diagnostic(
    text: str,
    span: TextSpan,
    tokens: tuple[Token, ...],
    *,
    dialect: Dialect,
) -> Diagnostic | None:
    for definition_start_index, definition_end_index in _iter_with_cte_definition_ranges(tokens):
        decision = _evaluate_cte_definition_read_only(
            text,
            span,
            tokens,
            definition_start_index,
            definition_end_index,
            dialect=dialect,
        )
        if decision.is_read_only:
            continue

        diagnostic = decision.diagnostic
        message = "Statement is not proven read-only" if diagnostic is None else diagnostic.message
        if not message.startswith("CTE definitions must be read-only: "):
            message = f"CTE definitions must be read-only: {message}"

        return Diagnostic(
            code=DiagnosticCode.BLOCKED_STATEMENT,
            message=message,
            span=tokens[definition_start_index].span if diagnostic is None else diagnostic.span,
        )
    return None


def _evaluate_cte_definition_read_only(
    text: str,
    span: TextSpan,
    tokens: tuple[Token, ...],
    definition_start_index: int,
    definition_end_index: int,
    *,
    dialect: Dialect,
) -> SafetyDecision:
    absolute_start = tokens[definition_start_index].span.start
    absolute_end = tokens[definition_end_index].span.start
    local_start = absolute_start - span.start
    local_end = absolute_end - span.start
    definition_statement = build_split_statement(text[local_start:local_end], offset=absolute_start)
    return _READ_ONLY_POLICY.evaluate(parse_statement(definition_statement, dialect=dialect))


def _iter_with_cte_definition_ranges(tokens: tuple[Token, ...]) -> tuple[tuple[int, int], ...]:
    ranges: list[tuple[int, int]] = []
    index = 1
    while index < len(tokens):
        token = tokens[index]
        if token.kind == "word" and token.normalized == "AS":
            left_paren_index = _next_token_index(tokens, index)
            if left_paren_index is None or tokens[left_paren_index].text != "(":
                index += 1
                continue

            definition_start = _next_token_index(tokens, left_paren_index)
            right_paren_index = _find_matching_right_parenthesis(tokens, left_paren_index)
            if right_paren_index is None:
                return tuple(ranges)

            if definition_start is not None and tokens[definition_start].text != ")":
                ranges.append((definition_start, right_paren_index))

            index = right_paren_index + 1
            continue

        index += 1

    return tuple(ranges)


def _find_explain_subject_index(
    tokens: tuple[Token, ...],
    explain_output_formats: frozenset[str],
) -> int | None:
    start_index = 1
    if len(tokens) > 1 and tokens[1].kind == "word" and tokens[1].normalized == "USING":
        format_index = _next_token_index(tokens, 1)
        if (
            format_index is None
            or tokens[format_index].kind != "word"
            or tokens[format_index].normalized not in explain_output_formats
        ):
            return None
        start_index = format_index + 1
    return _statement_body_start(tokens, start_index)


def _statement_body_start(tokens: tuple[Token, ...], start: int) -> int | None:
    for index, token in enumerate(tokens[start:], start=start):
        if token.kind in {"word", "quoted_identifier"}:
            return index
    return None


def _is_path_element(tokens: tuple[Token, ...], index: int) -> bool:
    if index == 0:
        return False
    return tokens[index - 1].text in {":", "."}


def _next_token_index(tokens: tuple[Token, ...], index: int) -> int | None:
    next_index = index + 1
    return next_index if next_index < len(tokens) else None


def _find_matching_right_parenthesis(tokens: tuple[Token, ...], left_paren_index: int) -> int | None:
    depth = 0
    for index, token in enumerate(tokens[left_paren_index:], start=left_paren_index):
        if token.text == "(":
            depth += 1
            continue
        if token.text == ")":
            depth -= 1
            if depth == 0:
                return index
    return None


def _next_word(tokens: tuple[Token, ...], index: int) -> str | None:
    next_index = index + 1
    while next_index < len(tokens):
        token = tokens[next_index]
        if token.kind == "word":
            return token.normalized
        if token.kind == "quoted_identifier":
            next_index += 1
            continue
        if token.text in {",", "(", ")"}:
            next_index += 1
            continue
        return token.text
    return None
