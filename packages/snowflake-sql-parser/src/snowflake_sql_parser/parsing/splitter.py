"""Top-level statement splitting for SQL text."""

import attrs

from ..core import DiagnosticCode, SplitStatement, SQLAnalysisError, TextPiece, TextSpan
from ..core.contracts import analysis_contract
from ..dialects import SNOWFLAKE_DIALECT
from ..lexing import Token, TokenType, tokenize

_SCRIPTING_HEADER_KEYWORDS = frozenset({"IF", "FOR", "WHILE", "CASE"})
_SCRIPTING_BODY_KEYWORDS = frozenset({"BEGIN", "LOOP", "REPEAT"})


@attrs.define(slots=True)
class _ScriptingFrame:
    kind: str
    phase: str
    end_follower: str | None = None


@attrs.define(slots=True)
class _ScriptingState:
    root_index: int
    frames: list[_ScriptingFrame]
    at_statement_start: bool


@analysis_contract
def build_split_statement(sql: str, *, offset: int = 0) -> SplitStatement:
    """Build a split statement object from already isolated SQL text."""

    piece = _build_text_piece(sql, 0, len(sql), offset)
    if piece is None:
        raise SQLAnalysisError("Empty SQL statement", code=DiagnosticCode.EMPTY_SQL)
    if not tokenize(piece.text):
        raise SQLAnalysisError("Empty SQL statement", code=DiagnosticCode.EMPTY_SQL)
    _ = split_statements(piece.text)

    pipe_segment_pieces = split_pipe_segments(piece.text, offset=piece.span.start)
    return SplitStatement(
        text=piece.text,
        span=piece.span,
        pipe_segment_pieces=pipe_segment_pieces,
    )


@analysis_contract
def split_statements(sql: str) -> tuple[SplitStatement, ...]:
    """Split SQL into top-level statements."""

    tokens = tokenize(sql)
    statement_start = _next_statement_start(tokens, 0)
    if statement_start is None:
        return ()
    _reject_invalid_begin_semicolon_block(tokens)

    statements: list[SplitStatement] = []
    parenthesis_depth = 0
    scripting_state = _initial_scripting_state(tokens, statement_start)

    for index, token in enumerate(tokens[statement_start:], start=statement_start):
        if token.text == "(":
            parenthesis_depth += 1
            continue

        if token.text == ")" and parenthesis_depth > 0:
            parenthesis_depth -= 1
            continue

        if parenthesis_depth == 0:
            if scripting_state is not None and index != scripting_state.root_index and token.kind == "word":
                _advance_scripting_state(scripting_state, tokens, index)
            elif (
                scripting_state is not None
                and index != scripting_state.root_index
                and scripting_state.at_statement_start
                and token.text != ";"
            ):
                scripting_state.at_statement_start = False

            if token.text == ";" and (scripting_state is None or not scripting_state.frames):
                statements.extend(
                    _materialize_statement(
                        sql,
                        tokens[statement_start].span.start,
                        token.span.start,
                    )
                )
                statement_start = _next_statement_start(tokens, index + 1)
                if statement_start is None:
                    return tuple(statements)

                parenthesis_depth = 0
                scripting_state = _initial_scripting_state(tokens, statement_start)
            elif token.text == ";" and scripting_state is not None:
                scripting_state.at_statement_start = True

    statements.extend(_materialize_statement(sql, tokens[statement_start].span.start, len(sql)))
    return tuple(statements)


@analysis_contract
def split_pipe_segments(sql: str, *, offset: int = 0) -> tuple[TextPiece, ...]:
    """Split a single top-level statement into pipe-chain segments."""

    tokens = tokenize(sql)
    if not tokens:
        return ()

    segments: list[TextPiece] = []
    segment_start = 0
    parenthesis_depth = 0

    for index, token in enumerate(tokens):
        if token.text == "(":
            parenthesis_depth += 1
            continue

        if token.text == ")" and parenthesis_depth > 0:
            parenthesis_depth -= 1
            continue

        if token.text == "->>" and parenthesis_depth == 0:
            piece = _build_text_piece(
                sql,
                tokens[segment_start].span.start,
                token.span.start,
                offset,
            )
            if piece is None:
                raise SQLAnalysisError(
                    "Pipe chains must not contain empty segments",
                    code=DiagnosticCode.INVALID_PIPE_CHAIN,
                )
            segments.append(piece)
            segment_start = _next_statement_start(tokens, index + 1)
            if segment_start is None:
                raise SQLAnalysisError(
                    "Pipe chains must end with a SQL segment",
                    code=DiagnosticCode.INVALID_PIPE_CHAIN,
                )

    piece = _build_text_piece(
        sql,
        tokens[segment_start].span.start,
        len(sql),
        offset,
    )
    if piece is None:
        raise SQLAnalysisError(
            "Pipe chains must not contain empty segments",
            code=DiagnosticCode.INVALID_PIPE_CHAIN,
        )
    segments.append(piece)

    return tuple(segments)


def _materialize_statement(sql: str, start: int, end: int) -> tuple[SplitStatement, ...]:
    piece = _build_text_piece(sql, start, end, 0)
    if piece is None:
        return ()

    _validate_scripting_statement_tokens(tokenize(piece.text, offset=piece.span.start))
    return (
        SplitStatement(
            text=piece.text,
            span=piece.span,
            pipe_segment_pieces=split_pipe_segments(piece.text, offset=piece.span.start),
        ),
    )


def _build_text_piece(
    sql: str,
    start: int,
    end: int,
    offset: int,
) -> TextPiece | None:
    trimmed_start = start
    trimmed_end = end

    while trimmed_start < trimmed_end and sql[trimmed_start].isspace():
        trimmed_start += 1

    while trimmed_end > trimmed_start and sql[trimmed_end - 1].isspace():
        trimmed_end -= 1

    if trimmed_start >= trimmed_end:
        return None

    return TextPiece(
        text=sql[trimmed_start:trimmed_end],
        span=TextSpan(offset + trimmed_start, offset + trimmed_end),
    )


def _next_statement_start(tokens: tuple[Token, ...], start: int) -> int | None:
    index = start
    while index < len(tokens) and tokens[index].text == ";":
        index += 1
    return index if index < len(tokens) else None


def _reject_invalid_begin_semicolon_block(tokens: tuple[Token, ...]) -> None:
    if len(tokens) < 3:
        return

    first_token = tokens[0]
    if first_token.kind != "word" or first_token.upper_text != "BEGIN":
        return
    if tokens[1].text != ";":
        return

    last_token = tokens[-1]
    second_to_last = tokens[-2] if len(tokens) > 1 else None
    if (last_token.kind == "word" and last_token.upper_text == "END") or (
        second_to_last is not None
        and second_to_last.kind == "word"
        and second_to_last.upper_text == "END"
        and last_token.type in {TokenType.IDENTIFIER, TokenType.QUOTED_IDENTIFIER}
    ):
        _raise_invalid_scripting(tokens[1])


def _validate_scripting_statement_tokens(tokens: tuple[Token, ...]) -> None:
    if not tokens:
        return

    first_token = tokens[0]
    if first_token.kind != "word":
        return

    match first_token.upper_text:
        case "BEGIN":
            if _is_begin_block(tokens, 0):
                next_token = _next_token(tokens, 0)
                if next_token is not None and next_token.text == ";":
                    _raise_invalid_scripting(next_token)
        case "IF":
            _validate_if_statement(tokens)
        case "WHILE":
            _validate_while_statement(tokens)
        case "FOR":
            _validate_for_statement(tokens)
        case "REPEAT":
            _validate_repeat_statement(tokens)
        case "CASE":
            _validate_case_statement(tokens)
        case _:
            return


def _validate_if_statement(tokens: tuple[Token, ...]) -> None:
    assert tokens
    next_index = _require_parenthesized_condition(tokens, 1)
    _expect_word(tokens, next_index, {"THEN"})


def _validate_while_statement(tokens: tuple[Token, ...]) -> None:
    assert tokens
    next_index = _require_parenthesized_condition(tokens, 1)
    _expect_word(tokens, next_index, {"DO", "LOOP"})


def _validate_for_statement(tokens: tuple[Token, ...]) -> None:
    first_token = tokens[0] if tokens else None
    if first_token is None:
        return
    if len(tokens) < 4:
        _raise_invalid_scripting(first_token)
    assert len(tokens) >= 4

    iterator = tokens[1]
    if iterator.type not in {TokenType.IDENTIFIER, TokenType.QUOTED_IDENTIFIER}:
        _raise_invalid_scripting(iterator)

    _expect_word(tokens, 2, {"IN"})

    source_start = 3
    has_reverse = False
    if (
        source_start < len(tokens)
        and tokens[source_start].kind == "word"
        and tokens[source_start].upper_text == "REVERSE"
    ):
        has_reverse = True
        source_start += 1

    delimiter_index = _find_top_level_header_delimiter(tokens, source_start, {"DO", "LOOP"})
    if delimiter_index is None or delimiter_index == source_start:
        _raise_invalid_scripting(tokens[source_start - 1] if source_start <= len(tokens) - 1 else tokens[-1])
    assert delimiter_index is not None

    source_tokens = tokens[source_start:delimiter_index]
    is_range_loop = any(token.kind == "word" and token.upper_text == "TO" for token in source_tokens)
    delimiter = tokens[delimiter_index].upper_text

    if not is_range_loop and (has_reverse or delimiter != "DO"):
        _raise_invalid_scripting(tokens[delimiter_index])


def _validate_repeat_statement(tokens: tuple[Token, ...]) -> None:
    assert tokens
    until_index = _find_last_word(tokens, "UNTIL")
    if until_index is None:
        _raise_invalid_scripting(tokens[0])
    assert until_index is not None

    condition_index = until_index + 1
    next_index = _require_parenthesized_condition(tokens, condition_index)
    _expect_word(tokens, next_index, {"END"})
    _expect_word(tokens, next_index + 1, {"REPEAT"})


def _validate_case_statement(tokens: tuple[Token, ...]) -> None:
    assert tokens
    when_index = _find_first_word(tokens, "WHEN", start=1)
    then_index = _find_first_word(tokens, "THEN", start=1)
    if when_index is None or then_index is None or then_index < when_index:
        _raise_invalid_scripting(tokens[0])


def _require_parenthesized_condition(tokens: tuple[Token, ...], start_index: int) -> int:
    assert tokens
    if start_index >= len(tokens):
        _raise_invalid_scripting(tokens[-1])
    assert start_index < len(tokens)
    if tokens[start_index].text != "(":
        _raise_invalid_scripting(tokens[start_index])

    right_paren_index = _find_matching_right_parenthesis(tokens, start_index)
    if right_paren_index is None or right_paren_index == start_index + 1:
        _raise_invalid_scripting(tokens[start_index])
    assert right_paren_index is not None
    return right_paren_index + 1


def _expect_word(tokens: tuple[Token, ...], index: int, expected: frozenset[str] | set[str]) -> None:
    assert tokens
    if index >= len(tokens):
        _raise_invalid_scripting(tokens[-1])
    assert index < len(tokens)
    token = tokens[index]
    if token.kind != "word" or token.upper_text not in expected:
        _raise_invalid_scripting(token)


def _find_top_level_header_delimiter(
    tokens: tuple[Token, ...],
    start_index: int,
    delimiters: frozenset[str] | set[str],
) -> int | None:
    parenthesis_depth = 0
    for index, token in enumerate(tokens[start_index:], start=start_index):
        if token.text == "(":
            parenthesis_depth += 1
            continue
        if token.text == ")" and parenthesis_depth > 0:
            parenthesis_depth -= 1
            continue
        if parenthesis_depth == 0 and token.kind == "word" and token.upper_text in delimiters:
            return index
    return None


def _find_first_word(tokens: tuple[Token, ...], target: str, *, start: int = 0) -> int | None:
    for index, token in enumerate(tokens[start:], start=start):
        if token.kind == "word" and token.upper_text == target:
            return index
    return None


def _find_last_word(tokens: tuple[Token, ...], target: str) -> int | None:
    for index in range(len(tokens) - 1, -1, -1):
        token = tokens[index]
        if token.kind == "word" and token.upper_text == target:
            return index
    return None


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


def _raise_invalid_scripting(token: Token) -> None:
    raise SQLAnalysisError(
        "Unexpected input in Snowflake Scripting block",
        code=DiagnosticCode.UNEXPECTED_INPUT,
        span=token.span,
    )


def _initial_scripting_state(
    tokens: tuple[Token, ...],
    statement_start: int,
) -> _ScriptingState | None:
    first_token = tokens[statement_start]
    if first_token.kind != "word":
        return None

    if first_token.upper_text == "DECLARE":
        return _ScriptingState(
            root_index=statement_start,
            frames=[_make_scripting_frame("DECLARE")],
            at_statement_start=False,
        )

    if first_token.upper_text == "BEGIN" and _is_begin_block(tokens, statement_start):
        return _ScriptingState(
            root_index=statement_start,
            frames=[_make_scripting_frame("BEGIN")],
            at_statement_start=True,
        )

    if first_token.upper_text in _SCRIPTING_HEADER_KEYWORDS:
        return _ScriptingState(
            root_index=statement_start,
            frames=[_make_scripting_frame(first_token.upper_text)],
            at_statement_start=False,
        )

    if first_token.upper_text in {"LOOP", "REPEAT"}:
        return _ScriptingState(
            root_index=statement_start,
            frames=[_make_scripting_frame(first_token.upper_text)],
            at_statement_start=True,
        )

    return None


def _make_scripting_frame(keyword: str) -> _ScriptingFrame:
    phase = "header" if keyword in _SCRIPTING_HEADER_KEYWORDS else "body"
    end_follower = None
    if keyword == "DECLARE":
        phase = "declarations"
    if keyword in {"LOOP", "REPEAT"}:
        end_follower = keyword
    return _ScriptingFrame(kind=keyword, phase=phase, end_follower=end_follower)


def _advance_scripting_state(
    state: _ScriptingState,
    tokens: tuple[Token, ...],
    index: int,
) -> None:
    if not state.frames:
        state.at_statement_start = False
        return

    token = tokens[index]
    upper = token.upper_text
    top = state.frames[-1]

    if state.at_statement_start:
        if _advance_scripting_statement_start(state, tokens, index, upper, top):
            return
        state.at_statement_start = False
        return

    if top.phase == "header" and _advance_scripting_header(state, tokens, index, upper, top):
        return

    state.at_statement_start = False


def _advance_scripting_statement_start(
    state: _ScriptingState,
    tokens: tuple[Token, ...],
    index: int,
    upper: str,
    top: _ScriptingFrame,
) -> bool:
    if upper == "DECLARE":
        state.frames.append(_make_scripting_frame("DECLARE"))
        state.at_statement_start = False
        return True

    if upper == "BEGIN" and _is_begin_block(tokens, index):
        if top.kind == "DECLARE" and top.phase == "declarations":
            top.phase = "body"
        state.frames.append(_make_scripting_frame("BEGIN"))
        state.at_statement_start = True
        return True

    if upper in {"IF", "FOR", "WHILE", "LOOP", "REPEAT", "CASE"}:
        state.frames.append(_make_scripting_frame(upper))
        state.at_statement_start = upper in _SCRIPTING_BODY_KEYWORDS
        return True

    if upper == "ELSEIF" and top.kind == "IF":
        top.phase = "header"
        state.at_statement_start = False
        return True

    if upper == "ELSE" and top.kind in {"IF", "CASE"}:
        top.phase = "body"
        state.at_statement_start = True
        return True

    if upper == "WHEN" and top.kind == "CASE":
        top.phase = "header"
        state.at_statement_start = False
        return True

    if upper == "UNTIL" and top.kind == "REPEAT":
        top.phase = "header"
        state.at_statement_start = False
        return True

    if upper == "END":
        return _handle_scripting_end(state, tokens, index)

    return False


def _advance_scripting_header(
    state: _ScriptingState,
    tokens: tuple[Token, ...],
    index: int,
    upper: str,
    top: _ScriptingFrame,
) -> bool:
    if upper == "END":
        return _handle_scripting_end(state, tokens, index)

    if top.kind in {"IF", "CASE"} and upper == "THEN":
        top.phase = "body"
        state.at_statement_start = True
        return True

    if top.kind in {"FOR", "WHILE"} and upper in {"DO", "LOOP"}:
        top.phase = "body"
        top.end_follower = top.kind if upper == "DO" else "LOOP"
        state.at_statement_start = True
        return True

    return False


def _handle_scripting_end(
    state: _ScriptingState,
    tokens: tuple[Token, ...],
    index: int,
) -> bool:
    if _try_close_scripting_frame(state, tokens, index):
        state.at_statement_start = False
        return True

    raise SQLAnalysisError(
        "Unexpected input in Snowflake Scripting block",
        code=DiagnosticCode.UNEXPECTED_INPUT,
        span=tokens[index].span,
    )


def _try_close_scripting_frame(
    state: _ScriptingState,
    tokens: tuple[Token, ...],
    index: int,
) -> bool:
    if not state.frames:
        return False

    top = state.frames[-1]
    next_token = _next_token(tokens, index)
    next_value = None
    if next_token is not None:
        next_value = next_token.upper_text if next_token.kind == "word" else next_token.text

    if top.kind == "BEGIN" and _is_begin_block_terminator(tokens, index):
        _ = state.frames.pop()
        if state.frames and state.frames[-1].kind == "DECLARE" and state.frames[-1].phase == "body":
            _ = state.frames.pop()
        return True

    if top.kind == "IF" and next_value == "IF":
        _ = state.frames.pop()
        return True

    if top.kind == "FOR" and next_value == top.end_follower:
        _ = state.frames.pop()
        return True

    if top.kind == "WHILE" and next_value == top.end_follower:
        _ = state.frames.pop()
        return True

    if top.kind == "LOOP" and next_value == top.end_follower:
        _ = state.frames.pop()
        return True

    if top.kind == "REPEAT" and next_value == top.end_follower:
        _ = state.frames.pop()
        return True

    if top.kind == "CASE" and next_value in {None, ";", "CASE"}:
        _ = state.frames.pop()
        return True

    return False


def _is_begin_block(tokens: tuple[Token, ...], index: int) -> bool:
    next_token = _next_token(tokens, index)
    if next_token is None:
        return False

    next_value = next_token.upper_text if next_token.kind == "word" else next_token.text
    return next_value not in SNOWFLAKE_DIALECT.begin_transaction_followers


def _is_begin_block_terminator(tokens: tuple[Token, ...], index: int) -> bool:
    next_token = _next_token(tokens, index)
    if next_token is None or next_token.text == ";":
        return True
    if next_token.type not in {TokenType.IDENTIFIER, TokenType.QUOTED_IDENTIFIER}:
        return False

    label_terminator = _next_token(tokens, index + 1)
    return label_terminator is None or label_terminator.text == ";"


def _next_token(tokens: tuple[Token, ...], index: int) -> Token | None:
    next_index = index + 1
    return tokens[next_index] if next_index < len(tokens) else None
