# Improve Type Safety Phase 1: Identifier Quoting & Safe SQL Construction

## Date
2025-08-16

## User Request
- Adopt TDD (red -> implement -> green) per function/class.
- Start with Phase 1 of previously proposed plan.
- After creating dev note, wait for user confirmation before implementing.

## Goal (Phase 1)
Introduce a safe SQL identifier quoting utility and apply it to existing adapters building fully-qualified Snowflake object names to reduce risk of injection and reserved word conflicts.

## Scope
Files to be added/modified in Phase 1:
- ADD: `src/mcp_snowflake/kernel/sql_utils.py` (new quoting function `quote_ident` + helper `is_simple_identifier`)
- UPDATE: `adapter/describe_table_handler.py`
- UPDATE: `adapter/list_schemas_handler.py`
- UPDATE: `adapter/list_tables_handler.py`
- UPDATE: `adapter/list_views_handler.py`

(Other adapters/tools will be reviewed but only updated if they perform direct identifier concatenation.)

## Adapter Coverage Review
Full scan of `src/mcp_snowflake/adapter` performed to confirm identifier construction patterns:

| Adapter File | Builds Identifiers? | Current Quoting Strategy | Action in Phase 1 | Notes |
|--------------|---------------------|---------------------------|-------------------|-------|
| `describe_table_handler.py` | Yes (DATABASE.SCHEMA.TABLE) | Raw f-string no quoting | Apply `fully_qualified` | Core target |
| `list_schemas_handler.py` | Yes (DATABASE) | Raw f-string | Apply `quote_ident` (single part) | SHOW SCHEMAS IN DATABASE |
| `list_tables_handler.py` | Yes (DATABASE.SCHEMA) | Raw f-string | Apply `fully_qualified` (2 parts) | SHOW TABLES IN SCHEMA |
| `list_views_handler.py` | Yes (DATABASE.SCHEMA) | Raw f-string | Apply `fully_qualified` (2 parts) | SHOW VIEWS IN SCHEMA |
| `sample_table_data_handler.py` | Yes (DATABASE.SCHEMA.TABLE + columns) | Manual `"{...}"` wrapping & column loop quoting | Optional (defer) | Already quotes; can unify later to reduce duplication |
| `analyze_table_statistics_handler.py` | Yes (DATABASE.SCHEMA.TABLE + columns in generated SQL) | Manual `"{...}"` wrapping inside `generate_statistics_sql` | Optional (defer) | More complex generation; unify in later phase (needs column quoting helper) |
| `execute_query_handler.py` | No (uses raw user SQL) | N/A | None | Out of scope |

Decision: Phase 1 keeps scope minimal (only raw unsafe concatenations). `sample_table_data_handler.py` and `analyze_table_statistics_handler.py` remain unchanged to avoid widening diff; they will be candidates for Phase 1.5 or Phase 2 refactor (introduce shared `quote_column_list(columns: list[str])`).

Out-of-scope note added so reviewers know omission is intentional.

## Requirements
- Provide `quote_ident(name: str) -> str` that:
  - Leaves identifiers of pattern `[A-Z_][A-Z0-9_]*` (and no double quotes) unchanged (UPPERCASE safety assumption)
  - Otherwise returns a double-quoted identifier with internal `"` escaped as `""`.
  - Empty string should raise `ValueError`.
  - Leading/trailing whitespace should be trimmed before validation; whitespace inside triggers quoting.
- Provide `fully_qualified(database: str, schema: str | None, name: str) -> str`:
  - Applies `quote_ident` to each non-empty part
  - If `schema` is None, returns `db.name`; else `db.schema.name`.
- Refactor adapters to use these helpers when composing SQL.
- Preserve external behavior (tests relying on output content should still pass; only internal query strings change).
- Add unit tests (new file `tests/kernel/test_sql_utils.py`):
  - `test_quote_ident_simple_pass_through`
  - `test_quote_ident_needs_quoting`
  - `test_quote_ident_escapes_internal_quotes`
  - `test_quote_ident_invalid_empty`
  - `test_fully_qualified_with_schema`
  - `test_fully_qualified_without_schema`

## TDD Plan
1. Write failing tests in `tests/kernel/test_sql_utils.py` (RED)
2. Implement `sql_utils.py` with functions (GREEN)
3. Refactor adapters to use the helpers; run full test suite (GREEN)
4. Ensure no behavior changes in handler outputs.

## Edge Cases Considered
- Mixed case identifiers (e.g., `MyTable`) must be quoted to preserve case.
- Identifiers containing spaces or hyphens require quoting.
- Double quotes inside name: `A"B` => `"A""B"` after quoting.
- Empty or whitespace-only name -> ValueError.

## Non-Goals (Phase 1)
- No changes to handler argument names.
- No introduction of aliasing or Enum refactors.
- No MIME/type changes in tool responses.

## Risks & Mitigations
- Risk: Tests asserting raw query strings might fail. Mitigation: Search not performed yet; if failures arise, adjust tests to be agnostic or update expectations.
- Risk: Accidental double quoting. Mitigation: Only quote when pattern fails or contains `"`.

## Next Step
Await user approval to proceed with TDD implementation steps.

## Implementation Log

### TDD Cycle 1: RED → GREEN → REFACTOR
**2025-08-16 実装完了**

#### RED Phase
- Created `tests/kernel/test_sql_utils.py` with 8 test cases covering:
  - Simple identifier pass-through
  - Identifiers needing quoting (mixed case, special chars)
  - Internal quote escaping
  - Empty identifier validation
  - Fully qualified name generation (2-part & 3-part)
- Initial test run failed as expected (module not found)

#### GREEN Phase
- Implemented `src/mcp_snowflake/kernel/sql_utils.py` with:
  - `quote_ident(name: str) -> str`: Quotes identifiers when needed
  - `fully_qualified(database: str, schema: str | None, name: str) -> str`: Builds safe qualified names
  - Regex pattern `[A-Z_][A-Z0-9_]*` for simple identifiers
  - Double quote escaping (`"` → `""`)
- All 8 tests passed

#### REFACTOR Phase
- Updated 4 adapter files to use new utilities:
  - `describe_table_handler.py`: `f"DESCRIBE TABLE {fully_qualified(database, schema, table_name)}"`
  - `list_schemas_handler.py`: `f"SHOW SCHEMAS IN DATABASE {quote_ident(database)}"`
  - `list_tables_handler.py`: `f"SHOW TABLES IN SCHEMA {quote_ident(database)}.{quote_ident(schema)}"`
  - `list_views_handler.py`: `f"SHOW VIEWS IN SCHEMA {quote_ident(database)}.{quote_ident(schema)}"`
- Full test suite: **304 tests passed** - no regressions

### Security Improvements Achieved
- SQL injection prevention through proper identifier quoting
- Reserved word conflict resolution
- Mixed case identifier preservation
- Special character handling in object names

### Deferred Items (Future Phases)
- `sample_table_data_handler.py` and `analyze_table_statistics_handler.py` already have manual quoting - will unify in Phase 1.5
- Column name quoting helper for list operations

Phase 1 **COMPLETE** ✅
