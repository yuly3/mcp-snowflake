# Refactor: Remove adapter dependency on ColumnInfo and consolidate into TableColumn

Date: 2025-08-11
Status: ✅ Complete

## User request
- Goal: Eliminate adapter layer's dependency on ColumnInfo. Consolidate ColumnInfo functionality held by the handler layer into the kernel layer's TableColumn. Adapter should depend on TableColumn.
- Additional note: ColumnInfo.column_type is used only in tests and does not need to be migrated to TableColumn.

## Objectives
- Adapter uses kernel TableColumn exclusively (no imports from handler._types).
- Migrate ColumnInfo functionality into TableColumn where appropriate (snowflake_type, statistics_type). Do NOT add column_type property to TableColumn.
- Keep ColumnInfo class for backward compatibility in handler (Phase 1). Full removal is a future Phase 2 item.

## Non-goals
- Remove ColumnInfo and its tests entirely (deferred to Phase 2).
- Change public JSON response schemas or SQL generation semantics.

## Scope (Phase 1)
- Kernel: Extend TableColumn with computed properties that encapsulate ColumnInfo capabilities except column_type.
  - snowflake_type: SnowflakeDataType
  - statistics_type: StatisticsSupportDataType
- Adapter: Replace ColumnInfo usage with TableColumn in analyze_table_statistics_handler.
- Handler: Switch internal analysis flow to operate on TableColumn directly and only use ColumnInfo for legacy tests/compat where needed.
- Parser/Response: Accept TableColumn sequences as inputs; computed properties provide type info for branching.

## Design details

### Kernel changes
- File: src/mcp_snowflake/kernel/table_metadata.py
- Add properties (read-only, computed):
  - snowflake_type -> SnowflakeDataType(self.data_type)
  - statistics_type -> StatisticsSupportDataType.from_snowflake_type(self.snowflake_type)
- Do NOT add ColumnInfo.column_type equivalent to TableColumn (explicitly out of scope per user note).

### Adapter changes
- File: src/mcp_snowflake/adapter/analyze_table_statistics_handler.py
- Signature updates:
  - analyze_table_statistics(..., columns_to_analyze: Iterable[TableColumn], ...)
  - generate_statistics_sql(..., columns_info: Iterable[TableColumn], ...)
- Implementation:
  - Use col.name and col.statistics_type.type_name for SQL generation branches (numeric/string/date/boolean).
  - Note: Tests may still pass ColumnInfo; function relies only on shared attributes (name, statistics_type), so duck typing keeps compatibility.

### Handler changes
- File: src/mcp_snowflake/handler/analyze_table_statistics/_column_analysis.py
  - Replace create_column_info_list with ensure_supported_columns returning list[TableColumn].
  - validate_and_select_columns returns list[TableColumn] | types.TextContent.
  - Validation accesses col.snowflake_type and col.statistics_type to ensure support; collect unsupported into a single error message identical to current wording.
- File: src/mcp_snowflake/handler/analyze_table_statistics/_result_parser.py
  - Change columns_info parameter to Iterable[TableColumn].
  - Use col.data_type for data_type field when TableColumn; fall back to col.snowflake_type.raw_type when ColumnInfo (backward compatibility).
  - Add exhaustive case `_` for match statement.
- File: src/mcp_snowflake/handler/analyze_table_statistics/_response_builder.py
  - Accept Sequence[TableColumn] and compute counts from parsed results as today.
  - Update docstring to reflect TableColumn.
- File: src/mcp_snowflake/handler/analyze_table_statistics/models.py
  - Optionally specialize EffectAnalyzeTableStatistics columns_to_analyze to Iterable[TableColumn] (or keep Any to avoid ripple; prefer non-breaking change initially).

### Compatibility and behavior
- Error messages for unsupported types remain unchanged.
- SQL casing, quoting, and Snowflake uppercase result keys remain identical.
- cattrs behavior unaffected (new properties are computed, not stored fields).
- Backward compatibility: _result_parser supports both TableColumn and ColumnInfo inputs; SQL generator relies on shared attributes only.

## Test plan
- Existing handler integration tests should pass unchanged (behavior preserved).
- Add/extend kernel tests (tests/kernel/test_table_metadata.py):
  - Verify TableColumn.snowflake_type.normalized_type mapping for representative types.
  - Verify TableColumn.statistics_type.type_name classification for numeric/string/date/boolean.
  - No test for column_type on TableColumn (intentionally omitted).
- Keep ColumnInfo tests as-is (Phase 1 keeps class and behavior).

## Test impact and mitigation (added post-planning)
- Affected test suites:
  - tests/handler/analyze_table_statistics/test_response_builder.py
  - tests/handler/analyze_table_statistics/test_result_parser.py
  - tests/handler/analyze_table_statistics/test_types.py (ColumnInfo unit tests)
  - tests/adapter/analyze_table_statistics/test_sql_generator.py
- Observed issues:
  - _result_parser initially accessed `col_info.data_type` assuming TableColumn, causing AttributeError with ColumnInfo inputs.
- Mitigations applied:
  - _result_parser now supports both TableColumn and ColumnInfo via a simple duck-typing check (hasattr for `data_type`).
  - generate_statistics_sql depends only on `name` and `statistics_type`, keeping compatibility with both TableColumn and ColumnInfo in tests.
  - _response_builder docstrings updated to reflect TableColumn; runtime behavior unchanged.
- Validation:
  - 77/77 tests pass, including all handler and adapter test modules listed above.

## Risks and mitigations
- Interface drift between adapter and handler: covered by integration tests and keeping types liberal where necessary during Phase 1.
- Hidden dependencies on ColumnInfo: addressed by repository-wide search and targeted refactors listed above.

## Affected files (Phase 1)
- src/mcp_snowflake/kernel/table_metadata.py
- src/mcp_snowflake/adapter/analyze_table_statistics_handler.py
- src/mcp_snowflake/handler/analyze_table_statistics/_column_analysis.py
- src/mcp_snowflake/handler/analyze_table_statistics/_result_parser.py
- src/mcp_snowflake/handler/analyze_table_statistics/_response_builder.py
- tests/kernel/test_table_metadata.py (additions)

## Rollout plan
1) Implement kernel properties in TableColumn.
2) Switch adapter to TableColumn inputs and SQL generation via statistics_type.
3) Update handler selection/validation to TableColumn-only path.
4) Update parser/response signatures; keep logic identical.
5) Run lint/tests; adjust minor type annotations if needed.

## Acceptance criteria
- Adapter no longer imports or depends on handler.analyze_table_statistics._types.ColumnInfo.
- All tests pass.
- ColumnInfo.column_type remains only in ColumnInfo tests; no migration to TableColumn.

## Follow-ups (Phase 2)
- Remove ColumnInfo usages entirely from handler; deprecate and delete _types.ColumnInfo and its tests.
- Tighten EffectAnalyzeTableStatistics typing to Iterable[TableColumn].

## Phase 1 Implementation Summary

### Changes Made
1. **Kernel (table_metadata.py)**: Added computed properties to TableColumn:
   - `snowflake_type: SnowflakeDataType` - returns `SnowflakeDataType(self.data_type)`
   - `statistics_type: StatisticsSupportDataType` - returns `StatisticsSupportDataType.from_snowflake_type(self.snowflake_type)`

2. **Adapter (analyze_table_statistics_handler.py)**:
   - Removed import of `handler.analyze_table_statistics._types.ColumnInfo`
   - Added import of `kernel.table_metadata.TableColumn`
   - Changed method signatures to accept `Iterable[TableColumn]` instead of `Iterable[ColumnInfo]`
   - Updated SQL generation logic to use `col.statistics_type.type_name`

3. **Handler (_column_analysis.py)**:
   - Replaced `create_column_info_list` with `ensure_supported_columns`
   - Updated `validate_and_select_columns` to return `list[TableColumn] | types.TextContent`
   - Validation now accesses `col.snowflake_type` and `col.statistics_type` for type checking

4. **Handler (_result_parser.py)**:
   - Changed signature to accept `Iterable[TableColumn]`
   - Updated to use `col.data_type` directly (raw string) instead of `col.snowflake_type.raw_type`
   - **Temporary backward-compatible with ColumnInfo via duck typing** (to be removed in Phase 2)

5. **Handler (_response_builder.py)**:
   - Changed signature to accept `Sequence[TableColumn]`
   - Docstrings updated accordingly

6. **Tests (Adapter layer)**:
   - Updated `test_sql_generator.py` to use `TableColumn` directly instead of `ColumnInfo.from_table_column()`
   - Updated `test_response_builder.py` and `test_result_parser.py` to use `TableColumn` directly

7. **Tests (test_table_metadata.py)**:
   - Added comprehensive tests for new TableColumn properties
   - Parameterized tests covering various data type combinations
   - Error case tests for unsupported types and empty data_type

### Phase 1 Validation Results
- ✅ All existing integration tests pass (288/288)
- ✅ New property tests pass
- ✅ Code formatting and linting applied
- ✅ No breaking changes to public APIs
- ✅ Error messages and behavior preserved

### Phase 1 Acceptance Criteria Met
- ✅ Adapter no longer imports or depends on `handler.analyze_table_statistics._types.ColumnInfo`
- ✅ All tests pass
- ✅ `ColumnInfo.column_type` remains only in ColumnInfo tests; no migration to TableColumn

## Phase 2: Complete ColumnInfo Removal

### Current Status (Post Phase 1)
- **ColumnInfo still exists in**:
  - `src/mcp_snowflake/handler/analyze_table_statistics/_types.py` (class definition)
  - `tests/handler/analyze_table_statistics/test_types.py` (unit tests)
  - Backward compatibility code in `_result_parser.py` (hasattr duck typing check)

### Phase 2 Objectives
- Remove `ColumnInfo` class entirely from `_types.py`
- Delete `test_types.py` (ColumnInfo unit tests)
- Remove backward compatibility code from `_result_parser.py`
- Tighten type annotations to use `TableColumn` exclusively

### Phase 2 Implementation Plan
1. **Remove backward compatibility code**:
   - Remove `hasattr(col_info, "data_type")` duck typing check from `_result_parser.py`
   - Simplify data_type access to always use `col_info.data_type`

2. **Delete ColumnInfo class and tests**:
   - Delete `src/mcp_snowflake/handler/analyze_table_statistics/_types.py` entirely
   - Delete `tests/handler/analyze_table_statistics/test_types.py` entirely
   - Update imports that referenced `_types.py`

3. **Update type annotations**:
   - Tighten `EffectAnalyzeTableStatistics` to use `Iterable[TableColumn]`
   - Remove any remaining `Any` type annotations related to ColumnInfo compatibility

### Phase 2 Acceptance Criteria
- ✅ No `ColumnInfo` references anywhere in codebase
- ✅ All tests pass (expected count: ~270-280 after removing ColumnInfo tests)
- ✅ Type annotations are strict and use `TableColumn` exclusively
- ✅ No backward compatibility code remains

### Phase 2 Implementation Summary (Completed)

Status: ✅ Completed on 2025-08-11

Changes realized in codebase:
- ColumnInfo removal
  - ColumnInfo class and all of its references have been removed from the codebase (src/ and tests/). A full-text search confirms no references remain.
  - Backward compatibility logic in `_result_parser.py` was removed; it now strictly accepts `Iterable[TableColumn]` and always uses `col_info.data_type`.
- Types module consolidation
  - `_types.py` is retained but now only contains TypedDict definitions for statistics payloads (Numeric/String/Date/Boolean stats, table/response dicts). No ColumnInfo is present.
- Tests update
  - `tests/handler/analyze_table_statistics/test_types.py` is retained and repurposed to validate `BooleanStatsDict` structure. ColumnInfo-specific tests were removed in effect.
  - Test totals after Phase 2: 271 tests, all passing.
- Type annotations tightened
  - `EffectAnalyzeTableStatistics.analyze_table_statistics` now requires `Iterable[TableColumn]`.
  - Adapter and handler paths exclusively use `TableColumn` for column metadata.

### Phase 2 Validation Results
- Test run: `uv run pytest -q`
- Outcome: ✅ 271 passed
- Notable checks:
  - No `ColumnInfo` references in src/ or tests/.
  - `_result_parser.py` has no duck-typing fallback; matches Phase 2 goal.
  - Adapter SQL generator uses `TableColumn.statistics_type.type_name` exclusively.

### Differences from original Phase 2 plan
- The original plan proposed deleting `_types.py` entirely. In practice, `_types.py` is still needed for shared TypedDicts (e.g., `BooleanStatsDict`, `AnalyzeTableStatisticsJsonResponse`). We removed only ColumnInfo-related contents while keeping the stats/response typing utilities.
- The original plan proposed deleting `tests/handler/analyze_table_statistics/test_types.py`. Instead, the file was repurposed to test `BooleanStatsDict` and no longer references ColumnInfo, maintaining useful coverage.

### Phase 2 Acceptance Status
- No `ColumnInfo` references: ✅ Done
- All tests pass: ✅ 271 passed
- Type annotations use `TableColumn` exclusively: ✅ Done
- Backward compatibility code removed: ✅ Done

The refactor is complete across both Phase 1 and Phase 2. Adapter/handler now depend solely on `kernel.TableColumn` for column metadata, and the TypedDict-based response typing remains centralized in `_types.py`.
