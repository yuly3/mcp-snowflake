# Refactor: Move parse_statistics_result to adapter layer and change EffectAnalyzeTableStatistics return type

Date: 2025-08-24

## User Request

- Goal: Move `parse_statistics_result` to adapter layer and change `EffectAnalyzeTableStatistics.analyze_table_statistics` return type from `dict` to `TableStatisticsParseResult`.
- Add `StatisticsResultParseError` to the expected exceptions for `EffectAnalyzeTableStatistics.analyze_table_statistics`.
- Because the adapter's `analyze_table_statistics_handler.py` will grow large, modularize it into `analyze_table_statistics_handler/{handler, sql generator, result parser}`.
- After impact analysis, create a plan and discuss with the user; update tests accordingly.

## Impact Analysis

- Implementation files impacted:
  - Move: `src/mcp_snowflake/handler/analyze_table_statistics/_result_parser.py` → adapter layer
  - Update: `src/mcp_snowflake/adapter/analyze_table_statistics_handler.py` → become package `analyze_table_statistics_handler/` split into `handler.py`, `sql_generator.py`, `result_parser.py`, with `__init__.py` re-exports.
  - Update: `src/mcp_snowflake/handler/analyze_table_statistics/models.py` → change protocol return type and add exception to contract docstring.
  - Update: `src/mcp_snowflake/handler/analyze_table_statistics/__init__.py` → stop calling parser directly; use `TableStatisticsParseResult` returned by effect.
- Tests impacted:
  - Move parsing tests: `tests/handler/analyze_table_statistics/test_result_parser.py` → `tests/adapter/analyze_table_statistics/test_result_parser.py` (imports updated to adapter path).
  - Update mocks: `tests/mock_effect_handler/analyze_table_statistics.py` to return `TableStatisticsParseResult` (use moved parser to convert dict row to parsed result).
  - Update inline Mock in `tests/tool/analyze_table_statistics/test_errors.py` to throw `StatisticsResultParseError` from the effect (by using moved parser or raising directly).
  - Handler main tests under `tests/handler/analyze_table_statistics/main/*` should continue to work with the new return type; adjust mocks accordingly.

## Proposed Plan

1. Create adapter package structure:
   - `src/mcp_snowflake/adapter/analyze_table_statistics_handler/__init__.py` re-export `AnalyzeTableStatisticsEffectHandler` and `generate_statistics_sql`.
   - `src/mcp_snowflake/adapter/analyze_table_statistics_handler/sql_generator.py` → move `generate_statistics_sql` from current adapter file.
   - `src/mcp_snowflake/adapter/analyze_table_statistics_handler/result_parser.py` → move and keep logic from handler `_result_parser.py` as-is.
   - `src/mcp_snowflake/adapter/analyze_table_statistics_handler/handler.py` → move class from adapter file and change `analyze_table_statistics` to call `parse_statistics_result` and return `TableStatisticsParseResult`.
   - Remove old `src/mcp_snowflake/adapter/analyze_table_statistics_handler.py` after migration.

2. Update protocol and handler layer:
   - In `models.py`, change `EffectAnalyzeTableStatistics.analyze_table_statistics` return type to `Awaitable[TableStatisticsParseResult]` and docstring for exceptions to include `StatisticsResultParseError`.
   - In `handler/__init__.py`, remove direct call to `parse_statistics_result` and use the parsed result returned by `effect.analyze_table_statistics(...)` to build JSON response.
   - Adjust imports: the handler package should no longer import `_result_parser`; instead rely on effect.

3. Update tests and mocks:
   - Move parsing test file to adapter tests directory and update imports to `mcp_snowflake.adapter.analyze_table_statistics_handler.result_parser`（再エクスポートは行わない）。
   - Modify `tests/mock_effect_handler/analyze_table_statistics.py` to:
     - Return `TableStatisticsParseResult` using the moved `parse_statistics_result` and provided `columns_to_analyze`.
   - Update inline mock in `tests/tool/analyze_table_statistics/test_errors.py` to return `TableStatisticsParseResult` or raise `StatisticsResultParseError` when needed (e.g., by invoking `parse_statistics_result`).

4. Dev notes process:
   - This document serves as the Planning phase record under `docs/dev-notes/2025-08-24/`.
   - After implementation, update this file with summary of changes and test results.

## Assumptions

- Keep data models (`TableStatisticsParseResult`, `StatsDict`, etc.) in handler layer as-is to avoid churn; adapter will import them.
- Public import compatibility: keep `from mcp_snowflake.adapter.analyze_table_statistics_handler import generate_statistics_sql` working by re-exporting in the new package's `__init__.py`.
- `parse_statistics_result` は adapter パッケージの `__init__.py` では再エクスポートしない（テストは `...analyze_table_statistics_handler.result_parser` から import する）。
- Tool-level error mapping already includes `StatisticsResultParseError`, so only the origin of the exception changes (now from effect), not the tool behavior.

## Decisions

- Do not re-export `parse_statistics_result`; tests update their import path to `mcp_snowflake.adapter.analyze_table_statistics_handler.result_parser`.
- SQL generator tests are under adapter（現状維持）。handler/main テストは変更しない。

## Next Steps (upon approval)

- Implement file moves/splits and type changes.
- Update mocks and tests accordingly.
- Run formatting and tests via uv:
  - `uv run ruff check --fix --unsafe-fixes . && uv run ruff format .`
  - `uv run pytest -q`

## Implementation Summary (Completion Phase)

### Changes Made

1. **Created adapter package structure** ✅
   - `src/mcp_snowflake/adapter/analyze_table_statistics_handler/` package created
   - `__init__.py`: Re-exports `AnalyzeTableStatisticsEffectHandler` and `generate_statistics_sql`
   - `sql_generator.py`: Moved `generate_statistics_sql` from original adapter file
   - `result_parser.py`: Moved from `src/mcp_snowflake/handler/analyze_table_statistics/_result_parser.py`
   - `handler.py`: Updated class to call `parse_statistics_result` internally and return `TableStatisticsParseResult`
   - Removed original `src/mcp_snowflake/adapter/analyze_table_statistics_handler.py`

2. **Updated protocol and handler layer** ✅
   - `models.py`: Changed `EffectAnalyzeTableStatistics.analyze_table_statistics` return type to `Awaitable[TableStatisticsParseResult]`
   - Added `StatisticsResultParseError` to docstring exceptions
   - `handler/__init__.py`: Removed direct `parse_statistics_result` call, now uses parsed result from effect
   - Cleaned unused imports

3. **Updated tests and mocks** ✅
   - Moved `tests/handler/analyze_table_statistics/test_result_parser.py` → `tests/adapter/analyze_table_statistics/test_result_parser.py`
   - Updated imports to `mcp_snowflake.adapter.analyze_table_statistics_handler.result_parser`
   - Modified `tests/mock_effect_handler/analyze_table_statistics.py` to return `TableStatisticsParseResult`
   - Updated inline mock in `tests/tool/analyze_table_statistics/test_errors.py` to invoke parser and raise `StatisticsResultParseError`
   - Fixed inline mock in `tests/handler/analyze_table_statistics/main/test_selection_cases.py` to use parser

4. **Quality assurance** ✅
   - `uv run ruff check --fix --unsafe-fixes . && uv run ruff format .`: All checks passed, 118 files unchanged
   - `uv run pytest`: 455 tests passed in 6.01s

### Files Modified
- `src/mcp_snowflake/handler/analyze_table_statistics/models.py`: Protocol return type and exception
- `src/mcp_snowflake/handler/analyze_table_statistics/__init__.py`: Removed parser call
- `tests/mock_effect_handler/analyze_table_statistics.py`: Updated to return parsed result
- `tests/tool/analyze_table_statistics/test_errors.py`: Updated inline mock
- `tests/handler/analyze_table_statistics/main/test_selection_cases.py`: Fixed local mock
- `tests/adapter/analyze_table_statistics/test_result_parser.py`: Updated import paths

### Files Created
- `src/mcp_snowflake/adapter/analyze_table_statistics_handler/__init__.py`
- `src/mcp_snowflake/adapter/analyze_table_statistics_handler/handler.py`
- `src/mcp_snowflake/adapter/analyze_table_statistics_handler/sql_generator.py`
- `src/mcp_snowflake/adapter/analyze_table_statistics_handler/result_parser.py`

### Files Deleted
- `src/mcp_snowflake/adapter/analyze_table_statistics_handler.py` (replaced by package)
- `src/mcp_snowflake/handler/analyze_table_statistics/_result_parser.py` (moved to adapter)

### Result
✅ All requirements fulfilled:
- `parse_statistics_result` moved to adapter layer
- `EffectAnalyzeTableStatistics.analyze_table_statistics` return type changed from `dict` to `TableStatisticsParseResult`
- `StatisticsResultParseError` added to expected exceptions
- Adapter handler modularized into package with 3 separate files
- Tests updated and moved accordingly
- All 455 tests passing
- Code formatted and linted
