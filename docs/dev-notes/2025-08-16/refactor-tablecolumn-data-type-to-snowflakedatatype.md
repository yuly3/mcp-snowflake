# Refactor: TableColumn.data_type to SnowflakeDataType

## User Prompt
> ゴール: TableColumnのdata_typeをSnowflakeDataTypeにする。影響を調査し実装計画を提案。\n> 追加確認: 外部クライアントは文字列を期待しない / JSONシリアライズは raw のみ。

## Agreed Plan (Option 1: Direct Replacement)
- Break existing `data_type: str` -> `data_type: SnowflakeDataType` (no external string clients => safe)
- Keep JSON serialization as raw string only (`raw_type`).
- Provide attrs converter so constructor still accepts either `str` or `SnowflakeDataType`.
- Simplify `snowflake_type` property -> just return `self.data_type`.
- `statistics_type` unchanged (references updated property).
- Validation (unsupported / empty) moves to construction time.
- Update tests accordingly (adjust assertions & exception timing) using TDD micro-cycles.

## TDD Strategy
We apply red->green cycles per artifact:
1. Adjust or write new tests expecting new behavior (failing initially):
   - Construction with valid string sets `data_type.raw_type` & normalized_type.
   - Construction with empty string raises ValueError (no need to access property).
   - Unsupported but parsable for snowflake (e.g. VARIANT) still constructs; `statistics_type` access for unsupported category raises same ValueError (existing behavior retained).
   - JSON serialization (cattrs) outputs raw string (ensure hook or existing mapping updated).
2. Implement converter & refactor class.
3. Run tests -> green.
4. Cleanup & ensure ruff passes.

## Serialization Detail
- Add cattrs hook (if not present) so `unstructure(TableColumn)` uses `raw_type` for `data_type`.
- If TableColumn isn't yet part of any outbound JSON, skip until needed (minimal change principle). Will confirm after failing test if needed.

## Edge Cases
- Precision/length specifiers (`NUMBER(10,2)`, `VARCHAR(100)`) keep `raw_type` intact and normalized removes parentheses.
- Empty -> immediate ValueError.

## Open Decisions (default answers unless user changes)
- Expose helper property `raw_data_type`? => Not initially; `column.data_type.raw_type` is explicit.
- Back-compat alias property `legacy_data_type_str`? => Not needed.

## Next Steps
Await user approval to proceed with test-first changes.

---
(After implementation this note will be updated with summary and any deviations.)

## Implementation Summary

**Completed successfully on 2025-08-16.**

### What was done:
1. **TableColumn refactor**: Changed `data_type: str` → `data_type: SnowflakeDataType` with attrs converter
2. **Converter implementation**: Added `_to_snowflake_data_type()` function accepting both `str` and `SnowflakeDataType`
3. **Property simplification**: `snowflake_type` now simply returns `self.data_type` (no re-parsing)
4. **JSON serialization fixes**:
   - Updated cattrs converter to handle SnowflakeDataType → raw_type string
   - Fixed JSON output in describe_table handler
   - Fixed JSON output in analyze_table_statistics response_builder and result_parser
5. **Error message updates**: Updated error messages to use raw_type instead of object representation
6. **Test updates**: Updated all tests to expect new behavior and attributes

### Key changes:
- Construction-time validation: Empty/invalid data_type now raises ValueError during `TableColumn()` creation
- Improved performance: No repeated parsing of data_type strings
- Type safety: All data_type access is now type-safe SnowflakeDataType
- Maintained compatibility: JSON outputs still use raw string format as requested

### TDD Process:
- Successfully applied RED → GREEN → REFACTOR cycles
- All 283 tests passing after refactoring
- All JSON serialization works correctly with raw_type format

### No deviations from original plan.
The refactoring improves type safety while maintaining external compatibility.
