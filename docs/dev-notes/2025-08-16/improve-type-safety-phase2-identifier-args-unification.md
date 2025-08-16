# Improve Type Safety Phase 2: Identifier Argument Unification & Aliasing

## Date
2025-08-16

## Objective
Unify inconsistent identifier argument names (`schema_name`, `table_name`) across tools/handlers to a consistent trio: `database`, `schema`, `table`.

UPDATED (Breaking Change Policy): Remove legacy names outright (no transitional aliases). This phase intentionally introduces a breaking API change to simplify codebase and avoid dual maintenance cost.

## Motivation
- Reduce cognitive overhead and potential for argument misordering.
- Prepare for future introduction of `TableIdentifier` value object.
- Establish forward-compatible public API naming before further refactors (Enums, column quoting helpers).

## Current Inconsistencies
- Handlers / Tools use `schema_name`, `table_name` while adapters, kernel domain models (`TableInfo.schema`, `TableColumn.name`) already use `schema`, `name`.
- Tests and docs rely on old field names; naive rename would break them.

## Scope (Phase 2)
Apply to all read-only metadata & data sampling features:
- describe_table (handler/tool args)
- list_tables (handler/tool args)
- list_views (handler/tool args)
- sample_table_data (handler/tool args)
- analyze_table_statistics (handler/tool args + nested response builder references)

Out of scope: execute_query (no schema/table args), internal domain models (already consistent).

## Deliverables
1. Rename all public argument model fields to `schema`, `table` (replace, not alias).
2. Update tool `inputSchema` to expose only `schema` / `table` (remove `schema_name` / `table_name`).
3. Refactor handler, response builder, and any string formatting to use new names.
4. Update all tests to use new field names (remove legacy usage entirely).
5. Add BREAKING CHANGE note in this dev note & (optionally) in CHANGELOG / README.
6. Grep verification step to ensure zero occurrences of `schema_name` / `table_name` in `src/` except historical notes.

## Backward Compatibility Strategy
None. This is an intentional breaking change. Major/minor version bump required depending on semantic versioning scheme (recommend: minor if still pre-1.0, else major).

## Rationale for Immediate Removal
- Code simplicity: avoids conditional/alias complexity.
- Clearer documentation & onboarding.
- Prevents silent precedence ambiguities when both legacy & new keys supplied.
- Aligns earlier with future `TableIdentifier` introduction.

## Affected Files
### Source
- `src/mcp_snowflake/handler/describe_table.py`
- `src/mcp_snowflake/handler/list_tables.py`
- `src/mcp_snowflake/handler/list_views.py`
- `src/mcp_snowflake/handler/sample_table_data.py`
- `src/mcp_snowflake/handler/analyze_table_statistics/__init__.py` (argument model & internal references)
- `src/mcp_snowflake/handler/analyze_table_statistics/_response_builder.py` (uses args.schema_name/table_name)
- `src/mcp_snowflake/tool/describe_table.py`
- `src/mcp_snowflake/tool/list_tables.py`
- `src/mcp_snowflake/tool/list_views.py`
- `src/mcp_snowflake/tool/sample_table_data.py`
- `src/mcp_snowflake/tool/analyze_table_statistics.py`

### Tests
- `tests/handler/test_describe_table.py`
- `tests/handler/test_list_tables.py`
- `tests/handler/test_list_views.py`
- `tests/handler/test_sample_table_data.py`
- `tests/handler/analyze_table_statistics/...` (multiple test modules referencing args)

### Documentation
- Current dev notes (Phase 1 & this Phase 2 file)
- README examples (if any show old field names – TBD)

## Detailed Change Plan
1. Modify existing Pydantic models (no alias fields) – rename fields directly.
2. Update tool definitions (`inputSchema`): keys list + required arrays updated to new names.
3. Replace all `schema_name` / `table_name` references in handlers, response builders, adapters (only if any stray), and tests.
4. Adjust any JSON output text that interpolates old variable names (should remain structural – likely no change needed).
5. Run full test suite; fix failing tests.
6. Add BREAKING CHANGE note section at end of this file after implementation.
7. Grep confirm zero legacy identifiers remain (except inside dev-notes historical sections).

## Edge Cases / Risks
- External clients using old field names will fail validation immediately (clear error message from Pydantic). Provide upgrade note.
- README / examples must be synchronized to avoid confusion.
- Forgotten rename in one handler could cause inconsistent behavior (mitigated by grep + tests).

## Open Questions (Resolved)
- Deprecation banner: Not needed (legacy removed). Instead: concise BREAKING CHANGE note in changelog.
- Runtime warnings: Not applicable.

## Metrics of Success
- All tests pass.
- New tests confirm dual-key acceptance.
- No handler references to `schema_name` / `table_name` remain (grep zero matches except in alias definitions & tests verifying legacy support).

## TDD Steps
1. RED: Update an existing test to use new field names only (causing initial failures due to missing fields); remove legacy usages in that test module.
2. GREEN: Rename fields in corresponding handler args + tool schema; make test pass.
3. RED: Update next test module; repeat until all modules migrated.
4. REFACTOR: Global grep to ensure no occurrences; run full suite once at end.

## Grep Baseline Counts (pre-change)
(schema_name occurrences & table_name occurrences captured via automation before modification.)

## Post-Implementation Checklist
- [x] All tests updated to use `schema` / `table`
- [x] All handlers updated
- [x] All tool inputSchema definitions updated (no legacy keys)
- [x] Grep shows zero `schema_name` / `table_name` in `src/` & `tests/`
- [x] Dev note implementation log appended with diff summary & BREAKING CHANGE note

## IMPLEMENTATION COMPLETED - 2025-08-16

### Phase 2 Implementation Summary

**Phase 2「引数名統一」が完全に完了しました。** TDD手法を採用して段階的に実装を進め、すべての目標を達成しています。

### 実装したモジュール群
1. **describe_table** ✅ 完了 (13 tests passed)
2. **list_tables** ✅ 完了 (12 tests passed)
3. **list_views** ✅ 完了 (12 tests passed)
4. **sample_table_data** ✅ 完了 (18 tests passed)
5. **analyze_table_statistics** ✅ 完了 (46 tests passed)

**総計: 101個のテストが全て通過**

### 主要な実装変更

#### 1. Pydantic Field Alias パターン
```python
class DescribeTableArgs(BaseModel):
    database: str
    schema_: str = Field(alias="schema")  # Pydanticのschemaメソッド衝突回避
    table_: str = Field(alias="table")
```

#### 2. Tool inputSchema の統一
```python
# Before
"required": ["database", "schema_name", "table_name"]

# After
"required": ["database", "schema", "table"]
```

#### 3. Protocol 定義の統一
```python
# Before
async def describe_table(self, database: str, schema: str, table_name: str) -> TableInfo

# After
async def describe_table(self, database: str, schema: str, table: str) -> TableInfo
```

### 対応した追加問題

#### エラー修正フェーズ（残りのanalyze_table_statistics関連）

**1. テストファイルでの引数名エラー**
- **対象**: `tests/handler/analyze_table_statistics/` の全テストファイル
- **修正**: `AnalyzeTableStatisticsArgs` 作成時の `schema_name=`/`table_name=` → `schema=`/`table=`

**2. モックオブジェクトのプロトコル不一致**
- **対象**: 3つのモッククラス
  - `MockEffectHandler`
  - `MockEffectWithQueryError`
  - `MockEffectWithQueryTracking`
- **修正**: メソッドシグネチャの `table_name`/`schema_name` → `table`/`schema`

**3. プロトコル定義の不一致**
- **対象**: `EffectAnalyzeTableStatistics` プロトコル
- **修正**: `analyze_table_statistics` メソッドの統一されたシグネチャ

**4. docstring の更新**
- **対象**: アダプタと関数のドキュメント
- **修正**: パラメータドキュメントの統一（`table_name` → `table`、`schema_name` → `schema`）

### 破壊的変更の確認

✅ **意図的な破壊的変更を実装**
- すべてのツールAPIが `schema_name`/`table_name` から `schema`/`table` に変更完了
- 旧APIとの互換性なし（設計方針通り）
- MCP-Snowflake全体で一貫した引数名を実現

### 技術的成果

1. **型安全性の向上**: Pydantic Field aliasパターンでメソッド衝突を回避
2. **API一貫性の実現**: 全モジュールで統一された引数名
3. **テストカバレッジ維持**: 101個すべてのテストが通過
4. **コード品質向上**: docstring、プロトコル定義、モックオブジェクトも統一

### Breaking Change Notice

**BREAKING CHANGE (v0.2.0)**: All table-related tools now use `schema` and `table` parameters instead of `schema_name` and `table_name`.

**Migration Guide**:
```python
# Before (v0.1.x)
{
  "database": "my_db",
  "schema_name": "public",
  "table_name": "users"
}

# After (v0.2.0+)
{
  "database": "my_db",
  "schema": "public",
  "table": "users"
}
```

**Affected Tools**: `describe_table`, `list_tables`, `list_views`, `sample_table_data`, `analyze_table_statistics`

### 次のフェーズ準備完了

Phase 2の完了により、以下の将来的な改善への基盤が整いました：
- `TableIdentifier` value object の導入
- Enum型の活用
- さらなる型安全性の向上

Phase 1（SQL識別子クォート）とPhase 2（引数名統一）の両方が完了し、MCP-Snowflakeプロジェクトの型安全性とAPI一貫性が大幅に向上しました。

## Status: COMPLETED ✅

Phase 2「Identifier Argument Unification & Aliasing」が2025-08-16に完了しました。

**成果**:
- 破壊的変更を含む完全なAPI統一
- 101個のテストが全て通過
- 型安全性とコード品質の大幅向上
- 将来の改善（TableIdentifier等）への基盤構築完了
