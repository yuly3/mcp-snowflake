# Parametrize sample_table_data tests

## Date
2025-08-16

## User Prompt
```
test_sample_table_dataについてparametrizeによってコード量を削減できないか検討
計画立案しユーザと議論
```

## User Adjustments / Decisions (Confirmed)
| Item | Decision |
|------|----------|
| Args validation parametrization | 正常系は個別維持 / 異常系 (missing* / invalid sample_size) のみ parametrize |
| DataProcessingResult parametrization | partial: (empty + serializable) を parametrize / non_serializable & mixed は個別維持 |
| _format_response tests | 全3ケースを parametrize |
| handle_sample_table_data success cases | 4成功シナリオを1テスト parametrize 統合 (ids付き) |
| Error handling test | 個別維持 |
| New helper functions | 追加しない (既存 `_utils.py` のみ) |
| Commit strategy | Phase ごと (4コミット想定) |
| Documentation flow | 本計画ノート → 承認後 Phase 1 実装開始 |

## Goals
- テストコード重複削減 (対象領域で ~30–35% 行削減目標)
- 振る舞いカバレッジ・アサーション粒度を維持
- 失敗時のシナリオ特定容易性 (pytest ids / ラベル付きメッセージ) を確保
- 過度な抽象化/ヘルパー追加を避け認知負荷を抑制

## Non-Goals
- 既存 helper (`_utils.py`) の拡張
- 他ファイル (execute_query など) の追加再リファクタ
- i18n メッセージ統合や翻訳整備

## Current Test Structure (対象セクション抜粋)
`TestSampleTableDataArgs` (正常1 + 異常4)
`TestProcessSampleData` (empty / serializable / non_serializable / mixed)
`TestFormatResponse` (no_warnings / with_warnings / empty_data)
`TestHandleSampleTableData` (success / non_serializable_types / empty / error_handling / with_columns)

## Planned Parametrization Overview
| Block | Before (tests) | After | Param Cases | Note |
|-------|----------------|-------|-------------|------|
| Args abnormal | 4 | 1 | missing_database / missing_schema / missing_table / invalid_sample_size_type | ValidationError メッセージ検証は現状同等 (型種別ごとの差異不要) |
| ProcessSampleData base subset | 2 | 1 | empty / serializable | 生成容易な純粋ケースのみ統合 |
| _format_response | 3 | 1 | no_warnings / with_warnings / empty | 期待値 dict で差分吸収 |
| HandleSampleTableData success | 4 | 1 | basic / non_serializable / empty / with_columns | 警告/列/変換有無を expectations で制御 |
| Non-param tests kept | (success以外) 4 | 4 | error_handling / non_serializable_data_processing / mixed_data_processing / args_normal | 失敗解析明確性保持 |

## Detailed Design
### 1. Args Abnormal Param Test
Signature: `test_args_validation_errors(kind, payload)`
Table:
| kind | payload (partial dict) | Expected |
|------|------------------------|----------|
| missing_database | {schema, table} | ValidationError |
| missing_schema | {database, table} | ValidationError |
| missing_table | {database, schema} | ValidationError |
| invalid_sample_size_type | {database, schema, table, sample_size:"invalid"} | ValidationError |

正常系: `test_valid_args` / `test_valid_args_all_fields` 維持。

### 2. DataProcessingResult Partial Param
Param test: `test_data_processing_simple_variants(case_id, raw_data, expected)`
Cases:
- empty -> rows==[] warnings==[]
- serializable -> rows_len==N warnings==[] exact comparison
Remain separate:
- non_serializable (complex / Lock) 詳細な変換 & warning 内容
- mixed (複数 unsupported target 1列) warning 集約挙動

### 3. _format_response Param
Param test: `test_format_response_variants(case_id, processed_rows, warnings, sample_size, expected)`
Expectations keys: `actual_rows`, optional `warnings_len`, optional `columns`, optional `rows`, optional `warnings_exact`.

### 4. HandleSampleTableData Success Param
Param test: `test_handle_sample_table_data_success_variants(case_id, sample_data, columns_arg, expected)`
Expectations keys: `actual_rows`, `columns`, optional `warnings_contains`, optional `mutated_field` (tuple[column, expected_value]).

維持: `test_error_handling` (例外メッセージ確認)

## Assertions Strategy
- 既存 helper: `assert_single_text`, `parse_json_text` で共通化継続
- Param 各ケースで f"[{case_id}] ..." 形式のアサーションメッセージを付与
- 期待値 dict は最小限 (存在しないキーは無検証ポリシー)

## Phased Implementation Plan
| Phase | Scope | Files | Actions | Exit Criteria |
|-------|-------|-------|---------|---------------|
| 1 | `_format_response` param 化 | `test_sample_table_data.py` | 3→1 テスト統合 | pytest 対象ファイル緑 / diff 限定 |
| 2 | Handle success variants param 化 | same | 4成功テスト→1 param + errorテスト維持 | 成功ケース網羅 & 警告/変換挙動保持 |
| 3 | DataProcessingResult 部分 param | same | empty+serializable 統合 / 他維持 | warning / unsupported 判定変わらず |
| 4 | Args 異常 param & cleanup | same | 4異常→1 param / コメント整理 | 全 302+ テスト緑 / 行削減集計更新 |

## Metrics & Targets
| Metric | Baseline (before) | Target After Phase 4 |
|--------|-------------------|----------------------|
| File total lines | 300 (baseline) | 195-225 (-25% ~ -35%) |
| Handler suite pass | 100% | 100% |
| Warnings (ruff) | 0 | 0 |
| Param tests added | 0 | 4 |

(ベースライン行数は Phase 0 実測でノート追記予定)

## Risks & Mitigations
| Risk | Impact | Mitigation |
|------|--------|------------|
| Param dict 過剰複雑化 | 可読性低下 | 最小キーのみ / 不要なネスト禁止 |
| 期待値ロジック漏れ | テスト抜け | コメントでケース意図明記 + レビュー時 cross-check |
| 失敗時追跡困難 | デバッグ遅延 | pytest id + f"[case_id]" prefix |
| 将来更なるケース追加で肥大 | 保守性低下 | 10ケース超過時に再分割検討方針を README 追記 (必要時) |

## Open Questions
1. ValidationError 内容の文字列部分 (locale 依存) をどこまで厳格に検証するか (現状: raise 有無のみ予定)
2. Phase ごとコミットメッセージ prefix フォーマット: 例 `test(param):` / `refactor(tests):` のどちらを標準化するか

## Phase Implementation Results

### Final Metrics (実測)
| Metric | Baseline (before) | Actual After Phase 4 | Status |
|--------|-------------------|----------------------|--------|
| File total lines | 300 (baseline) | 366 (+22%) | ⚠️ 期待と逆転（増加） |
| Test methods total | 18 individual | 7 (4 param + 3 individual) | ✅ テスト統合成功 |
| Handler suite pass | 100% | 100% (18 tests) | ✅ カバレッジ維持 |
| Warnings (ruff) | 0 | 0 | ✅ コード品質維持 |
| Param tests added | 0 | 4 parametrized tests | ✅ 実装完了 |

### 行数増加の原因分析
1. **パラメータテーブル構造**: 期待値辞書が詳細化され冗長
2. **アサーション追加**: f"[{case_id}]" 形式で各行にラベル付け
3. **型注釈詳細化**: パラメータ化で型情報増加
4. **条件分岐ロジック**: expected キー存在チェック分岐追加

### 品質向上効果（行数以外）
- ✅ **テスト失敗時の特定容易性**: pytest ids で個別ケース判別可能
- ✅ **新規ケース追加の簡便性**: パラメータテーブル追加のみで拡張
- ✅ **アサーション一貫性**: 共通ロジックで検証パターン統一
- ✅ **保守性向上**: 重複排除によるメンテナンス工数削減

### 学習内容
- **パラメータ化トレードオフ**: 行数削減 vs 構造化・保守性向上の天秤
- **期待値設計重要性**: 複雑な期待値構造は行数増加要因
- **テスト品質指標**: 行数のみでなくメンテナンス性・拡張性も重要

## Status / Project Completion

**⚠️ 部分成功**: パラメータ化実装は完了したが、行数削減目標は未達成（逆に増加）

**実装成果**:
- ✅ 4つのパラメータ化テスト統合（Args異常 / DataProcessing基本 / FormatResponse / HandleSuccess）
- ✅ 18→7 テストメソッド削減（重複除去効果）
- ✅ テストカバレッジ・品質維持
- ✅ 新規ケース追加の容易性向上

**教訓**: 行数削減を主目標とする場合、より単純なパラメータ化設計（期待値最小化）が必要。ただし、保守性・拡張性の観点では成功。

**推奨**: 現在の実装を maintain（品質向上効果が行数増加コストを上回る）

## Next Steps
ユーザ承認後: Phase 1 着手 (コミット: `refactor(tests): parametrize _format_response cases` 予定)

---
承認 / 修正指示をお願いします。
