# Refactor: StatisticsSupportDataType 再設計

## 1. ユーザ要求 (Prompt)
- StatisticsSupportDataType を再設計。
  - 内部フィールドとして `SnowflakeDataType` を保持する。
  - コンストラクタは `SnowflakeDataType` のみ受け取り、統計未サポート型なら `ValueError`。
  - `from_snowflake_type` はコンストラクタを利用し、`ValueError` を握りつぶして `None` を返す。
- `.type_name` プロパティ互換維持 (Yes)。
- `TableColumn.statistics_type` プロパティの戻り値を Optional に変更し、呼び出し側で制御 (Yes)。
- 非サポート型列が UI/API に現れる可能性あり (Yes)。
- 派生情報の追加保持は今回行わない (No)。
- 影響範囲を芋づる式に完全把握。
- 実装はクラス/モジュール単位で Red -> Implement -> Green の TDD サイクル。
- 本ドキュメント作成後にユーザ確認を受け実装着手。

## 2. 現行設計サマリ
```
class StatisticsSupportDataType:
    type_name: Literal["numeric","string","date","boolean"]
    @classmethod
    def from_snowflake_type(sf_type) -> Self | raises ValueError (unsupported)
```
- `TableColumn.statistics_type` は都度 `StatisticsSupportDataType.from_snowflake_type(self.data_type)` を返す。
- 非サポート型アクセス時に ValueError を発生させ、利用側 (`select_and_classify_columns`) は try/except で分類。
- 利用側は `.type_name` に直接アクセス (`generate_statistics_sql`, `_result_parser.parse_statistics_result` など)。

## 3. 新設計概要
```
@attrs.define(frozen=True)
class StatisticsSupportDataType:
    snowflake_type: SnowflakeDataType
    _classification: Literal["numeric","string","date","boolean"] = attrs.field(init=False)
    def __attrs_post_init__(...):
        # snowflake_type の is_numeric 等で判定。未サポートなら ValueError。
    @property
    def type_name(self) -> Literal["numeric","string","date","boolean"]:
        return self._classification
    @classmethod
    def from_snowflake_type(cls, sf_type: SnowflakeDataType) -> Self | None:
        try: return cls(sf_type)
        except ValueError: return None
```
```
TableColumn.statistics_type: Optional[StatisticsSupportDataType]
    -> return StatisticsSupportDataType.from_snowflake_type(self.data_type)
```
- 分類文字列互換維持 (`.type_name` プロパティ)。
- 例外駆動 → None 駆動へ移行。
- 既存の理由文字列 ("Unsupported Snowflake data type for statistics: <RAW>") は互換維持 (select_and_classify_columns で生成)。

## 4. 影響範囲 (関数 / クラス / テスト) 詳細
| 種別 | シンボル | 現行挙動 | 変更点 / 対応 | 備考 |
|------|----------|----------|---------------|------|
| Class | `StatisticsSupportDataType` | 文字列分類を直接保持 | SnowflakeDataType保持 + 判定 | 生成 API 破壊的変更 |
| Method | `StatisticsSupportDataType.from_snowflake_type` | Unsupported で ValueError | Unsupported で None | 呼び出しサイト変更 |
| Property | `TableColumn.statistics_type` | StatisticsSupportDataType (常に / unsupported で例外) | Optional[StatisticsSupportDataType] (unsupported= None) | シグネチャ & テスト修正 |
| Function | `select_and_classify_columns` | try/except ValueError | None 判定へ変更 | 例外依存排除 |
| Function | `generate_statistics_sql` | `.statistics_type.type_name` 前提 (非None) | 呼び出し元で supported columns のみ渡す前提を明示 | Docstring & defensive assert (任意) |
| Function | `parse_statistics_result` | `.statistics_type.type_name` 前提 | 同上 (supported のみ) | 前処理で保証 |
| Handler | `AnalyzeTableStatisticsEffectHandler.analyze_table_statistics` | 既存フロー | classification 出力から supported のみ SQL 生成 | 既存でも supported のみ渡している想定 (要確認) |
| Tests | `tests/kernel/test_data_types.py` | 直接 `StatisticsSupportDataType("numeric")` | 新: `StatisticsSupportDataType(SnowflakeDataType("NUMBER"))` へ更新 | 旧 API テスト削除 |
| Tests | `test_data_types::test_from_snowflake_type_unsupported_raises_error` | ValueError 期待 | None 期待へ変更 | 別途 constructor 直接呼び出しで ValueError テスト追加 |
| Tests | `tests/kernel/test_table_metadata.py::test_unsupported_data_type_raises_error` | `column.statistics_type` で ValueError | `column.statistics_type is None` を期待 | |
| Tests | `tests/handler/analyze_table_statistics/test_column_analysis.py` | reason は例外メッセージ | reason を手動生成 (同一文言) | 振る舞い等価 |
| Tests | SQL 生成/結果パーサ関連 (間接) | 前提不変 | 追加の None ケース不要 (supported のみ) | |
| Docs | 過去 dev-notes | 旧仕様記述 | 差分追記 | 破壊変更の履歴明確化 |
| Export | `kernel/__init__.py` | クラス公開 | そのまま | Breaking note を README / dev-notes に記述 |

### 追加で芋づる式に確認した未影響 / 注意点
- `SnowflakeDataType` 自体の API 変更不要。
- `_result_parser` / `generate_statistics_sql` は classification を経た列集合向けなので None チェックは原則不要だが、将来の誤用防止のため `assert col_info.statistics_type is not None` を追加するオプション。今回は最小変更方針のため **追加しない**（テスト拡散抑制）。
- 他 grep: `.statistics_type.type_name` 以外の `statistics_type` 参照は無し。
- 例外文字列をテストが厳密一致マッチしているため、新ロジックによる reason 生成は同じ文字列を使う。

## 5. TDD 実行計画 (Red -> Green サイクル)
| フェーズ | 目的 | 具体的変更 (テスト/コード) | 期待 (Red/Green) |
|----------|------|-----------------------------|-----------------|
| Phase 1 | 新 API をテストに先行反映 | `test_data_types` 修正: 旧 direct string init テスト削除/置換, unsupported で from_ が None, constructor unsupported で ValueError | Red (実装未完) |
| Phase 2 | `test_table_metadata` Optional 化 |  ValueError 期待テスト→ None 期待 / 他アサーション調整 | Red |
| Phase 3 | `test_column_analysis` None ベース分類 | try/except 前提から None 判定にテスト書き換え (理由文字列は同一) | Red |
| Phase 4 | 実装: `StatisticsSupportDataType` 再設計, `TableColumn.statistics_type` Optional 化 | コード追加 | まだ一部失敗 (select_and_classify_columns 未対応) |
| Phase 5 | 実装: `select_and_classify_columns` None 判定に変更 | | 主要テスト Green |
| Phase 6 | 残テスト調整 & クリーニング | 不要 import 削除 / type hints 更新 | 全 Green |
| Phase 7 | Ruff / Format / 追加ドキュメント追記 | 一貫性 | 全 Green 維持 |

### 細分化 (作業順序詳細)
1. Phase 1 テスト編集: `TestStatisticsSupportDataType` 更新
   - `test_init_with_valid_type_name` → `test_constructor_with_supported_snowflake_type` にリネーム & 内容変更
   - 新規: `test_constructor_with_unsupported_snowflake_type_raises_value_error`
2. Phase 2 テスト編集: `test_table_metadata`
   - `test_unsupported_data_type_raises_error` → `test_statistics_type_is_none_for_unsupported_type`
3. Phase 3 テスト編集: `test_column_analysis`
   - try/except 不使用。`col.statistics_type is None` で unsupported 特定 (ただし関数は API 変えない: return format そのまま)
4. Phase 4 実装: `data_types.py` 改修
5. Phase 4-2 実装: `table_metadata.py` Optional 型ヒント変更
6. Phase 5 実装: `_column_analysis.py` 分類ロジック変更 (例外→None)
7. Phase 6 確認: 他参照 (`_result_parser.py`, `analyze_table_statistics_handler.generate_statistics_sql`) はインターフェース上変更不要
8. Phase 6 テスト再実行 / フィクス
9. Phase 7 ドキュメント: 本ファイルに "Implementation Summary" セクション追加 (完了後)

## 6. API / 型変更一覧
| シンボル | 旧型 | 新型 |
|----------|------|------|
| `StatisticsSupportDataType.__init__` | (type_name: Literal[...]) | (snowflake_type: SnowflakeDataType) |
| `StatisticsSupportDataType.from_snowflake_type` | (sf_type) -> Self raises ValueError | (sf_type) -> Self | None |
| `TableColumn.statistics_type` | -> StatisticsSupportDataType | -> StatisticsSupportDataType | None |

## 7. 互換性ポリシー
- 破壊的変更: コンストラクタシグネチャ / from メソッド挙動 / プロパティ Optional 化。
- バージョン付け戦略: 次マイナーバージョンで CHANGELOG 記載 (例: 0.x 系なら minor で周知)。
- 例外→None 変更のため呼び出し側で null 安全性チェックが必要なことを README 追記検討 (本タスク外)。

## 8. リスク & 緩和
| リスク | 内容 | 緩和施策 |
|--------|------|----------|
| 呼び出し側で None 未対応 | NPE 的 AttributeError 発生 | classification 経由の導線を保持 / テストで網羅 |
| 例外ベースの既存外部コード破壊 | 外部が try/except している可能性 | CHANGELOG & ドキュメント通知 |
| migrate 漏れ | grep 網羅失敗 | 影響範囲 grep 完了 (statistics_type / .type_name / Unsupported message) & 再検索 |

## 9. ロールバック戦略
- `from_snowflake_type` の None 戻りを ValueError raise に戻すだけで短期復旧。
- 旧コンストラクタ互換レイヤを暫定追加する fallback パッチ案:
  ```python
  @overload
  def __init__(self, type_name: str): ...  # deprecated wrapper
  ```
  → 今回は追加しない (シンプル維持)。

## 10. Implementation Summary
### 実装完了 (全 Phase 成功)
- **Phase 1 ✅**: `TestStatisticsSupportDataType` を新API用に更新 (Red 状態達成)
  - `test_init_with_valid_type_name` → `test_constructor_with_supported_snowflake_type`
  - `test_from_snowflake_type_unsupported_raises_error` → `test_from_snowflake_type_unsupported_returns_none`
  - 新規追加: `test_constructor_with_unsupported_snowflake_type_raises_value_error`
- **Phase 2 ✅**: `test_table_metadata` Optional 化対応 (Red 状態達成)
  - `test_unsupported_data_type_raises_error` → `test_statistics_type_is_none_for_unsupported_type`
- **Phase 3 ✅**: `test_column_analysis` は既存実装で互換していることが判明 (テスト変更不要)
- **Phase 4 ✅**: `StatisticsSupportDataType` クラス再設計実装
  - フィールド: `type_name: Literal[...]` → `snowflake_type: SnowflakeDataType` + `_classification` (内部)
  - コンストラクタ: 未サポート型で ValueError 発生
  - `type_name` プロパティで互換性維持
  - `from_snowflake_type`: ValueError を握りつぶし None 返却
- **Phase 4-2 ✅**: `TableColumn.statistics_type` を Optional 型に変更
- **Phase 5 ✅**: `select_and_classify_columns` を None 判定に変更 (try/except 排除)
  - 理由文字列生成は同一フォーマット維持
- **Phase 6 ✅**: 全テスト実行・確認 (284 passed)
- **Phase 7 ✅**: Ruff チェック・フォーマット完了

### 技術成果
- **破壊的変更の最小化**: `.type_name` プロパティ互換により既存コードの大半が無変更
- **型安全性向上**: Optional 型により非サポート列の扱いが明示的
- **例外制御フロー排除**: try/except → None 判定によるクリーンなコード
- **テスト網羅性**: 20 + 15 + 5 = 40件の関連テスト全て Green
- **後方互換性**: 理由文字列 ("Unsupported Snowflake data type for statistics: <RAW>") 完全維持

### API 変更サマリ
| 変更 | 旧 | 新 |
|------|----|----|
| コンストラクタ | `StatisticsSupportDataType("numeric")` | `StatisticsSupportDataType(SnowflakeDataType("NUMBER"))` |
| from_snowflake_type 非サポート | ValueError | None |
| TableColumn.statistics_type | StatisticsSupportDataType | StatisticsSupportDataType \| None |
| .type_name アクセス | フィールド | プロパティ (互換) |

### 影響ファイル
- **実装**: `data_types.py`, `table_metadata.py`, `_column_analysis.py`
- **テスト**: `test_data_types.py`, `test_table_metadata.py`
- **未変更 (設計通り)**: SQL生成, 結果パーサ, その他ハンドラ

## 11. 未決事項 (現時点なし)

## 12. 次アクション
- 本計画内容の承認 or 修正フィードバック。
- 承認後 Phase 1 から順次 TDD 実行。

---
(承認待ち)
