# Introduce StatisticsSupportColumn

## 1. 背景 / 動機
`TableColumn.statistics_type` を Optional 化した結果、統計処理経路 (SQL 生成・結果パース・レスポンス生成) で `statistics_type` へのアクセス時に型チェッカ (pyright 等) が `reportOptionalMemberAccess` を報告。ランタイム上は `select_and_classify_columns` でフィルタ済みだが、型上の保証が不足している。

目的: "統計計算対象である" ことを型で表現する薄いラッパ `StatisticsSupportColumn` を導入し、後続処理から Optional 分岐を排除し安全性と可読性を向上。

## 2. 要求 / スコープ
- 新クラス `StatisticsSupportColumn` を別ファイル ( `kernel/statistics_support_column.py`) に追加。
- 生成ファクトリ: `StatisticsSupportColumn.from_table_column(col: TableColumn) -> StatisticsSupportColumn | None`。
- 保持情報: `base: TableColumn`, `statistics_type: StatisticsSupportDataType (Non-Optional)`。
- 主要プロパティ委譲: `name`, `data_type`, `nullable`, `ordinal_position` (必要最小)。
- `__all__` に公開 (外部使用も許容)。
- 統計処理パイプライン (select → SQL生成 → 実行 → 結果パース → レスポンス) を `StatisticsSupportColumn` ベースに移行。
- 既存 JSON 出力互換性維持。
- Unsupported 列集合は引き続き `TableColumn` のまま扱い、理由文字列ロジック維持。

## 3. 非スコープ (今回含めない)
- `TableColumn` の構造変更。
- `StatisticsSupportDataType` の仕様変更。
- 追加のキャッシュ / パフォーマンス最適化。
- 外部公開ドキュメント (README) 更新 (必要なら後続タスク)。

## 4. 設計概要
```python
@attrs.define(frozen=True, slots=True)
class StatisticsSupportColumn:
    base: TableColumn
    statistics_type: StatisticsSupportDataType  # Non-Optional

    @property
    def name(self) -> str: return self.base.name
    @property
    def data_type(self) -> SnowflakeDataType: return self.base.data_type
    @property
    def nullable(self) -> bool: return self.base.nullable
    @property
    def ordinal_position(self) -> int: return self.base.ordinal_position

    @classmethod
    def from_table_column(cls, col: TableColumn) -> Self | None:
        st = col.statistics_type
        if st is None:
            return None
        return cls(base=col, statistics_type=st)
```

### 利用フロー差分
| ステップ | 現行 | 新方式 |
|----------|------|--------|
| 列分類 | `supported: list[TableColumn]` | `supported: list[StatisticsSupportColumn]` |
| SQL生成 | `TableColumn.statistics_type!` | `StatisticsSupportColumn.statistics_type` |
| 結果パース | 同左 | 同左 |
| レスポンス | `columns_to_analyze: Sequence[TableColumn]` | `Sequence[StatisticsSupportColumn]` |
| Unsupported | list[TableColumn] | 変更なし |

## 5. 影響範囲 (芋づる式)
| 種別 | 対象 | 変更内容 | 備考 |
|------|------|----------|------|
| 新規 | `kernel/statistics_support_column.py` | クラス定義 & テスト | 公開 |
| 関数 | `_column_analysis.select_and_classify_columns` | supported 生成で変換 | 戻り値型変更 |
| Protocol | `EffectAnalyzeTableStatistics.analyze_table_statistics` | 引数 columns 型変更 | 破壊的 (内部) |
| Adapter | `AnalyzeTableStatisticsEffectHandler.analyze_table_statistics` | シグネチャ更新 | 呼び出し側調整必要なし (同ファイル) |
| 関数 | `generate_statistics_sql` | 引数型変更 | 内部参照簡素化 |
| 関数 | `parse_statistics_result` | 引数型変更 | Optional除去 |
| 関数 | `build_response` | 引数型変更 | JSON 出力不変 |
| テスト | `test_column_analysis` | supported 要素型アサート更新 | 変換検証追加 |
| テスト | SQL生成関連 | 引数型に合わせ変換ヘルパー使用 | |
| テスト | result_parser/response_builder | 同上 | |
| エクスポート | `kernel/__init__.py` | `StatisticsSupportColumn` 追加 | |
| 型警告 | `.statistics_type.type_name` | 解消 | `Optional` 警告消滅 |

## 6. TDD 実行計画 (Phases)
| Phase | 目的 | 作業 | 期待 |
|-------|------|------|------|
| 0 | 基盤 | 新クラス + 単体テスト (from_table_column) | Green |
| 1 | 分類置換 | select_and_classify_columns 修正 + テスト更新 (RED→Green) | Green |
| 2 | Protocol/Adapter | Effect & Adapter シグネチャ更新 + モック修正 (RED→Green) | Green |
| 3 | SQL/Parser | generate_statistics_sql / parse_statistics_result 引数型変更 + テスト更新 | Green |
| 4 | Response | build_response シグネチャ変更 + テスト更新 | Green |
| 5 | ハンドラ統合 | handle_analyze_table_statistics 呼び出し確認 (全テスト) | Green |
| 6 | 型整備 | 不要アサーション削除 / 型警告再確認 | Green |
| 7 | Docs | 本ノートに実装サマリ追記 | - |
| 8 | 整理 | Ruff & 全テスト | Green |

## 7. テスト追加/変更ポイント
- 新: `tests/kernel/test_statistics_support_column.py` (予定)
  - Supported 変換成功ケース (numeric/string/date/boolean 各1)
  - Unsupported 変換 None (VARIANT/OBJECT など)
- 既存分類テスト: supported 要素型確認 (`isinstance(sc, StatisticsSupportColumn)`) および順序保持
- SQL/Parser/Response の fixture 作成部で `StatisticsSupportColumn.from_table_column` による事前変換ヘルパー追加

## 8. リスク & 緩和
| リスク | 内容 | 緩和 |
|--------|------|------|
| 破壊的変更 | 内部 Protocol シグネチャ変更 | 内部利用限定、CHANGELOG/ノート記述 |
| 重複プロパティ | TableColumn と二重管理 | `base` 委譲 + 不変設計で同期不要 |
| テスト拡散 | 多数の import 修正 | grep + incremental update |

## 9. ロールバック戦略
- 迅速ロールバック: 新クラス未使用に戻すパッチ (select_and_classify_columns の変換削除 + シグネチャ復元)。
- ステップごと commit で段階 revert 可能。

## 10. 実装詳細メモ (ドラフト)
- ファイル: `src/mcp_snowflake/kernel/statistics_support_column.py`
- `__all__` 追加: `"StatisticsSupportColumn"`
- 追加 import: `from .statistics_support_column import StatisticsSupportColumn`
- 変換ヘルパー (テスト用): `def to_supported(columns: Iterable[TableColumn]) -> list[StatisticsSupportColumn]: ...`
  (本体は不要なら実装見送り)

## 11. 追加検討（任意・今回は未実施）
- `__getattr__` 透過委譲による API 互換 (静的解析の明示性から不採用)
- `statistics_type_name` キャッシュ文字列プロパティ (不要)

## 12. 次アクション
- ✅ Phase 0 完了: StatisticsSupportColumn クラス作成 + テスト (12件すべて通過)
- ✅ Phase 1 完了: select_and_classify_columns 関数修正 (戻り値型変更)
- ✅ Phase 2 完了: Protocol & Adapter シグネチャ更新
- ✅ Phase 3 完了: SQL生成・結果パース関数更新
- ✅ Phase 4 完了: レスポンスビルダー更新
- ✅ Phase 5 完了: ハンドラ統合テスト (54件すべて通過)
- ✅ Phase 6 完了: 型警告解消確認 (ruff clean)
- ✅ Phase 7 完了: 本ノート完了報告更新

## 実装完了サマリー (2025-08-16)

### 達成項目
- **新型 StatisticsSupportColumn**: 統計対応列の型安全ラッパー実装
- **Optional 型チェック排除**: `.statistics_type.type_name` アクセス時の型警告解消
- **処理パイプライン更新**: 分類→SQL生成→実行→パース→レスポンスの全工程を StatisticsSupportColumn ベースに移行
- **テスト完全性**: 既存 54 テスト + 新規 12 テスト = 計 66 テスト全通過
- **後方互換性**: JSON 出力形式・エラーメッセージ形式は完全維持

### 技術的成果
- **型安全性向上**: `statistics_type` への Optional チェック不要
- **可読性改善**: 統計処理経路で列の統計対応が型レベルで保証
- **保守性向上**: 統計非対応列の混入を型システムで防止

### 破壊的変更の範囲 (内部のみ)
- Protocol: `EffectAnalyzeTableStatistics.analyze_table_statistics` 引数型
- Adapter: `AnalyzeTableStatisticsEffectHandler.analyze_table_statistics` 同上
- 内部関数: `generate_statistics_sql`, `parse_statistics_result`, `build_response`
- すべて内部 API のため外部影響なし

---
✅ **タスク完了**: StatisticsSupportColumn 導入による型安全性向上達成
