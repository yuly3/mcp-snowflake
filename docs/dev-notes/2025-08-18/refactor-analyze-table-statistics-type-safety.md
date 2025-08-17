# Refactor: AnalyzeTableStatisticsTool 正常系型安全化

## 日付
2025-08-18

## ユーザープロンプト
> ゴール: AnalyzeTableStatisticsTool.perfomの正常系ハンドリングがtype safeになる
> select_and_classify_columnsを「supported_columnsが無かった」ことを意味する新規構造を返却するように拡張（戻り値が`ClassifiedColumns | ColumnDoesNotExist | {新規class}`となる）
> 実装手順にはTDDアプローチを採用、function/classレベルでred->implement->greenのサイクルを実行
> 開発ノートを出力して待機

## 背景 / 現状課題
現状 `select_and_classify_columns` は `ClassifiedColumns | ColumnDoesNotExist` を返却。サポート対象列が 0 のケースを型で区別せず、handler で `if not supported_columns:` により `ColumnDoesNotExist` (not_existed_columns=[]) を再構築して "No supported columns" ケースを表現している。これは:
- 列未存在エラーと "サポート列ゼロ" を同じ型で曖昧に表現
- 空リスト判定ロジックに依存し型安全性が低い
- 責務が `select_and_classify_columns` と handler に分散

## 目的
1. "サポート列ゼロ" を新しい型 `NoSupportedColumns` で明示化
2. `AnalyzeTableStatisticsTool.perform` のパターンマッチで状態分岐を型で完全に表現 (空リスト判定排除)
3. 既存の外部 API / エラーメッセージ互換性維持

## スコープ
- 変更ファイル:
  - `src/mcp_snowflake/handler/analyze_table_statistics/_types.py`
  - `src/mcp_snowflake/handler/analyze_table_statistics/_column_analysis.py`
  - `src/mcp_snowflake/handler/analyze_table_statistics/__init__.py`
  - `src/mcp_snowflake/tool/analyze_table_statistics.py`
- 新規テスト:
  - `tests/handler/analyze_table_statistics/test_column_classification.py`
- 既存テストは文言変えないことで基本無修正通過想定

## 新規型設計
```python
@attrs.define(frozen=True, slots=True)
class NoSupportedColumns:
    unsupported_columns: list[TableColumn]
```
意味: (指定 or 全) 列は存在確認済みだが統計サポート可能な列が一つもない。

## 関数仕様更新
`select_and_classify_columns` 戻り値: `ClassifiedColumns | ColumnDoesNotExist | NoSupportedColumns`
- 優先順位:
  1. 未存在列がある -> `ColumnDoesNotExist`
  2. 未存在列なし & supported 0 -> `NoSupportedColumns`
  3. それ以外 -> `ClassifiedColumns`

## handler 振る舞い
`handle_analyze_table_statistics` 戻り値:
`AnalyzeTableStatisticsJsonResponse | ColumnDoesNotExist | NoSupportedColumns`
- `NoSupportedColumns` はそのまま上位へ返却 (従来の再構築削除)。

## tool 振る舞い
`perform` 内 pattern match:
- `ColumnDoesNotExist(not_existed_columns=[...])` -> "Columns not found" エラー
- `NoSupportedColumns(unsupported_columns=[...])` -> "No supported columns for statistics. Unsupported columns: ..."
- `AnalyzeTableStatisticsJsonResponse` -> 成功 (要約 + JSON)

## 後方互換性
- 外部 JSON 成功レスポンス構造: 変更なし
- エラーメッセージ文字列: 変更なし
- 追加された内部型のみ

## TDD サイクル計画
### Cycle 1 (RED)
目的: `select_and_classify_columns` がサポート列ゼロで `NoSupportedColumns` を返すテスト追加。
手順:
1. 新規テスト `test_column_classification.py` 作成
2. サポート不可型のみのダミー列リストを用意 (VARIANT/BINARY 等)
3. アサート: 戻り値が `NoSupportedColumns` 型で unsupported_columns の数一致
期待: まだ型未実装で ImportError or AttributeError で RED

### Cycle 2 (GREEN)
1. `_types.py` に `NoSupportedColumns` 追加
2. `_column_analysis.py` 内で supported 0 の場合 `NoSupportedColumns` を返却
3. テスト実行 -> GREEN

### Cycle 3 (RED)
目的: handler が `NoSupportedColumns` をそのまま返却するテスト
1. 既存 handler 用テストファイル(なければ新規)に `NoSupportedColumns` ケース: モック describe_table が unsup 列のみ返却、analyze は呼ばれないことを確認 (spy/monkeypatch) もしくは simpler: 戻り値型 instance チェック
期待: 現状 handler は ColumnDoesNotExist を返すため失敗

### Cycle 4 (GREEN)
1. handler の match 分岐に `NoSupportedColumns` 追加 & 旧 if supported==0 ロジック削除
2. テスト GREEN

### Cycle 5 (RED)
目的: tool.perform が `NoSupportedColumns` を正しくメッセージ化
1. 既存 `test_perform_with_no_supported_columns` を拡張/新規テストで内部呼び出し結果がメッセージ表示されることを確認 (既存テキストは変わらないので内部型差分検証には monkeypatch で handler を直接呼び出す stub を仕込むか、 handler をモックして `NoSupportedColumns` を返させる)
2. 現状 tool は ColumnDoesNotExist の空 not_existed_columns 分岐なので RED

### Cycle 6 (GREEN)
1. `tool/analyze_table_statistics.py` に `NoSupportedColumns` 分岐追加
2. テスト GREEN

### Cycle 7 (Refactor / Cleanup)
- 型アノテーション更新 (`handle_analyze_table_statistics` 戻り値 Union)
- 不要コメント削除
- Ruff / format

## テスト追加方針詳細
ファイル: `tests/handler/analyze_table_statistics/test_column_classification.py`
- Test 1: only unsupported -> NoSupportedColumns
- Test 2: missing columns -> ColumnDoesNotExist (優先順位確認)
- Test 3: supported + unsupported -> ClassifiedColumns (既存パス確認) (任意)

## 想定されるリスクと対策
| リスク | 対策 |
|--------|------|
| 既存テストが ColumnDoesNotExist を想定している | メッセージ不変なのでパス。内部型非依存確認 |
| 新規 Union 拡張で型警告 | mypy 等未導入。型ヒント更新で解消 |
| ツール match 漏れ | TDD Cycle 5/6 で検出 |

## 実行順序 (コマンド例)
1. RED: `uv run pytest tests/handler/analyze_table_statistics/test_column_classification.py::TestSelectAndClassifyColumns::test_no_supported_columns`
2. 実装 -> GREEN
3. 以降サイクルごとに対象テスト実行 → 全体回帰 `uv run pytest -q`
4. Lint/format: `uv run ruff check --fix --unsafe-fixes . && uv run ruff format .`

## 今後の拡張余地 (Out of Scope)
- 成功レスポンスも attrs/dataclass 化し discriminated union による完全型安全
- エラー型を統一して `error_type: Literal[...]` 付き構造体返却

## 次アクション (待機中)
ユーザー承認後、Cycle 1 から着手。

---
ステータス: ✅ **実装完了** (2025-08-18)

## 実装結果

### TDD サイクル実行結果
- **Cycle 1 (RED)**: ✅ `select_and_classify_columns` の `NoSupportedColumns` テスト追加→ImportError で RED 成功
- **Cycle 2 (GREEN)**: ✅ `NoSupportedColumns` 型追加、関数拡張→テスト GREEN 成功
- **Cycle 3 (RED)**: ✅ handler テスト追加→UnboundLocalError で RED 成功
- **Cycle 4 (GREEN)**: ✅ handler の match 分岐拡張→テスト GREEN 成功
- **Cycle 5 (RED)**: ✅ tool テスト追加→TypeError で RED 成功
- **Cycle 6 (GREEN)**: ✅ tool の pattern match 拡張→テスト GREEN 成功
- **Cycle 7 (Refactor)**: ✅ 型ヒント更新、Lint/Format、既存テスト修正→全テスト通過

### 変更されたファイル
1. **`src/mcp_snowflake/handler/analyze_table_statistics/_types.py`**
   - `NoSupportedColumns` 型追加

2. **`src/mcp_snowflake/handler/analyze_table_statistics/_column_analysis.py`**
   - 戻り値型を `ClassifiedColumns | ColumnDoesNotExist | NoSupportedColumns` に拡張
   - supported_columns が空の場合に `NoSupportedColumns` を返却するロジック追加

3. **`src/mcp_snowflake/handler/analyze_table_statistics/__init__.py`**
   - `handle_analyze_table_statistics` 戻り値型拡張
   - match 分岐に `NoSupportedColumns` 追加
   - 旧二段階チェック削除（型安全性向上）

4. **`src/mcp_snowflake/tool/analyze_table_statistics.py`**
   - match 分岐に `NoSupportedColumns` 追加
   - 旧 `ColumnDoesNotExist(existed_columns=...)` 分岐削除
   - 型安全な pattern match 完成

### 新規テストファイル
1. **`tests/handler/analyze_table_statistics/test_column_classification.py`**
   - `select_and_classify_columns` の分類動作検証（3ケース）
   - NoSupportedColumns, ColumnDoesNotExist, ClassifiedColumns 各パターン

2. **`tests/handler/analyze_table_statistics/test_handler.py`**
   - handler が NoSupportedColumns を正しく返却することを確認

3. **`tests/tool/analyze_table_statistics/test_no_supported_columns_handling.py`**
   - tool.perform が NoSupportedColumns を適切なエラーメッセージに変換することを確認

### 既存テスト修正
- `tests/handler/.../main/test_error_cases.py` - NoSupportedColumns 期待に変更
- `tests/handler/.../main/test_partial_results.py` - NoSupportedColumns 期待に変更

### 達成された目標
✅ **型安全性**: NoSupportedColumns により「列未存在」と「サポート列ゼロ」を型で明確分離
✅ **責務分離**: select_and_classify_columns が完全な状態分類を担当、handler の二段階判定削除
✅ **パターンマッチ**: tool.perform で全ケースを型安全に処理、空リスト判定排除
✅ **後方互換性**: 外部 API (JSON構造・エラーメッセージ) 完全保持
✅ **テスト網羅性**: RED-GREEN-Refactor で各レイヤーの動作を段階的に検証

### 最終確認
- 全既存テスト: ✅ 通過 (メッセージ文言変更なし)
- 新規テスト: ✅ 通過 (6テスト追加)
- Lint/Format: ✅ 適用
- 型安全性: ✅ Union 拡張完了、cast 最小限使用

**リファクタ完了 - 型安全な正常系ハンドリング実現**
