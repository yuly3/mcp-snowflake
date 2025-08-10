# implement-partial-analyze-table-statistics

日付: 2025-08-11

## ユーザ要望（原文）
- analyze_table_statistics では unsupported なカラムがあった場合エラーメッセージを返却するが、supported なカラムのみでクエリを実行し、unsupported なカラムの存在と合わせて返却する。
- supported=0 の場合はエラーメッセージ（mcp.types.TextContent、unsupported 一覧・理由付き）を返却
- レスポンス schema に追加するのは unsupported_columns のみでよい（{ name: str, data_type: str }、reason 不要）
- supported>=1 かつ unsupported>=1 の場合、summary_text 内で unsupported の存在に言及
- cattrs 関連の改修はないはず、AnalyzeTableStatisticsJsonResponse の field を増やす対応は必要

## 合意した実装方針（提案）
以下の変更で、部分的な結果（partial）を返却できるようにします。既存の成功パスは維持し、混在時（supported>=1 かつ unsupported>=1）は「正常（部分結果）」扱いにします。

### 変更点サマリ
- SQL は supported 列のみで実行（unsupported は除外）
- JSON 応答に unsupported_columns: [{name, data_type}] を新設
- supported=0 のときは SQL 実行を行わず、TextContent でエラーを返却（unsupported 一覧は「name(type)」をコンマ区切りで列挙）
- summary_text に unsupported の存在を一行言及（混在ケースのみ）

## 具体的なシグネチャ変更（提案：確定待ち）

### 型定義（_types.py）
- import 追加: `from typing import NotRequired`
- 新規:
  - class UnsupportedColumnDict(TypedDict):
    - name: str
    - data_type: str
- 変更:
  - class TableStatisticsDict(TypedDict):
    - table_info: TableInfoDict
    - column_statistics: dict[str, StatsDict]
    - unsupported_columns: NotRequired[list[UnsupportedColumnDict]]  ← 省略可能（存在時のみ出力）
- 変更なし:
  - AnalyzeTableStatisticsJsonResponse は `table_statistics: TableStatisticsDict` のまま（構造は維持）

注: Optional は Python 3.13 で `typing.NotRequired` を使用（本プロジェクトは `requires-python = ">=3.13"`）。

### レスポンスビルダー（_response_builder.py）
- 変更（引数を追加）:
  - 変更前:
    - def build_response(args, result_row, columns_to_analyze) -> list[types.Content]
  - 変更後（案）:
    - def build_response(
        args: AnalyzeTableStatisticsArgs,
        result_row: Mapping[str, Any],
        columns_to_analyze: Sequence[TableColumn],
        unsupported_columns: Sequence[TableColumn] = (),
      ) -> list[types.Content]
    - デフォルト空タプルで後方互換を確保
- 追加仕様:
  - JSON `table_statistics` に `unsupported_columns` を付与（存在時のみ）。name/data_type のみを出力
  - summary_text に unsupported の存在を言及（文言確定: "Note: Some columns were not analyzed due to unsupported data types. N column(s) skipped.")

### 列選別ロジック（_column_analysis.py）
- 新規（分類関数の導入：既存テスト互換のため新関数で提供）:
  - UnsupportedInfo 型（内部用）:
    - tuple[TableColumn, str]  # (列, reason)
  - def select_and_classify_columns(
        all_columns: list[TableColumn],
        requested_columns: Sequence[str],
      ) -> types.TextContent | tuple[list[TableColumn], list[UnsupportedInfo]]
    - 仕様:
      - missing column/空リスト等の既存エラーパスは TextContent を返却（現行踏襲）
  - 正常時は (supported_columns, unsupported_info) を返す
  - reason は内部保持にとどめ、エラーメッセージには出力しない（ユーザ合意により name と type のみ列挙）
- 変更なし:
  - 既存 validate_and_select_columns は互換維持のためそのまま（従来通り unsupported 存在でエラーを返す）。
  - ハンドラ側は新関数 select_and_classify_columns を使用し、旧関数を温存。

### ハンドラ（__init__.py の handle_analyze_table_statistics）
- 変更点:
  - validate_and_select_columns の代わりに select_and_classify_columns を使用
  - supported==0 のとき:
    - SQL 実行をスキップ
    - TextContent でエラーメッセージを返却（「全列 unsupported」である旨＋各列を "name(type)" 形式でコンマ区切り列挙）
  - supported>=1 のとき:
    - effect.analyze_table_statistics は supported 列のみ渡して実行
    - build_response に unsupported_columns として TableColumn の配列を渡す（JSON は name/data_type のみ埋め込み）

## 出力契約（I/O）
- 正常（supported>=1）:
  - Text: summary_text（既存＋unsupported の存在を一行言及（混在時のみ））
  - JSON: {
      "table_statistics": {
        "table_info": {...},
        "column_statistics": {...},
        "unsupported_columns"?: [{"name": str, "data_type": str}],
      }
    }
- エラー（supported=0）:
  - TextContent 1件のみ
  - 例:
  - Error: No supported columns for statistics. Unsupported columns: col1(VARIANT), col2(OBJECT)

## テスト計画（抜粋）
- 混在ケース（supported>=1, unsupported>=1）
  - JSON に unsupported_columns が含まれる
  - summary_text に unsupported 言及がある
  - column_statistics は supported 列のみ
- 全列 unsupported（supported=0）
  - TextContent 単一返却
  - メッセージに name(type) がコンマ区切りで列挙される
- 回帰（全列 supported）
  - 既存の構造・値が維持（unsupported_columns は absent または空）

## 非機能・互換性
- 新フィールドは Optional のため後方互換
- SQL 複雑度は減少/不変（unsupported 除外）
- cattrs の追加改修なし

## オープンな確認事項（要ご判断）
（確定済み）
1) `unsupported_columns` の配置は `table_statistics` 直下 → OK
2) summary_text の文言は "Note: Some columns were not analyzed due to unsupported data types. N column(s) skipped." → OK
3) エラーメッセージ（supported=0）内の列挙は「Error: No supported columns for statistics. Unsupported columns:」の後に "name(type)" をコンマ区切り → OK

---
承認いただければ、この計画に沿って実装・テスト・ノートの「完了報告」更新まで進めます。

## 実装手順（TDDで進行）
各モジュール単位で red → implement → green のサイクルを回します。テストは最小のハッピーパス＋境界/エラーを先に赤にしてから実装します。

1) _types.py（JSONスキーマ拡張）
  - RED: tests/handler/analyze_table_statistics/test_types.py に `unsupported_columns` の存在を検証する新テストを追加（欠落で失敗）
  - Implement: UnsupportedColumnDict と TableStatisticsDict の NotRequired[unsupported_columns] を追加
  - GREEN: 型テストが通ることを確認

2) _column_analysis.py（分類関数の新設）
  - RED: tests/handler/analyze_table_statistics/test_column_analysis.py に `select_and_classify_columns` の新テストを追加
    - 正常: supported/unsupported が期待どおりに分類される
    - エラー: 存在しない列/空リストは TextContent を返す
  - Implement: `select_and_classify_columns` を実装（既存 validate_and_select_columns は現状維持）
  - GREEN: 新規テストが通ることを確認

3) _response_builder.py（unsupported_columns の出力＋summary 追記）
  - RED: tests/handler/analyze_table_statistics/test_response_builder.py に以下を追加
    - unsupported_columns が存在する場合、JSON に name/data_type が出力される
    - summary_text に注意文が追記される
  - Implement: build_response に `unsupported_columns` 引数（デフォルト空）を追加し、JSON/summary を拡張
  - GREEN: テストが通ることを確認

4) __init__.py（ハンドラ分岐とエラーメッセージ）
  - RED: tests/handler/analyze_table_statistics/test_main.py（または main/ ディレクトリの統合テスト）に以下を追加
    - 混在: 正常完了、JSON に unsupported_columns を含み、summary に注意文
    - 全列unsupported: TextContent 単体で "Error: No supported columns for statistics. Unsupported columns: name(type), ..." を返す
  - Implement: ハンドラで `select_and_classify_columns` を使用。supported==0 は SQL 実行せず TextContent を返却。supported>=1 は supported のみで実行し、build_response に unsupported を渡す
  - GREEN: 統合テストが通ることを確認

5) 品質ゲート
  - ruff: `uv run ruff check --fix --unsafe-fixes .` / `uv run ruff format .`
  - pytest（段階ごと）: 対象テストを個別に → 最後に `uv run pytest -q`

備考
- 既存テストを壊さないため、旧API（validate_and_select_columns）は互換維持。新関数を追加で導入します。
- 実装順は依存の浅い型→関数→ビルダー→ハンドラの順で進め、段階ごとに green を確認します。

---

## 実装完了報告（2025-08-11）

### 実装内容
TDDアプローチで以下を順次実装しました：

1) **_types.py**: UnsupportedColumnDict + TableStatisticsDict拡張 ✅
   - UnsupportedColumnDict (name: str, data_type: str) を新設
   - TableStatisticsDict に NotRequired[unsupported_columns] を追加
   - テスト: 4/4 passed

2) **_column_analysis.py**: select_and_classify_columns新設 ✅
   - 既存 validate_and_select_columns は互換維持
   - 新関数で supported/unsupported を分類して返却
   - テスト: 5/5 passed

3) **_response_builder.py**: unsupported_columns出力+summary追記 ✅
   - build_response に unsupported_columns 引数（デフォルト空）追加
   - JSON に unsupported_columns フィールド（存在時のみ）
   - summary_text に "Note: Some columns were not analyzed..." 追記
   - テスト: 8/8 passed（既存+新規）

4) **__init__.py**: ハンドラ分岐+エラーメッセージ ✅
   - select_and_classify_columns を使用に変更
   - supported=0: TextContent でエラー返却（"name(type)" カンマ区切り）
   - supported>=1: 部分結果で成功（build_response に unsupported も渡す）
   - テスト: 4/4 passed（新規）+ 既存テスト修正

5) **品質ゲート** ✅
   - ruff check + format: All checks passed!
   - pytest 全体: 285/285 passed
   - 回帰テスト: analyze_table_statistics 52/52 passed

### 変更ファイル
- `src/mcp_snowflake/handler/analyze_table_statistics/_types.py`
- `src/mcp_snowflake/handler/analyze_table_statistics/_column_analysis.py`
- `src/mcp_snowflake/handler/analyze_table_statistics/_response_builder.py`
- `src/mcp_snowflake/handler/analyze_table_statistics/__init__.py`
- `tests/handler/analyze_table_statistics/test_types.py`
- `tests/handler/analyze_table_statistics/test_column_analysis.py`
- `tests/handler/analyze_table_statistics/test_response_builder.py`
- `tests/handler/analyze_table_statistics/main/test_partial_results.py` (新規)
- `tests/handler/analyze_table_statistics/main/test_error_cases.py` (テスト修正)

### 動作確認済みシナリオ
- ✅ 混在（supported>=1 + unsupported>=1）: JSON に unsupported_columns、summary に注意書き
- ✅ 全列 unsupported（supported=0）: TextContent エラー「Error: No supported columns for statistics. Unsupported columns: name(type), ...」
- ✅ 全列 supported: 従来通り（unsupported_columns なし）
- ✅ 指定列の混在: 期待どおりの分類・処理
- ✅ 後方互換性: 既存の build_response(args, result_row, columns_to_analyze) 呼び出しも動作

### 最終確認
- JSON スキーマ: unsupported_columns は NotRequired のため後方互換
- エラーメッセージ: name(type) 形式でユーザフレンドリー
- パフォーマンス: unsupported 除外により SQL 複雑度は減少
- cattrs: 追加改修不要（JSON の自動変換は既存機構を利用）

**実装完了**: 要求仕様を満たし、TDD で品質確保、回帰なしで deployment ready です。
