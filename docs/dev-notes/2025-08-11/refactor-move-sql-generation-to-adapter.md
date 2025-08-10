---
title: Move SQL generation for analyze_table_statistics from handler to adapter
date: 2025-08-11
author: automation
status: completed
---

# リファクタリング計画: SQL生成を adapter の責務へ移行（analyze_table_statistics）

> 注記（2025-08-11 追記）
>
> 本メモ内の ColumnInfo に関する記述は当時の設計背景の説明として残しています。現在の実装では、列メタデータは kernel 層の `TableColumn` に統一されており、adapter/handler は `TableColumn.statistics_type` 等のプロパティを使用します。詳細は「Refactor: Remove adapter dependency on ColumnInfo and consolidate into TableColumn」を参照してください。

## ユーザー要望（プロンプト）

- ゴール: SQL生成は adapter の責務だが analyze_table_statistics では handler が持っている。これを adapter に移行する。
- これに伴い EffectAnalyzeTableStatistics から EffectExecuteQuery を削除し、_execute_statistics_query に近い Effect method を新設。
- 新 method 名は analyze_table_statistics。
- 戻り値は単一行の dict。
- test_sql_generator.py は adapter のテストとして場所を移動（adapter テストディレクトリが無いため新規作成）。

## 目的と非目的

- 目的
  - SQL 生成責務を adapter に集約し、handler の責務を調整（制御・合成・レスポンス構築に集中）。
  - Effect インターフェースを実装意図に沿って簡潔化。
  - 振る舞いは現状維持（後方互換）。
- 非目的
  - ColumnInfo 型の所在整理（今回は据え置き）。
  - パフォーマンス最適化や追加機能の実装。

## 現状整理（抜粋）

- SQL 生成関数 generate_statistics_sql は handler/analyze_table_statistics/_sql_generator.py に存在。
- handler/analyze_table_statistics/__init__.py が _execute_statistics_query で SQL 生成と実行を行う。
- EffectAnalyzeTableStatistics は EffectDescribeTable と EffectExecuteQuery を継承（生 SQL 実行責務が混在）。
- adapter 側 AnalyzeTableStatisticsEffectHandler は DescribeTableEffectHandler と ExecuteQueryEffectHandler を多重継承。

## 合意事項（設計方針）

1) 新 Effect メソッド
   - 名称: analyze_table_statistics
   - 役割: 統計用 SQL の生成と実行を adapter 側で完結し、単一行の dict を返す。
   - 例外: 実行失敗または無データ時は例外を投げる（従来相当）。

2) プロトコル変更
   - EffectAnalyzeTableStatistics から EffectExecuteQuery の継承を削除。
   - 代わりに async def analyze_table_statistics(...) -> dict[str, Any] を追加。

3) SQL 生成の移設
   - 新規: adapter/analyze_table_statistics_sql.py に generate_statistics_sql を移動。
   - 既存の handler 側 _sql_generator.py は削除（または薄いフォワーダを一時置き、今回削除方針）。

4) handler 側の責務縮小
   - _execute_statistics_query を削除し、effect.analyze_table_statistics の呼び出しに一本化。
   - generate_statistics_sql の import/使用を排除。

5) adapter 実装
   - AnalyzeTableStatisticsEffectHandler は DescribeTableEffectHandler のみを継承に変更。
   - async def analyze_table_statistics(...) を実装：
     - generate_statistics_sql(...) で SQL を生成。
     - SnowflakeClient.execute_query(...) を呼び出し、先頭行のみ返す（無ければ例外）。

6) テスト移動・更新
   - tests/handler/analyze_table_statistics/test_sql_generator.py を tests/adapter/analyze_table_statistics/test_sql_generator.py に移動。
   - import を mcp_snowflake.adapter.analyze_table_statistics_sql.generate_statistics_sql へ変更。
   - MockEffectHandler（tests/handler/analyze_table_statistics/main/test_fixtures.py）に analyze_table_statistics(...) を追加し、従来の execute_query 経由を置換。
   - 既存の成功/失敗ケースは挙動不変となることを確認。

## 新メソッドの契約（I/O）

- シグネチャ（案）
  - async def analyze_table_statistics(
        database: str,
        schema_name: str,
        table_name: str,
        columns_to_analyze: Iterable[ColumnInfo],
        top_k_limit: int,
    ) -> dict[str, Any]

- 入力
  - database, schema_name, table_name: 対象テーブル識別子
  - columns_to_analyze: 既存 ColumnInfo 型（当面 handler 内の型を流用）
  - top_k_limit: 既存引数に合わせる

- 出力
  - 統計クエリ結果の単一行（dict[str, Any]）

- エラー
  - 実行失敗（クエリエラー等）または無データ時に例外を送出

## 変更ステップ（実装順）

1. プロトコル更新
   - src/mcp_snowflake/handler/analyze_table_statistics/models.py
     - EffectAnalyzeTableStatistics から EffectExecuteQuery を除去
     - analyze_table_statistics(...) を追加

2. SQL 生成の移植
   - 新規: src/mcp_snowflake/adapter/analyze_table_statistics_sql.py に generate_statistics_sql を移設
   - 既存: src/mcp_snowflake/handler/analyze_table_statistics/_sql_generator.py を削除

3. adapter 実装更新
   - src/mcp_snowflake/adapter/analyze_table_statistics_handler.py
     - ExecuteQueryEffectHandler の継承を削除
     - analyze_table_statistics(...) を実装（SQL 生成→実行→先頭行返却）

4. handler の簡素化
   - src/mcp_snowflake/handler/analyze_table_statistics/__init__.py
     - _execute_statistics_query を削除
     - handle_analyze_table_statistics 内で effect.analyze_table_statistics(...) を呼ぶよう変更

5. テストの移動・調整
   - 新規: tests/adapter/analyze_table_statistics/test_sql_generator.py
     - import を adapter 側に変更
   - 既存: tests/handler/analyze_table_statistics/main/test_fixtures.py
     - MockEffectHandler に analyze_table_statistics を実装
     - 不要になった execute_query のモックは削除または残置可（段階移行中は残置でも可）

## 影響ファイル一覧（予定）

- 追加
  - src/mcp_snowflake/adapter/analyze_table_statistics_sql.py
  - tests/adapter/analyze_table_statistics/test_sql_generator.py
- 変更
  - src/mcp_snowflake/handler/analyze_table_statistics/models.py
  - src/mcp_snowflake/adapter/analyze_table_statistics_handler.py
  - src/mcp_snowflake/handler/analyze_table_statistics/__init__.py
  - tests/handler/analyze_table_statistics/main/test_fixtures.py
- 削除
  - src/mcp_snowflake/handler/analyze_table_statistics/_sql_generator.py

## テスト戦略

- 既存の analyze_table_statistics の成功/エラー系テストがグリーンであることを確認。
- SQL 生成テストを adapter 配下に移動後も全アサーションが成立することを確認。
- 品質ゲート
  - uv run ruff check --fix --unsafe-fixes .
  - uv run ruff format .
  - uv run pytest --doctest-modules .

## リスクと対策

- ColumnInfo 型の所在（handler 依存）が adapter 参照となる点
  - 当面許容。将来的に kernel 等の共有層へ移設検討（別タスク）。
- 参照先変更による import 崩れ
  - 静的チェック（ruff）とユニットテストで検知。

## ロールアウト

- 上記順で段階的に変更し、各段階でテスト実行。
- 破壊的変更はプロトコルの一箇所のみ（EffectExecuteQuery 継承除去）。呼び出し側が Mock/Adapter のみであるため影響は局所的。

## 完了の定義（DoD）

- すべてのテストがグリーン。
- handler から SQL 生成ロジックが消えている。
- adapter に analyze_table_statistics と SQL 生成が実装されている。
- ドキュメント（本ファイル）に実装結果が追記されている。

## 実装後に本メモへ追記する内容（完了フェーズ）

### 実装結果サマリ

**完了日時**: 2025-08-11
**テスト結果**: 全 276 テスト通過（100%成功）

### 実装差分の要約

✅ **プロトコル更新完了**
- EffectAnalyzeTableStatistics から EffectExecuteQuery 継承を削除
- analyze_table_statistics(...) -> dict[str, Any] メソッドをプロトコルに追加

✅ **SQL生成の移植完了**
- src/mcp_snowflake/adapter/analyze_table_statistics_sql.py を新規作成
- generate_statistics_sql を handler から adapter に移設
- 旧 handler 側の _sql_generator.py を削除

✅ **adapter実装更新完了**
- AnalyzeTableStatisticsEffectHandler は DescribeTableEffectHandler のみ継承に変更
- analyze_table_statistics(...) メソッドを実装（SQL生成→実行→先頭行返却）

✅ **handler簡素化完了**
- _execute_statistics_query 関数を削除
- handle_analyze_table_statistics 内で effect.analyze_table_statistics(...) を呼ぶよう変更
- 不要な import（generate_statistics_sql, ColumnInfo等）を除去

✅ **テスト移動・調整完了**
- tests/adapter/analyze_table_statistics/ ディレクトリを新規作成
- test_sql_generator.py を adapter 配下に移設（import パス更新）
- MockEffectHandler に analyze_table_statistics(...) メソッドを追加
- 旧 handler 側の test_sql_generator.py を削除
- MockEffectWithQueryTracking（test_selection_cases.py）を新プロトコルに適合

### 発生した仕様変更や微調整

- **ColumnInfo 型の参照**: adapter が handler の型に依存する構造となった（計画通り）
- **テスト互換性**: 一部テストで独自 Mock を使用していたため、新プロトコルに適合させる調整が必要だった
- **Lint対応**: docstring の imperative mood 調整、未使用 import 削除等の細かい調整

### テスト結果のサマリ

- **SQL生成テスト**: tests/adapter/analyze_table_statistics/test_sql_generator.py で 8/8 通過
- **handler機能テスト**: tests/handler/analyze_table_statistics/ で 55/55 通過
- **全体テスト**: 276/276 通過（doctest含む）
- **品質チェック**: ruff check/format で全ファイル適合

### 責務分離の達成

- **handler**: SQL を一切持たず、制御・合成・レスポンス構築に専念
- **adapter**: SQL 生成から実行まで完結し、統計クエリ結果の単一行を返却
- **既存振る舞い**: 完全に保持（後方互換）

### 今後の改善検討項目

- ColumnInfo 型を kernel 層等の共有モジュールに移設し、依存方向を整理
- adapter 内の SQL 生成ロジックをさらに細分化（大規模になった場合）
