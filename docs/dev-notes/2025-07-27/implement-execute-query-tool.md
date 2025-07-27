# SQL実行ツールの実装

## ユーザーリクエスト

任意のread sqlを受け取り実行、結果を返却するtoolを実装して。前準備であるSQL解析を実装した時の開発ノートが`docs/dev-notes/2025-07-27/implement-sql-write-detector.md`にあるので参考にすると良いだろう。

## 実装計画

### 1. 新しいツールの実装

1. **execute_queryツールの作成**
   - SQL文を受け取り、Write操作をチェック
   - Read SQLのみを実行許可
   - Snowflakeでのクエリ実行と結果返却

2. **セキュリティ機能**
   - 既存のSQL Write検知器を使用した安全性チェック
   - Write SQLが検出された場合はエラーを返す

3. **結果の表示**
   - `sample_table_data`と同様のJSON構造でテーブル形式の結果を表示
   - データの型変換とwarning処理
   - エラーハンドリング

### 2. 実装ファイル

- `src/mcp_snowflake/handler/execute_query.py` - ハンドラー実装
- `src/mcp_snowflake/tool/execute_query.py` - ツール実装
- `tests/handler/test_execute_query.py` - テスト実装

### 3. 安全性の確保

- SQL Write検知器による事前チェック
- Read操作のみ許可
- 適切なエラーハンドリング

### 4. JSON応答構造（sample_table_dataと同様）

```json
{
  "query_result": {
    "sql": "SELECT * FROM table",
    "execution_time_ms": 150,
    "row_count": 25,
    "columns": ["col1", "col2", "col3"],
    "rows": [
      {"col1": "value1", "col2": "value2", "col3": "value3"}
    ],
    "warnings": ["warning message if any"]
  }
}
```

## 実装開始日時

2025-07-27 18:35

## 実装内容

### 1. SQL実行ツールの実装完了

#### 実装ファイル
- `src/mcp_snowflake/handler/execute_query.py` - ハンドラー実装
- `src/mcp_snowflake/tool/execute_query.py` - ツール実装
- `tests/handler/test_execute_query.py` - 包括的なテスト

#### 主要機能
- **ExecuteQueryToolクラス**: 安全なSQL実行の中核クラス
- **TypedDict型定義**: `QueryResultDict`と`ExecuteQueryJsonResponse`で型安全性を確保
- **安全性重視の設計**: Write SQL検知器によりWrite操作を自動ブロック
- **統一されたJSON応答**: `sample_table_data`と同様の構造でテーブル形式の結果を返却

#### セキュリティ機能
- **SQL Write検知器連携**: 既存の`SQLWriteDetector`を使用してWrite操作を事前チェック
- **Read操作限定**: SELECT, SHOW, DESCRIBE, EXPLAIN等のみ実行許可
- **タイムアウト保護**: デフォルト30秒、最大300秒の実行時間制限

#### JSON応答構造

```json
{
  "query_result": {
    "sql": "SELECT * FROM users LIMIT 5",
    "execution_time_ms": 150,
    "row_count": 5,
    "columns": ["id", "name", "email"],
    "rows": [
      {"id": 1, "name": "Alice", "email": "alice@example.com"}
    ],
    "warnings": []
  }
}
```

#### 多言語対応
- **docstringとコメント**: 日本語対応
- **例外メッセージとサーバーレスポンス**: 英語で統一

### 2. テスト実装

- **8つのテストケース**で網羅的検証
- Write SQL検知テスト
- 空結果処理テスト
- タイムアウト設定テスト
- エラーハンドリングテスト
- データ処理機能テスト
- JSON応答フォーマットテスト

### 3. 型安全性の確保

- TypedDictを使用した戻り値の型定義
- Pydanticを使用した引数バリデーション
- 全メソッドに適切な型注釈

### 4. CLI統合

- `__main__.py`に`ExecuteQueryTool`を追加
- MCPサーバーに自動登録

## 完了日時

2025-07-27 18:50

## 使用可能なクエリ例

```sql
-- データ取得
SELECT * FROM users LIMIT 10;
SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department;

-- メタデータ確認
SHOW TABLES IN SCHEMA analytics;
DESCRIBE TABLE sales_data;

-- パフォーマンス分析
EXPLAIN SELECT * FROM large_table WHERE indexed_column = 'value';

-- Snowflake専用構文
SELECT * FROM table SAMPLE (10 ROWS);
SHOW WAREHOUSES;
```

## 次のステップ

安全で柔軟なSQL実行基盤が完成したため、以下の拡張が可能：

1. **クエリ結果キャッシュ機能**
2. **クエリ履歴管理**
3. **パフォーマンスメトリクス収集**
4. **バッチクエリ実行**

### 1. execute_queryツールの実装完了

#### 実装ファイル
- `src/mcp_snowflake/handler/execute_query.py` - ハンドラー実装
- `src/mcp_snowflake/tool/execute_query.py` - ツール実装
- `tests/handler/test_execute_query.py` - 包括的なテスト（8テストケース）

#### 主要機能
- **SQL安全性チェック**: 既存のSQL Write検知器を使用してWrite操作を事前ブロック
- **クエリ実行**: Snowflakeでの安全なRead SQLの実行
- **結果処理**: `sample_table_data`と同様の構造でJSON形式の結果返却
- **エラーハンドリング**: 適切なエラーメッセージとログ出力

#### セキュリティ機能
- Write SQL（INSERT、UPDATE、DELETE、CREATE等）を自動検出してブロック
- Read SQL（SELECT、SHOW、DESCRIBE、EXPLAIN等）のみ実行許可
- タイムアウト設定によるリソース保護（デフォルト30秒、最大300秒）

#### JSON応答構造
```json
{
  "query_result": {
    "sql": "実行されたSQL文",
    "execution_time_ms": 150,
    "row_count": 25,
    "columns": ["列名1", "列名2", "列名3"],
    "rows": [
      {"列名1": "値1", "列名2": "値2", "列名3": "値3"}
    ],
    "warnings": ["警告メッセージがあれば"]
  }
}
```

#### データ型変換とWarning処理
- `sample_table_data`と同じ`cattrs`ベースの型変換システムを使用
- 対応していないデータ型は適切なwarningメッセージと共に`<unsupported_type: 型名>`として表示

### 2. システム統合

#### SnowflakeClientの拡張
- `execute_query`メソッドを追加（汎用的なクエリ実行機能）
- 既存の特化メソッド（`list_schemas`等）との一貫性を保持

#### MCP Server統合
- CLIにツールを登録（`__main__.py`）
- ツールの定義とハンドリング機能
- 既存ツールとの統一的なインターフェース

### 3. テスト実装

- **8つのテストケース**で包括的検証
  - 正常なクエリ実行テスト
  - Write SQL検知・ブロックテスト
  - 空の結果セットテスト
  - カスタムタイムアウトテスト
  - エラーハンドリングテスト
  - データ処理機能テスト
  - レスポンス形式テスト

- **全テスト通過**: 既存105テスト + 新規8テスト = 全113テスト成功

### 4. 型安全性の確保

- TypedDictを使用した`QueryResultDict`と`ExecuteQueryJsonResponse`
- Pydanticを使用した`ExecuteQueryArgs`でバリデーション
- 全メソッドに適切な型注釈

## 完了日時

2025-07-27 18:50

## 使用例

```sql
-- 成功例（Read操作）
SELECT id, name, email FROM users LIMIT 10;
SHOW TABLES IN SCHEMA my_schema;
DESCRIBE TABLE my_table;
EXPLAIN SELECT * FROM large_table WHERE id = 1;

-- ブロック例（Write操作）
INSERT INTO users (name) VALUES ('test');  -- ❌ ブロックされる
UPDATE users SET name = 'new' WHERE id = 1;  -- ❌ ブロックされる
DELETE FROM users WHERE id = 1;  -- ❌ ブロックされる
CREATE TABLE test (id INT);  -- ❌ ブロックされる
```

## 次のステップ

安全なSQL実行ツールが完成したため、以下の機能拡張が可能になりました：
1. 複雑なJOINクエリやデータ分析
2. パフォーマンス分析（EXPLAIN文の活用）
3. メタデータの詳細調査
4. データ品質チェック用のクエリ実行

## 追加実装: データ処理の共通化

### 背景
`execute_query`と`sample_table_data`のハンドラーで重複したデータ処理実装があることが判明し、共通化を実施しました。

### 実装内容

#### 共通モジュールの作成
- `src/mcp_snowflake/handler/data_processing/` - 共通データ処理ディレクトリ
- `src/mcp_snowflake/handler/data_processing/common.py` - 共通処理実装
- `tests/handler/test_data_processing.py` - 共通モジュールのテスト

#### 強固な型定義
```python
class RowProcessingResult(TypedDict):
    """単一行処理結果の型定義."""
    processed_row: dict[str, Any]
    warnings: list[str]

class DataProcessingResult(TypedDict):
    """データ処理結果の型定義."""
    processed_rows: list[dict[str, Any]]
    warnings: list[str]
```

#### 統一された関数名
- `process_row_data()` - 単一行データ処理
- `process_multiple_rows_data()` - 複数行データ処理

#### 重複排除
- `execute_query.py`の`process_query_row_data`、`process_query_result_data`を削除
- `sample_table_data.py`の`process_row_data`、`process_sample_data`を削除
- 両ハンドラーで共通モジュールを使用

#### 多言語対応
- **docstringとコメント**: 日本語
- **例外メッセージとサーバーレスポンス**: 英語で統一

### テスト結果
- **共通モジュール**: 8テスト通過
- **全体**: 113テスト通過（+8テスト追加）

### 改善効果
- ✅ **重複コード削除**: 60行以上のコード重複を排除
- ✅ **型安全性向上**: TypedDictによる強固な型定義
- ✅ **保守性向上**: 単一責任による変更の影響範囲限定
- ✅ **テストカバレッジ向上**: 共通処理の包括的テスト

### 完了日時（共通化）
2025-07-27 19:05
