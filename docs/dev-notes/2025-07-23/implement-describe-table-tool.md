# Snowflake MCP Server - Describe Table Tool Implementation

## ユーザープロンプト

このリポジトリではSnowflake MCPサーバを実装している。新たにdescribe table toolを実装するのが今回の依頼。まずは既存のコードベースを確認して詳細な実装計画を提示してほしい。

## 既存コードベース分析

### 現在の実装状況

このプロジェクトは以下の3つのツールを既に実装している：

1. **list_schemas** - データベース内のスキーマ一覧を取得
2. **list_tables** - スキーマ内のテーブル一覧を取得  
3. **list_views** - スキーマ内のビュー一覧を取得

### アーキテクチャ構成

プロジェクトは以下の構成で実装されている：

```
src/mcp_snowflake/
├── handler/           # ビジネスロジック層
│   ├── list_schemas.py
│   ├── list_tables.py
│   └── list_views.py
├── tool/             # MCP Tool定義層
│   ├── base.py
│   ├── list_schemas.py
│   ├── list_tables.py
│   └── list_views.py
├── snowflake_client.py   # Snowflakeクライアント
├── settings.py          # 設定管理
└── __main__.py         # エントリーポイント
```

### 既存実装パターン

各ツールは以下のパターンで実装されている：

1. **Handler層** (`handler/`)
   - `Args` クラス（Pydanticモデル）でパラメータ定義
   - `Effect` プロトコルで抽象化された副作用定義
   - `handle_*` 関数でビジネスロジック実装

2. **Tool層** (`tool/`)
   - `Tool` 基底クラスを継承
   - MCP プロトコルに準拠したツール定義
   - パラメータ検証とhandler呼び出し

3. **Client層** (`snowflake_client.py`)
   - Snowflakeとの実際の通信処理
   - 非同期実行のためのThreadPoolExecutor使用

## Describe Table Tool 実装計画

### 1. 要求仕様

**機能**: 指定されたテーブルの構造（カラム情報）を取得する

**入力パラメータ**:
- `database` (string, required): データベース名
- `schema_name` (string, required): スキーマ名  
- `table_name` (string, required): テーブル名

**出力**: テーブルのカラム情報（カラム名、データ型、NULL可否、デフォルト値等）

### 2. 実装ファイル

以下のファイルを新規作成・更新する：

#### 新規作成ファイル
1. `src/mcp_snowflake/handler/describe_table.py`
2. `src/mcp_snowflake/tool/describe_table.py`
3. `tests/handler/test_describe_table.py`

#### 更新ファイル
1. `src/mcp_snowflake/handler/__init__.py`
2. `src/mcp_snowflake/tool/__init__.py`
3. `src/mcp_snowflake/snowflake_client.py`
4. `src/mcp_snowflake/__main__.py`

### 3. Snowflake SQLクエリ

テーブル構造の取得には以下のSQLを使用：

```sql
DESCRIBE TABLE {database}.{schema}.{table_name}
```

または詳細情報が必要な場合：

```sql
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COLUMN_DEFAULT,
    COMMENT
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = '{schema}' 
    AND TABLE_NAME = '{table_name}'
    AND TABLE_CATALOG = '{database}'
ORDER BY ORDINAL_POSITION
```

### 4. 実装ステップ

1. **Handler層の実装**
   - `DescribeTableArgs` クラスの定義
   - `EffectDescribeTable` プロトコルの定義
   - `handle_describe_table` 関数の実装

2. **Client層の実装**
   - `SnowflakeClient.describe_table()` メソッドの追加

3. **Tool層の実装**
   - `DescribeTableTool` クラスの実装
   - MCP tool定義の作成

4. **統合・登録**
   - `__main__.py` での新ツールの登録
   - `__init__.py` ファイルの更新

5. **テストの実装**
   - ユニットテストの作成
   - モックを使用したテスト

### 5. 出力フォーマット

テーブル構造情報は構造化JSON + 自然言語説明のハイブリッド形式で返す：

```
Table Schema: {database}.{schema}.{table_name}

This table has {column_count} columns with the following structure:

```json
{
  "table_info": {
    "database": "{database}",
    "schema": "{schema}",
    "name": "{table_name}",
    "column_count": 4,
    "columns": [
      {
        "name": "ID",
        "data_type": "NUMBER(38,0)",
        "nullable": false,
        "default_value": null,
        "comment": "Primary key for customer records",
        "ordinal_position": 1
      },
      {
        "name": "NAME",
        "data_type": "VARCHAR(100)",
        "nullable": true,
        "default_value": null,
        "comment": "Customer full name",
        "ordinal_position": 2
      }
    ]
  }
}
```

**Key characteristics:**
- Primary key: {primary_key_columns}
- Required fields: {required_fields}
- Optional fields: {optional_fields}
```

このハイブリッド形式の利点：
- **LLM理解性**: 自然言語での説明でコンテキスト提供
- **プログラム処理**: JSON部分で構造化データ提供  
- **一貫性**: 既存のtext形式を維持しつつ拡張
- **拡張性**: 将来的な情報追加が容易

### 6. エラーハンドリング

- テーブルが存在しない場合のエラー処理
- 権限がない場合のエラー処理
- ネットワークエラーの処理
- パラメータ検証エラーの処理

### 7. テスト戦略

- 正常系テスト（テーブル構造取得成功）
- 異常系テスト（テーブル不存在、権限エラー等）
- パラメータ検証テスト
- モックを使用した単体テスト

この実装計画に従って、既存のパターンを踏襲しながら describe table 機能を実装することで、一貫性のある高品質なコードを提供できます。

## 実装完了後の振り返り

実装が正常に完了し、実際のSnowflakeデータベースでの動作確認も成功しました。今後の同様の機能拡張タスクに向けて以下の改善点を特定しました。

### 今後の改善可能な点

#### 1. 型定義の早期策定
- **改善点**: TypedDictの定義を設計段階で策定すべき
- **理由**: 実装後に追加したが、最初から定義しておけばより型安全な実装が可能
- **具体例**: `TableJsonResponse`, `ColumnDict`等のレスポンス型を計画段階で定義

#### 2. プライマリキー検出の強化
- **改善点**: より正確なプライマリキー判定ロジック
- **現状**: 命名規則による推測のみ
- **提案**: Snowflakeの`INFORMATION_SCHEMA.TABLE_CONSTRAINTS`を活用した正確な検出

```sql
-- 正確なプライマリキー取得の例
SELECT COLUMN_NAME 
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? 
  AND CONSTRAINT_NAME LIKE 'PK_%'
```

#### 3. パフォーマンス考慮
- **改善点**: 大規模テーブルに対する最適化
- **検証結果**: 70カラムのテーブルでも問題なく動作したが、数百カラムの場合を考慮
- **提案**: カラム数に応じた分割処理や、出力の簡略化オプション

#### 4. 出力フォーマットの柔軟性
- **改善点**: 用途に応じた複数の出力モード
- **提案**: 
  - `--format=json`: JSON形式のみ
  - `--format=summary`: 要約のみ  
  - `--format=hybrid`: 現在の形式（デフォルト）

#### 5. エラーメッセージの改善
- **改善点**: より具体的で実用的なエラー情報
- **具体例**: 
  ```
  現在: "Error: Failed to describe table: Table not found"
  改善: "Error: Table 'DB.SCHEMA.TABLE' not found. Available tables in schema: [TABLE1, TABLE2, ...]"
  ```

これらの改善点を次回の機能拡張時に適用することで、さらに高品質で使いやすいツールを提供できます。
