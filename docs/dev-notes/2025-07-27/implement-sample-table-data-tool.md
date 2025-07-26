# Sample Table Data Tool Implementation Plan

**Date**: 2025-07-27  
**Task**: `SELECT {columns} FROM {database}.{schema}.{table} SAMPLE ROW ({n} ROWS)`を使用してテーブルのサンプルデータを取得するmcp toolを実装する

## User Requirements

- sample_sizeの上限は設けずクエリタイムアウトで事実上の上限を設ける（60秒）
- jsonで直接扱えない型のオブジェクトを取得した場合は、対応していない列が含まれている旨を返却する
- JSON処理はApp layer(handler)で実装する
- SnowflakeClientのtestは今回のscopeから外す

## Implementation Plan

### 1. Tool Specification
- **Tool Name**: `sample_table_data`
- **Function**: 指定されたテーブルから指定行数のサンプルデータを取得
- **SQL Query**: `SELECT * FROM {database}.{schema}.{table} SAMPLE ROW ({n} ROWS)` または `SELECT {columns} FROM {database}.{schema}.{table} SAMPLE ROW ({n} ROWS)`

### 2. Parameters Design
```json
{
  "database": "string (required) - データベース名",
  "schema_name": "string (required) - スキーマ名", 
  "table_name": "string (required) - テーブル名",
  "sample_size": "integer (optional, default: 10) - サンプル行数（上限なし）",
  "columns": "array<string> (optional) - 取得するカラム名のリスト（指定なしの場合は全カラム）"
}
```

### 3. Response Design
```json
{
  "sample_data": {
    "database": "string",
    "schema": "string", 
    "table": "string",
    "sample_size": "integer",
    "actual_rows": "integer",
    "columns": ["column1", "column2", ...],
    "rows": [
      {"column1": "value1", "column2": "value2"},
      {"column1": "value3", "column2": "value4"}
    ],
    "warnings": ["対応していない型の列 'blob_column' が含まれています"] // JSON非対応型がある場合
  }
}
```

### 4. Architecture Design

```
Tool Layer → Handler Layer (App Layer) → SnowflakeClient Layer
                    ↑
              JSON処理をここで実装
```

#### Responsibility Separation

| Layer | 責任 |
|-------|------|
| **Tool Layer** | パラメータvalidation、ツール定義 |
| **Handler Layer (App Layer)** | ビジネスロジック、JSON処理、レスポンス整形、エラーハンドリング |
| **SnowflakeClient Layer** | Snowflake接続、クエリ実行、タイムアウト制御（60秒） |

### 5. File Structure

#### 5.1 New Files
1. **`src/mcp_snowflake/handler/sample_table_data.py`**
   - `SampleTableDataArgs` (Pydanticモデル)
   - `EffectSampleTableData` (Protocol)
   - `handle_sample_table_data` (メインハンドラー)
   - `_check_json_serializable` (JSON処理ヘルパー)
   - `_process_sample_data` (データ変換)
   - `_format_response` (レスポンス整形)

2. **`src/mcp_snowflake/tool/sample_table_data.py`**
   - `SampleTableDataTool` クラス

3. **`tests/handler/test_sample_table_data.py`**
   - ハンドラーのユニットテスト（Mockを使用）

#### 5.2 Modified Files
1. **`src/mcp_snowflake/handler/__init__.py`** - 新しいハンドラーのexport追加
2. **`src/mcp_snowflake/tool/__init__.py`** - 新しいツールのexport追加
3. **`src/mcp_snowflake/snowflake_client.py`** - `sample_table_data` メソッド追加
4. **`src/mcp_snowflake/cli.py`** - 新しいツールの登録

### 6. JSON Processing (Handler Layer)

#### 6.1 JSON Non-compatible Types
対応が困難な型の例：
- `BINARY`、`VARBINARY`
- `GEOGRAPHY`、`GEOMETRY`
- 複雑なオブジェクト型
- 非常に大きなテキスト（制限を超える場合）

#### 6.2 Processing Logic
```python
def _check_json_serializable(value: Any) -> tuple[bool, str | None]:
    """値がJSONシリアライズ可能かチェック"""
    try:
        json.dumps(value)
        return True, None
    except (TypeError, ValueError):
        return False, str(type(value).__name__)

def _process_sample_data(
    raw_rows: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[str]]:
    """生データをJSON対応形式に変換し、警告を生成"""
    # 実装詳細...
```

### 7. Error Handling
- テーブルが存在しない場合
- サンプルサイズが無効な場合（負の値など）
- 指定されたカラムが存在しない場合
- **クエリタイムアウト（60秒）**
- Snowflake接続エラー
- JSON serialization エラー

### 8. Test Strategy

#### 8.1 Handler Level Tests (In Scope)
- **正常ケース**:
  - 基本的なサンプルデータ処理
  - カラム指定ケース
  - JSON serialization可能なデータの処理

- **JSON非対応型処理**:
  - 非対応型データの警告生成
  - 混在データ（対応型＋非対応型）の処理

- **エラーケース**:
  - 無効なパラメータ
  - 空のデータセット
  - EffectSampleTableDataからの例外

- **Mock使用**: `EffectSampleTableData`をMockして、SnowflakeClient依存を排除

#### 8.2 SnowflakeClient Level Tests (Out of Scope)
- 今回のスコープ外 - テスト実装なし
- 実装は行うが、テストは後のスコープで対応

### 9. Implementation Order
1. ハンドラーの実装（`handler/sample_table_data.py`）
2. ツールクラスの実装（`tool/sample_table_data.py`）
3. SnowflakeClientへのメソッド追加（**テストなし**）
4. CLIへの登録
5. **Handler層のテストのみ実装**（`tests/handler/test_sample_table_data.py`）
6. 動作確認

### 10. Key Technical Decisions
- クエリタイムアウト: 60秒でSnowflakeClient層で制御
- JSON非対応型: `<unsupported_type: {type_name}>`で置換し、警告メッセージを生成
- sample_sizeに上限なし（タイムアウトが事実上の制限）
- JSON処理はHandler層で実装（責任分離）

## Next Steps
実装開始準備完了。上記計画に基づいて順次実装を進める。

## Implementation Status
✅ **COMPLETED** - 2025-07-27

### Implemented Files
1. ✅ `src/mcp_snowflake/handler/sample_table_data.py` - Handler層の実装
2. ✅ `src/mcp_snowflake/tool/sample_table_data.py` - Tool層の実装 
3. ✅ `src/mcp_snowflake/snowflake_client.py` - `sample_table_data`メソッド追加
4. ✅ `src/mcp_snowflake/handler/__init__.py` - 新しいハンドラーのexport追加
5. ✅ `src/mcp_snowflake/tool/__init__.py` - 新しいツールのexport追加
6. ✅ `src/mcp_snowflake/__main__.py` - CLIへの登録
7. ✅ `tests/handler/test_sample_table_data.py` - 包括的なテスト実装

### Test Results
- ✅ 新機能のテスト: 20/20 passed
- ✅ 全体のテスト: 65/65 passed
- ✅ 既存機能の互換性: 確認済み

### Key Features Implemented
- ✅ `SAMPLE ROW ({n} ROWS)` SQLクエリの実装
- ✅ 60秒クエリタイムアウト制御
- ✅ JSON非対応型の検出と警告生成
- ✅ Handler層でのJSON処理（責任分離）
- ✅ カラム指定オプション
- ✅ 包括的エラーハンドリング
- ✅ パラメータvalidation

### Technical Notes
- SQL injection警告（S608）は既存コードベースのパターンに従い許容
- SnowflakeClientのテストは今回のスコープ外として除外
- uvコマンドを使用した開発・テスト環境での動作確認済み

実装完了。新しいMCPツール `sample_table_data` が利用可能です。
