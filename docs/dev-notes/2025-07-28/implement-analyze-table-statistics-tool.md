# Implement analyze_table_statistics Tool

## ユーザーリクエスト
SnowflakeにはAPPROX_PERCENTILE, APPROX_TOP_K, APPROX_COUNT_DISTINCTといった高速に統計値を推定する関数が存在する。これらを利用して指定されたテーブルについて一般的な統計情報を高速に取得するtoolを実装できないだろうか

## 詳細化された実装計画

### 1. 機能概要
Snowflakeの高速統計関数を活用して、指定されたテーブルの各列について効率的に統計情報を取得するツールを実装する。

### 2. 技術調査結果

#### 2.1 対応する統計関数とデータ型
**数値列（NUMBER型）**:
- ✅ APPROX_PERCENTILE: 四分位数計算に対応
- ✅ APPROX_COUNT_DISTINCT: 重複除外カウントに対応
- ✅ COUNT/AVG/MIN/MAX: 基本統計に対応

**文字列列（VARCHAR型）**:
- ✅ APPROX_COUNT_DISTINCT: 重複除外カウントに対応
- ✅ APPROX_TOP_K: 最頻値計算に対応
- ✅ MIN/MAX/LENGTH: 文字列長統計に対応

**日付/タイムスタンプ列（DATE/TIMESTAMP型）**:
- ❌ APPROX_PERCENTILE: 非対応（SQLエラー）
- ✅ APPROX_COUNT_DISTINCT: 重複除外カウントに対応
- ✅ MIN/MAX: 期間計算に対応

#### 2.2 統計関数の出力型
- `APPROX_PERCENTILE(numeric_col, percentile)` → NUMBER
- `APPROX_COUNT_DISTINCT(any_col)` → NUMBER
- `APPROX_TOP_K(string_col, k)` → ARRAY (JSON形式: `[["value", count], ...]`)
- `MIN/MAX/AVG/COUNT` → データ型に応じた値

### 3. ツール仕様

#### 3.1 ツール名
`analyze_table_statistics`

#### 3.2 パラメータ（Pydantic BaseModel）
```python
class AnalyzeTableStatisticsArgs(BaseModel):
    database: str
    schema_name: str
    table_name: str
    columns: list[str] = Field(default_factory=list)  # 空リストの場合は全列
    top_k_limit: int = Field(default=10, ge=1, le=100)  # 最頻値の取得数
```

#### 3.3 JSON入力スキーマ
```json
{
  "type": "object",
  "properties": {
    "database": {
      "type": "string",
      "description": "Database name containing the table"
    },
    "schema_name": {
      "type": "string", 
      "description": "Schema name containing the table"
    },
    "table_name": {
      "type": "string",
      "description": "Name of the table to analyze"
    },
    "columns": {
      "type": "array",
      "items": {"type": "string"},
      "description": "List of column names to analyze (if not specified, all columns will be analyzed)",
      "default": []
    },
    "top_k_limit": {
      "type": "integer",
      "minimum": 1,
      "maximum": 100,
      "default": 10,
      "description": "Number of top values to retrieve for string columns"
    }
  },
  "required": ["database", "schema_name", "table_name"]
}
```

#### 3.4 出力形式（TypedDict）
```python
class NumericStatsDict(TypedDict):
    """TypedDict for numeric column statistics."""
    column_type: str  # "numeric"
    data_type: str
    count: int
    null_count: int
    distinct_count_approx: int
    min: float
    max: float
    avg: float
    percentile_25: float
    percentile_50: float  # median
    percentile_75: float

class StringStatsDict(TypedDict):
    """TypedDict for string column statistics."""
    column_type: str  # "string"
    data_type: str
    count: int
    null_count: int
    distinct_count_approx: int
    min_length: int
    max_length: int
    top_values: list[list[Any]]  # [[value, count], ...]

class DateStatsDict(TypedDict):
    """TypedDict for date column statistics."""
    column_type: str  # "date"
    data_type: str
    count: int
    null_count: int
    distinct_count_approx: int
    min_date: str
    max_date: str
    date_range_days: int

class TableInfoDict(TypedDict):
    """TypedDict for table information."""
    database: str
    schema: str
    table: str
    total_rows: int
    analyzed_columns: int

class TableStatisticsDict(TypedDict):
    """TypedDict for the complete table statistics response."""
    table_info: TableInfoDict
    column_statistics: dict[str, NumericStatsDict | StringStatsDict | DateStatsDict]

class AnalyzeTableStatisticsJsonResponse(TypedDict):
    """TypedDict for the complete JSON response structure."""
    table_statistics: TableStatisticsDict
```

#### 3.5 JSON出力例
```json
{
  "table_statistics": {
    "table_info": {
      "database": "SNOWFLAKE_SAMPLE_DATA",
      "schema": "TPCH_SF1", 
      "table": "ORDERS",
      "total_rows": 1500000,
      "analyzed_columns": 3
    },
    "column_statistics": {
      "O_TOTALPRICE": {
        "column_type": "numeric",
        "data_type": "NUMBER(12,2)",
        "count": 1500000,
        "null_count": 0,
        "distinct_count_approx": 1452327,
        "min": 857.71,
        "max": 555285.16,
        "avg": 151219.53763164,
        "percentile_25": 77892.25,
        "percentile_50": 144423.51,
        "percentile_75": 215503.06
      },
      "O_ORDERSTATUS": {
        "column_type": "string",
        "data_type": "VARCHAR(1)",
        "count": 1500000,
        "null_count": 0,
        "distinct_count_approx": 3,
        "min_length": 1,
        "max_length": 1,
        "top_values": [
          ["O", 732044],
          ["F", 729413], 
          ["P", 38543]
        ]
      },
      "O_ORDERDATE": {
        "column_type": "date",
        "data_type": "DATE",
        "count": 1500000,
        "null_count": 0,
        "distinct_count_approx": 2428,
        "min_date": "1992-01-01",
        "max_date": "1998-08-02",
        "date_range_days": 2405
      }
    }
  }
}
```

### 4. 実装アプローチ

#### 4.1 処理フロー
1. **列情報取得**: `DESCRIBE TABLE`で列の型情報を取得
2. **列の分類**: データ型に基づいて数値/文字列/日付列に分類
3. **動的SQL生成**: 各列タイプに応じた統計関数を含むSQLを生成
4. **統計情報取得**: 単一クエリで全統計情報を取得
5. **結果整形**: 列タイプ別の構造化データとして返却

#### 4.2 SQL生成例
```sql
SELECT 
  COUNT(*) as total_rows,
  
  -- 数値列統計 (O_TOTALPRICE)
  COUNT(O_TOTALPRICE) as numeric_O_TOTALPRICE_count,
  SUM(CASE WHEN O_TOTALPRICE IS NULL THEN 1 ELSE 0 END) as numeric_O_TOTALPRICE_null_count,
  MIN(O_TOTALPRICE) as numeric_O_TOTALPRICE_min,
  MAX(O_TOTALPRICE) as numeric_O_TOTALPRICE_max,
  AVG(O_TOTALPRICE) as numeric_O_TOTALPRICE_avg,
  APPROX_PERCENTILE(O_TOTALPRICE, 0.25) as numeric_O_TOTALPRICE_q1,
  APPROX_PERCENTILE(O_TOTALPRICE, 0.5) as numeric_O_TOTALPRICE_median,
  APPROX_PERCENTILE(O_TOTALPRICE, 0.75) as numeric_O_TOTALPRICE_q3,
  APPROX_COUNT_DISTINCT(O_TOTALPRICE) as numeric_O_TOTALPRICE_distinct,
  
  -- 文字列列統計 (O_ORDERSTATUS)
  COUNT(O_ORDERSTATUS) as string_O_ORDERSTATUS_count,
  SUM(CASE WHEN O_ORDERSTATUS IS NULL THEN 1 ELSE 0 END) as string_O_ORDERSTATUS_null_count,
  MIN(LENGTH(O_ORDERSTATUS)) as string_O_ORDERSTATUS_min_length,
  MAX(LENGTH(O_ORDERSTATUS)) as string_O_ORDERSTATUS_max_length,
  APPROX_COUNT_DISTINCT(O_ORDERSTATUS) as string_O_ORDERSTATUS_distinct,
  APPROX_TOP_K(O_ORDERSTATUS, 10) as string_O_ORDERSTATUS_top_values,
  
  -- 日付列統計 (O_ORDERDATE)
  COUNT(O_ORDERDATE) as date_O_ORDERDATE_count,
  SUM(CASE WHEN O_ORDERDATE IS NULL THEN 1 ELSE 0 END) as date_O_ORDERDATE_null_count,
  MIN(O_ORDERDATE) as date_O_ORDERDATE_min,
  MAX(O_ORDERDATE) as date_O_ORDERDATE_max,
  DATEDIFF('day', MIN(O_ORDERDATE), MAX(O_ORDERDATE)) as date_O_ORDERDATE_range_days,
  APPROX_COUNT_DISTINCT(O_ORDERDATE) as date_O_ORDERDATE_distinct

FROM table_name;
```

#### 4.3 データ型分類ロジック
```python
def classify_column_type(data_type: str) -> str:
    data_type_upper = data_type.upper()
    
    # 数値型の判定
    if any(numeric_type in data_type_upper for numeric_type in ['NUMBER', 'INT', 'FLOAT', 'DOUBLE', 'DECIMAL']):
        return 'numeric'
    
    # 日付型の判定
    if any(date_type in data_type_upper for date_type in ['DATE', 'TIMESTAMP', 'TIME']):
        return 'date'
    
    # その他は文字列型として扱う
    return 'string'
```

### 5. ファイル構成
- `src/mcp_snowflake/tool/analyze_table_statistics.py`
- `src/mcp_snowflake/handler/analyze_table_statistics.py`
- `tests/handler/test_analyze_table_statistics.py`

### 6. 実装上の考慮事項

#### 6.1 パフォーマンス
- 単一SQLクエリで全統計情報を取得することで、複数回のDB接続を回避
- APPROX_系関数の使用により、大容量テーブルでも高速実行

#### 6.2 エラーハンドリング
- 存在しないテーブル/列の指定時のエラー処理
- データ型に対応しない統計関数の実行時エラー処理
- NULL値のみの列に対する適切な統計値計算

#### 6.3 制限事項
- 日付/タイムスタンプ列ではパーセンタイル計算は提供しない
- APPROX_TOP_Kの結果はJSON配列として返却され、パース処理が必要
- 非常に大きなテーブルでも統計関数の近似計算により高速実行可能

## 次のステップ
1. ツール基底クラスの実装
2. ハンドラーの実装とテスト
3. 実際のサンプルデータでの動作確認
4. エラーケースのテスト実装
