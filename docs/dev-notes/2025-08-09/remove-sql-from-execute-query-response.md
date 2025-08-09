# execute_query toolレスポンスからSQLクエリの除去

## ユーザからのリクエスト

**ゴール**: execute_query toolのレスポンスからリクエストのSQLを取り除く。

## 現状の分析

### 現在のレスポンス構造
`execute_query`ツールは現在、以下のJSON構造でレスポンスを返しています：

```json
{
  "query_result": {
    "sql": "SELECT * FROM table_name",
    "execution_time_ms": 150,
    "row_count": 10,
    "columns": ["col1", "col2"],
    "rows": [...],
    "warnings": [...]
  }
}
```

### 問題点
- `sql`フィールドがリクエストで送信されたSQLクエリをそのまま含んでいる
- これにより、レスポンスサイズが不必要に大きくなる
- クライアント側では既にクエリを知っているため、冗長な情報となる

## 実装計画

### 1. レスポンス構造の修正

#### 変更対象ファイル
- `src/mcp_snowflake/handler/execute_query.py`

#### 変更内容

1. **TypedDictの更新**
   - `QueryResultDict`から`sql: str`フィールドを削除

2. **フォーマット関数の修正**
   - `_format_query_response`関数の`sql`パラメータを削除
   - 関数内でSQLをレスポンスに含めないよう修正

3. **ハンドラー関数の修正**
   - `handle_execute_query`関数で`_format_query_response`を呼び出す際の引数からSQLを削除

### 2. テストの更新

#### 変更対象ファイル
- `tests/handler/test_execute_query.py`

#### 変更内容

1. **テストケースの修正** ✅
   - レスポンス構造を検証するテストで、SQLフィールドの存在確認を`"sql" not in query_result`から厳密なキーセット比較`set(query_result.keys()) == EXPECTED_RESPONSE_KEYS`に変更
   - 期待するレスポンスキーセットを`EXPECTED_RESPONSE_KEYS`として共通定数化
   - `test_format_query_response`テストで新しい関数シグネチャに対応

2. **レスポンス検証の更新** ✅
   - JSONレスポンスを解析するテストで厳密なキーセット検証を実装

### 3. 修正後のレスポンス構造

```json
{
  "query_result": {
    "execution_time_ms": 150,
    "row_count": 10,
    "columns": ["col1", "col2"],
    "rows": [...],
    "warnings": [...]
  }
}
```

## 影響範囲

### 破壊的変更について
- この変更はAPIレスポンスの構造を変更するため、**破壊的変更**となります
- 既存のクライアントが`sql`フィールドに依存している場合、動作しなくなる可能性があります

### 影響を受けるコンポーネント
- execute_queryツールを利用するすべてのクライアント
- レスポンスの`sql`フィールドを参照するコード

## 実装手順

1. **テストの修正** ✅
   - レスポンス検証方法を`"sql" not in query_result`から`set(query_result.keys()) == expected_keys`に変更
   - 期待するキーセット`EXPECTED_RESPONSE_KEYS`を共通定数として定義
   - テストを実行してredになることを確認済み

2. **TypedDictとフォーマット関数の修正** ✅
   - `QueryResultDict`から`sql: str`フィールドを削除
   - `_format_query_response`関数から`sql`パラメータを削除
   - 関数のdocstringとパラメータ仕様を更新

3. **ハンドラー関数の更新** ✅
   - `handle_execute_query`関数で`_format_query_response`を呼び出す際の引数からSQLを削除
   - テストの`test_format_query_response`も新しいシグネチャに対応

4. **動作確認** ✅
   - 全テスト（12個）がgreenになることを確認
   - レスポンス構造からSQLフィールドが除去されることを確認

## メリット

1. **レスポンスサイズの削減**: 不要なSQLクエリ文字列を削除することで、レスポンスサイズが小さくなります
2. **情報の重複排除**: クライアントが既に知っているSQLクエリを返さないことで、データの冗長性を削減します
3. **帯域幅の節約**: 特に長いSQLクエリの場合、ネットワーク帯域幅を節約できます

## リスク・注意点

1. **破壊的変更**: 既存のクライアントコードが影響を受ける可能性があります
2. **デバッグの困難性**: レスポンスからSQLが見えなくなるため、ログ分析やデバッグが若干困難になる可能性があります

## 実装の進捗

### 完了済み ✅

1. **テストの修正とRed確認** ✅
   - 期待するレスポンス構造の厳密な定義
   - `EXPECTED_RESPONSE_KEYS`の共通定数化
   - キーセット比較による厳密な検証
   - 現在の実装でテストがredになることを確認

2. **実装の修正** ✅
   - `QueryResultDict`からSQLフィールド削除
   - `_format_query_response`関数の引数修正
   - ハンドラー関数の呼び出し修正
   - テスト関数の引数修正

3. **Green確認** ✅
   - 修正後に全12個のテストがgreenになることを確認
   - レスポンス構造からSQLフィールドが完全に除去されることを確認

## 実装結果

### 修正前のレスポンス構造
```json
{
  "query_result": {
    "sql": "SELECT * FROM table_name",
    "execution_time_ms": 150,
    "row_count": 10,
    "columns": ["col1", "col2"],
    "rows": [...],
    "warnings": [...]
  }
}
```

### 修正後のレスポンス構造
```json
{
  "query_result": {
    "execution_time_ms": 150,
    "row_count": 10,
    "columns": ["col1", "col2"],
    "rows": [...],
    "warnings": [...]
  }
}
```

## 達成された目標

- ✅ レスポンスサイズの削減
- ✅ 情報の冗長性排除  
- ✅ 帯域幅の節約
- ✅ 破壊的変更の実装完了
- ✅ 全テストケースの正常動作確認

---

*計画作成日: 2025-08-09*  
*実装完了日: 2025-08-09*
