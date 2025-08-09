# data_processingモジュールのkernelへの移行

## ユーザーのリクエスト

data_processingの内容をkernelに移動させprocess_row_dataとprocess_multiple_rows_dataをそれぞれRowProcessingResultとDataProcessingResultのメソッドとする。テストの修正に注意

## 実装計画

### 1. 現状分析

- `data_processing`モジュールは`handler/data_processing/`に存在
- `DataProcessingResult`と`RowProcessingResult`はTypedDictとして定義
- `process_row_data`と`process_multiple_rows_data`は独立した関数
- 以下のファイルで使用されている：
  - `handler/execute_query.py`
  - `handler/sample_table_data.py`
  - `tests/handler/test_execute_query.py`
  - `tests/handler/test_sample_table_data.py`
  - `tests/handler/test_data_processing.py`

### 2. 実装ステップ

#### Phase 1: kernelにdata_processing.pyを作成

1. `src/mcp_snowflake/kernel/data_processing.py`を作成
2. TypedDictではなくattrsクラスとして`DataProcessingResult`と`RowProcessingResult`を定義
3. 各クラスに対応する処理メソッドを追加：
   - `RowProcessingResult.from_raw_row(raw_row)`クラスメソッド
   - `DataProcessingResult.from_raw_rows(raw_rows)`クラスメソッド

#### Phase 2: kernelの__init__.pyを更新

- 新しいクラスをエクスポートに追加

#### Phase 3: 既存のハンドラーファイルを更新

- `execute_query.py`と`sample_table_data.py`のインポートを変更
- 関数呼び出しをメソッド呼び出しに変更

#### Phase 4: テストファイルを更新

- インポートパスの変更
- 関数呼び出しをメソッド呼び出しに変更
- TypedDictから通常のクラスに変更されることによるアサーションの調整

#### Phase 5: 古いdata_processingディレクトリの削除

- `handler/data_processing/`ディレクトリを完全削除

### 3. 設計詳細

#### 新しいクラス設計（attrs使用）

```python
@attrs.define(frozen=True)
class RowProcessingResult:
    processed_row: dict[str, Any]
    warnings: list[str]

    @classmethod
    def from_raw_row(cls, raw_row: dict[str, Any]) -> "RowProcessingResult":
        # 既存のprocess_row_data()のロジック
        pass

@attrs.define(frozen=True)
class DataProcessingResult:
    processed_rows: list[dict[str, Any]]
    warnings: list[str]

    @classmethod
    def from_raw_rows(cls, raw_rows: list[dict[str, Any]]) -> "DataProcessingResult":
        # 既存のprocess_multiple_rows_data()のロジック
        pass
```

#### APIの変更

**Before:**
```python
from mcp_snowflake.handler.data_processing import process_multiple_rows_data
result = process_multiple_rows_data(raw_rows)
```

**After:**
```python
from mcp_snowflake.kernel import DataProcessingResult
result = DataProcessingResult.from_raw_rows(raw_rows)
```

### 4. テスト考慮事項

- TypedDictから通常のクラスになるため、dict的な操作ではなく属性アクセスが必要
- テストでの結果アクセス方法の変更：
  - `result["processed_rows"]` → `result.processed_rows`
  - `result["warnings"]` → `result.warnings`

## 実装開始

ユーザーの了承を得て実装を開始します。

## 実装完了

### 完了した作業内容

✅ **kernelにdata_processingモジュールを作成**
- `src/mcp_snowflake/kernel/data_processing.py` を作成
- `RowProcessingResult`と`DataProcessingResult`をattrsクラスとして定義
- `from_raw_row()`, `from_raw_rows()`クラスメソッドを実装

✅ **kernelの__init__.pyを更新**
- 新しいクラスをエクスポートに追加

✅ **ハンドラーファイルを修正**
- `execute_query.py`: インポートと関数呼び出しを修正
- `sample_table_data.py`: インポートと関数呼び出しを修正
- TypedDictのdictアクセスから、attrsクラスの属性アクセスに変更

✅ **テストファイルを修正・移動**
- `test_data_processing.py`: `tests/handler/` → `tests/kernel/` に移動
- `test_execute_query.py`
- `test_sample_table_data.py`
- インポートパス変更、関数呼び出し→メソッド呼び出し、dictアクセス→属性アクセス

✅ **レガシーdata_processingディレクトリの削除**
- `src/mcp_snowflake/handler/data_processing/` を削除

### 最終動作確認

- **全267個のテストが成功** 🎉
- 既存機能に影響なし
- アーキテクチャの改善完了

### 技術的変更内容

- **TypedDict → attrsクラス**: 不変でメソッドを持てるクラスに変更
- **関数 → クラスメソッド**: `process_row_data()` → `RowProcessingResult.from_raw_row()`
- **dictアクセス → 属性アクセス**: `result["processed_rows"]` → `result.processed_rows`
- **アーキテクチャ改善**: handler層 → kernel層への適切な依存方向

data_processingモジュールのkernelへの移行とリファクタリングが完全に成功しました。
