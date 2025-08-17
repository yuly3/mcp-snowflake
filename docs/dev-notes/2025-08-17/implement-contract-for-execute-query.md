# SnowflakeClient.execute_query にcontractを設定

## 要求

SnowflakeClient.execute_queryにcontractデコレータを設定し、適切なknown_errを設定する。

## 実装プラン

### 調査結果

Snowflake connectorのエラー階層：
- Error（基底クラス）
  - DatabaseError
    - ProgrammingError - SQL構文エラー、タイムアウト
    - OperationalError - データベース操作エラー
    - DataError - データ処理エラー
    - IntegrityError - 整合性制約違反
    - InternalError - 内部エラー
    - NotSupportedError - サポートされていない操作
  - InterfaceError - インターフェースエラー
  - 各種HTTPエラー（InternalServerError、ServiceUnavailableError等）

### 現在の処理

現在のexecute_queryメソッドでは：
1. ProgrammingErrorを個別に処理（errno=604でTimeoutErrorに変換）
2. その他のExceptionを一般的なExceptionに変換

### 提案するcontract設定

**known_err**として設定すべきエラー：
1. **TimeoutError** - 明示的にタイムアウト時に発生
2. **ProgrammingError** - SQL構文エラー等の予期されるエラー
3. **OperationalError** - データベース操作エラー
4. **DataError** - データ処理エラー
5. **IntegrityError** - 整合性制約違反
6. **NotSupportedError** - サポートされていない操作

これらは業務ロジック上予期される正常なエラー処理の範囲内。

**ContractViolationError**にマップすべきエラー：
- InterfaceError - 接続インターフェースの問題
- InternalError - Snowflake内部エラー
- 各種HTTPエラー - ネットワークやサーバの問題
- その他予期しない例外

### 実装手順

1. snowflake.connector.errorsから必要なエラークラスをインポート
2. expression.contractをインポート
3. execute_queryメソッドにcontract_asyncデコレータを適用
4. 適切なknown_errを設定
5. テストの追加/更新

## 実装詳細

```python
from expression.contract import contract_async
from snowflake.connector import (
    DictCursor,
    ProgrammingError,
    SnowflakeConnection,
    OperationalError,
    DataError,
    IntegrityError,
    NotSupportedError,
)

@contract_async(known_err=(
    TimeoutError,
    ProgrammingError,
    OperationalError,
    DataError,
    IntegrityError,
    NotSupportedError,
))
async def execute_query(self, ...): ...
```
