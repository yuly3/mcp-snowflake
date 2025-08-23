# QueryRegistry Design for Async execute_query

Date: 2025-08-23
Status: PoC実装完了
Owner: mcp-snowflake

## 概要

長時間実行クエリ向けの非同期版execute_queryツールを実現するため、Snowflake execute_async APIを使用したクエリライフサイクル管理機構（QueryRegistry）を設計・実装する。

### 設計方針
- **Snowflake execute_async前提**: cursor.execute_async, is_still_running, get_results_from_sfqidを直接制御
- **スレッドセーフ**: asyncio.Lockによる排他制御、ThreadPoolExecutorでSnowflake API呼び出し
- **統合API**: create/executeを単一のexecute_query()に統合
- **実キャンセル**: SYSTEM$CANCEL_QUERYによる実際のクエリ停止

## PoC実装結果（2025-08-23完了）

### 実装成功項目
✅ QueryRegistry基盤クラス実装完了
✅ SnowflakeConnectionProvider実装完了
✅ MockSnowflakeConnectionProvider（テスト用）実装完了
✅ 統合テスト6つすべてパス
✅ 基本的なクエリライフサイクル管理動作確認完了

### 設計変更事項

#### 1. EventLoop管理方式の変更

**変更前（設計）:**
```python
def __init__(
    self,
    loop: asyncio.AbstractEventLoop,
    connection_provider: SnowflakeConnectionProvider,
    executor: ThreadPoolExecutor,
)
```

**変更後（実装）:**
```python
def __init__(
    self,
    connection_provider: SnowflakeConnectionProvider,
    executor: ThreadPoolExecutor,
)
```

**変更理由:**
- テスト環境でのEvent Loopが異なる問題（"Task attached to a different loop"エラー）
- 実行時に`asyncio.get_event_loop()`を使用することで柔軟性向上
- 初期化時のLoop依存を排除することで使いやすさ向上

#### 2. SnowflakeAPI呼び出しの簡略化

**設計時の課題:**
- `get_results_from_sfqid`がSnowflakeConnector公式APIに存在しない
- `cursor.get_results_from_sfqid(sfqid)`での結果取得が動作しない

**実装での対応:**
```python
# 代替実装: cursorから直接結果取得
with conn.cursor() as cursor:
    cursor.get_results_from_sfqid(sfqid)
    # cursor自体をイテレートして結果取得
```

### 検証済み機能

1. **非同期クエリ実行**: ✅
   - `execute_query()` による統合実行（create + start）
   - `QueryStatus.PENDING` → `QueryStatus.RUNNING` → `QueryStatus.SUCCEEDED`の遷移

2. **クエリキャンセル機能**: ✅
   - `cancel()` による実キャンセル実行
   - `SYSTEM$CANCEL_QUERY` の呼び出し確認
   - ポーリングタスクの適切な停止

3. **結果取得**: ✅
   - インライン結果の保存と取得
   - ページング対応の `fetch_result()`
   - カラムメタデータの正確な処理

4. **クエリ管理**: ✅
   - 複数クエリの同時管理
   - 状態フィルタ付きクエリリスト
   - TTL自動削除機能

5. **リソース管理**: ✅
   - コネクション適切なクローズ
   - タスクのキャンセル処理
   - 全リソースのクリーンアップ

### 未実装・今後の課題

#### 1. Snowflake API完全対応
現在の実装はMock環境での動作確認のみ。実際のSnowflake接続での動作確認が必要。

#### 2. エラーハンドリング強化
- ネットワークエラー時の処理
- Snowflake側エラーの詳細な分類
- リトライ機構

#### 3. パフォーマンス最適化
- コネクションプーリング
- ポーリング間隔の適応調整
- メモリ使用量最適化

## 次のステップ

### Phase 2: 実環境統合（推定1日）
1. 実Snowflake環境での動作テスト
2. Snowflake APIの詳細調査と修正
3. ApplicationContext統合

### Phase 3: プロダクション対応（推定1日）
1. エラーハンドリング強化
2. ログ出力の整備
3. パフォーマンス調整

### Phase 4: MCP統合（推定0.5日）
1. async系ハンドラー4つの実装
2. ツール定義追加
3. E2Eテスト

## 設計検証結果

✅ **基本アーキテクチャ**: 設計通りに動作。QueryRegistryによるライフサイクル管理は有効
⚠️ **EventLoop管理**: 設計を簡略化。実用性を重視して変更
✅ **MockingStrategy**: 継承ベースのMockが効果的に機能
✅ **テストカバレッジ**: 主要な機能のテストが網羅的に実装済み

**結論**: PoC実装により設計の実現可能性が確認された。一部設計変更はあったものの、Core機能は期待通りに動作する。

## API 契約（MVP）

## 確定データモデル

### 基本型定義

```python
from enum import Enum
from datetime import datetime, timedelta
import attrs
from typing import Any

class QueryStatus(Enum):
    """クエリ実行状態"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"
    TIMEOUT = "timeout"

@attrs.frozen
class QueryOptions:
    """クエリ実行オプション"""
    query_timeout: timedelta | None = attrs.field(default=None)
    max_inline_rows: int = attrs.field(default=1000)
    poll_interval: float = attrs.field(default=1.0)

@attrs.frozen
class ColumnMeta:
    """列メタデータ"""
    name: str
    type: str

@attrs.frozen
class ErrorInfo:
    """エラー情報"""
    type: str
    message: str
    code: int | None = None

@attrs.frozen
class SnowflakeInfo:
    """Snowflake固有情報（snapshot用）"""
    sfqid: str | None = None

@attrs.define
class QueryRecord:
    """内部クエリレコード（可変）"""
    query_id: str
    sql: str
    status: QueryStatus
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    options: QueryOptions = attrs.field(factory=QueryOptions)
    row_count: int | None = None
    columns: list[ColumnMeta] = attrs.field(factory=list)
    result_inline: list[dict[str, Any]] | None = None
    error: ErrorInfo | None = None
    ttl_expires_at: datetime | None = None
    runtime: "QueryRuntime | None" = None  # 内部ランタイム情報

@attrs.frozen
class QuerySnapshot:
    """外部返却用スナップショット（不変）"""
    query_id: str
    sql: str
    status: QueryStatus
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    execution_time_seconds: float | None = None
    row_count: int | None = None
    columns: list[ColumnMeta] = attrs.field(factory=list)
    error: ErrorInfo | None = None
    snowflake: SnowflakeInfo = attrs.field(factory=SnowflakeInfo)
```

### 内部ランタイム情報

```python
@attrs.define
class QueryRuntime:
    """内部ランタイム情報（snapshotには含めない）"""
    sfqid: str | None = None
    connection: Any = None  # SnowflakeConnection
    task: Any = None  # asyncio.Task
    poll_interval: float = 1.0
```

## QueryRegistry 確定設計

### SnowflakeConnectionProvider（具象クラス）

```python
from concurrent.futures import ThreadPoolExecutor
from snowflake.connector import SnowflakeConnection

class SnowflakeConnectionProvider:
    """Snowflake接続の提供と管理"""

    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        warehouse: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        role: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Snowflake接続設定を保持"""
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.extra_params = kwargs

    def get_connection(self) -> SnowflakeConnection:
        """
        新しいSnowflake接続を作成して返す
        クエリ実行用もキャンセル用も同じメソッドを使用
        """
        params = {
            "account": self.account,
            "user": self.user,
            "password": self.password,
        }
        if self.warehouse:
            params["warehouse"] = self.warehouse
        if self.database:
            params["database"] = self.database
        if self.schema:
            params["schema"] = self.schema
        if self.role:
            params["role"] = self.role
        params.update(self.extra_params)

        return SnowflakeConnection(**params)
```

### QueryRegistry メインクラス

```python
class QueryRegistry:
    """Snowflake非同期クエリの完全な実行管理"""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        connection_provider: SnowflakeConnectionProvider,
        executor: ThreadPoolExecutor,
    ) -> None:
        """
        Parameters:
        - loop: asyncio イベントループ
        - connection_provider: Snowflake接続の提供者
        - executor: Snowflake API呼び出し用のスレッドプール
        """
        self._loop = loop
        self._connection_provider = connection_provider
        self._executor = executor
        self._lock = asyncio.Lock()
        self._store: dict[str, QueryRecord] = {}

    async def execute_query(
        self,
        sql: str,
        options: QueryOptions | None = None
    ) -> str:
        """
        クエリを作成し即座に非同期実行を開始

        内部フロー:
        1. QueryRecord を作成（status=pending）
        2. ThreadPoolExecutor で execute_async() 実行
        3. sfqid を取得してランタイムに保存
        4. status=running に更新
        5. ポーリングタスクを開始
        """

    async def cancel(self, query_id: str) -> bool:
        """
        クエリの実キャンセルを実行

        内部フロー:
        1. 新しい connection で SYSTEM$CANCEL_QUERY(sfqid)
        2. ポーリングタスクも停止
        3. status=canceled に更新
        """

    async def get_snapshot(self, query_id: str) -> QuerySnapshot | None:
        """クエリの現在状態を不変スナップショットで取得"""

    async def fetch_result(
        self,
        query_id: str,
        offset: int = 0,
        limit: int | None = None
    ) -> QueryPage | None:
        """ページング対応でクエリ結果を取得（インラインのみ）"""

    async def list_queries(
        self,
        status_filter: QueryStatus | None = None
    ) -> list[QuerySnapshot]:
        """全クエリをリストアップ（状態フィルタ可能）"""

    async def prune_expired(self) -> int:
        """TTL切れクエリを削除し、削除数を返却"""

    async def close(self) -> None:
        """全リソースのクリーンアップ"""
```

### 実行フローの詳細

```python
async def execute_query(self, sql: str, options: QueryOptions | None = None) -> str:
    """execute_query の統合実装"""

    # 1. QueryRecord を作成（pending状態）
    query_id = generate_query_id()
    if options is None:
        options = QueryOptions()

    now = datetime.now(timezone.utc)
    record = QueryRecord(
        query_id=query_id,
        sql=sql,
        status=QueryStatus.PENDING,
        created_at=now,
        options=options,
        ttl_expires_at=now + timedelta(hours=24),
    )

    async with self._lock:
        self._store[query_id] = record

    # 2. 即座に実行開始
    try:
        sfqid = await self._execute_async_sync(query_id, sql)

        # 3. ランタイム情報を設定
        async with self._lock:
            record = self._store[query_id]
            record.status = QueryStatus.RUNNING
            record.started_at = datetime.now(timezone.utc)
            if not record.runtime:
                record.runtime = QueryRuntime()
            record.runtime.sfqid = sfqid
            record.runtime.poll_interval = options.poll_interval

        # 4. ポーリングタスクを開始
        task = asyncio.create_task(self._poll_until_done(query_id))
        async with self._lock:
            record.runtime.task = task

    except Exception as e:
        # 実行開始失敗
        await self._set_failed(query_id, e)
        raise

    return query_id

async def _poll_until_done(self, query_id: str) -> None:
    """ポーリングループの具体実装"""

    def _check_status(sfqid: str, conn: SnowflakeConnection) -> bool:
        return conn.is_still_running(
            conn.get_query_status_throw_if_error(sfqid)
        )

    while True:
        # タイムアウトチェック
        if self._is_timeout_exceeded(query_id):
            await self._handle_timeout(query_id)
            return

        # Snowflake ポーリング
        sfqid, conn = await self._get_runtime_info(query_id)
        is_running = await self._loop.run_in_executor(
            self._executor, _check_status, sfqid, conn
        )

        if not is_running:
            break

        await asyncio.sleep(poll_interval)

    # 完了処理
    await self._handle_completion(query_id)

async def _cancel_query_sync(self, sfqid: str) -> None:
    """SYSTEM$CANCEL_QUERY の実行（新しい接続を使用）"""

    def _sync_cancel():
        # キャンセル用に新しい接続を取得
        conn = self._connection_provider.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT SYSTEM$CANCEL_QUERY('{sfqid}')")
                return cursor.fetchone()
        finally:
            conn.close()  # キャンセル専用なので即座にクローズ

    await self._loop.run_in_executor(self._executor, _sync_cancel)
```

## テスト戦略

### MockSnowflakeConnectionProvider（継承ベース）

```python
class MockSnowflakeConnectionProvider(SnowflakeConnectionProvider):
    """テスト用のモック接続プロバイダー"""

    def __init__(self):
        # 親クラスのコンストラクタは呼ばない（テスト用）
        self.mock_connections = {}
        self.query_states = {}  # sfqid -> (is_running, result)
        self.cancel_calls = []  # キャンセル呼び出し記録

    def get_connection(self) -> MockConnection:
        """モック接続を返す"""
        return MockConnection(self)

    def simulate_query_completion(self, sfqid: str, result: list[dict]):
        """テストでクエリ完了をシミュレート"""
        self.query_states[sfqid] = (False, result)

class MockConnection:
    """Snowflake接続のモック"""

    def __init__(self, provider: MockSnowflakeConnectionProvider):
        self.provider = provider
        self._cursor = None

    def cursor(self):
        self._cursor = MockCursor(self.provider)
        return self._cursor

    def close(self):
        pass  # モックなので何もしない

class MockCursor:
    """Snowflakeカーソルのモック"""

    def __init__(self, provider: MockSnowflakeConnectionProvider):
        self.provider = provider
        self.sfqid = None

    def execute_async(self, sql: str):
        """execute_asyncをシミュレート"""
        import uuid
        self.sfqid = str(uuid.uuid4())
        # デフォルトは実行中状態
        self.provider.query_states[self.sfqid] = (True, None)

    def execute(self, sql: str):
        """通常のexecute（キャンセル用）"""
        if "SYSTEM$CANCEL_QUERY" in sql:
            self.provider.cancel_calls.append(sql)

    def fetchone(self):
        return {"result": "success"}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
```

### 統合テスト例

```python
async def test_execute_query_integration():
    provider = MockSnowflakeConnectionProvider()
    registry = QueryRegistry(loop, provider, executor)

    # 統合実行（create + execute）
    query_id = await registry.execute_query("SELECT * FROM big_table")

    # 実行中をアサート
    snapshot = await registry.get_snapshot(query_id)
    assert snapshot.status == QueryStatus.RUNNING
    sfqid = snapshot.snowflake.sfqid
    assert sfqid is not None

    # 外部から完了をトリガー
    provider.simulate_query_completion(sfqid, [{"count": 1000}])
    await asyncio.sleep(0.1)  # ポーリング待機

    # 完了を確認
    snapshot = await registry.get_snapshot(query_id)
    assert snapshot.status == QueryStatus.SUCCEEDED

    # 結果取得
    page = await registry.fetch_result(query_id)
    assert page.rows == [{"count": 1000}]

async def test_cancel_integration():
    provider = MockSnowflakeConnectionProvider()
    registry = QueryRegistry(loop, provider, executor)

    # 長時間クエリを開始
    query_id = await registry.execute_query("SELECT * FROM huge_table")

    # キャンセル実行
    success = await registry.cancel(query_id)
    assert success is True

    # キャンセルSQLが呼ばれたことを確認
    assert len(provider.cancel_calls) == 1
    assert "SYSTEM$CANCEL_QUERY" in provider.cancel_calls[0]

    # 最終的にキャンセル状態になることを確認
    await asyncio.sleep(0.1)
    snapshot = await registry.get_snapshot(query_id)
    assert snapshot.status == QueryStatus.CANCELED
```

## 実装スケジュール

1. **Phase 1: 基盤実装** (1日)
   - 型定義の完全実装
   - SnowflakeConnectionProviderの実装
   - QueryRegistryの再実装（execute_query統合）

2. **Phase 2: テスト完備** (0.5日)
   - MockSnowflakeConnectionProvider
   - 全API統合テスト

3. **Phase 3: プロジェクト統合** (1日)
   - ApplicationContext拡張
   - async系ハンドラー4つの実装
   - ツール定義追加

4. **Phase 4: 検証** (0.5日)
   - E2Eテスト
   - 長時間クエリでの動作確認


## プロジェクト統合方針

### 1. ApplicationContextの拡張

```python
# context.py
from query_registry import QueryRegistry, SnowflakeConnectionProvider

@attrs.define
class ApplicationContext:
    settings: Settings
    snowflake_client: SnowflakeClient
    query_registry: QueryRegistry = attrs.field(init=False)

    def __attrs_post_init__(self):
        """QueryRegistry初期化"""
        import asyncio
        from concurrent.futures import ThreadPoolExecutor

        loop = asyncio.get_event_loop()
        executor = ThreadPoolExecutor(max_workers=4)

        # 本番用接続プロバイダー
        provider = SnowflakeConnectionProvider(
            user=self.settings.snowflake.user,
            password=self.settings.snowflake.password,
            account=self.settings.snowflake.account,
            database=self.settings.snowflake.database,
            schema=self.settings.snowflake.schema,
            warehouse=self.settings.snowflake.warehouse,
        )

        self.query_registry = QueryRegistry(loop, provider, executor)

    async def cleanup(self):
        """リソースクリーンアップ"""
        await self.query_registry.cleanup()
```

### 2. ツールハンドラー群の実装

```python
# handler/async_execute_query.py
async def handle_async_execute_query(
    ctx: ApplicationContext,
    params: AsyncExecuteQueryParams
) -> HandlerResult[AsyncExecuteQueryResult]:
    """非同期版execute_queryのハンドラー"""

    query_id = await ctx.query_registry.execute_query(
        sql=params.sql,
        options=QueryOptions(
            timeout_seconds=params.timeout_seconds,
            max_rows=params.max_rows,
        )
    )

    return HandlerResult.success(
        AsyncExecuteQueryResult(
            query_id=query_id,
            message="Query execution started asynchronously"
        )
    )

# handler/query_status.py
async def handle_query_status(
    ctx: ApplicationContext,
    params: QueryStatusParams
) -> HandlerResult[QueryStatusResult]:
    """クエリ実行状況確認"""

    snapshot = await ctx.query_registry.get_snapshot(params.query_id)

    return HandlerResult.success(
        QueryStatusResult(
            query_id=params.query_id,
            status=snapshot.status.value,
            created_at=snapshot.created_at,
            updated_at=snapshot.updated_at,
            snowflake_query_id=snapshot.snowflake.sfqid,
            duration_seconds=snapshot.execution_time_seconds,
            error_message=snapshot.error,
        )
    )

# handler/query_result.py
async def handle_query_result(
    ctx: ApplicationContext,
    params: QueryResultParams
) -> HandlerResult[QueryResultResult]:
    """クエリ結果取得"""

    page = await ctx.query_registry.fetch_result(
        query_id=params.query_id,
        page_size=params.page_size or 1000,
        offset=params.offset or 0,
    )

    return HandlerResult.success(
        QueryResultResult(
            query_id=params.query_id,
            rows=page.rows,
            total_rows=page.total_rows,
            has_more=page.has_more,
            columns=page.columns,
        )
    )

# handler/query_cancel.py
async def handle_query_cancel(
    ctx: ApplicationContext,
    params: QueryCancelParams
) -> HandlerResult[QueryCancelResult]:
    """クエリキャンセル"""

    success = await ctx.query_registry.cancel(params.query_id)

    return HandlerResult.success(
        QueryCancelResult(
            query_id=params.query_id,
            canceled=success,
            message="Query cancellation requested" if success else "Failed to cancel query"
        )
    )

    return HandlerResult.success(
        QueryResultResult(
            query_id=params.query_id,
            rows=page.rows,
            total_rows=page.total_rows,
            has_more=page.has_more,
            columns=page.columns,
        )
    )

# handler/query_cancel.py
async def handle_query_cancel(
    ctx: ApplicationContext,
    params: QueryCancelParams
) -> HandlerResult[QueryCancelResult]:
    """クエリキャンセル"""

    success = await ctx.query_registry.cancel(params.query_id)

    return HandlerResult.success(
        QueryCancelResult(
            query_id=params.query_id,
            canceled=success,
            message="Query cancellation requested" if success else "Failed to cancel query"
        )
    )
```

1. **Phase 1: 基盤実装** (1日)
   - 型定義の完全実装
   - SnowflakeConnectionProviderの実装（簡略化版）
   - QueryRegistryの再実装（execute_query統合）

2. **Phase 2: テスト完備** (0.5日)
   - MockSnowflakeConnectionProvider
   - 全API統合テスト

3. **Phase 3: プロジェクト統合** (1日)
   - ApplicationContext拡張
   - async系ハンドラー4つの実装
   - ツール定義追加

4. **Phase 4: 検証** (0.5日)
   - E2Eテスト
   - 長時間クエリでの動作確認