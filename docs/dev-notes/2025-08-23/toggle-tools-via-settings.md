# Toggle Tools via Settings - Implementation Plan (TDD)

## ユーザープロンプト

- ゴール: Settings によって tool ごとに on/off が設定できる
- off の tool は handle_list_tools から返されない
- デフォルトは全て on
- 追加テスト:
  - settings key の集合と tool 名の集合が一致しているか
  - tool 名に重複がないか
- 実装手順は TDD アプローチ（function/class ごとに red -> implement -> green）
- 開発ノート出力後、ユーザ判断を仰ぐ

## 目的と成功条件

- 設定で各ツールの有効/無効を切替可能。
- 無効ツールは list_tools 応答から除外され、呼び出しも不可（登録されない）。
- 既定値は全ツール有効（後方互換）。
- テストで以下を担保:
  - 設定キーとツール名の集合一致。
  - ツール名重複なし。
  - デフォルトと上書き（TOML/環境変数）の動作。
  - ツールの登録が設定に従ってフィルタされる。

## 影響範囲

- 設定: `src/mcp_snowflake/settings.py`
- エントリポイント/登録: `src/mcp_snowflake/__main__.py`
- ドキュメント: `README.md`（Configuration セクション）
- テスト: `tests/` 追加・更新
- 既存ツール一覧（name）:
  - analyze_table_statistics
  - describe_table
  - execute_query
  - list_schemas
  - list_tables
  - list_views
  - sample_table_data

## 設計方針

- 新規 `ToolsSettings`（Pydantic BaseModel）を追加し、各ツールの boolean を用意（既定 True）。
- `Settings` に `tools: ToolsSettings` を追加。未指定時にも既定 True で初期化。
- TOML 例:
  ```toml
  [tools]
  list_tables = false
  execute_query = true
  ```
- 環境変数（Pydantic v2, env_nested_delimiter="__" 利用）例: PowerShell
  ```powershell
  $env:TOOLS__LIST_TABLES = "false"
  $env:TOOLS__EXECUTE_QUERY = "true"
  ```
- `SnowflakeServerContext` に `settings` を保持し、`build_tools()` で `settings.tools` を参照して有効なツールのみ生成・登録。
- `handle_list_tools()` は `server_context.tools` から構築済み一覧を返す現行仕様のままで要件充足。

## TDD 実装シナリオ（red -> green）

1) ツール名健全性テスト（新規）
- ファイル: `tests/test_tool_names.py`
- テスト:
  - test_tool_name_unique: `src/mcp_snowflake/tool/__init__.py` 経由でツールクラスを列挙し、インスタンスの `name` に重複がないこと。
  - test_settings_keys_match_tool_names: `ToolsSettings` のフィールド名集合とツール `name` 集合が一致すること。
- red: テストを先に作成（現時点では ToolsSettings が無くて落ちる）。
- green: `ToolsSettings` を実装、集合一致を満たす。

2) 設定デフォルト/上書きテスト（更新/新規）
- 既存: `tests/test_settings.py`
- 追加:
  - test_tools_default_all_enabled: TOML 未指定時に全 True。
  - test_tools_toml_override: フィクスチャの TOML に `[tools]` を追加し一部 False にして反映されること。
  - test_tools_env_override: 環境変数で上書き可能（`TOOLS__LIST_TABLES=false` など）。
- red: 先にテスト追加。
- green: `Settings` に `tools` を追加し、環境変数/ファイルから読み込む。

3) 登録フィルタリングテスト（新規）
- ファイル: `tests/test_tool_registration.py`
- テスト:
  - test_build_tools_respects_settings: `SnowflakeServerContext` にダミー `SnowflakeClient` を挿して `build_tools()` 実行、`settings.tools` で False 指定のツールが `server_context.tools` に含まれないこと。
  - test_handle_list_tools_excludes_disabled: 上記コンテキストを使い `handle_list_tools()` が有効ツールのみ返すこと。
- 実装メモ:
  - 依存注入のため `SnowflakeServerContext` に `settings` を保持させる変更を行い、`main()` でセット。
  - `SnowflakeClient` はコンストラクタだけ必要（クエリ実行は呼ばない）。必要なら簡易モッククラスを使う。
- red: 先にテスト。
- green: `__main__.py` を実装変更。

4) ドキュメントテスト（省略可能）
- `README.md` のサンプル設定が構文的に正しいかは手動確認とする。

## 変更予定ファイル

- 追加/変更: `src/mcp_snowflake/settings.py`（`ToolsSettings` を追加・`Settings` 拡張）
- 変更: `src/mcp_snowflake/__main__.py`（`settings` 保持、`build_tools()` 条件分岐）
- 追加: `tests/test_tool_names.py`（集合一致/重複なし）
- 変更: `tests/test_settings.py`（ツール設定テストの追記）
- 追加: `tests/test_tool_registration.py`（登録フィルタリング）
- 変更: `README.md`（設定項目のドキュメント）

## エッジケース/考慮

- `[tools]` セクション未定義 → 全 True。
- 一部のみ定義 → 未定義は True。
- 全 False → list_tools は空配列（許容）。
- ツール名変更/追加時 → 集合一致テストがブレークして通知。
- 環境変数の大文字/小文字 → ドキュメントでは大文字例を提示。

## 作業順序（細分化）

1. テスト: `tests/test_tool_names.py` 追加（2 件）。
2. 実装: `ToolsSettings` と `Settings.tools` を追加（green）。
3. テスト: `tests/test_settings.py` に 3 件追記（default/ TOML/ env）。
4. 実装: 読み込み/既定の調整（green）。
5. テスト: `tests/test_tool_registration.py` 追加（2 件）。
6. 実装: `__main__.py` の登録ロジックを設定連動に変更（green）。
7. ドキュメント更新。
8. フォーマット/静的解析/テスト一式（uv 経由）。

## 完了条件

- すべての新規・既存テストがグリーン。
- `ruff` フォーマット/チェックに問題なし。
- README に設定手順が反映されている。

## ロールバック戦略

- 設定項目追加によりリリース影響は後方互換（既定 True）。不具合時は `build_tools()` を固定配列に戻す簡易リバートが可能。

---

この計画で進めてよいかご確認ください。合意後、TDD 手順で実装に着手します。

## 実装するクラス/関数と主要ロジック（概要）

### 設定モデル
- 追加: `ToolsSettings`（`pydantic.BaseModel`）
  - フィールド（既定 True）:
    - `analyze_table_statistics: bool = True`
    - `describe_table: bool = True`
    - `execute_query: bool = True`
    - `list_schemas: bool = True`
    - `list_tables: bool = True`
    - `list_views: bool = True`
    - `sample_table_data: bool = True`
- 変更: `Settings`
  - フィールド追加: `tools: ToolsSettings`
  - 読み込み順序は既存の `settings_customise_sources` を踏襲（TOML→init→env→dotenv→secrets）

### ツール登録
- 変更: `SnowflakeServerContext`
  - フィールド追加: `settings: Settings | None = None`
  - `build_tools()` で `settings.tools` を参照し、有効なツールのみ生成:
    - 有効時にのみ各 `*Tool` を生成し、`self.tools = {tool.name: tool for tool in tools}`
- 変更: `main()`
  - `settings = Settings.build(...)` 後に `server_context.settings = settings`
  - 以降の処理は現状維持

### ハンドラ
- `handle_list_tools()` は現状のまま（登録済みツールのみ返却）
- `handle_call_tool()` も現状のまま（未登録ツールは `Unknown tool`）

### テストに必要な補助
- ツール名列挙は `src/mcp_snowflake/tool/__init__.py` のエクスポートを利用
- `SnowflakeClient` は登録時の生成のみで実行不要のため、必要なら簡易モックを定義

### ドキュメント
- README に `[tools]` セクションと PowerShell の環境変数例を追加

---

## 🎯 IMPLEMENTATION COMPLETED (2025-08-23)

### 実装完了サマリー

✅ **全要件実装完了** - TDD手法により以下を実装:

1. **ToolsSettings クラス** (`src/mcp_snowflake/settings.py`)
   - 7つのツール対応 boolean フィールド (デフォルト: True)
   - TOML/環境変数によるオーバーライド対応

2. **条件付きツール登録** (`src/mcp_snowflake/__main__.py`)
   - `SnowflakeServerContext.settings` フィールド追加
   - `build_tools()` で設定ベースの選択的ツール生成
   - `if __name__ == "__main__"` ガード追加

3. **検証テスト** (`tests/test_tool_names.py`)
   - `test_tool_name_unique()`: ツール名重複チェック
   - `test_settings_keys_match_tool_names()`: 設定キー/ツール名一致検証

4. **設定動作テスト** (`tests/test_settings.py`)
   - デフォルト全有効、TOML上書き、環境変数上書きを検証

5. **登録フィルタリングテスト** (`tests/test_tool_registration.py`)
   - 設定ベース選択的登録、`handle_list_tools` 除外を検証

### テスト結果
```
新規テスト: 8/8 PASSED
全体テスト: 449/449 PASSED
コード品質: All ruff checks PASSED
```

### 設定例

**TOML 設定**:
```toml
[tools]
execute_query = false          # クエリ実行無効化
analyze_table_statistics = true # 統計分析有効
describe_table = true
list_schemas = true
list_tables = true
list_views = true
sample_table_data = true
```

**PowerShell 環境変数**:
```powershell
$env:TOOLS__EXECUTE_QUERY="false"
$env:TOOLS__DESCRIBE_TABLE="false"
# 全ツール設定可能
```

### TDD実装検証
- ✅ Red→Green サイクル全コンポーネントで実施
- ✅ 各機能単位でテスト先行作成→実装
- ✅ 統合テストでエンドツーエンド動作確認
- ✅ 既存機能への影響なし (449テスト継続通過)

### ドキュメント更新完了
- ✅ README.md に [tools] 設定セクション追加
- ✅ PowerShell環境変数例追加
- ✅ 各ツール設定オプション文書化

### 変更ファイル
- **変更**: `src/mcp_snowflake/settings.py`, `src/mcp_snowflake/__main__.py`, `README.md`
- **追加**: `tests/test_tool_names.py`, `tests/test_tool_registration.py`
- **更新**: `tests/test_settings.py` (3テスト追加), `tests/fixtures/test.mcp_snowflake.toml`

**🏁 機能実装・テスト・ドキュメント化が全て完了し、本番導入可能状態です。**
