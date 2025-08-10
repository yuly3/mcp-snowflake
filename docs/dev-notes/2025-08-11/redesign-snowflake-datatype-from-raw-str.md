# Redesign SnowflakeDataType: normalized_type をフィールド化 + from_raw_str 追加／呼び出し側は None 分岐

日付: 2025-08-11

## ユーザープロンプト
- リファクタリング依頼
- ゴール: SnowflakeDataType を再デザイン。
  - normalized_type を field に加える
  - post_init で NormalizedSnowflakeDataType.__args__ に含まれていなければ例外
  - normalized_type property に変わって `(cls, str) -> Self | None` のシグネチャを持つ from_raw_str を実装
- 呼び出し側は from_raw_str を用いて None チェックで分岐するように変更
- 変更対象・シグネチャを具体化し開発ノートを作成

## 実装計画（Planning）

### 1. SnowflakeDataType の再設計

**フィールド構成**:
- raw_type: str（現状維持）
- normalized_type: NormalizedSnowflakeDataType（init=False, frozen）

**__attrs_post_init__ での処理**:
- 正規化を実行（大文字化、括弧除去、エイリアス変換、最終検証）
- 正規化結果が NormalizedSnowflakeDataType.__args__ に含まれなければ ValueError("Unsupported Snowflake data type: {raw}")
- object.__setattr__(self, "normalized_type", normalized) で設定

**新規メソッド**:
- from_raw_str を追加: `(cls, s: str) -> Self | None`
  - s が空白 or 未対応型 → None を返す
  - 対応型 → インスタンスを返す
- 正規化ロジックを private staticmethod `_normalize_raw_type(s: str) -> NormalizedSnowflakeDataType | None` に抽出

**メソッド更新**:
- 既存の @property normalized_type は廃止（同名フィールドへ置換）
- is_numeric / is_string / is_date / is_boolean / is_supported_for_statistics は self.normalized_type をフィールド参照するように変更（振る舞いは不変）

### 2. 呼び出し側の変更（None 分岐対応）

**src/mcp_snowflake/kernel/table_metadata.py**:
- TableColumn.snowflake_type プロパティ:
  - 生成を `SnowflakeDataType.from_raw_str(self.data_type)` に変更
  - 戻り値が None の場合は ValueError を送出（メッセージは "Unsupported Snowflake data type: {raw}" に統一）
- これにより、呼び出し側の方針転換（None 分岐）が可能に（ただしプロパティの戻り型は従来通り SnowflakeDataType のまま）

### 3. テスト更新

**tests/kernel/test_data_types.py**:
- `test_normalized_type_with_unsupported_type_raises_error`: 例外の発生箇所を「プロパティアクセス時→コンストラクタ時」に変更
- `from_raw_str` の新規テスト追加: 成功・未対応・空白の3ケース

**tests/kernel/test_table_metadata.py**:
- エラーメッセージの互換性は維持不要（統一メッセージに変更）
- snowflake_type 内部実装が from_raw_str ベースに変わっても、公開 API の期待は変えない

### 4. API シグネチャ（具体化）

```python
@attrs.define(frozen=True)
class SnowflakeDataType:
    raw_type: str
    normalized_type: NormalizedSnowflakeDataType = attrs.field(init=False)

    def __attrs_post_init__(self) -> None: ...

    @classmethod
    def from_raw_str(cls, s: str) -> Self | None: ...

    @staticmethod
    def _normalize_raw_type(s: str) -> NormalizedSnowflakeDataType | None: ...

    def is_numeric(self) -> bool: ...
    def is_string(self) -> bool: ...
    def is_date(self) -> bool: ...
    def is_boolean(self) -> bool: ...
    def is_supported_for_statistics(self) -> bool: ...
```

### 5. 正規化仕様（現状踏襲）

- 大文字化・前後空白トリム
- 括弧以降の除去（例: VARCHAR(255) → VARCHAR）
- エイリアス変換:
  - NUMERIC→DECIMAL, INTEGER→INT, DOUBLE PRECISION→DOUBLE, FLOAT4/8→FLOAT, CHARACTER→CHAR, DATETIME→TIMESTAMP_NTZ, VARBINARY→BINARY
- `NormalizedSnowflakeDataType.__args__` に含まれるかで妥当性判定

### 6. 重要な変更点

**例外タイミング変更**:
- 従来: `sf_type.normalized_type` アクセス時に未対応型で ValueError
- 変更後: `SnowflakeDataType(raw)` 生成時に未対応型で ValueError。空文字も個別メッセージは発さず未対応型として扱う。
- 回避手段: `SnowflakeDataType.from_raw_str(raw)` で None 分岐を行う

**エラーメッセージ統一**:
- 従来: 空文字 = "raw_type cannot be empty", 未対応型 = "Unsupported Snowflake data type: {raw}"
- 変更後: 空文字・未対応型ともに "Unsupported Snowflake data type: {raw}"

### 7. 移行ガイド

- 既存コード：`sf_type.normalized_type` アクセスはそのまま利用可能
- 新規コード：例外を避けたい場合は `SnowflakeDataType.from_raw_str(raw)` を使用
- デバッグ：未対応型の例外が生成時に発生することを認識

## 実装手順（TDD: red → implement → green）

### Cycle 1: SnowflakeDataType の API 変更テストを先に更新（red）

**対象ファイル**: `tests/kernel/test_data_types.py`

**変更内容**:
1) 未対応型の例外タイミングを「プロパティアクセス時 → 生成時」に変更
```python
# 変更前
sf_type = SnowflakeDataType("UNSUPPORTED_TYPE")
with pytest.raises(ValueError, match="Unsupported Snowflake data type"):
    _ = sf_type.normalized_type

# 変更後
with pytest.raises(ValueError, match="Unsupported Snowflake data type"):
    _ = SnowflakeDataType("UNSUPPORTED_TYPE")
```

2) `from_raw_str` の新規テストを追加
```python
def test_from_raw_str_success(self) -> None:
    sf_type = SnowflakeDataType.from_raw_str("VARCHAR(10)")
    assert sf_type is not None
    assert sf_type.normalized_type == "VARCHAR"

def test_from_raw_str_with_unsupported_type_returns_none(self) -> None:
    assert SnowflakeDataType.from_raw_str("UNSUPPORTED") is None

def test_from_raw_str_with_empty_string_returns_none(self) -> None:
    assert SnowflakeDataType.from_raw_str("   ") is None
```

**期待される結果（red）**: まだ実装していないため、新規/更新テストが失敗する。

### Cycle 2: SnowflakeDataType の実装（implement）

**対象ファイル**: `src/mcp_snowflake/kernel/data_types.py`

**実装内容**:
1) フィールド追加と post_init での検証
2) 正規化ロジックを `_normalize_raw_type` staticmethod に抽出
3) `from_raw_str` クラスメソッドを追加
4) 既存メソッドの参照先を normalized_type フィールドに更新

**期待される結果**: Cycle 1 で追加/更新した `test_data_types.py` がグリーンに近づく。

### Cycle 3: 呼び出し側の `from_raw_str` 化（implement → green）

**対象ファイル**: `src/mcp_snowflake/kernel/table_metadata.py`

**実装内容**:
```python
@property
def snowflake_type(self) -> SnowflakeDataType:
    sf_type = SnowflakeDataType.from_raw_str(self.data_type)
    if sf_type is None:
        raise ValueError(f"Unsupported Snowflake data type: {self.data_type}")
    return sf_type
```

**期待される結果（green）**: `tests/kernel/test_table_metadata.py` を含め、関連テストがすべてパスする。

### 品質ゲート

```bash
uv run ruff check --fix --unsafe-fixes src tests
uv run ruff format src tests
uv run pytest -q
```

## 実装完了（Completion）

**実施日**: 2025-08-11
**ステータス**: ✅ 完了（TDD 3サイクルすべて green 達成）

### 実装サマリ

**Cycle 1（red）**: テスト先行更新により期待される API 変更を定義
- `tests/kernel/test_data_types.py` を更新
- 未対応型の例外タイミングを「プロパティアクセス時→コンストラクタ時」に変更
- `from_raw_str` 新規テスト追加（成功、失敗、空文字の3ケース）
- 期待通り red 状態を確認

**Cycle 2（implement）**: SnowflakeDataType の実装変更
- `src/mcp_snowflake/kernel/data_types.py` を全面改修
- normalized_type をフィールド化（`attrs.field(init=False)`）
- __attrs_post_init__ で正規化・検証を実行し、結果をフィールドに設定
- 既存の正規化ロジックを `_normalize_raw_type` staticmethod に抽出
- `from_raw_str` クラスメソッドを追加（None 分岐対応）
- is_numeric/is_string/is_date/is_boolean メソッドは normalized_type フィールド参照に変更
- 空文字のエラーメッセージを "Unsupported Snowflake data type" に統一

**Cycle 3（green）**: 呼び出し側の修正
- `src/mcp_snowflake/kernel/table_metadata.py` の TableColumn.snowflake_type を from_raw_str 使用に変更
- `tests/kernel/test_table_metadata.py` のエラーメッセージ期待値を統一
- 全体テスト green 達成

### テスト結果
- **data_types.py テスト**: 96 passed (追加テスト 3件含む)
- **table_metadata.py テスト**: 22 passed (エラーメッセージ変更対応済み)
- **プロジェクト全体**: 282 passed（リグレッションなし）

### 影響範囲最終確認

**直接的な影響**:
- `src/mcp_snowflake/kernel/data_types.py`: 全面改修
- `src/mcp_snowflake/kernel/table_metadata.py`: TableColumn.snowflake_type の実装変更
- `tests/kernel/test_data_types.py`: テスト 3件追加、期待値 2件更新
- `tests/kernel/test_table_metadata.py`: 期待値 1件更新

**互換性**:
- 公開 API（normalized_type フィールドアクセス）は後方互換
- 例外タイミングの早期化により、デバッグ時の例外発生箇所が変化
- エラーメッセージの統一により、特定文言に依存するコードがあれば要注意

**品質確認**:
- Ruff linting: All checks passed
- Ruff formatting: 1 file reformatted
- 全テスト: 282 passed（リグレッションなし）

**TDD の効果**: red → implement → green のサイクルにより、仕様変更を安全かつ効率的に実行。特に例外タイミングやメッセージ変更などの破壊的変更も、テスト先行で期待値を明確化することで、実装ミスを防ぎつつスムーズに進行。

---

補足: Python は本プロジェクトの規約に従い `uv` を使用して開発・テストを実行します。
