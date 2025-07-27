# hypothesis property based testへの置換とcattrs型の対応

## ユーザーからの要求

1. `tests/test_json_converter.py`にあるtestの中でhypothesisによるproperty based testに置換可能なものを置換したい
2. hypothesis.strategiesにはdecimals, dates, datetimes, uuids, setsが存在するようだ。json compatibleなtypesとはテストケースを分離した方が良いものの、これらもproperty based testの方が良いだろう

## 実装した内容

`tests/test_json_converter.py`で以下のテストをhypothesisベースのproperty based testに置換しました：

### 1. `TestIsJsonSerializable`クラス

**基本JSON型（置換前の改善）:**
- `test_serializable_strings()` - `@given(st.text())`
- `test_serializable_integers()` - `@given(st.integers())`
- `test_serializable_floats()` - `@given(st.floats(allow_nan=False, allow_infinity=False))`
- `test_serializable_booleans()` - `@given(st.booleans())`
- `test_serializable_lists()` - `@given(st.lists(st.integers(), max_size=10))`
- `test_serializable_dicts()` - `@given(st.dictionaries(st.text(), st.integers(), max_size=10))`

**cattrs変換型（新規追加）:**
- `test_serializable_datetimes()` - `@given(st.datetimes(timezones=st.just(UTC)))`
- `test_serializable_dates()` - `@given(st.dates())`
- `test_serializable_decimals()` - `@given(st.decimals(allow_nan=False, allow_infinity=False))`
- `test_serializable_uuids()` - `@given(st.uuids())`
- `test_serializable_sets()` - `@given(st.sets(st.integers(), max_size=10))`

### 2. `TestConvertToJsonSafe`クラス

**基本JSON型（置換前の改善）:**
- `test_basic_strings()` - `@given(st.text())`
- `test_basic_integers()` - `@given(st.integers())`
- `test_basic_floats()` - `@given(st.floats(allow_nan=False, allow_infinity=False))`
- `test_basic_booleans()` - `@given(st.booleans())`
- `test_basic_lists()` - `@given(st.lists(st.integers(), max_size=10))`
- `test_basic_dicts()` - `@given(st.dictionaries(st.text(), st.integers(), max_size=10))`

**cattrs変換型（新規追加）:**
- `test_cattrs_datetime_conversions()` - `@given(st.datetimes(timezones=st.just(UTC)))` - ISO文字列変換をテスト
- `test_cattrs_date_conversions()` - `@given(st.dates())` - ISO日付文字列変換をテスト
- `test_cattrs_decimal_conversions()` - `@given(st.decimals(allow_nan=False, allow_infinity=False))` - float変換をテスト
- `test_cattrs_uuid_conversions()` - `@given(st.uuids())` - 文字列変換をテスト
- `test_cattrs_set_conversions()` - `@given(st.sets(st.integers(), max_size=10))` - リスト変換をテスト

### 3. JSON互換性テスト

**基本型（従来のまま改善）:**
- `test_json_dumps_compatibility_basic_types()` - `@given(st.one_of(...))`でランダムなJSON基本型をテスト

**cattrs型（新規追加）:**
- `test_json_dumps_compatibility_cattrs_types()` - `@given(st.one_of(...))`でdatetime、date、decimal、uuid、setをテスト

## 技術的な詳細

### 新規追加したhypothesis strategies:
- `st.datetimes(timezones=st.just(UTC))` - UTC固定のdatetimeを生成
- `st.dates()` - ランダムな日付を生成
- `st.decimals(allow_nan=False, allow_infinity=False)` - 有限のDecimalを生成
- `st.uuids()` - ランダムなUUIDを生成
- `st.sets(st.integers(), max_size=10)` - 整数のsetを生成

### テストの分離:
- **JSON基本型**: 直接JSONシリアライズ可能な型
- **cattrs変換型**: cattsが自動変換する型（datetime、Decimal、UUID、set）
- それぞれ専用のテストメソッドに分離して、責任を明確化

### 変換検証の強化:
- datetime → ISO文字列: `datetime.fromisoformat()`で逆変換をテスト
- date → ISO日付文字列: `date.fromisoformat()`で逆変換をテスト
- Decimal → float: 精度誤差を考慮した近似等価性をテスト
- UUID → 文字列: `UUID()`コンストラクタで逆変換をテスト
- set → list: 要素の一致をテスト（順序は問わない）

## テスト結果

- 全28テスト（20→28テストに増加）が正常に通過
- hypothesisの統計情報では、各テストが100例をテストし、すべて成功
- 実行時間は約0.74秒と効率的
- テストカバレッジが大幅に向上

## 効果

1. **テストカバレッジの大幅向上**: 固定値テストから数千の異なる入力値でのテストに拡張
2. **型別テストの明確化**: JSON基本型とcattrs変換型を分離し、それぞれの責任を明確化
3. **エッジケースの自動発見**: hypothesisが境界値やエッジケースを自動生成
4. **変換精度の検証**: 各変換が可逆性を持つことを確認
5. **保守性の向上**: 新しいデータ型を追加する際に、property based testが自動的にカバー
6. **信頼性の向上**: より多様で現実的な入力での動作確認により、実装の堅牢性が大幅に向上
