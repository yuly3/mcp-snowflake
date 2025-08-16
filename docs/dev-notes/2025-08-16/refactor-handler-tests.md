# Refactor handler tests (line reduction & dedup)

## Date
2025-08-16

## User Prompt
```
handlerのtestを探索しコード量の削減を検討
意味的な重複、テストケースのマージ、helper関数の導入などの可能性がないか思考
計画を提案しユーザと議論
```

### User Decisions / Constraints (Confirmed)
| Item | Decision |
|------|----------|
| list_* (schemas/tables/views) file merge | No (keep 3 separate files) |
| describe_table consolidation | Two groups; success scenarios will use pytest parametrize for clarity |
| execute_query & sample_table_data commonization | Only format (JSON shape) verification shared; keep behavior-specific tests separate |
| Parametrization level | Low overall; exception: describe_table success cases use parametrize |
| Helper module filename | `_utils.py` approved |
| EXPECTED_RESPONSE_KEYS centralization | Only for describe_table |

## Goals
- Reduce repetitive assertion / setup code while preserving clarity.
- Maintain or improve coverage (target: ±1% lines/branches from current).
- Avoid over-parameterization (readability priority).

## Non-Goals
- Large-scale redesign of analyze_table_statistics test suite (left intact apart from optional future tasks).
- Merging list_* test files.

## High-Level Strategy
1. Introduce lightweight shared helpers in `tests/handler/_utils.py` for common patterns (single TextContent extraction, JSON parsing & key assertion, simple factory for columns / table info).
2. Refactor each handler test file incrementally to use helpers, without aggressive parametrization.
3. Consolidate `describe_table` tests into **two** comprehensive tests:
    - `test_describe_table_success_variants` (covers: base success, empty table, all nullable, all required, pure JSON) implemented with `@pytest.mark.parametrize` for clearer reporting of individual scenario failures (user requested parametrize here despite low-param preference globally).
    - `test_describe_table_error_variants` (covers: effect handler exception). Keep args validation tests separate (they test pydantic model behavior distinct from handler).
4. Add minimal shared format assertions for `execute_query` and `sample_table_data`: a helper `assert_tabular_json(result_dict, key_name, expected_columns, expected_row_count)`.
5. Keep individual behavioral tests (write-sql blocked; timeout; unsupported types warnings) untouched except replacing boilerplate with helpers where safe.
6. For list_* handlers, introduce tiny helper `assert_list_output(text: str, header: str, items: list[str])` used inside each file; do **not** unify into single module or parametrize across handlers.

## Proposed Helper Functions (Pseudo-code)
```python
# tests/handler/_utils.py
import json
import mcp.types as types
from typing import Any, Iterable, Sequence

def assert_single_text(result: Sequence[Any]) -> types.TextContent:
    assert len(result) == 1, f"Expected single content, got {len(result)}"
    content = result[0]
    assert isinstance(content, types.TextContent)
    assert content.type == "text"
    return content

def parse_json_text(content: types.TextContent) -> dict[str, Any]:
    return json.loads(content.text)

def assert_keys_exact(obj: dict[str, Any], expected: set[str]) -> None:
    assert set(obj.keys()) == expected, f"Keys mismatch: {set(obj) } != {expected}"

def assert_tabular_json(root: dict[str, Any], key: str, expected_cols: list[str], expected_row_count: int) -> dict[str, Any]:
    assert key in root
    data = root[key]
    assert data["columns"] == expected_cols
    assert data["row_count"] == expected_row_count or data.get("actual_rows") == expected_row_count
    return data

def assert_list_output(text: str, header: str, items: Iterable[str]) -> None:
    assert header in text
    for it in items:
        assert f"- {it}" in text

# Optional small factory for columns (describe_table)
from kernel.table_metadata import TableColumn

def col(name: str, data_type: str, nullable: bool, pos: int, comment: str | None = None, default: str | None = None) -> TableColumn:
    return TableColumn(name=name, data_type=data_type, nullable=nullable, ordinal_position=pos, comment=comment, default_value=default)
```

## File-Specific Refactor Plan (Pseudo-code Diffs)

### tests/handler/test_describe_table.py
- Remove repetitive JSON/key assertions inside each success test; replace with helper calls.
- Combine success scenarios using parametrize:
```python
@pytest.mark.parametrize(
    "label,columns_spec,checks",
    [
        ("base", [col("ID","NUMBER(38,0)", False,1), col("NAME","VARCHAR(100)", True,2)], {"expect_nullable_counts": (1,1)}),
        ("empty", [], {}),
        ("all_nullable", [col("A","VARCHAR(50)", True,1), col("B","INTEGER", True,2)], {"all_nullable": True}),
        ("all_required", [col("A","VARCHAR(50)", False,1), col("B","INTEGER", False,2)], {"all_required": True}),
        ("pure_json", [col("ID","NUMBER(38,0)", False,1), col("NAME","VARCHAR(100)", True,2)], {"pure_json": True}),
    ],
    ids=lambda x: x if isinstance(x, str) else None,
)
async def test_describe_table_success_variants(label, columns_spec, checks):
    # build TableInfo from columns_spec
    # call handler
    # use assert_single_text + parse_json_text + assert_keys_exact
    # scenario-specific conditional assertions based on checks dict
```
- Keep args validation class untouched (still explicit tests for readability).
- Create second grouped test for error case: `test_describe_table_error_variants`.
- Delete now-redundant individual success test functions.

### tests/handler/test_execute_query.py
- Introduce helper usage:
  - Replace manual 3-line pattern (len==1, isinstance, type) with `assert_single_text`.
  - After parsing JSON: `assert_tabular_json(response_data, "query_result", expected_cols, expected_row_count)`.
- Keep distinct tests: success, write blocked, empty, timeout, execution error, processing functions, format function.
- Minor: factor repeated EXPECTED_RESPONSE_KEYS check by asserting with `assert_keys_exact`.

### tests/handler/test_sample_table_data.py
- Similar to execute_query: use `assert_single_text`, `parse_json_text`, `assert_tabular_json` (with key "sample_data").
- DataProcessingResult tests remain but could share small helper if obvious; minimal change.

### tests/handler/test_list_schemas.py / test_list_tables.py / test_list_views.py
- Add local usage of `assert_single_text` and `assert_list_output`.
- Keep each scenario test separate (success / empty / exception / single / naming variations).

## Expected Line Impact (Approx.)
| File | Before (est.) | After (target) | Reduction |
|------|---------------|----------------|-----------|
| test_describe_table.py | 470 | 240-260 | ~45% |
| test_execute_query.py | 190 | 160 | ~15% |
| test_sample_table_data.py | 240 | 200 | ~17% |
| test_list_* (3 files) | 300 | 270 | ~10% |
| Helpers added | +120 | — | — |
| Net | ~1,200 | ~990 | ~17% overall |

## Coverage Considerations
- All unique behavioral branches remain represented (error handling, empty set, unsupported types, write-block, nullable patterns).
- Consolidation in describe_table merges structurally identical success assertions; ensures each variant still executed.

## Rollout Phases
1. Add `_utils.py` + migrate `execute_query` & `sample_table_data` simplest helper usage.
2. Refactor list_* files to use helpers (no logic change).
3. Refactor `describe_table` into grouped tests.
4. Cleanup: remove dead constants / redundant EXPECTED_RESPONSE_KEYS when moved to helper or inline.
5. Run full test suite each phase (commit checkpoints).

## Risks & Mitigations
| Risk | Mitigation |
|------|------------|
| Harder to pinpoint failing variant inside grouped describe_table test | Include scenario label in assertion messages; optional `print(label)` on failure; structured for loops with `pytest.fail(f"[{label}] ...")` on branch-specific failure |
| Helper misuse masking a subtle difference | Keep direct field assertions for semantic differences (nullable/all required) |
| Future contributor confusion | Add docstring in `_utils.py` explaining narrow scope & not for production code |

## Status / Next Action
User approved helper file name and parametrize usage specifically for describe_table success scenarios. EXPECTED_RESPONSE_KEYS centralization limited to describe_table. Proceed to Phase 1 implementation (helpers + minimal adoption) once user signals to start.

---
Please review and confirm the open questions or request adjustments before implementation.
