from hypothesis import given
from hypothesis import strategies as st

from expression import option


@given(st.one_of(st.integers(min_value=0, max_value=9), st.none()))
def test_unwrap_or_int(v: int | None) -> None:
    result = option.unwrap_or(v, 42)
    if v is None:
        assert result == 42
    else:
        assert result == v


@given(st.one_of(st.integers(min_value=-10, max_value=-1), st.none()))
def test_unwrap_or_negative_int(v: int | None) -> None:
    result = option.unwrap_or(v, 42)
    if v is None:
        assert result == 42
    else:
        assert result == v
