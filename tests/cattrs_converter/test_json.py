"""
Tests for JSON converter functionality.
"""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from threading import Lock
from typing import Any
from uuid import UUID

from hypothesis import given
from hypothesis import strategies as st

from cattrs_converter import convert_to_json_safe


class TestConvertToJsonSafe:
    """Test convert_to_json_safe function."""

    @given(st.text())
    def test_basic_strings(self, text: str) -> None:
        """Test conversion of strings with property-based testing."""
        assert convert_to_json_safe(text) == text

    @given(st.integers())
    def test_basic_integers(self, integer: int) -> None:
        """Test conversion of integers with property-based testing."""
        assert convert_to_json_safe(integer) == integer

    @given(st.floats(allow_nan=False, allow_infinity=False))
    def test_basic_floats(self, float_val: float) -> None:
        """Test conversion of floats with property-based testing."""
        assert convert_to_json_safe(float_val) == float_val

    @given(st.booleans())
    def test_basic_booleans(self, bool_val: bool) -> None:  # noqa: FBT001
        """Test conversion of booleans with property-based testing."""
        assert convert_to_json_safe(bool_val) is bool_val

    def test_basic_none(self) -> None:
        """Test conversion of None."""
        assert convert_to_json_safe(None) is None

    @given(st.lists(st.integers(), max_size=10))
    def test_basic_lists(self, int_list: list[int]) -> None:
        """Test conversion of lists with property-based testing."""
        assert convert_to_json_safe(int_list) == int_list

    @given(st.dictionaries(st.text(), st.integers(), max_size=10))
    def test_basic_dicts(self, dictionary: dict[str, int]) -> None:
        """Test conversion of dictionaries with property-based testing."""
        assert convert_to_json_safe(dictionary) == dictionary

    @given(st.datetimes(timezones=st.just(UTC)))
    def test_cattrs_datetime_conversions(self, dt: datetime) -> None:
        """Test datetime conversion with property-based testing."""
        result = convert_to_json_safe(dt)
        assert isinstance(result, str)
        # Should be valid ISO format
        parsed_dt = datetime.fromisoformat(result)
        assert parsed_dt == dt

    @given(st.dates())
    def test_cattrs_date_conversions(self, d: date) -> None:
        """Test date conversion with property-based testing."""
        result = convert_to_json_safe(d)
        assert isinstance(result, str)
        # Should be valid ISO format
        parsed_date = date.fromisoformat(result)
        assert parsed_date == d

    @given(st.decimals(allow_nan=False, allow_infinity=False))
    def test_cattrs_decimal_conversions(self, dec: Decimal) -> None:
        """Test decimal conversion with property-based testing."""
        result = convert_to_json_safe(dec)
        assert isinstance(result, float)
        # Should be approximately equal (accounting for float precision)
        assert abs(result - float(dec)) < 1e-10

    @given(st.uuids())
    def test_cattrs_uuid_conversions(self, uuid_obj: UUID) -> None:
        """Test UUID conversion with property-based testing."""
        result = convert_to_json_safe(uuid_obj)
        assert isinstance(result, str)
        # Should be valid UUID string
        parsed_uuid = UUID(result)
        assert parsed_uuid == uuid_obj

    @given(st.sets(st.integers(), max_size=10))
    def test_cattrs_set_conversions(self, int_set: set[int]) -> None:
        """Test set conversion with property-based testing."""
        result = convert_to_json_safe(int_set)
        assert isinstance(result, list)
        # Should contain the same elements (order may differ)
        assert set(result) == int_set

    def test_unsupported_types(self) -> None:
        """Test types that remain unsupported after cattrs conversion."""
        # Complex number
        result = convert_to_json_safe(1 + 2j)
        assert result == "<unsupported_type: complex>"

        # Lock object
        result = convert_to_json_safe(Lock())
        assert result == "<unsupported_type: lock>"

        # Function
        def test_func() -> None:
            pass

        result = convert_to_json_safe(test_func)
        assert result == "<unsupported_type: function>"

    @given(
        st.one_of(
            st.text(),
            st.integers(),
            st.floats(allow_nan=False, allow_infinity=False),
            st.booleans(),
            st.none(),
            st.lists(st.integers(), max_size=5),
            st.dictionaries(st.text(), st.integers(), max_size=5),
        ),
    )
    def test_json_dumps_compatibility_basic_types(self, value: Any) -> None:
        """Test that converted basic values can be serialized with json.dumps()."""
        converted = convert_to_json_safe(value)
        # Should not raise an exception
        json_str = json.dumps(converted)
        assert isinstance(json_str, str)
        # Should be able to parse back
        parsed = json.loads(json_str)
        assert parsed == converted

    @given(
        st.one_of(
            st.datetimes(timezones=st.just(UTC)),
            st.dates(),
            st.decimals(allow_nan=False, allow_infinity=False),
            st.uuids(),
            st.sets(st.integers(), max_size=5),
        ),
    )
    def test_json_dumps_compatibility_cattrs_types(self, value: Any) -> None:
        """Test that converted cattrs values can be serialized with json.dumps()."""
        converted = convert_to_json_safe(value)
        # Should not raise an exception
        json_str = json.dumps(converted)
        assert isinstance(json_str, str)
        # Should be able to parse back
        parsed = json.loads(json_str)
        assert parsed == converted
