"""
Tests for JSON converter functionality.
"""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from threading import Lock
from typing import Any
from uuid import UUID

import pytest
from hypothesis import given
from hypothesis import strategies as st

from cattrs_converter import (
    Jsonable,
    JsonImmutableConverter,
)


@pytest.fixture(scope="class")
def converter() -> JsonImmutableConverter:
    """Fixture for creating a JSON contextual converter."""
    return JsonImmutableConverter()


class TestJsonImmutableConverter:
    """Test JsonImmutableConverter class."""

    @given(st.text())
    def test_basic_strings(self, converter: JsonImmutableConverter, text: str) -> None:
        """Test conversion of strings with property-based testing."""
        assert converter.unstructure(text) == text

    @given(st.integers())
    def test_basic_integers(
        self,
        converter: JsonImmutableConverter,
        integer: int,
    ) -> None:
        """Test conversion of integers with property-based testing."""
        assert converter.unstructure(integer) == integer

    @given(st.floats(allow_nan=False, allow_infinity=False))
    def test_basic_floats(
        self,
        converter: JsonImmutableConverter,
        float_val: float,
    ) -> None:
        """Test conversion of floats with property-based testing."""
        assert converter.unstructure(float_val) == float_val

    @given(st.booleans())
    def test_basic_booleans(
        self,
        converter: JsonImmutableConverter,
        bool_val: bool,  # noqa: FBT001
    ) -> None:
        """Test conversion of booleans with property-based testing."""
        assert converter.unstructure(bool_val) is bool_val

    def test_basic_none(self, converter: JsonImmutableConverter) -> None:
        """Test conversion of None."""
        assert converter.unstructure(None) is None

    @given(st.lists(st.integers(), max_size=10))
    def test_basic_lists(
        self,
        converter: JsonImmutableConverter,
        int_list: list[int],
    ) -> None:
        """Test conversion of lists with property-based testing."""
        assert converter.unstructure(int_list) == int_list

    @given(st.dictionaries(st.text(), st.integers(), max_size=10))
    def test_basic_dicts(
        self,
        converter: JsonImmutableConverter,
        dictionary: dict[str, int],
    ) -> None:
        """Test conversion of dictionaries with property-based testing."""
        assert converter.unstructure(dictionary) == dictionary

    @given(st.datetimes(timezones=st.just(UTC)))
    def test_cattrs_datetime_conversions(
        self,
        converter: JsonImmutableConverter,
        dt: datetime,
    ) -> None:
        """Test datetime conversion with property-based testing."""
        result = converter.unstructure(dt)
        assert isinstance(result, str)
        # Should be valid ISO format
        parsed_dt = datetime.fromisoformat(result)
        assert parsed_dt == dt

    @given(st.dates())
    def test_cattrs_date_conversions(
        self,
        converter: JsonImmutableConverter,
        d: date,
    ) -> None:
        """Test date conversion with property-based testing."""
        result = converter.unstructure(d)
        assert isinstance(result, str)
        # Should be valid ISO format
        parsed_date = date.fromisoformat(result)
        assert parsed_date == d

    @given(st.decimals(allow_nan=False, allow_infinity=False))
    def test_cattrs_decimal_conversions(
        self,
        converter: JsonImmutableConverter,
        dec: Decimal,
    ) -> None:
        """Test decimal conversion with property-based testing."""
        result = converter.unstructure(dec)
        assert isinstance(result, float)
        # Should be approximately equal (accounting for float precision)
        assert abs(result - float(dec)) < 1e-10

    @given(st.uuids())
    def test_cattrs_uuid_conversions(
        self,
        converter: JsonImmutableConverter,
        uuid_obj: UUID,
    ) -> None:
        """Test UUID conversion with property-based testing."""
        result = converter.unstructure(uuid_obj)
        assert isinstance(result, str)
        # Should be valid UUID string
        parsed_uuid = UUID(result)
        assert parsed_uuid == uuid_obj

    @given(st.sets(st.integers(), max_size=10))
    def test_cattrs_set_conversions(
        self,
        converter: JsonImmutableConverter,
        int_set: set[int],
    ) -> None:
        """Test set conversion with property-based testing."""
        result = converter.unstructure(int_set)
        assert isinstance(result, list)
        # Should contain the same elements (order may differ)
        assert set(result) == int_set

    def test_unsupported_types(self, converter: JsonImmutableConverter) -> None:
        """Test types that remain unsupported after cattrs conversion."""
        # Complex number
        result = converter.unstructure_safely(1 + 2j)
        assert result == "<unsupported_type: complex>"

        # Lock object
        result = converter.unstructure_safely(Lock())
        assert result == "<unsupported_type: lock>"

        # Function
        def test_func() -> None:
            pass

        result = converter.unstructure_safely(test_func)
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
    def test_json_dumps_compatibility_basic_types(
        self,
        converter: JsonImmutableConverter,
        value: Any,
    ) -> None:
        """Test that converted basic values can be serialized with json.dumps()."""
        converted = converter.unstructure(value)
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
    def test_json_dumps_compatibility_cattrs_types(
        self,
        converter: JsonImmutableConverter,
        value: Any,
    ) -> None:
        """Test that converted cattrs values can be serialized with json.dumps()."""
        converted = converter.unstructure(value)
        # Should not raise an exception
        json_str = json.dumps(converted)
        assert isinstance(json_str, str)
        # Should be able to parse back
        parsed = json.loads(json_str)
        assert parsed == converted


class TestRegisterUnstructureHook:
    def test_register_unstructure_hook(
        self,
        converter: JsonImmutableConverter,
    ) -> None:
        class CustomType:
            def __init__(self, value: int) -> None:
                self.value = value

        def custom_type_hook(obj: CustomType) -> dict[str, Jsonable]:
            return {"custom_value": obj.value}

        result = converter.unstructure_safely(CustomType(42))
        assert result == "<unsupported_type: CustomType>"

        new_converter = converter.register_unstructure_hook(
            CustomType,
            custom_type_hook,
        )
        result = new_converter.unstructure(CustomType(42))
        assert result == {"custom_value": 42}

        result = converter.unstructure_safely(CustomType(42))
        assert result == "<unsupported_type: CustomType>"
