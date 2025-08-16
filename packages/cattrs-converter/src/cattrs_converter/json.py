"""
JSON conversion utilities using cattrs for flexible type handling.
"""

from decimal import Decimal
from typing import Any, TypeGuard
from uuid import UUID

from cattrs.preconf.json import JsonConverter, make_converter

from ._converter import ImmutableConverter

Jsonable = None | bool | int | float | str | list["Jsonable"] | dict[str, "Jsonable"]


class JsonImmutableConverter(ImmutableConverter[JsonConverter, Jsonable]):
    """
    A specialized ImmutableConverter for JSON-safe data conversion.

    This converter automatically handles common Python types that need special
    treatment when converting to JSON, such as Decimal and UUID objects.
    It ensures all output values are JSON-compatible.

    The converter comes pre-configured with hooks for:
    - Decimal: converted to float
    - UUID: converted to string
    - datetime: converted to ISO format string (via cattrs default behavior)

    Examples
    --------
    >>> converter = JsonImmutableConverter()
    >>> converter.unstructure("hello")
    'hello'
    >>> converter.unstructure(42)
    42
    >>> converter.unstructure([1, 2, 3])
    [1, 2, 3]
    >>> converter.unstructure({"key": "value"})
    {'key': 'value'}
    """

    def __init__(self) -> None:
        converter = make_converter()

        # Register custom unstructure hooks for common types
        converter.register_unstructure_hook(Decimal, _convert_decimal_to_float)
        converter.register_unstructure_hook(UUID, _convert_uuid_to_str)

        super().__init__(converter, is_json_compatible_type)

    def unstructure_safely(self, value: Any) -> Jsonable:
        """
        Convert a value to a JSON-safe representation using cattrs.

        This method attempts to convert any value to a JSON-compatible format.
        If the conversion results in an unsupported type, it returns a string
        representation indicating the unsupported type.

        Parameters
        ----------
        value : Any
            The value to convert

        Returns
        -------
        Jsonable
            JSON-safe representation of the value

        Examples
        --------
        >>> converter = JsonImmutableConverter()
        >>> converter.unstructure_safely("hello")
        'hello'
        >>> converter.unstructure_safely(42)
        42
        >>> converter.unstructure_safely([1, 2, 3])
        [1, 2, 3]
        >>> converter.unstructure_safely({"key": "value"})
        {'key': 'value'}
        >>> from datetime import datetime
        >>> dt = datetime(2025, 8, 10, 12, 30, 45)
        >>> converter.unstructure_safely(dt)
        '2025-08-10T12:30:45'
        >>> from decimal import Decimal
        >>> converter.unstructure_safely(Decimal("123.45"))
        123.45
        >>> import uuid
        >>> converter.unstructure_safely(uuid.UUID('12345678-1234-5678-1234-567812345678'))
        '12345678-1234-5678-1234-567812345678'
        """
        unstructured = self._converter.unstructure(value)
        if self._type_guard(unstructured):
            return unstructured
        return f"<unsupported_type: {type(value).__name__}>"


def _convert_decimal_to_float(dec: Decimal) -> float:
    """
    Convert Decimal to float for JSON compatibility.

    Parameters
    ----------
    dec : Decimal
        Decimal object to convert

    Returns
    -------
    float
        Float representation of the decimal

    Examples
    --------
    >>> from decimal import Decimal
    >>> _convert_decimal_to_float(Decimal("123.45"))
    123.45
    >>> _convert_decimal_to_float(Decimal("0"))
    0.0
    """
    return float(dec)


def _convert_uuid_to_str(uuid_obj: UUID) -> str:
    """
    Convert UUID to string for JSON compatibility.

    Parameters
    ----------
    uuid_obj : UUID
        UUID object to convert

    Returns
    -------
    str
        String representation of the UUID

    Examples
    --------
    >>> import uuid
    >>> test_uuid = uuid.UUID('12345678-1234-5678-1234-567812345678')
    >>> _convert_uuid_to_str(test_uuid)
    '12345678-1234-5678-1234-567812345678'
    """
    return str(uuid_obj)


def is_json_compatible_type(value: Any) -> TypeGuard[Jsonable]:
    """
    Check if a value is a JSON-compatible type.

    JSON supports: null, bool, int, float, str, list, dict (with string keys)

    Parameters
    ----------
    value : Any
        The value to check

    Returns
    -------
    bool
        True if the value is JSON-compatible

    Examples
    --------
    >>> is_json_compatible_type(None)
    True
    >>> is_json_compatible_type("hello")
    True
    >>> is_json_compatible_type(42)
    True
    >>> is_json_compatible_type([1, 2, 3])
    True
    >>> is_json_compatible_type({"key": "value"})
    True
    >>> is_json_compatible_type({1: "invalid_key"})
    False
    >>> import datetime
    >>> is_json_compatible_type(datetime.datetime.now())
    False
    """
    if value is None:
        return True
    if isinstance(value, bool | int | float | str):
        return True
    if isinstance(value, list):
        return all(is_json_compatible_type(item) for item in value)
    if isinstance(value, dict):
        return all(isinstance(key, str) for key in value) and all(
            is_json_compatible_type(val) for val in value.values()
        )
    return False
