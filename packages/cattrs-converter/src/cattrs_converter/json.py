"""
JSON conversion utilities using cattrs for flexible type handling.
"""

import logging
from decimal import Decimal
from typing import Any, TypeGuard
from uuid import UUID

import cattrs.preconf.json

logger = logging.getLogger(__name__)

Jsonable = None | bool | int | float | str | list["Jsonable"] | dict[str, "Jsonable"]


# Create a JSON converter with pre-configured settings
converter = cattrs.preconf.json.make_converter()


def convert_decimal_to_float(dec: Decimal) -> float:
    """
    Convert Decimal to float.

    Parameters
    ----------
    dec : Decimal
        Decimal object to convert

    Returns
    -------
    float
        Float representation of the decimal
    """
    return float(dec)


def convert_uuid_to_str(uuid_obj: UUID) -> str:
    """
    Convert UUID to string.

    Parameters
    ----------
    uuid_obj : UUID
        UUID object to convert

    Returns
    -------
    str
        String representation of the UUID
    """
    return str(uuid_obj)


# Register custom unstructure hooks for common types
converter.register_unstructure_hook(Decimal, convert_decimal_to_float)
converter.register_unstructure_hook(UUID, convert_uuid_to_str)


def is_json_compatible_type(value: Any) -> TypeGuard[Jsonable]:
    """
    Check if a value is a JSON-compatible type.

    JSON supports: null, bool, int, float, str, list, dict

    Parameters
    ----------
    value : Any
        The value to check

    Returns
    -------
    bool
        True if the value is JSON-compatible
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


def convert_to_json_safe(value: Any) -> Jsonable:
    """
    Convert a value to a JSON-safe representation using cattrs.

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
    >>> convert_to_json_safe("hello")
    'hello'
    >>> convert_to_json_safe(42)
    42
    >>> convert_to_json_safe([1, 2, 3])
    [1, 2, 3]
    >>> convert_to_json_safe({"key": "value"})
    {'key': 'value'}
    >>> from datetime import datetime
    >>> dt = datetime(2025, 8, 10, 12, 30, 45)
    >>> convert_to_json_safe(dt)
    '2025-08-10T12:30:45'
    >>> from decimal import Decimal
    >>> convert_to_json_safe(Decimal("123.45"))
    123.45
    >>> import uuid
    >>> convert_to_json_safe(uuid.UUID('12345678-1234-5678-1234-567812345678'))
    '12345678-1234-5678-1234-567812345678'
    """
    unstructured = converter.unstructure(value)
    if is_json_compatible_type(unstructured):
        return unstructured
    return f"<unsupported_type: {type(value).__name__}>"
