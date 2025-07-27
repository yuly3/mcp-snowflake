"""
JSON conversion utilities using cattrs for flexible type handling.
"""

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import cattrs.preconf.json

logger = logging.getLogger(__name__)

# Create a JSON converter with pre-configured settings
converter = cattrs.preconf.json.make_converter()


def convert_datetime_to_iso(dt: datetime) -> str:
    """
    Convert datetime to ISO format string.

    Parameters
    ----------
    dt : datetime
        Datetime object to convert

    Returns
    -------
    str
        ISO formatted datetime string
    """
    return dt.isoformat()


def convert_date_to_iso(d: date) -> str:
    """
    Convert date to ISO format string.

    Parameters
    ----------
    d : date
        Date object to convert

    Returns
    -------
    str
        ISO formatted date string
    """
    return d.isoformat()


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
converter.register_unstructure_hook(datetime, convert_datetime_to_iso)
converter.register_unstructure_hook(date, convert_date_to_iso)
converter.register_unstructure_hook(Decimal, convert_decimal_to_float)
converter.register_unstructure_hook(UUID, convert_uuid_to_str)


def is_json_serializable(value: Any) -> tuple[bool, str | None]:
    """
    Check if a value can be converted to JSON using cattrs.

    Parameters
    ----------
    value : Any
        The value to check for JSON serializability

    Returns
    -------
    tuple[bool, str | None]
        Tuple containing (is_serializable, type_name_if_not_serializable)
    """
    unstructured = converter.unstructure(value)
    if _is_json_compatible_type(unstructured):
        return True, None
    return False, str(type(value).__name__)


def _is_json_compatible_type(value: Any) -> bool:
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
        return all(_is_json_compatible_type(item) for item in value)
    if isinstance(value, dict):
        return all(isinstance(key, str) for key in value) and all(
            _is_json_compatible_type(val) for val in value.values()
        )
    return False


def convert_to_json_safe(value: Any) -> Any:
    """
    Convert a value to a JSON-safe representation using cattrs.

    Parameters
    ----------
    value : Any
        The value to convert

    Returns
    -------
    Any
        JSON-safe representation of the value
    """
    unstructured = converter.unstructure(value)
    if _is_json_compatible_type(unstructured):
        return unstructured
    return f"<unsupported_type: {type(value).__name__}>"
