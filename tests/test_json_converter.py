"""
Tests for JSON converter functionality.
"""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from threading import Lock
from uuid import UUID

from mcp_snowflake.json_converter import (
    convert_to_json_safe,
    is_json_serializable,
)


class TestIsJsonSerializable:
    """Test is_json_serializable function."""

    def test_serializable_basic_types(self) -> None:
        """Test JSON serializable basic types."""
        assert is_json_serializable("string") == (True, None)
        assert is_json_serializable(123) == (True, None)
        assert is_json_serializable(123.45) == (True, None)
        assert is_json_serializable(True) == (True, None)
        assert is_json_serializable(None) == (True, None)
        assert is_json_serializable([1, 2, 3]) == (True, None)
        assert is_json_serializable({"key": "value"}) == (True, None)

    def test_serializable_with_cattrs_conversion(self) -> None:
        """Test types that cattrs converts to JSON-compatible types."""
        # datetime/date → ISO strings
        dt = datetime(2025, 7, 27, 10, 30, 45, tzinfo=UTC)
        assert is_json_serializable(dt) == (True, None)

        d = date(2025, 7, 27)
        assert is_json_serializable(d) == (True, None)

        # Decimal → float
        dec = Decimal("123.45")
        assert is_json_serializable(dec) == (True, None)

        # UUID → string
        uuid_obj = UUID("12345678-1234-5678-1234-567812345678")
        assert is_json_serializable(uuid_obj) == (True, None)

        # set → list (automatic cattrs conversion)
        assert is_json_serializable({1, 2, 3}) == (True, None)

    def test_non_serializable_types(self) -> None:
        """Test non-JSON serializable types."""
        # Complex number (cattrs doesn't convert to JSON-compatible type)
        is_serializable, type_name = is_json_serializable(1 + 2j)
        assert is_serializable is False
        assert type_name == "complex"

        # Lock object (cattrs doesn't convert to JSON-compatible type)
        is_serializable, type_name = is_json_serializable(Lock())
        assert is_serializable is False
        assert type_name == "lock"

        # Function (cattrs doesn't convert to JSON-compatible type)
        def test_func() -> None:
            pass

        is_serializable, type_name = is_json_serializable(test_func)
        assert is_serializable is False
        assert type_name == "function"


class TestConvertToJsonSafe:
    """Test convert_to_json_safe function."""

    def test_basic_types(self) -> None:
        """Test conversion of basic JSON types."""
        assert convert_to_json_safe("string") == "string"
        assert convert_to_json_safe(123) == 123
        assert convert_to_json_safe(123.45) == 123.45
        assert convert_to_json_safe(True) is True
        assert convert_to_json_safe(None) is None
        assert convert_to_json_safe([1, 2, 3]) == [1, 2, 3]
        assert convert_to_json_safe({"key": "value"}) == {"key": "value"}

    def test_cattrs_conversions(self) -> None:
        """Test types that cattrs automatically converts."""
        # datetime → ISO string
        dt = datetime(2025, 7, 27, 10, 30, 45, tzinfo=UTC)
        result = convert_to_json_safe(dt)
        assert result == "2025-07-27T10:30:45+00:00"

        # date → ISO string
        d = date(2025, 7, 27)
        result = convert_to_json_safe(d)
        assert result == "2025-07-27"

        # Decimal → float
        dec = Decimal("123.45")
        result = convert_to_json_safe(dec)
        assert result == 123.45

        # UUID → string
        uuid_obj = UUID("12345678-1234-5678-1234-567812345678")
        result = convert_to_json_safe(uuid_obj)
        assert result == "12345678-1234-5678-1234-567812345678"

        # set → list
        result = convert_to_json_safe({3, 1, 2})
        assert isinstance(result, list)
        assert sorted(result) == [1, 2, 3]

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

    def test_json_dumps_compatibility(self) -> None:
        """Test that converted values can be serialized with json.dumps()."""
        test_values = [
            "string",
            123,
            123.45,
            True,
            None,
            [1, 2, 3],
            {"key": "value"},
            datetime(2025, 7, 27, 10, 30, 45, tzinfo=UTC),
            date(2025, 7, 27),
            Decimal("123.45"),
            UUID("12345678-1234-5678-1234-567812345678"),
            {1, 2, 3},  # set
        ]

        for value in test_values:
            converted = convert_to_json_safe(value)
            # Should not raise an exception
            json_str = json.dumps(converted)
            assert isinstance(json_str, str)
            # Should be able to parse back
            parsed = json.loads(json_str)
            assert parsed == converted
