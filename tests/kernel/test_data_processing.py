"""Tests for data processing module."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from cattrs_converter import JsonImmutableConverter
from kernel import DataProcessingResult, RowProcessingResult


@pytest.fixture(scope="module")
def converter() -> JsonImmutableConverter:
    return JsonImmutableConverter()


class TestProcessRowData:
    """Test process_row_data function."""

    def test_process_row_data_success(self, converter: JsonImmutableConverter) -> None:
        """Test processing a single row with serializable data."""
        raw_row = {"id": 1, "name": "Alice", "score": 95.5}

        result = RowProcessingResult.from_raw_row(converter, raw_row)

        assert result.processed_row == {"id": 1, "name": "Alice", "score": 95.5}
        assert result.warnings == []

    def test_process_row_data_with_unsupported_type(
        self,
        converter: JsonImmutableConverter,
    ) -> None:
        """Test processing a single row with unsupported data type."""
        from threading import Lock

        raw_row = {"id": 1, "name": "Alice", "lock": Lock()}

        result = RowProcessingResult.from_raw_row(converter, raw_row)

        assert result.processed_row["id"] == 1
        assert result.processed_row["name"] == "Alice"
        assert result.processed_row["lock"] == "<unsupported_type: lock>"
        assert len(result.warnings) == 1
        assert "Column 'lock' contains unsupported data type" in result.warnings

    def test_process_row_data_empty(self, converter: JsonImmutableConverter) -> None:
        """Test processing an empty row."""
        raw_row: dict[str, object] = {}

        result = RowProcessingResult.from_raw_row(converter, raw_row)

        assert result.processed_row == {}
        assert result.warnings == []


class TestProcessMultipleRowsData:
    """Test process_multiple_rows_data function."""

    def test_process_multiple_rows_data_success(
        self,
        converter: JsonImmutableConverter,
    ) -> None:
        """Test processing multiple rows with serializable data."""
        raw_rows = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.0},
        ]

        result = DataProcessingResult.from_raw_rows(converter, raw_rows)

        assert len(result.processed_rows) == 2
        assert result.processed_rows[0] == {"id": 1, "name": "Alice", "score": 95.5}
        assert result.processed_rows[1] == {"id": 2, "name": "Bob", "score": 87.0}
        assert result.warnings == []

    def test_process_multiple_rows_data_empty(
        self,
        converter: JsonImmutableConverter,
    ) -> None:
        """Test processing empty data."""
        result = DataProcessingResult.from_raw_rows(converter, [])

        assert result.processed_rows == []
        assert result.warnings == []

    def test_process_multiple_rows_data_with_warnings(
        self,
        converter: JsonImmutableConverter,
    ) -> None:
        """Test processing data with unsupported types."""
        raw_rows = [
            {"id": 1, "name": "Alice", "complex_num": 1 + 2j},
            {"id": 2, "name": "Bob", "complex_num": 3 + 4j},
        ]

        result = DataProcessingResult.from_raw_rows(converter, raw_rows)

        assert len(result.processed_rows) == 2
        assert result.processed_rows[0]["id"] == 1
        assert result.processed_rows[0]["name"] == "Alice"
        assert result.processed_rows[0]["complex_num"] == "<unsupported_type: complex>"
        assert result.processed_rows[1]["id"] == 2
        assert result.processed_rows[1]["name"] == "Bob"
        assert result.processed_rows[1]["complex_num"] == "<unsupported_type: complex>"

        # Warning should be deduplicated
        assert len(result.warnings) == 1
        assert "Column 'complex_num' contains unsupported data type" in result.warnings


class TestDataProcessingProperties:
    """Property-based tests for data processing functions."""

    @given(
        st.dictionaries(
            st.text(),
            st.one_of(
                st.integers(),
                st.floats(allow_nan=False, allow_infinity=False),
                st.text(),
                st.booleans(),
                st.none(),
            ),
        ),
    )
    def test_process_row_data_with_json_compatible_types(
        self,
        converter: JsonImmutableConverter,
        raw_row: dict[str, object],
    ) -> None:
        """Property test: process_row_data should handle any JSON-compatible data without errors."""

        result = RowProcessingResult.from_raw_row(converter, raw_row)

        # All keys should be preserved
        assert set(result.processed_row.keys()) == set(raw_row.keys())

        # For JSON-compatible types, no warnings should be generated
        if raw_row:  # Only check if row is not empty
            # All values should be processed successfully for JSON-compatible types
            for key, value in raw_row.items():
                processed_value = result.processed_row[key]
                if isinstance(value, int | float | str | bool | type(None)):
                    assert processed_value == value

    @given(
        st.lists(
            st.dictionaries(
                st.text(),
                st.one_of(
                    st.integers(),
                    st.floats(allow_nan=False, allow_infinity=False),
                    st.text(),
                    st.booleans(),
                    st.none(),
                ),
            ),
            max_size=10,
        ),
    )
    def test_process_multiple_rows_data_properties(
        self,
        converter: JsonImmutableConverter,
        raw_rows: list[dict[str, object]],
    ) -> None:
        """Property test: process_multiple_rows_data should handle any list of JSON-compatible data."""

        result = DataProcessingResult.from_raw_rows(converter, raw_rows)

        # Row count should be preserved
        assert len(result.processed_rows) == len(raw_rows)

        # If input is empty, output should be empty
        if not raw_rows:
            assert result.processed_rows == []
            assert result.warnings == []

    @given(st.dictionaries(st.text(), st.just(object())))
    def test_process_row_data_with_unsupported_types(
        self,
        converter: JsonImmutableConverter,
        raw_row: dict[str, object],
    ) -> None:
        """Property test: process_row_data should handle unsupported types gracefully."""

        result = RowProcessingResult.from_raw_row(converter, raw_row)

        # All keys should be preserved
        assert set(result.processed_row.keys()) == set(raw_row.keys())

        # For unsupported types, should generate warnings
        if raw_row:  # Only check if row is not empty
            assert len(result.warnings) > 0

            # All unsupported values should be converted to type strings
            for key in raw_row:
                processed_value = result.processed_row[key]
                assert isinstance(processed_value, str)
                assert processed_value.startswith("<unsupported_type:")
                assert processed_value.endswith(">")

    @given(
        st.lists(
            st.dictionaries(st.text(min_size=1), st.just(1 + 2j), min_size=1),
            min_size=1,
            max_size=5,
        ),
    )
    def test_warning_deduplication(
        self,
        converter: JsonImmutableConverter,
        raw_rows: list[dict[str, complex]],
    ) -> None:
        """Property test: warnings should be deduplicated across multiple rows."""

        result = DataProcessingResult.from_raw_rows(converter, raw_rows)

        # Should have warnings (complex numbers are unsupported)
        assert len(result.warnings) > 0

        # Warnings should be unique (deduplicated)
        assert len(result.warnings) == len(set(result.warnings))

        # Each warning should mention the unsupported type
        for warning in result.warnings:
            assert "unsupported data type" in warning
