"""Conftest for analyze_table_statistics tool tests."""

import pytest

from cattrs_converter import JsonImmutableConverter


@pytest.fixture(scope="package")
def json_converter() -> JsonImmutableConverter:
    """Create a JsonImmutableConverter instance for tool tests."""
    return JsonImmutableConverter()
