import pytest

from cattrs_converter import JsonImmutableConverter


@pytest.fixture(scope="package")
def json_converter() -> JsonImmutableConverter:
    return JsonImmutableConverter()
