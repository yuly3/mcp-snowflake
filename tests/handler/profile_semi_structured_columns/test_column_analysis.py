"""Tests for semi-structured column selection and classification."""

from kernel.table_metadata import TableColumn
from mcp_snowflake.handler.profile_semi_structured_columns import (
    NoSemiStructuredColumns,
    SemiStructuredColumnDoesNotExist,
)
from mcp_snowflake.handler.profile_semi_structured_columns._column_analysis import (
    select_and_classify_columns,
)
from mcp_snowflake.handler.profile_semi_structured_columns._types import (
    ClassifiedSemiStructuredColumns,
)


def test_select_and_classify_columns_all() -> None:
    """Should select only VARIANT/ARRAY/OBJECT columns when no explicit request."""
    columns = [
        TableColumn(name="id", data_type="NUMBER", nullable=False, ordinal_position=1),
        TableColumn(name="payload", data_type="VARIANT", nullable=True, ordinal_position=2),
        TableColumn(name="tags", data_type="ARRAY", nullable=True, ordinal_position=3),
        TableColumn(name="metadata", data_type="OBJECT", nullable=True, ordinal_position=4),
    ]

    result = select_and_classify_columns(columns, [])
    assert isinstance(result, ClassifiedSemiStructuredColumns)
    assert [col.name for col in result.supported_columns] == ["payload", "tags", "metadata"]
    assert [col.name for col in result.unsupported_columns] == ["id"]


def test_select_and_classify_columns_not_found() -> None:
    """Should return not-found error when requested columns are missing."""
    columns = [
        TableColumn(name="payload", data_type="VARIANT", nullable=True, ordinal_position=1),
    ]

    result = select_and_classify_columns(columns, ["payload", "missing_col"])
    assert isinstance(result, SemiStructuredColumnDoesNotExist)
    assert result.not_existed_columns == ["missing_col"]


def test_select_and_classify_columns_no_supported() -> None:
    """Should return no-supported error when request has only scalar columns."""
    columns = [
        TableColumn(name="id", data_type="NUMBER", nullable=False, ordinal_position=1),
        TableColumn(name="name", data_type="VARCHAR", nullable=True, ordinal_position=2),
    ]

    result = select_and_classify_columns(columns, [])
    assert isinstance(result, NoSemiStructuredColumns)
    assert [col.name for col in result.unsupported_columns] == ["id", "name"]
