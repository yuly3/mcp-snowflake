"""Tests for profile_semi_structured_columns main handler."""

import pytest
from pydantic import ValidationError

from kernel.table_metadata import DataBase, Schema, TableColumn, TableInfo
from mcp_snowflake.handler.profile_semi_structured_columns import (
    ProfileSemiStructuredColumnsArgs,
    ProfileSemiStructuredColumnsResult,
    handle_profile_semi_structured_columns,
)
from mcp_snowflake.handler.profile_semi_structured_columns.models import (
    SemiStructuredProfileParseResult,
)

from ...mock_effect_handler import MockProfileSemiStructuredColumns


@pytest.mark.asyncio
async def test_handle_profile_semi_structured_columns_success() -> None:
    """Should return structured profile result on success."""
    table_info = TableInfo(
        database=DataBase("test_db"),
        schema=Schema("test_schema"),
        name="events",
        column_count=2,
        columns=[
            TableColumn(name="payload", data_type="VARIANT", nullable=True, ordinal_position=1),
            TableColumn(name="id", data_type="NUMBER", nullable=False, ordinal_position=2),
        ],
    )

    profile_result = SemiStructuredProfileParseResult(
        total_rows=1000,
        sampled_rows=500,
        column_profiles={
            "payload": {
                "column_type": "VARIANT",
                "null_count": 10,
                "non_null_count": 490,
                "null_ratio": 0.02,
                "top_level_type_distribution": {
                    "OBJECT": 400,
                    "ARRAY": 80,
                    "STRING": 10,
                    "NUMBER": 0,
                    "BOOLEAN": 0,
                    "NULL": 10,
                },
            }
        },
        path_profiles=[],
        warnings=["Approximate profile based on sampling"],
    )
    effect = MockProfileSemiStructuredColumns(
        table_info=table_info,
        profile_result=profile_result,
    )

    args = ProfileSemiStructuredColumnsArgs.model_validate({
        "database": "test_db",
        "schema": "test_schema",
        "table": "events",
    })
    result = await handle_profile_semi_structured_columns(args, effect)

    assert isinstance(result, ProfileSemiStructuredColumnsResult)
    assert result.database == "test_db"
    assert result.sampled_rows == 500
    assert result.analyzed_columns == 1
    assert result.analyzed_column_names == ["payload"]
    assert len(result.unsupported_columns) == 1  # "id" is NUMBER, unsupported for this tool
    assert result.column_profiles["payload"]["column_type"] == "VARIANT"
    assert result.warnings == ["Approximate profile based on sampling"]


def test_profile_args_validation_bounds() -> None:
    """Should validate numeric bounds on tool arguments."""
    with pytest.raises(ValidationError):
        _ = ProfileSemiStructuredColumnsArgs.model_validate({
            "database": "db",
            "schema": "sch",
            "table": "tbl",
            "sample_rows": 0,
        })
