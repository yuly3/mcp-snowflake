"""Success and validation tests for ProfileSemiStructuredColumnsTool."""

import json

import mcp.types as types
import pytest

from cattrs_converter import JsonImmutableConverter
from kernel.table_metadata import DataBase, Schema, TableColumn, TableInfo
from mcp_snowflake.handler.profile_semi_structured_columns.models import (
    SemiStructuredProfileParseResult,
)
from mcp_snowflake.tool.profile_semi_structured_columns import (
    ProfileSemiStructuredColumnsTool,
)

from ...mock_effect_handler import MockProfileSemiStructuredColumns


def test_name_and_definition(json_converter: JsonImmutableConverter) -> None:
    """Tool name and definition should match expected contract."""
    tool = ProfileSemiStructuredColumnsTool(
        json_converter,
        MockProfileSemiStructuredColumns(),
    )
    assert tool.name == "profile_semi_structured_columns"

    definition = tool.definition
    assert definition.name == "profile_semi_structured_columns"
    assert definition.inputSchema is not None
    assert set(definition.inputSchema["required"]) == {"database", "schema", "table"}
    assert "sample_rows" in definition.inputSchema["properties"]
    assert "max_depth" in definition.inputSchema["properties"]
    assert "include_path_stats" in definition.inputSchema["properties"]


@pytest.mark.asyncio
async def test_perform_success(json_converter: JsonImmutableConverter) -> None:
    """Should return summary and JSON body on successful profiling."""
    table_info = TableInfo(
        database=DataBase("db"),
        schema=Schema("public"),
        name="events",
        column_count=1,
        columns=[TableColumn(name="payload", data_type="VARIANT", nullable=True, ordinal_position=1)],
    )
    profile_result = SemiStructuredProfileParseResult(
        total_rows=2000,
        sampled_rows=1000,
        column_profiles={
            "payload": {
                "column_type": "VARIANT",
                "null_count": 100,
                "non_null_count": 900,
                "null_ratio": 0.1,
                "top_level_type_distribution": {
                    "OBJECT": 800,
                    "ARRAY": 100,
                    "STRING": 0,
                    "NUMBER": 0,
                    "BOOLEAN": 0,
                    "NULL": 100,
                },
            }
        },
        path_profiles=[],
        warnings=["Path profiling is limited to max_depth=4"],
    )
    tool = ProfileSemiStructuredColumnsTool(
        json_converter,
        MockProfileSemiStructuredColumns(
            table_info=table_info,
            profile_result=profile_result,
        ),
    )

    result = await tool.perform({
        "database": "db",
        "schema": "public",
        "table": "events",
    })

    assert len(result) == 2
    assert isinstance(result[0], types.TextContent)
    assert "Semi-Structured Profile: db.public.events" in result[0].text
    assert isinstance(result[1], types.TextContent)

    body = json.loads(result[1].text)
    assert body["semi_structured_profile"]["profile_info"]["analyzed_columns"] == 1
    assert "payload" in body["semi_structured_profile"]["column_profiles"]


@pytest.mark.asyncio
async def test_perform_invalid_args(json_converter: JsonImmutableConverter) -> None:
    """Should return a validation error string for invalid arguments."""
    tool = ProfileSemiStructuredColumnsTool(
        json_converter,
        MockProfileSemiStructuredColumns(),
    )
    result = await tool.perform({
        "database": "db",
        "schema": "public",
        "table": "events",
        "sample_rows": 0,
    })

    assert len(result) == 1
    assert isinstance(result[0], types.TextContent)
    assert "Error: Invalid arguments for profile_semi_structured_columns:" in result[0].text
