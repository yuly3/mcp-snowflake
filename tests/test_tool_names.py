"""Test for tool name consistency and uniqueness."""

from unittest.mock import Mock

from mcp_snowflake.settings import ToolsSettings
from mcp_snowflake.tool import (
    AnalyzeTableStatisticsTool,
    DescribeTableTool,
    ExecuteQueryTool,
    ListSchemasTool,
    ListTablesTool,
    ListViewsTool,
    ProfileSemiStructuredColumnsTool,
    SampleTableDataTool,
)


def test_tool_name_unique() -> None:
    """Test that all tool names are unique (no duplicates)."""
    mock_effect_handler = Mock()
    mock_json_converter = Mock()

    analyze_tool = AnalyzeTableStatisticsTool(mock_json_converter, mock_effect_handler)
    execute_tool = ExecuteQueryTool(mock_json_converter, mock_effect_handler)
    sample_tool = SampleTableDataTool(mock_json_converter, mock_effect_handler)
    profile_tool = ProfileSemiStructuredColumnsTool(mock_json_converter, mock_effect_handler)

    describe_tool = DescribeTableTool(mock_effect_handler)
    list_schemas_tool = ListSchemasTool(mock_effect_handler)
    list_tables_tool = ListTablesTool(mock_effect_handler)
    list_views_tool = ListViewsTool(mock_effect_handler)

    tools = [
        analyze_tool,
        describe_tool,
        execute_tool,
        list_schemas_tool,
        list_tables_tool,
        list_views_tool,
        profile_tool,
        sample_tool,
    ]

    tool_names = [tool.name for tool in tools]

    # Check for uniqueness
    assert len(tool_names) == len(set(tool_names)), f"Duplicate tool names found: {tool_names}"


def test_settings_keys_match_tool_names() -> None:
    """Test that ToolsSettings field names match exactly with tool names."""
    mock_effect_handler = Mock()
    mock_json_converter = Mock()

    analyze_tool = AnalyzeTableStatisticsTool(mock_json_converter, mock_effect_handler)
    execute_tool = ExecuteQueryTool(mock_json_converter, mock_effect_handler)
    sample_tool = SampleTableDataTool(mock_json_converter, mock_effect_handler)
    profile_tool = ProfileSemiStructuredColumnsTool(mock_json_converter, mock_effect_handler)

    describe_tool = DescribeTableTool(mock_effect_handler)
    list_schemas_tool = ListSchemasTool(mock_effect_handler)
    list_tables_tool = ListTablesTool(mock_effect_handler)
    list_views_tool = ListViewsTool(mock_effect_handler)

    tools = [
        analyze_tool,
        describe_tool,
        execute_tool,
        list_schemas_tool,
        list_tables_tool,
        list_views_tool,
        profile_tool,
        sample_tool,
    ]

    actual_tool_names = {tool.name for tool in tools}
    settings_field_names = set(ToolsSettings.model_fields.keys())

    assert actual_tool_names == settings_field_names, (
        f"Tool names {actual_tool_names} do not match settings fields {settings_field_names}. "
        + f"Missing in settings: {actual_tool_names - settings_field_names}, "
        + f"Extra in settings: {settings_field_names - actual_tool_names}"
    )
