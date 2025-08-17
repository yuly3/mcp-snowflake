# Tool Refactoring and Testing Implementation Guide

## Overview

This document provides a systematic approach for refactoring existing tools and implementing comprehensive tests based on the improvements made to `DescribeTableTool`. The refactoring focuses on improving error handling, standardizing architecture, and ensuring comprehensive test coverage.

## Target Tools

The following tools should be refactored using this guide:

- `ExecuteQueryTool` (src/mcp_snowflake/tool/execute_query.py)
- `ListSchemasTool` (src/mcp_snowflake/tool/list_schemas.py)
- `ListTablesTool` (src/mcp_snowflake/tool/list_tables.py)
- `ListViewsTool` (src/mcp_snowflake/tool/list_views.py)
- `SampleTableDataTool` (src/mcp_snowflake/tool/sample_table_data.py)
- `AnalyzeTableStatisticsTool` (src/mcp_snowflake/tool/analyze_table_statistics.py)

## Phase 1: Architecture Analysis

### 1.1 Current State Assessment

For each tool, assess the current implementation:

```bash
# Check current implementation
grep -r "class.*Tool" src/mcp_snowflake/tool/
grep -r "async def perform" src/mcp_snowflake/tool/
grep -r "except" src/mcp_snowflake/tool/
```

### 1.2 Handler Layer Analysis

Check if the corresponding handler follows the improved pattern:

- Returns structured data (not MCP types)
- Has proper error handling delegation
- Uses proper type annotations
- Has comprehensive docstrings

### 1.3 Mock Infrastructure Check

Verify if mock effect handlers exist in `tests/mock_effect_handler/`:

```bash
ls tests/mock_effect_handler/
```

## Phase 2: Handler Layer Refactoring

### 2.1 Handler Return Type Standardization

**Pattern**: Convert handlers to return structured data instead of MCP types.

**Before (Anti-pattern)**:
```python
async def handle_xyz(
    args: XyzArgs,
    effect_handler: EffectXyz,
) -> list[types.TextContent]:
    try:
        data = await effect_handler.xyz(args.param1, args.param2)
    except Exception as e:
        logger.exception("Error in xyz")
        return [types.TextContent(type="text", text=f"Error: {e}")]

    return [types.TextContent(type="text", text=json.dumps(data, indent=2))]
```

**After (Recommended)**:
```python
async def handle_xyz(
    args: XyzArgs,
    effect_handler: EffectXyz,
) -> XyzJsonResponse:
    """Handle xyz tool call.

    Parameters
    ----------
    args : XyzArgs
        The arguments for the xyz operation.
    effect_handler : EffectXyz
        The effect handler for the xyz operation.

    Returns
    -------
    XyzJsonResponse
        The structured response containing the xyz information.

    Raises
    ------
    TimeoutError
        If query execution times out
    ProgrammingError
        SQL syntax errors or other programming errors
    OperationalError
        Database operation related errors
    DataError
        Data processing related errors
    IntegrityError
        Referential integrity constraint violations
    NotSupportedError
        When an unsupported database feature is used
    """
    data = await effect_handler.xyz(args.param1, args.param2)

    return XyzJsonResponse(
        xyz_info={
            "param1": data.param1,
            "param2": data.param2,
            # ... other fields
        }
    )
```

### 2.2 Protocol Documentation Enhancement

**Pattern**: Add comprehensive docstrings to effect handler protocols.

```python
class EffectXyz(Protocol):
    def xyz(
        self,
        param1: Type1,
        param2: Type2,
    ) -> Awaitable[XyzResult]:
        """Execute xyz operation.

        Parameters
        ----------
        param1 : Type1
            Description of param1.
        param2 : Type2
            Description of param2.

        Returns
        -------
        Awaitable[XyzResult]
            The result of the xyz operation.

        Raises
        ------
        TimeoutError
            If query execution times out
        ProgrammingError
            SQL syntax errors or other programming errors
        OperationalError
            Database operation related errors
        DataError
            Data processing related errors
        IntegrityError
            Referential integrity constraint violations
        NotSupportedError
            When an unsupported database feature is used
        """
        ...
```

### 2.3 TypedDict Response Definitions

**Pattern**: Define structured response types using TypedDict.

```python
from typing import TypedDict

class XyzInfoDict(TypedDict):
    """TypedDict for xyz information in JSON response."""

    param1: Type1
    param2: Type2
    # ... other fields

class XyzJsonResponse(TypedDict):
    """TypedDict for the complete xyz JSON response structure."""

    xyz_info: XyzInfoDict
```

## Phase 3: Tool Layer Enhancement

### 3.1 Comprehensive Error Handling

**Pattern**: Implement structured exception handling in the tool layer.

```python
import json
from collections.abc import Mapping, Sequence
from typing import Any

import mcp.types as types
from pydantic import ValidationError
from snowflake.connector import (
    DataError,
    IntegrityError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)

from expression.contract import ContractViolationError

class XyzTool(Tool):
    async def perform(
        self,
        arguments: Mapping[str, Any] | None,
    ) -> Sequence[types.Content]:
        try:
            args = XyzArgs.model_validate(arguments or {})
        except ValidationError as e:
            return [
                types.TextContent(
                    type="text",
                    text=f"Error: Invalid arguments for xyz: {e}",
                )
            ]

        try:
            result = await handle_xyz(args, self.effect_handler)
        except (
            TimeoutError,
            ProgrammingError,
            OperationalError,
            DataError,
            IntegrityError,
            NotSupportedError,
            ContractViolationError,
        ) as e:
            match e:
                case TimeoutError():
                    text = f"Error: Query timed out: {e}"
                case ProgrammingError():
                    text = f"Error: SQL syntax error or other programming error: {e}"
                case OperationalError():
                    text = f"Error: Database operation related error: {e}"
                case DataError():
                    text = f"Error: Data processing related error: {e}"
                case IntegrityError():
                    text = f"Error: Referential integrity constraint violation: {e}"
                case NotSupportedError():
                    text = f"Error: Unsupported database feature used: {e}"
                case ContractViolationError():
                    text = f"Error: Unexpected error: {e}"
            return [types.TextContent(type="text", text=text)]

        return [types.TextContent(type="text", text=json.dumps(result, indent=2))]
```

## Phase 4: Mock Infrastructure Creation

### 4.1 Mock Effect Handler Creation

**Pattern**: Create standardized mock effect handlers.

Create `tests/mock_effect_handler/{tool_name}.py`:

```python
from typing_extensions import Any
from kernel.some_metadata import SomeResult

class MockXyz:
    """Mock implementation of EffectXyz protocol."""

    def __init__(
        self,
        result_data: SomeResult | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.result_data = result_data
        self.should_raise = should_raise

    async def xyz(
        self,
        param1: Any,  # noqa: ARG002
        param2: Any,  # noqa: ARG002
    ) -> SomeResult:
        if self.should_raise:
            raise self.should_raise
        if self.result_data is None:
            # Return minimal default
            return SomeResult(
                field1="default_value1",
                field2="default_value2",
                # ... other default fields
            )
        return self.result_data
```

### 4.2 Mock Registry Update

Update `tests/mock_effect_handler/__init__.py`:

```python
from .describe_table import MockDescribeTable
from .xyz import MockXyz  # Add new mock

__all__ = [
    "MockDescribeTable",
    "MockXyz",  # Add to exports
]
```

## Phase 5: Comprehensive Testing

### 5.1 Test File Structure

Create `tests/tool/test_{tool_name}.py` with the following structure:

```python
"""Test for XyzTool."""

import json

import mcp.types as types
import pytest
from snowflake.connector import (
    DataError,
    IntegrityError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)

from expression.contract import ContractViolationError
from kernel.some_metadata import SomeData, SomeResult
from mcp_snowflake.tool.xyz import XyzTool

from ..mock_effect_handler import MockXyz


class TestXyzTool:
    """Test XyzTool."""

    def test_name_property(self) -> None:
        """Test name property."""
        mock_effect = MockXyz()
        tool = XyzTool(mock_effect)
        assert tool.name == "expected_tool_name"

    def test_definition_property(self) -> None:
        """Test definition property."""
        mock_effect = MockXyz()
        tool = XyzTool(mock_effect)
        definition = tool.definition

        assert definition.name == "expected_tool_name"
        assert definition.description is not None
        assert "key_word_in_description" in definition.description
        assert definition.inputSchema is not None

        # Check required fields
        input_schema = definition.inputSchema
        assert input_schema["type"] == "object"
        assert set(input_schema["required"]) == {"param1", "param2"}  # Expected required fields

        # Check properties
        properties = input_schema["properties"]
        assert "param1" in properties
        assert "param2" in properties

    # ... continue with other test methods
```

### 5.2 Essential Test Cases

Each tool test should include:

1. **Property Tests**:
   - `test_name_property()`: Verify tool name
   - `test_definition_property()`: Verify tool definition structure

2. **Success Cases**:
   - `test_perform_success()`: Basic successful operation
   - `test_perform_minimal_data()`: Minimal valid data scenario
   - `test_perform_complex_data()`: Complex data scenario

3. **Error Handling**:
   - `test_perform_with_empty_arguments()`: Empty arguments
   - `test_perform_with_invalid_arguments()`: Invalid arguments
   - `test_perform_with_empty_dict_arguments()`: Empty dict
   - `test_perform_with_exceptions()`: All 7 exception types (parametrized test)

4. **Edge Cases**:
   - Tool-specific edge cases based on functionality

### 5.3 Parametrized Exception Testing

**Pattern**: Use parametrized tests for comprehensive exception coverage.

```python
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("exception", "expected_message_prefix"),
    [
        (TimeoutError("Connection timeout"), "Error: Query timed out:"),
        (
            ProgrammingError("SQL syntax error"),
            "Error: SQL syntax error or other programming error:",
        ),
        (
            OperationalError("Database connection error"),
            "Error: Database operation related error:",
        ),
        (DataError("Invalid data"), "Error: Data processing related error:"),
        (
            IntegrityError("Constraint violation"),
            "Error: Referential integrity constraint violation:",
        ),
        (
            NotSupportedError("Feature not supported"),
            "Error: Unsupported database feature used:",
        ),
        (
            ContractViolationError("Contract violation"),
            "Error: Unexpected error:",
        ),
    ],
)
async def test_perform_with_exceptions(
    self,
    exception: Exception,
    expected_message_prefix: str,
) -> None:
    """Test exception handling in perform method."""
    mock_effect = MockXyz(should_raise=exception)
    tool = XyzTool(mock_effect)

    arguments = {
        "param1": "test_value1",
        "param2": "test_value2",
    }
    result = await tool.perform(arguments)

    assert len(result) == 1
    assert isinstance(result[0], types.TextContent)
    assert result[0].type == "text"
    assert result[0].text.startswith(expected_message_prefix)
    assert str(exception) in result[0].text
```

## Phase 6: Adapter Layer Enhancement (Optional)

### 6.1 Logging Enhancement

If the tool has an associated adapter, enhance logging:

```python
import logging

logger = logging.getLogger(__name__)

class XyzEffectHandler:
    async def xyz(self, param1: Type1, param2: Type2) -> Result:
        query = f"SELECT * FROM {param1} WHERE condition = {param2}"

        try:
            results = await self.client.execute_query(query, timeout)
        except Exception:
            logger.exception(
                "failed to execute xyz operation",
                extra={
                    "param1": param1,
                    "param2": param2,
                    "query": query,
                },
            )
            raise

        return self._process_results(results)
```

## Phase 7: Validation and Testing

### 7.1 Run Tests

```bash
# Test specific tool
uv run pytest tests/tool/test_{tool_name}.py -v

# Test all tools
uv run pytest tests/tool/ -v

# Run full test suite
uv run pytest . -v
```

### 7.2 Code Quality Checks

```bash
# Run linting
uv run ruff check --fix --unsafe-fixes src/mcp_snowflake/tool/{tool_name}.py
uv run ruff format src/mcp_snowflake/tool/{tool_name}.py

# Check tests
uv run ruff check --fix --unsafe-fixes tests/tool/test_{tool_name}.py
uv run ruff format tests/tool/test_{tool_name}.py
```

## Phase 8: Documentation

### 8.1 Implementation Notes

Create implementation notes in `docs/dev-notes/2025-08-17/implement-{tool-name}-refactoring.md`:

```markdown
# {ToolName} Refactoring Implementation

## User Request
Refactor {ToolName} following the DescribeTableTool pattern with comprehensive error handling and testing.

## Implementation Plan
1. Refactor handler to return structured data
2. Enhance tool layer error handling
3. Create comprehensive test suite
4. Update mock infrastructure

## Changes Made
- [List specific changes]

## Test Results
- X test cases implemented
- Full coverage of error scenarios
- All tests passing
```

## Checklist Template

For each tool refactoring, use this checklist:

### Handler Layer
- [ ] Handler returns structured data (not MCP types)
- [ ] Error handling delegated to tool layer
- [ ] Proper TypedDict response definitions
- [ ] Comprehensive protocol docstrings
- [ ] Proper type annotations

### Tool Layer
- [ ] Comprehensive exception handling (7 types + ValidationError)
- [ ] JSON serialization of structured response
- [ ] Proper import organization
- [ ] Match-case exception handling

### Mock Infrastructure
- [ ] Mock effect handler created
- [ ] Mock added to __init__.py exports
- [ ] Mock supports error injection
- [ ] Mock provides sensible defaults

### Testing
- [ ] Property tests (name, definition)
- [ ] Success scenarios (basic, minimal, complex)
- [ ] Error handling tests (arguments, exceptions)
- [ ] Parametrized exception tests
- [ ] Edge case coverage
- [ ] All tests passing

### Quality
- [ ] Linting passes
- [ ] No unused imports
- [ ] Consistent formatting
- [ ] Documentation updated

## Expected Outcomes

After applying this refactoring guide to all tools:

1. **Consistent Architecture**: All tools follow the same error handling and response patterns
2. **Comprehensive Testing**: Each tool has 10-15 test cases covering all scenarios
3. **Better Error Messages**: Users receive clear, specific error messages
4. **Maintainability**: Standardized mock infrastructure and testing patterns
5. **Type Safety**: Proper type annotations and structured responses

## Tools Priority Order

Recommended order for refactoring (based on complexity and usage):

1. `ListSchemasTool` - Simple, good starting point
2. `ListTablesTool` - Similar to schemas
3. `ListViewsTool` - Similar pattern to tables
4. `ExecuteQueryTool` - More complex, important functionality
5. `SampleTableDataTool` - Moderate complexity
6. `AnalyzeTableStatisticsTool` - Most complex, do last

This systematic approach ensures consistency across all tools while maintaining code quality and comprehensive test coverage.
