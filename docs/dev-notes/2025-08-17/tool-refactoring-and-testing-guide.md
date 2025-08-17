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
        except TimeoutError as e:
            text = f"Error: Query timed out: {e}"
        except ProgrammingError as e:
            text = f"Error: SQL syntax error or other programming error: {e}"
        except OperationalError as e:
            text = f"Error: Database operation related error: {e}"
        except DataError as e:
            text = f"Error: Data processing related error: {e}"
        except IntegrityError as e:
            text = f"Error: Referential integrity constraint violation: {e}"
        except NotSupportedError as e:
            text = f"Error: Unsupported database feature used: {e}"
        except ContractViolationError as e:
            text = f"Error: Unexpected error: {e}"
        else:
            text = json.dumps(result, indent=2)
        return [types.TextContent(type="text", text=text)]
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

## Phase 6: Adapter Layer Logging Enhancement (Mandatory)

### 6.1 Logging Enhancement

**Important**: All adapters must have enhanced logging with structured extra fields for debugging and monitoring.

For tools with associated adapters, implement comprehensive logging:

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

### 6.2 Exception Count Summary

The comprehensive error handling should cover:
- **7 Snowflake-specific exceptions**: TimeoutError, ProgrammingError, OperationalError, DataError, IntegrityError, NotSupportedError
- **1 Application exception**: ContractViolationError
- **1 Validation exception**: ValidationError (Pydantic)
- **Total**: 9 exception types for robust error coverage
```

## Phase 7: Handler Test Updates and Validation

## Phase 7: Handler Test Updates and Validation

### 7.1 Handler Test Updates

**Critical**: After refactoring handlers to return structured data, existing handler tests must be updated to use the new mock infrastructure.

**Pattern**: Update handler tests to use new mock classes instead of old MockEffectHandler:

**Before (Old Pattern)**:
```python
@pytest.fixture
def mock_effect_handler() -> MockEffectHandler:
    return MockEffectHandler()

async def test_handle_xyz(mock_effect_handler):
    # Old test pattern
    pass
```

**After (New Pattern)**:
```python
@pytest.fixture
def mock_effect_handler() -> MockXyz:
    return MockXyz()

async def test_handle_xyz_success(mock_effect_handler):
    # Test the structured response directly
    args = XyzArgs(param1="test", param2="value")
    result = await handle_xyz(args, mock_effect_handler)

    assert isinstance(result, XyzJsonResponse)
    assert result["xyz_info"]["param1"] == expected_value
```

**Parameter Alias Issues**: Watch for field alias mismatches in test files:
- Pydantic models may use `Field(alias="...")` for parameter names
- Test arguments must use the **alias** name, not the field name
- Example: If model has `schema_: str = Field(alias="schema")`, use `"schema"` in test arguments

### 7.2 Mock Infrastructure Naming

**Pattern**: Use consistent naming for mock classes:

```python
# Recommended naming convention
MockDescribeTable  # ✅ Consistent with handler name
MockListSchemas    # ✅ Consistent pattern
MockListTables     # ✅ Clear and descriptive
MockExecuteQuery   # ✅ Follows pattern

# Avoid inconsistent naming
MockEffectHandler  # ❌ Too generic
MockXyzHandler     # ❌ Handler suffix not needed
```

### 7.3 Test Execution and Validation

```bash
# Test specific tool
uv run pytest tests/tool/test_{tool_name}.py -v

# Test all tools
uv run pytest tests/tool/ -v

# Run full test suite
uv run pytest . -v
```

### 7.3 Test Execution and Validation

```bash
# Test specific tool (recommended during development)
uv run pytest tests/tool/test_{tool_name}.py -v

# Test specific handler (after handler updates)
uv run pytest tests/handler/test_{tool_name}.py -v

# Test all tools (final validation)
uv run pytest tests/tool/ -v

# Test all handlers (final validation)
uv run pytest tests/handler/ -v

# Run full test suite
uv run pytest . -v
```

### 7.4 Code Quality Checks

```bash
# Run linting
uv run ruff check --fix --unsafe-fixes src/mcp_snowflake/tool/{tool_name}.py
uv run ruff format src/mcp_snowflake/tool/{tool_name}.py

# Check tests
uv run ruff check --fix --unsafe-fixes tests/tool/test_{tool_name}.py
uv run ruff format tests/tool/test_{tool_name}.py

# Check handler tests
uv run ruff check --fix --unsafe-fixes tests/handler/test_{tool_name}.py
uv run ruff format tests/handler/test_{tool_name}.py
```

### 7.5 Expected Test Coverage

After successful refactoring, expect the following test coverage:

**Tool Tests** (per tool):
- **2 Property tests**: name, definition
- **3-5 Success tests**: basic, minimal, complex, edge cases
- **3-4 Error tests**: empty args, invalid args, validation errors
- **7 Exception tests**: Parametrized Snowflake + ContractViolation exceptions
- **Total per tool**: ~15-17 test cases

**Handler Tests** (per handler):
- **1-3 Success tests**: basic functionality with structured response validation
- **2-4 Edge case tests**: empty data, complex data scenarios
- **Total per handler**: ~3-7 test cases

**Overall Numbers** (for 3-tool refactoring):
- Tool tests: ~45-50 test cases
- Handler tests: ~10-20 test cases
- **Total**: ~55-70 comprehensive tests

## Phase 8: Troubleshooting Common Issues

### 8.1 Parameter Alias Problems

**Issue**: Tests fail with parameter validation errors.

**Cause**: Pydantic model field names differ from argument aliases.

**Solution**:
```python
# In Pydantic model
class XyzArgs(BaseModel):
    schema_: str = Field(alias="schema")  # Field name vs alias

# In tests - use ALIAS name
arguments = {
    "schema": "test_schema"  # ✅ Use alias name
    # "schema_": "test_schema"  # ❌ Don't use field name
}
```

### 8.2 Mock Infrastructure Issues

**Issue**: Tests can't find mock classes.

**Cause**: Mock not properly exported from `__init__.py`.

**Solution**:
```python
# In tests/mock_effect_handler/__init__.py
from .xyz import MockXyz

__all__ = [
    "MockDescribeTable",
    "MockXyz",  # ✅ Add new mock to exports
]
```

### 8.3 Handler Test Update Issues

**Issue**: Handler tests still expect MCP types.

**Cause**: Handler tests not updated after handler refactoring.

**Solution**: Update handler tests to expect structured TypedDict responses:
```python
# Before
assert isinstance(result[0], types.TextContent)

# After
assert isinstance(result, XyzJsonResponse)
assert "xyz_info" in result
```

## Phase 9: Documentation

### 9.1 Implementation Notes

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
- Tool Tests: X/X passed (comprehensive error coverage)
- Handler Tests: X/X passed (structured response validation)
- Quality Checks: Linting clean, formatting applied
- Total Test Cases: X implemented with full coverage

## Key Implementation Insights
- Parameter alias mapping challenges resolved
- Mock infrastructure naming standardized
- Logging enhancement implemented as mandatory
- Handler test updates required for new architecture
```

## Checklist Template

For each tool refactoring, use this checklist:

### Handler Layer
- [ ] Handler returns structured data (TypedDict, not MCP types)
- [ ] Error handling fully delegated to tool layer
- [ ] Proper TypedDict response definitions
- [ ] Comprehensive protocol docstrings with all 7 exceptions listed
- [ ] Proper type annotations throughout

### Tool Layer
- [ ] Comprehensive exception handling (8 types: 7 Snowflake + ContractViolationError)
- [ ] ValidationError handling for Pydantic arguments
- [ ] JSON serialization of structured response
- [ ] Proper import organization (grouped by type)
- [ ] Match-case exception handling with specific error messages

### Adapter Layer
- [ ] Mandatory logging enhancement implemented
- [ ] try-catch blocks with logger.exception()
- [ ] Structured extra fields for debugging
- [ ] Exception re-raising to tool layer

### Mock Infrastructure
- [ ] Mock class created with consistent naming (MockXxxTool pattern)
- [ ] Mock added to tests/mock_effect_handler/__init__.py exports
- [ ] Mock supports error injection via should_raise parameter
- [ ] Mock provides sensible default return values
- [ ] Mock follows Protocol interface exactly

### Testing - Tool Layer
- [ ] Property tests (name, definition with schema validation)
- [ ] Success scenarios (basic, minimal, complex data)
- [ ] Error handling tests (empty args, invalid args, validation)
- [ ] Parametrized exception tests (8 exception types)
- [ ] Edge case coverage (tool-specific scenarios)
- [ ] Expected ~15-17 test cases per tool

### Testing - Handler Layer
- [ ] Handler tests updated to expect structured responses
- [ ] Success tests with TypedDict response validation
- [ ] Edge case tests (empty data, complex scenarios)
- [ ] Parameter alias issues resolved (schema_ vs schema)
- [ ] Expected ~3-7 test cases per handler

### Quality Assurance
- [ ] All tool tests passing (target: ~45-50 for 3 tools)
- [ ] All handler tests passing (target: ~10-20 for 3 handlers)
- [ ] Linting passes with no warnings
- [ ] No unused imports or dead code
- [ ] Consistent formatting applied
- [ ] Documentation updated with implementation notes

## Expected Outcomes

After applying this refactoring guide to all tools:

1. **Consistent Architecture**: All tools follow the same error handling and response patterns
2. **Comprehensive Testing**: Each tool has 15-17 test cases, handlers have 3-7 test cases
3. **Better Error Messages**: Users receive clear, specific error messages for 8 exception types
4. **Maintainability**: Standardized mock infrastructure and testing patterns
5. **Type Safety**: Proper type annotations and structured TypedDict responses
6. **Production Readiness**: Mandatory logging with structured extra fields for debugging
7. **Quality Assurance**: ~55-70 comprehensive tests with full error coverage

## Implementation Results (ListSchemas/Tables/Views Example)

**Successfully completed example shows**:
- **47 Tool Tests**: Full coverage of 3 tools with all error scenarios
- **32 Handler Tests**: Updated to new architecture with structured response validation
- **Logging Enhancement**: Implemented across all 3 adapter files
- **Quality Metrics**: All tests passing, clean linting, consistent formatting

## Tools Priority Order

Recommended order for refactoring (based on implementation experience):

### Tier 1: Simple List Tools (Good starting point)
1. **`ListSchemasTool`** ✅ - Simple structure, single parameter
2. **`ListTablesTool`** ✅ - Two parameters, similar pattern
3. **`ListViewsTool`** ✅ - Similar to tables, establishes pattern

*Status: Completed with 47 tool tests + 32 handler tests passing*

### Tier 2: Data Operations (Moderate complexity)
4. **`SampleTableDataTool`** - Data retrieval, response formatting
5. **`ExecuteQueryTool`** - SQL execution, dynamic responses

### Tier 3: Complex Analysis (Advanced features)
6. **`AnalyzeTableStatisticsTool`** - Statistical calculations, complex data processing

**Lessons from Tier 1 Implementation**:
- Parameter alias issues (schema_ vs schema) are common - watch for Field(alias="...")
- Handler test updates are mandatory, not optional
- Mock naming should be consistent (MockListSchemas, not MockEffectHandler)
- Logging enhancement is required for production readiness
- Expected 15-17 tests per tool, 3-7 per handler for comprehensive coverage

This systematic approach ensures consistency across all tools while building complexity gradually. Each tier builds upon lessons learned from the previous tier.
