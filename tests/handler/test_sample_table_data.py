import json
from typing import Any

import mcp.types as types
import pytest
from pydantic import ValidationError

from mcp_snowflake.handler.sample_table_data import (
    SampleTableDataArgs,
    _format_response,
    handle_sample_table_data,
    process_sample_data,
)


class MockEffectSampleTableData:
    """Mock implementation of EffectSampleTableData protocol."""

    def __init__(
        self,
        sample_data: list[dict[str, Any]] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.sample_data = sample_data or []
        self.should_raise = should_raise

    async def sample_table_data(
        self,
        database: str,  # noqa: ARG002
        schema: str,  # noqa: ARG002
        table_name: str,  # noqa: ARG002
        sample_size: int,  # noqa: ARG002
        columns: list[str] | None,  # noqa: ARG002
    ) -> list[dict[str, Any]]:
        if self.should_raise:
            raise self.should_raise
        return self.sample_data


class TestSampleTableDataArgs:
    """Test SampleTableDataArgs validation."""

    def test_valid_args(self) -> None:
        """Test valid arguments."""
        args = SampleTableDataArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )
        assert args.database == "test_db"
        assert args.schema_name == "test_schema"
        assert args.table_name == "test_table"
        assert args.sample_size == 10  # default
        assert args.columns is None  # default

    def test_valid_args_with_optional_params(self) -> None:
        """Test valid arguments with optional parameters."""
        args = SampleTableDataArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
            sample_size=50,
            columns=["col1", "col2"],
        )
        assert args.database == "test_db"
        assert args.schema_name == "test_schema"
        assert args.table_name == "test_table"
        assert args.sample_size == 50
        assert args.columns == ["col1", "col2"]

    def test_missing_database(self) -> None:
        """Test missing database argument."""
        with pytest.raises(ValidationError):
            _ = SampleTableDataArgs.model_validate(
                {"schema_name": "test_schema", "table_name": "test_table"}
            )

    def test_missing_schema_name(self) -> None:
        """Test missing schema_name argument."""
        with pytest.raises(ValidationError):
            _ = SampleTableDataArgs.model_validate(
                {"database": "test_db", "table_name": "test_table"}
            )

    def test_missing_table_name(self) -> None:
        """Test missing table_name argument."""
        with pytest.raises(ValidationError):
            _ = SampleTableDataArgs.model_validate(
                {"database": "test_db", "schema_name": "test_schema"}
            )

    def test_invalid_sample_size(self) -> None:
        """Test invalid sample_size."""
        with pytest.raises(ValidationError):
            _ = SampleTableDataArgs.model_validate(
                {
                    "database": "test_db",
                    "schema_name": "test_schema",
                    "table_name": "test_table",
                    "sample_size": "invalid",
                }
            )


class TestProcessSampleData:
    """Test process_sample_data function."""

    def test_empty_data(self) -> None:
        """Test processing empty data."""
        processed_rows, warnings = process_sample_data([])
        assert processed_rows == []
        assert warnings == []

    def test_serializable_data(self) -> None:
        """Test processing JSON serializable data."""
        raw_data = [
            {"col1": "value1", "col2": 123},
            {"col1": "value2", "col2": 456},
        ]
        processed_rows, warnings = process_sample_data(raw_data)

        assert len(processed_rows) == 2
        assert processed_rows[0] == {"col1": "value1", "col2": 123}
        assert processed_rows[1] == {"col1": "value2", "col2": 456}
        assert warnings == []

    def test_non_serializable_data(self) -> None:
        """Test processing data with non-JSON serializable types."""
        from threading import Lock

        raw_data = [
            {"col1": "value1", "col2": 1 + 2j},  # complex number
            {"col1": "value2", "col2": Lock()},  # lock object
        ]
        processed_rows, warnings = process_sample_data(raw_data)

        assert len(processed_rows) == 2
        assert processed_rows[0]["col1"] == "value1"
        assert processed_rows[0]["col2"] == "<unsupported_type: complex>"
        assert processed_rows[1]["col1"] == "value2"
        assert processed_rows[1]["col2"] == "<unsupported_type: lock>"

        assert len(warnings) == 1
        assert "対応していない型の列 'col2' が含まれています" in warnings

    def test_mixed_data(self) -> None:
        """Test processing data with mixed serializable and non-serializable types."""
        raw_data = [
            {"col1": "value1", "col2": 123, "col3": 1 + 2j},
            {"col1": "value2", "col2": 456, "col3": 3 + 4j},
        ]
        processed_rows, warnings = process_sample_data(raw_data)

        assert len(processed_rows) == 2
        assert processed_rows[0]["col1"] == "value1"
        assert processed_rows[0]["col2"] == 123
        assert processed_rows[0]["col3"] == "<unsupported_type: complex>"
        assert processed_rows[1]["col1"] == "value2"
        assert processed_rows[1]["col2"] == 456
        assert processed_rows[1]["col3"] == "<unsupported_type: complex>"

        assert len(warnings) == 1
        assert "対応していない型の列 'col3' が含まれています" in warnings


class TestFormatResponse:
    """Test _format_response function."""

    def test_format_response_no_warnings(self) -> None:
        """Test formatting response without warnings."""
        processed_rows = [
            {"col1": "value1", "col2": 123},
            {"col1": "value2", "col2": 456},
        ]
        warnings = []

        response = _format_response(
            processed_rows, warnings, "test_db", "test_schema", "test_table", 10
        )

        assert response["sample_data"]["database"] == "test_db"
        assert response["sample_data"]["schema"] == "test_schema"
        assert response["sample_data"]["table"] == "test_table"
        assert response["sample_data"]["sample_size"] == 10
        assert response["sample_data"]["actual_rows"] == 2
        assert response["sample_data"]["columns"] == ["col1", "col2"]
        assert response["sample_data"]["rows"] == processed_rows
        assert response["sample_data"]["warnings"] == []

    def test_format_response_with_warnings(self) -> None:
        """Test formatting response with warnings."""
        processed_rows = [{"col1": "value1", "col2": "<unsupported_type: complex>"}]
        warnings = ["対応していない型の列 'col2' が含まれています"]

        response = _format_response(
            processed_rows, warnings, "test_db", "test_schema", "test_table", 5
        )

        assert response["sample_data"]["database"] == "test_db"
        assert response["sample_data"]["schema"] == "test_schema"
        assert response["sample_data"]["table"] == "test_table"
        assert response["sample_data"]["sample_size"] == 5
        assert response["sample_data"]["actual_rows"] == 1
        assert response["sample_data"]["columns"] == ["col1", "col2"]
        assert response["sample_data"]["rows"] == processed_rows
        assert response["sample_data"]["warnings"] == warnings

    def test_format_response_empty_data(self) -> None:
        """Test formatting response with empty data."""
        processed_rows = []
        warnings = []

        response = _format_response(
            processed_rows, warnings, "test_db", "test_schema", "test_table", 10
        )

        assert response["sample_data"]["database"] == "test_db"
        assert response["sample_data"]["schema"] == "test_schema"
        assert response["sample_data"]["table"] == "test_table"
        assert response["sample_data"]["sample_size"] == 10
        assert response["sample_data"]["actual_rows"] == 0
        assert response["sample_data"]["columns"] == []
        assert response["sample_data"]["rows"] == []
        assert response["sample_data"]["warnings"] == []


class TestHandleSampleTableData:
    """Test handle_sample_table_data function."""

    @pytest.mark.asyncio
    async def test_successful_sample_data_retrieval(self) -> None:
        """Test successful sample data retrieval."""
        sample_data = [
            {"col1": "value1", "col2": 123},
            {"col1": "value2", "col2": 456},
        ]
        mock_effect = MockEffectSampleTableData(sample_data=sample_data)

        args = SampleTableDataArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
            sample_size=10,
        )

        result = await handle_sample_table_data(args, mock_effect)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)

        response_data = json.loads(result[0].text)
        assert response_data["sample_data"]["database"] == "test_db"
        assert response_data["sample_data"]["schema"] == "test_schema"
        assert response_data["sample_data"]["table"] == "test_table"
        assert response_data["sample_data"]["actual_rows"] == 2
        assert response_data["sample_data"]["rows"] == sample_data

    @pytest.mark.asyncio
    async def test_sample_data_with_non_serializable_types(self) -> None:
        """Test sample data retrieval with non-JSON serializable types."""
        sample_data = [
            {"col1": "value1", "col2": 1 + 2j},
            {"col1": "value2", "col2": 3 + 4j},
        ]
        mock_effect = MockEffectSampleTableData(sample_data=sample_data)

        args = SampleTableDataArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )

        result = await handle_sample_table_data(args, mock_effect)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)

        response_data = json.loads(result[0].text)
        assert len(response_data["sample_data"]["warnings"]) == 1
        assert (
            "対応していない型の列 'col2' が含まれています"
            in response_data["sample_data"]["warnings"]
        )
        assert (
            response_data["sample_data"]["rows"][0]["col2"]
            == "<unsupported_type: complex>"
        )

    @pytest.mark.asyncio
    async def test_empty_sample_data(self) -> None:
        """Test handling of empty sample data."""
        mock_effect = MockEffectSampleTableData(sample_data=[])

        args = SampleTableDataArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )

        result = await handle_sample_table_data(args, mock_effect)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)

        response_data = json.loads(result[0].text)
        assert response_data["sample_data"]["actual_rows"] == 0
        assert response_data["sample_data"]["rows"] == []
        assert response_data["sample_data"]["columns"] == []

    @pytest.mark.asyncio
    async def test_error_handling(self) -> None:
        """Test error handling when effect handler raises exception."""
        mock_effect = MockEffectSampleTableData(
            should_raise=Exception("Database error")
        )

        args = SampleTableDataArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
        )

        result = await handle_sample_table_data(args, mock_effect)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert "Error: Failed to sample table data: Database error" in result[0].text

    @pytest.mark.asyncio
    async def test_with_columns_parameter(self) -> None:
        """Test sample data retrieval with columns parameter."""
        sample_data = [
            {"col1": "value1", "col3": "value3"},
            {"col1": "value2", "col3": "value4"},
        ]
        mock_effect = MockEffectSampleTableData(sample_data=sample_data)

        args = SampleTableDataArgs(
            database="test_db",
            schema_name="test_schema",
            table_name="test_table",
            sample_size=5,
            columns=["col1", "col3"],
        )

        result = await handle_sample_table_data(args, mock_effect)

        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)

        response_data = json.loads(result[0].text)
        assert response_data["sample_data"]["sample_size"] == 5
        assert response_data["sample_data"]["columns"] == ["col1", "col3"]
        assert response_data["sample_data"]["rows"] == sample_data
