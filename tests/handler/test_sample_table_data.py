from typing import Any

import pytest
from pydantic import ValidationError

from kernel import DataProcessingResult
from mcp_snowflake.handler.sample_table_data import (
    SampleTableDataArgs,
    _format_response,
    handle_sample_table_data,
)

from ._utils import assert_single_text, parse_json_text


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
        table: str,  # noqa: ARG002
        sample_size: int,  # noqa: ARG002
        columns: list[str],  # noqa: ARG002
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
            schema="test_schema",
            table="test_table",
        )
        assert args.database == "test_db"
        assert args.schema_ == "test_schema"
        assert args.table_ == "test_table"
        assert args.sample_size == 10  # default
        assert args.columns == []  # default

    def test_valid_args_all_fields(self) -> None:
        """Test valid arguments with all fields specified."""
        args = SampleTableDataArgs(
            database="test_db",
            schema="test_schema",
            table="test_table",
            sample_size=20,
            columns=["col1", "col2"],
        )
        assert args.database == "test_db"
        assert args.schema_ == "test_schema"
        assert args.table_ == "test_table"

    @pytest.mark.parametrize(
        ("case_id", "payload"),
        [
            ("missing_database", {"schema": "test_schema", "table": "test_table"}),
            ("missing_schema", {"database": "test_db", "table": "test_table"}),
            ("missing_table", {"database": "test_db", "schema": "test_schema"}),
            (
                "invalid_sample_size_type",
                {
                    "database": "test_db",
                    "schema": "test_schema",
                    "table": "test_table",
                    "sample_size": "invalid",
                },
            ),
        ],
        ids=[
            "missing_database",
            "missing_schema",
            "missing_table",
            "invalid_sample_size_type",
        ],
    )
    def test_args_validation_errors(
        self,
        case_id: str,  # noqa: ARG002
        payload: dict[str, Any],
    ) -> None:
        """Test argument validation error scenarios."""
        with pytest.raises(ValidationError, match=r".*"):
            _ = SampleTableDataArgs.model_validate(payload)


class TestProcessSampleData:
    """Test process_multiple_rows_data function."""

    @pytest.mark.parametrize(
        ("case_id", "raw_data", "expected"),
        [
            (
                "empty",
                [],
                {"processed_rows": [], "warnings": []},
            ),
            (
                "serializable",
                [{"col1": "value1", "col2": 123}, {"col1": "value2", "col2": 456}],
                {
                    "processed_rows": [
                        {"col1": "value1", "col2": 123},
                        {"col1": "value2", "col2": 456},
                    ],
                    "warnings": [],
                },
            ),
        ],
        ids=["empty", "serializable"],
    )
    def test_data_processing_simple_variants(
        self,
        case_id: str,
        raw_data: list[dict[str, Any]],
        expected: dict[str, Any],
    ) -> None:
        """Test processing simple data variants (empty and serializable)."""
        result = DataProcessingResult.from_raw_rows(raw_data)

        assert result.processed_rows == expected["processed_rows"], (
            f"[{case_id}] Processed rows mismatch"
        )
        assert result.warnings == expected["warnings"], f"[{case_id}] Warnings mismatch"

    def test_non_serializable_data(self) -> None:
        """Test processing data with non-JSON serializable types."""
        from threading import Lock

        raw_data = [
            {"col1": "value1", "col2": 1 + 2j},  # complex number
            {"col1": "value2", "col2": Lock()},  # lock object
        ]
        result = DataProcessingResult.from_raw_rows(raw_data)

        assert len(result.processed_rows) == 2
        assert result.processed_rows[0]["col1"] == "value1"
        assert result.processed_rows[0]["col2"] == "<unsupported_type: complex>"
        assert result.processed_rows[1]["col1"] == "value2"
        assert result.processed_rows[1]["col2"] == "<unsupported_type: lock>"

        assert len(result.warnings) == 1
        assert "Column 'col2' contains unsupported data type" in result.warnings

    def test_mixed_data(self) -> None:
        """Test processing data with mixed serializable and non-serializable types."""
        raw_data = [
            {"col1": "value1", "col2": 123, "col3": 1 + 2j},
            {"col1": "value2", "col2": 456, "col3": 3 + 4j},
        ]
        result = DataProcessingResult.from_raw_rows(raw_data)

        assert len(result.processed_rows) == 2
        assert result.processed_rows[0]["col1"] == "value1"
        assert result.processed_rows[0]["col2"] == 123
        assert result.processed_rows[0]["col3"] == "<unsupported_type: complex>"
        assert result.processed_rows[1]["col1"] == "value2"
        assert result.processed_rows[1]["col2"] == 456
        assert result.processed_rows[1]["col3"] == "<unsupported_type: complex>"

        assert len(result.warnings) == 1
        assert "Column 'col3' contains unsupported data type" in result.warnings


class TestFormatResponse:
    """Test _format_response function."""

    @pytest.mark.parametrize(
        ("case_id", "processed_rows", "warnings", "sample_size", "expected"),
        [
            (
                "no_warnings",
                [{"col1": "value1", "col2": 123}, {"col1": "value2", "col2": 456}],
                [],
                10,
                {
                    "actual_rows": 2,
                    "columns": ["col1", "col2"],
                    "warnings_len": 0,
                },
            ),
            (
                "with_warnings",
                [{"col1": "value1", "col2": "<unsupported_type: complex>"}],
                ["対応していない型の列 'col2' が含まれています"],
                5,
                {
                    "actual_rows": 1,
                    "columns": ["col1", "col2"],
                    "warnings_len": 1,
                },
            ),
            (
                "empty_data",
                [],
                [],
                10,
                {
                    "actual_rows": 0,
                    "columns": [],
                    "warnings_len": 0,
                },
            ),
        ],
        ids=["no_warnings", "with_warnings", "empty_data"],
    )
    def test_format_response_variants(
        self,
        case_id: str,
        processed_rows: list[dict[str, Any]],
        warnings: list[str],
        sample_size: int,
        expected: dict[str, Any],
    ) -> None:
        """Test _format_response function with various scenarios."""
        response = _format_response(
            processed_rows,
            warnings,
            "test_db",
            "test_schema",
            "test_table",
            sample_size,
        )

        sample_data = response["sample_data"]

        # Common assertions
        assert sample_data["database"] == "test_db", f"[{case_id}] Database mismatch"
        assert sample_data["schema"] == "test_schema", f"[{case_id}] Schema mismatch"
        assert sample_data["table"] == "test_table", f"[{case_id}] Table mismatch"
        assert sample_data["sample_size"] == sample_size, (
            f"[{case_id}] Sample size mismatch"
        )

        # Expected value assertions
        assert sample_data["actual_rows"] == expected["actual_rows"], (
            f"[{case_id}] Actual rows mismatch"
        )
        assert sample_data["columns"] == expected["columns"], (
            f"[{case_id}] Columns mismatch"
        )
        assert len(sample_data["warnings"]) == expected["warnings_len"], (
            f"[{case_id}] Warnings length mismatch"
        )
        assert sample_data["rows"] == processed_rows, f"[{case_id}] Rows mismatch"
        assert sample_data["warnings"] == warnings, (
            f"[{case_id}] Warnings content mismatch"
        )


class TestHandleSampleTableData:
    """Test handle_sample_table_data function."""

    @pytest.mark.parametrize(
        ("case_id", "sample_data", "columns_arg", "sample_size", "expected"),
        [
            (
                "basic",
                [{"col1": "value1", "col2": 123}, {"col1": "value2", "col2": 456}],
                None,
                10,
                {
                    "actual_rows": 2,
                    "columns": ["col1", "col2"],
                    "warnings_len": 0,
                },
            ),
            (
                "non_serializable",
                [
                    {"col1": "value1", "col2": 1 + 2j},
                    {"col1": "value2", "col2": 3 + 4j},
                ],
                None,
                10,
                {
                    "actual_rows": 2,
                    "columns": ["col1", "col2"],
                    "warnings_contains": "Column 'col2' contains unsupported data type",
                    "mutated_field": ("col2", "<unsupported_type: complex>"),
                },
            ),
            (
                "empty",
                [],
                None,
                10,
                {
                    "actual_rows": 0,
                    "columns": [],
                    "warnings_len": 0,
                },
            ),
            (
                "with_columns",
                [{"col1": "v1", "col3": "a"}, {"col1": "v2", "col3": "b"}],
                ["col1", "col3"],
                5,
                {
                    "actual_rows": 2,
                    "columns": ["col1", "col3"],
                    "warnings_len": 0,
                },
            ),
        ],
        ids=["basic", "non_serializable", "empty", "with_columns"],
    )
    @pytest.mark.asyncio
    async def test_handle_sample_table_data_success_variants(
        self,
        case_id: str,
        sample_data: list[dict[str, Any]],
        columns_arg: list[str] | None,
        sample_size: int,
        expected: dict[str, Any],
    ) -> None:
        """Test successful sample data retrieval scenarios with parametrized variants."""
        # Arrange
        mock_effect = MockEffectSampleTableData(sample_data=sample_data)
        args = SampleTableDataArgs(
            database="test_db",
            schema="test_schema",
            table="test_table",
            sample_size=sample_size,
            columns=columns_arg or [],
        )

        # Act
        result = await handle_sample_table_data(args, mock_effect)

        # Assert using helpers
        content = assert_single_text(result)
        response_data = parse_json_text(content)
        sample_data_obj = response_data["sample_data"]

        # Common assertions
        assert sample_data_obj["database"] == "test_db", (
            f"[{case_id}] Database mismatch"
        )
        assert sample_data_obj["schema"] == "test_schema", (
            f"[{case_id}] Schema mismatch"
        )
        assert sample_data_obj["table"] == "test_table", f"[{case_id}] Table mismatch"
        assert sample_data_obj["sample_size"] == sample_size, (
            f"[{case_id}] Sample size mismatch"
        )

        # Expected value assertions
        assert sample_data_obj["actual_rows"] == expected["actual_rows"], (
            f"[{case_id}] Actual rows mismatch"
        )
        assert sample_data_obj["columns"] == expected["columns"], (
            f"[{case_id}] Columns mismatch"
        )

        # Conditional assertions based on expected keys
        if "warnings_len" in expected:
            assert len(sample_data_obj["warnings"]) == expected["warnings_len"], (
                f"[{case_id}] Warnings length mismatch"
            )

        if "warnings_contains" in expected:
            assert any(
                expected["warnings_contains"] in warning
                for warning in sample_data_obj["warnings"]
            ), f"[{case_id}] Warning content not found"

        if "mutated_field" in expected:
            column_name, expected_value = expected["mutated_field"]
            actual_value = sample_data_obj["rows"][0][column_name]
            assert actual_value == expected_value, (
                f"[{case_id}] Mutated field {column_name} mismatch"
            )

        # For non-empty cases, verify rows structure
        if expected["actual_rows"] > 0 and "mutated_field" not in expected:
            assert sample_data_obj["rows"] == sample_data, (
                f"[{case_id}] Rows content mismatch"
            )

    @pytest.mark.asyncio
    async def test_error_handling(self) -> None:
        """Test error handling when effect handler raises exception."""
        mock_effect = MockEffectSampleTableData(
            should_raise=Exception("Database error"),
        )

        args = SampleTableDataArgs(
            database="test_db",
            schema="test_schema",
            table="test_table",
        )

        result = await handle_sample_table_data(args, mock_effect)

        content = assert_single_text(result)
        assert "Error: Failed to sample table data: Database error" in content.text
