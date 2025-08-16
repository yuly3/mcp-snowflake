"""Shared test fixtures for analyze_table_statistics tests."""

from typing import Any

from kernel.table_metadata import TableColumn, TableInfo


class MockEffectHandler:
    """Mock implementation of EffectAnalyzeTableStatistics protocol."""

    def __init__(
        self,
        table_data: TableInfo | None = None,
        query_result: list[dict[str, Any]] | None = None,
        should_raise: Exception | None = None,
    ) -> None:
        self.table_data = table_data or TableInfo(
            database="default_db",
            schema="default_schema",
            name="default_table",
            column_count=0,
            columns=[],
        )
        self.query_result = query_result or []
        self.should_raise = should_raise

    async def describe_table(
        self,
        database: str,  # noqa: ARG002
        schema: str,  # noqa: ARG002
        table: str,  # noqa: ARG002
    ) -> TableInfo:
        if self.should_raise:
            raise self.should_raise
        return self.table_data

    async def analyze_table_statistics(
        self,
        database: str,  # noqa: ARG002
        schema: str,  # noqa: ARG002
        table: str,  # noqa: ARG002
        columns_to_analyze: Any,  # noqa: ARG002
        top_k_limit: int,  # noqa: ARG002
    ) -> dict[str, Any]:
        if self.should_raise:
            raise self.should_raise
        if not self.query_result:
            raise ValueError("No data returned from statistics query")
        return self.query_result[0]


def create_test_table_info(
    columns: list[tuple[str, str, bool, int | None]] | None = None,
    database: str = "test_db",
    schema: str = "test_schema",
    table_name: str = "test_table",
) -> TableInfo:
    """Create a test TableInfo with simplified column definitions.

    Parameters
    ----------
    columns : list[tuple[str, str, bool, int | None]] | None
        List of (name, data_type, nullable, ordinal_position) tuples
    database : str
        Database name
    schema : str
        Schema name
    table_name : str
        Table name

    Returns
    -------
    TableInfo
        TableInfo object for testing
    """
    if columns is None:
        columns = [
            ("id", "NUMBER(10,0)", False, 1),
            ("name", "VARCHAR(50)", True, 2),
        ]

    table_columns = [
        TableColumn(
            name=col[0],
            data_type=col[1],
            nullable=col[2],
            default_value=None,
            comment=None,
            ordinal_position=col[3] if col[3] is not None else idx + 1,
        )
        for idx, col in enumerate(columns)
    ]

    return TableInfo(
        database=database,
        schema=schema,
        name=table_name,
        column_count=len(table_columns),
        columns=table_columns,
    )


def create_numeric_stats(
    column_name: str = "id",
    min_val: float = 1.0,
    max_val: float = 100.0,
    avg_val: float = 50.5,
    total_rows: int = 100,
) -> dict[str, Any]:
    """Create numeric column statistics for test query results."""
    prefix = f"NUMERIC_{column_name.upper()}"
    return {
        "TOTAL_ROWS": total_rows,
        f"{prefix}_COUNT": total_rows,
        f"{prefix}_NULL_COUNT": 0,
        f"{prefix}_MIN": min_val,
        f"{prefix}_MAX": max_val,
        f"{prefix}_AVG": avg_val,
        f"{prefix}_Q1": (min_val + avg_val) / 2,
        f"{prefix}_MEDIAN": avg_val,
        f"{prefix}_Q3": (avg_val + max_val) / 2,
        f"{prefix}_DISTINCT": total_rows,
    }


def create_string_stats(
    column_name: str = "name",
    min_length: int = 3,
    max_length: int = 20,
    distinct_count: int = 95,
    top_values: str = '[["John", 5], ["Jane", 3]]',
    total_rows: int = 100,
) -> dict[str, Any]:
    """Create string column statistics for test query results."""
    prefix = f"STRING_{column_name.upper()}"
    return {
        "TOTAL_ROWS": total_rows,
        f"{prefix}_COUNT": total_rows,
        f"{prefix}_NULL_COUNT": 0,
        f"{prefix}_MIN_LENGTH": min_length,
        f"{prefix}_MAX_LENGTH": max_length,
        f"{prefix}_DISTINCT": distinct_count,
        f"{prefix}_TOP_VALUES": top_values,
    }


def create_boolean_stats(
    column_name: str = "is_active",
    true_count: int = 720,
    false_count: int = 230,
    null_count: int = 50,
    total_rows: int = 1000,
) -> dict[str, Any]:
    """Create boolean column statistics for test query results."""
    prefix = f"BOOLEAN_{column_name.upper()}"
    non_null_count = true_count + false_count

    return {
        "TOTAL_ROWS": total_rows,
        f"{prefix}_COUNT": non_null_count,
        f"{prefix}_NULL_COUNT": null_count,
        f"{prefix}_TRUE_COUNT": true_count,
        f"{prefix}_FALSE_COUNT": false_count,
        f"{prefix}_TRUE_PERCENTAGE": round((true_count / non_null_count) * 100, 2),
        f"{prefix}_FALSE_PERCENTAGE": round((false_count / non_null_count) * 100, 2),
        f"{prefix}_TRUE_PERCENTAGE_WITH_NULLS": round(
            (true_count / total_rows) * 100,
            2,
        ),
        f"{prefix}_FALSE_PERCENTAGE_WITH_NULLS": round(
            (false_count / total_rows) * 100,
            2,
        ),
    }


def create_mixed_analysis_result(
    numeric_columns: list[str] | None = None,
    string_columns: list[str] | None = None,
    boolean_columns: list[str] | None = None,
    total_rows: int = 1000,
) -> dict[str, Any]:
    """Create a mixed column analysis result combining multiple column types."""
    if numeric_columns is None:
        numeric_columns = ["price"]
    if string_columns is None:
        string_columns = ["status"]
    if boolean_columns is None:
        boolean_columns = ["is_active"]

    result = {"TOTAL_ROWS": total_rows}

    # Add numeric columns
    for col in numeric_columns:
        stats = create_numeric_stats(col, total_rows=total_rows)
        result.update({k: v for k, v in stats.items() if k != "TOTAL_ROWS"})

    # Add string columns
    for col in string_columns:
        stats = create_string_stats(col, total_rows=total_rows)
        result.update({k: v for k, v in stats.items() if k != "TOTAL_ROWS"})

    # Add boolean columns
    for col in boolean_columns:
        stats = create_boolean_stats(col, total_rows=total_rows)
        result.update({k: v for k, v in stats.items() if k != "TOTAL_ROWS"})

    return result
