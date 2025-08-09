"""Tests for SQL Write Detector."""

import pytest

from mcp_snowflake.sql_analyzer import SQLAnalysisError, SQLWriteDetector


class TestSQLWriteDetector:
    """Tests for SQLWriteDetector class"""

    def test_write_sql_detection(self) -> None:
        """Test for Write SQL detection"""
        detector = SQLWriteDetector()

        # Write operations
        write_sqls = [
            "INSERT INTO table1 VALUES (1, 'test')",
            "UPDATE table1 SET col1 = 'new'",
            "DELETE FROM table1 WHERE id = 1",
            "CREATE TABLE test (id INT)",
            "DROP TABLE test",
            "ALTER TABLE test ADD COLUMN new_col VARCHAR(100)",
            "TRUNCATE TABLE test",
            "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.value = source.value",
            "COPY INTO table1 FROM @stage",
            "GRANT SELECT ON table1 TO role1",
        ]

        for sql in write_sqls:
            assert detector.is_write_sql(sql), f"Should detect as write SQL: {sql}"

    def test_read_sql_detection(self) -> None:
        """Test for Read SQL detection"""
        detector = SQLWriteDetector()

        # Read operations
        read_sqls = [
            "SELECT * FROM table1",
            "SELECT col1, col2 FROM table1 WHERE id = 1",
            "WITH cte AS (SELECT * FROM table1) SELECT * FROM cte",
            "SHOW TABLES",
            "DESCRIBE table1",
            "DESC table1",
            "EXPLAIN SELECT * FROM table1",
        ]

        for sql in read_sqls:
            assert not detector.is_write_sql(sql), f"Should detect as read SQL: {sql}"

    def test_snowflake_specific_syntax(self) -> None:
        """Test for Snowflake-specific syntax"""
        detector = SQLWriteDetector()

        # Snowflake Write operations
        snowflake_write_sqls = [
            "COPY INTO mytable FROM @mystage/myfile.csv",
            "CREATE STAGE mystage URL='s3://mybucket/mypath'",
            "CREATE PIPE mypipe AS COPY INTO mytable FROM @mystage",
        ]

        for sql in snowflake_write_sqls:
            assert detector.is_write_sql(sql), f"Should detect as write SQL: {sql}"

        # Snowflake Read operations
        snowflake_read_sqls = [
            "SELECT * FROM table1, LATERAL FLATTEN(input => table1.json_column)",
            "SELECT json_column:key::string FROM table1",
            "SELECT * FROM table1 AT (TIMESTAMP => '2023-01-01 00:00:00')",
            "SELECT * FROM table1 SAMPLE (10 ROWS)",
            "SELECT *, ROW_NUMBER() OVER (ORDER BY id) as rn FROM table1 QUALIFY rn = 1",
        ]

        for sql in snowflake_read_sqls:
            assert not detector.is_write_sql(sql), f"Should detect as read SQL: {sql}"

    def test_empty_sql(self) -> None:
        """Test for empty SQL"""
        detector = SQLWriteDetector()

        with pytest.raises(SQLAnalysisError, match="Empty SQL statement"):
            _ = detector.is_write_sql("")

        with pytest.raises(SQLAnalysisError, match="Empty SQL statement"):
            _ = detector.is_write_sql("   ")

    def test_multiple_statements(self) -> None:
        """Test for multiple statements"""
        detector = SQLWriteDetector()

        # Multiple Read operations
        read_sql = "SELECT * FROM table1; SELECT count(*) FROM table2;"
        assert not detector.is_write_sql(read_sql)

        # Mix of Read and Write operations
        mixed_sql = "SELECT * FROM table1; INSERT INTO table2 VALUES (1, 'test');"
        assert detector.is_write_sql(mixed_sql)

    def test_analyze_sql(self) -> None:
        """Test for analyze_sql method"""
        detector = SQLWriteDetector()

        # Write SQL analysis
        result = detector.analyze_sql("INSERT INTO table1 VALUES (1, 'test')")
        assert result["is_write"] is True
        assert len(result["statements"]) == 1
        assert result["statements"][0]["type"] == "INSERT"
        assert result["statements"][0]["first_token"] == "INSERT"  # noqa: S105
        assert result["statements"][0]["is_write"] is True
        assert "error" not in result

        # Read SQL analysis
        result = detector.analyze_sql("SELECT * FROM table1")
        assert result["is_write"] is False
        assert len(result["statements"]) == 1
        assert result["statements"][0]["type"] == "SELECT"
        assert result["statements"][0]["first_token"] == "SELECT"  # noqa: S105
        assert result["statements"][0]["is_write"] is False
        assert "error" not in result

        # Error case
        result = detector.analyze_sql("")
        assert result["is_write"] is True  # Return True for safety when error occurs
        assert "error" in result
        assert result["error"] is not None
