"""SQL Write検知器のテスト."""

import pytest

from mcp_snowflake.sql_analyzer import SQLAnalysisError, SQLWriteDetector


class TestSQLWriteDetector:
    """SQLWriteDetectorクラスのテスト"""

    def test_write_sql_detection(self) -> None:
        """Write SQLの検出テスト"""
        detector = SQLWriteDetector()

        # Write操作
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
        """Read SQLの検出テスト"""
        detector = SQLWriteDetector()

        # Read操作
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
        """Snowflake専用構文のテスト"""
        detector = SQLWriteDetector()

        # Snowflake Write操作
        snowflake_write_sqls = [
            "COPY INTO mytable FROM @mystage/myfile.csv",
            "CREATE STAGE mystage URL='s3://mybucket/mypath'",
            "CREATE PIPE mypipe AS COPY INTO mytable FROM @mystage",
        ]

        for sql in snowflake_write_sqls:
            assert detector.is_write_sql(sql), f"Should detect as write SQL: {sql}"

        # Snowflake Read操作
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
        """空のSQLのテスト"""
        detector = SQLWriteDetector()

        with pytest.raises(SQLAnalysisError, match="Empty SQL statement"):
            _ = detector.is_write_sql("")

        with pytest.raises(SQLAnalysisError, match="Empty SQL statement"):
            _ = detector.is_write_sql("   ")

    def test_multiple_statements(self) -> None:
        """複数のステートメントのテスト"""
        detector = SQLWriteDetector()

        # 複数のRead操作
        read_sql = "SELECT * FROM table1; SELECT count(*) FROM table2;"
        assert not detector.is_write_sql(read_sql)

        # Read操作とWrite操作の混合
        mixed_sql = "SELECT * FROM table1; INSERT INTO table2 VALUES (1, 'test');"
        assert detector.is_write_sql(mixed_sql)

    def test_analyze_sql(self) -> None:
        """analyze_sqlメソッドのテスト"""
        detector = SQLWriteDetector()

        # Write SQLの解析
        result = detector.analyze_sql("INSERT INTO table1 VALUES (1, 'test')")
        assert result["is_write"] is True
        assert len(result["statements"]) == 1
        assert result["statements"][0]["type"] == "INSERT"
        assert result["statements"][0]["first_token"] == "INSERT"  # noqa: S105
        assert result["statements"][0]["is_write"] is True
        assert "error" not in result

        # Read SQLの解析
        result = detector.analyze_sql("SELECT * FROM table1")
        assert result["is_write"] is False
        assert len(result["statements"]) == 1
        assert result["statements"][0]["type"] == "SELECT"
        assert result["statements"][0]["first_token"] == "SELECT"  # noqa: S105
        assert result["statements"][0]["is_write"] is False
        assert "error" not in result

        # エラーケース
        result = detector.analyze_sql("")
        assert result["is_write"] is True  # エラー時は安全のためTrue
        assert "error" in result
        assert result["error"] is not None
