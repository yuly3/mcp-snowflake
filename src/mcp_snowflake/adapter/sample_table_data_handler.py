"""SampleTableData EffectHandler implementation."""

import logging
from datetime import timedelta
from typing import Any

from ..snowflake_client import SnowflakeClient

logger = logging.getLogger(__name__)


class SampleTableDataEffectHandler:
    """EffectHandler for SampleTableData operations."""

    def __init__(self, client: SnowflakeClient) -> None:
        """Initialize with SnowflakeClient."""
        self.client = client

    async def sample_table_data(
        self,
        database: str,
        schema: str,
        table: str,
        sample_size: int,
        columns: list[str],
    ) -> list[dict[str, Any]]:
        """Retrieve sample data from a Snowflake table using SAMPLE ROW clause.

        Parameters
        ----------
        database : str
            Database name
        schema : str
            Schema name
        table : str
            Table name
        sample_size : int
            Number of sample rows to retrieve
        columns : list[str] | None
            List of column names to select (if None, selects all columns)

        Returns
        -------
        list[dict[str, Any]]
            List of dictionaries representing sample rows
        """
        column_list = ", ".join(f'"{col}"' for col in columns) if columns else "*"

        query = f"""
        SELECT {column_list}
        FROM "{database}"."{schema}"."{table}"
        SAMPLE ROW ({sample_size} ROWS)
        """  # noqa: S608

        logger.info(
            "Executing sample_table_data query for %s.%s.%s with sample_size=%d",
            database,
            schema,
            table,
            sample_size,
        )

        timeout = timedelta(seconds=60)
        return await self.client.execute_query(query, timeout)
