"""SearchColumns EffectHandler implementation."""

import json
import logging
from collections.abc import Sequence
from datetime import timedelta

from kernel.sql_utils import quote_ident, quote_literal
from kernel.table_metadata import DataBase

from ..handler.errors import MissingResponseColumnError
from ..handler.search_columns import SearchColumnsTableEntry
from ..snowflake_client import SnowflakeClient

logger = logging.getLogger(__name__)

_REQUIRED_COLUMNS = frozenset({"TABLE_SCHEMA", "TABLE_NAME", "COLUMNS"})


def _generate_search_columns_sql(
    database: DataBase,
    column_name_pattern: str | None,
    data_type: str | None,
    schema: str | None,
    table_name_pattern: str | None,
    limit: int,
) -> str:
    """Generate SQL for searching columns in INFORMATION_SCHEMA.

    Parameters
    ----------
    database : DataBase
        Database to search in.
    column_name_pattern : str | None
        ILIKE pattern for column names.
    data_type : str | None
        Exact data type filter.
    schema : str | None
        Schema name filter.
    table_name_pattern : str | None
        ILIKE pattern for table names.
    limit : int
        Maximum number of tables to return.

    Returns
    -------
    str
        The generated SQL query.
    """
    conditions = ["TABLE_SCHEMA != 'INFORMATION_SCHEMA'"]

    if column_name_pattern is not None:
        conditions.append(f"COLUMN_NAME ILIKE {quote_literal(column_name_pattern)}")

    if data_type is not None:
        conditions.append(f"DATA_TYPE = {quote_literal(data_type)}")

    if schema is not None:
        conditions.append(f"TABLE_SCHEMA = {quote_literal(schema)}")

    if table_name_pattern is not None:
        conditions.append(f"TABLE_NAME ILIKE {quote_literal(table_name_pattern)}")

    where_clause = " AND ".join(conditions)
    table_ref = f"{quote_ident(database)}.INFORMATION_SCHEMA.COLUMNS"

    return f"""\
WITH ranked AS (
  SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
         NULLIF(COMMENT, '') as COMMENT,
         DENSE_RANK() OVER (ORDER BY TABLE_SCHEMA, TABLE_NAME) as table_rank
  FROM {table_ref}
  WHERE {where_clause}
)
SELECT TABLE_SCHEMA, TABLE_NAME,
       ARRAY_AGG(
         OBJECT_CONSTRUCT('name', COLUMN_NAME, 'type', DATA_TYPE, 'comment', COMMENT)
       ) WITHIN GROUP (ORDER BY COLUMN_NAME) as COLUMNS
FROM ranked
WHERE table_rank <= {limit}
GROUP BY TABLE_SCHEMA, TABLE_NAME
ORDER BY TABLE_SCHEMA, TABLE_NAME"""  # noqa: S608


class SearchColumnsEffectHandler:
    """EffectHandler for SearchColumns operations."""

    def __init__(self, client: SnowflakeClient) -> None:
        """Initialize with SnowflakeClient."""
        self.client = client

    async def search_columns(
        self,
        database: DataBase,
        column_name_pattern: str | None,
        data_type: str | None,
        schema: str | None,
        table_name_pattern: str | None,
        limit: int,
    ) -> Sequence[SearchColumnsTableEntry]:
        """Search columns in a database."""
        query = _generate_search_columns_sql(
            database, column_name_pattern, data_type, schema, table_name_pattern, limit
        )

        try:
            results = await self.client.execute_query(query, timedelta(seconds=30))
        except Exception:
            logger.exception(
                "failed to execute search columns operation",
                extra={
                    "database": str(database),
                    "column_name_pattern": column_name_pattern,
                    "data_type": data_type,
                    "schema": schema,
                    "table_name_pattern": table_name_pattern,
                    "limit": limit,
                    "query": query,
                },
            )
            raise

        entries: list[SearchColumnsTableEntry] = []
        for row in results:
            missing = _REQUIRED_COLUMNS - row.keys()
            if missing:
                raise MissingResponseColumnError(
                    f"search_columns response is missing required columns: {', '.join(sorted(missing))}"
                )

            columns_raw = row["COLUMNS"]
            columns_parsed = json.loads(columns_raw) if isinstance(columns_raw, str) else columns_raw
            columns_json = json.dumps(columns_parsed, ensure_ascii=False, separators=(",", ":"))

            entries.append(
                SearchColumnsTableEntry(
                    schema=row["TABLE_SCHEMA"],
                    table=row["TABLE_NAME"],
                    columns_json=columns_json,
                )
            )

        return entries
