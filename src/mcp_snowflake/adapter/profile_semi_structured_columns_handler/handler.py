"""ProfileSemiStructuredColumns EffectHandler implementation."""

import logging
from datetime import timedelta
from typing import Literal, cast

from kernel.table_metadata import DataBase, Schema, Table, TableColumn

from ...handler.profile_semi_structured_columns.models import (
    SemiStructuredProfileParseResult,
    SemiStructuredProfileResultParseError,
)
from ...snowflake_client import SnowflakeClient
from ..describe_table_handler import DescribeTableEffectHandler
from .result_parser import (
    parse_column_profile_row,
    parse_count_value,
    parse_path_profile_rows,
    parse_top_values,
)
from .sql_generator import (
    generate_column_profile_sql,
    generate_path_profile_sql,
    generate_sampled_rows_sql,
    generate_top_level_keys_sql,
    generate_total_rows_sql,
)

logger = logging.getLogger(__name__)


class ProfileSemiStructuredColumnsEffectHandler(DescribeTableEffectHandler):
    """EffectHandler for profile_semi_structured_columns operations."""

    def __init__(
        self,
        client: SnowflakeClient,
        base_query_timeout_seconds: int = 90,
        path_query_timeout_seconds: int = 180,
    ) -> None:
        """Initialize with SnowflakeClient."""
        super().__init__(client)
        self.base_query_timeout = timedelta(seconds=base_query_timeout_seconds)
        self.path_query_timeout = timedelta(seconds=path_query_timeout_seconds)

    async def profile_semi_structured_columns(
        self,
        database: DataBase,
        schema: Schema,
        table: Table,
        columns_to_profile: list[TableColumn],
        sample_rows: int,
        max_depth: int,
        top_k_limit: int,
        include_path_stats: bool,  # noqa: FBT001
        include_value_samples: bool,  # noqa: FBT001
    ) -> SemiStructuredProfileParseResult:
        """Execute profiling queries and return parsed result."""
        total_rows_sql = generate_total_rows_sql(database, schema, table)
        sampled_rows_sql = generate_sampled_rows_sql(database, schema, table, sample_rows)

        try:
            total_rows_result = await self.client.execute_query(
                total_rows_sql,
                self.base_query_timeout,
            )
            sampled_rows_result = await self.client.execute_query(
                sampled_rows_sql,
                self.base_query_timeout,
            )
        except Exception:
            logger.exception(
                "failed to initialize semi-structured profiling",
                extra={
                    "database": database,
                    "schema": schema,
                    "table": table,
                    "sample_rows": sample_rows,
                    "max_depth": max_depth,
                    "top_k_limit": top_k_limit,
                    "include_path_stats": include_path_stats,
                    "include_value_samples": include_value_samples,
                },
            )
            raise

        if not total_rows_result:
            raise SemiStructuredProfileResultParseError("No rows returned for total row count")
        total_rows = parse_count_value(total_rows_result[0], "TOTAL_ROWS")

        if not sampled_rows_result:
            raise SemiStructuredProfileResultParseError("No rows returned for sampled row count")
        sampled_rows = parse_count_value(sampled_rows_result[0], "SAMPLED_ROWS")

        column_profiles = {}
        path_profiles = []

        for column in columns_to_profile:
            column_type = cast(
                "Literal['VARIANT', 'ARRAY', 'OBJECT']",
                column.data_type.normalized_type,
            )
            column_profile_sql = generate_column_profile_sql(
                database,
                schema,
                table,
                column.name,
                sample_rows,
            )

            try:
                column_profile_result = await self.client.execute_query(
                    column_profile_sql,
                    self.base_query_timeout,
                )
            except Exception:
                logger.exception(
                    "failed to profile semi-structured column",
                    extra={
                        "database": database,
                        "schema": schema,
                        "table": table,
                        "column": column.name,
                        "sample_rows": sample_rows,
                        "max_depth": max_depth,
                        "top_k_limit": top_k_limit,
                        "include_path_stats": include_path_stats,
                        "include_value_samples": include_value_samples,
                    },
                )
                raise

            if not column_profile_result:
                raise SemiStructuredProfileResultParseError(f"No rows returned for column profile: {column.name}")

            profile = parse_column_profile_row(
                column_profile_result[0],
                column_type,
            )

            if column.data_type.normalized_type in {"OBJECT", "VARIANT"}:
                top_level_keys_sql = generate_top_level_keys_sql(
                    database,
                    schema,
                    table,
                    column.name,
                    sample_rows,
                    top_k_limit,
                )
                try:
                    top_level_keys_result = await self.client.execute_query(
                        top_level_keys_sql,
                        self.base_query_timeout,
                    )
                except Exception:
                    logger.exception(
                        "failed to retrieve top-level keys",
                        extra={
                            "database": database,
                            "schema": schema,
                            "table": table,
                            "column": column.name,
                            "top_k_limit": top_k_limit,
                        },
                    )
                    raise
                if top_level_keys_result:
                    profile["top_level_keys_top_k"] = parse_top_values(
                        top_level_keys_result[0].get("TOP_LEVEL_KEYS_TOP_K"),
                        "TOP_LEVEL_KEYS_TOP_K",
                    )
                else:
                    profile["top_level_keys_top_k"] = []

            column_profiles[column.name] = profile

            if include_path_stats:
                path_profile_sql = generate_path_profile_sql(
                    database,
                    schema,
                    table,
                    column.name,
                    sample_rows,
                    max_depth,
                    top_k_limit,
                    include_value_samples,
                )
                try:
                    path_profile_result = await self.client.execute_query(
                        path_profile_sql,
                        self.path_query_timeout,
                    )
                except Exception:
                    logger.exception(
                        "failed to retrieve path profile",
                        extra={
                            "database": database,
                            "schema": schema,
                            "table": table,
                            "column": column.name,
                            "max_depth": max_depth,
                            "top_k_limit": top_k_limit,
                        },
                    )
                    raise
                path_profiles.extend(
                    parse_path_profile_rows(
                        path_profile_result,
                        column.name,
                        include_value_samples,
                    )
                )

        warnings: list[str] = []
        if sampled_rows < total_rows:
            warnings.append(
                f"Approximate profile based on SAMPLE ROW: sampled_rows={sampled_rows}, total_rows={total_rows}"
            )
        if include_path_stats:
            warnings.append(f"Path profiling is limited to max_depth={max_depth}")
        if include_path_stats and not include_value_samples:
            warnings.append("Path top_values are omitted because include_value_samples is false")

        return SemiStructuredProfileParseResult(
            total_rows=total_rows,
            sampled_rows=sampled_rows,
            column_profiles=column_profiles,
            path_profiles=path_profiles,
            warnings=warnings,
        )
