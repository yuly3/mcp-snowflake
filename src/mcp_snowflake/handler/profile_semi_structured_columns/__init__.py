"""Semi-structured column profiling module."""

import logging

from ._column_analysis import select_and_classify_columns
from ._types import ClassifiedSemiStructuredColumns
from .models import (
    EffectProfileSemiStructuredColumns,
    NoSemiStructuredColumns,
    ProfileSemiStructuredColumnsArgs,
    ProfileSemiStructuredColumnsJsonResponse,
    SemiStructuredColumnDoesNotExist,
    SemiStructuredProfileParseResult,
    SemiStructuredProfileResultParseError,
)

logger = logging.getLogger(__name__)

__all__ = [
    "EffectProfileSemiStructuredColumns",
    "NoSemiStructuredColumns",
    "ProfileSemiStructuredColumnsArgs",
    "ProfileSemiStructuredColumnsJsonResponse",
    "SemiStructuredColumnDoesNotExist",
    "SemiStructuredProfileParseResult",
    "SemiStructuredProfileResultParseError",
    "handle_profile_semi_structured_columns",
]


async def handle_profile_semi_structured_columns(
    args: ProfileSemiStructuredColumnsArgs,
    effect: EffectProfileSemiStructuredColumns,
) -> ProfileSemiStructuredColumnsJsonResponse | SemiStructuredColumnDoesNotExist | NoSemiStructuredColumns:
    """Handle profile_semi_structured_columns request."""
    table_info = await effect.describe_table(
        args.database,
        args.schema_,
        args.table_,
    )

    classified = select_and_classify_columns(table_info.columns, args.columns)
    match classified:
        case SemiStructuredColumnDoesNotExist() as not_found:
            return not_found
        case NoSemiStructuredColumns() as no_supported:
            return no_supported
        case ClassifiedSemiStructuredColumns(
            supported_columns=supported_columns,
            unsupported_columns=unsupported_columns,
        ):
            pass

    parsed = await effect.profile_semi_structured_columns(
        args.database,
        args.schema_,
        args.table_,
        supported_columns,
        args.sample_rows,
        args.max_depth,
        args.top_k_limit,
        args.include_path_stats,
        args.include_value_samples,
    )

    response: ProfileSemiStructuredColumnsJsonResponse = {
        "semi_structured_profile": {
            "profile_info": {
                "database": args.database,
                "schema": args.schema_,
                "table": args.table_,
                "total_rows": parsed.total_rows,
                "sampled_rows": parsed.sampled_rows,
                "analyzed_columns": len(supported_columns),
                "analyzed_column_names": [col.name for col in supported_columns],
            },
            "column_profiles": parsed.column_profiles,
            "path_profiles": parsed.path_profiles,
            "warnings": parsed.warnings,
        }
    }

    if unsupported_columns:
        response["semi_structured_profile"]["profile_info"]["unsupported_columns"] = [
            {"name": col.name, "data_type": col.data_type.raw_type} for col in unsupported_columns
        ]

    return response
