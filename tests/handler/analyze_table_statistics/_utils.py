from kernel.statistics_support_column import StatisticsSupportColumn
from kernel.table_metadata import TableColumn


def convert_to_statistics_support_columns(
    columns: list[TableColumn],
) -> list[StatisticsSupportColumn]:
    """Convert TableColumns to StatisticsSupportColumns for testing."""
    result: list[StatisticsSupportColumn] = []
    for col in columns:
        stats_col = StatisticsSupportColumn.from_table_column(col)
        if stats_col is not None:
            result.append(stats_col)
    return result
