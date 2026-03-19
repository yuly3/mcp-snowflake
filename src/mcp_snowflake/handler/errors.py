"""Handler-layer exceptions."""


class MissingResponseColumnError(Exception):
    """Required columns are missing from a Snowflake query response row."""
