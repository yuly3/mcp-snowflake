"""SQL utilities for safe identifier quoting."""

import re

# Pattern for simple identifiers that don't need quoting
# (uppercase letters, digits, underscores, must start with letter or underscore)
SIMPLE_IDENTIFIER_PATTERN = re.compile(r"^[A-Z_][A-Z0-9_]*$")


def quote_ident(name: str) -> str:
    """Quote a SQL identifier if necessary.

    Parameters
    ----------
    name : str
        The identifier to quote

    Returns
    -------
    str
        The quoted identifier if needed, otherwise the original

    Raises
    ------
    ValueError
        If the identifier is empty or whitespace-only
    """
    # Trim whitespace
    trimmed = name.strip()

    # Check for empty identifier
    if not trimmed:
        raise ValueError("Empty identifier")

    # If it matches the simple pattern and contains no quotes, return as-is
    if SIMPLE_IDENTIFIER_PATTERN.match(trimmed) and '"' not in trimmed:
        return trimmed

    # Otherwise, quote it and escape internal quotes
    escaped = trimmed.replace('"', '""')
    return f'"{escaped}"'


def fully_qualified(database: str, schema: str | None, name: str) -> str:
    """Create a fully qualified identifier.

    Parameters
    ----------
    database : str
        Database name
    schema : str | None
        Schema name (if None, creates two-part identifier)
    name : str
        Object name

    Returns
    -------
    str
        Fully qualified identifier with appropriate quoting
    """
    quoted_db = quote_ident(database)
    quoted_name = quote_ident(name)

    if schema is None:
        return f"{quoted_db}.{quoted_name}"

    quoted_schema = quote_ident(schema)
    return f"{quoted_db}.{quoted_schema}.{quoted_name}"
