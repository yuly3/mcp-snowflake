"""Session overrides for Snowflake role and warehouse."""

from typing import Any

from pydantic import BaseModel


class SessionOverridesMixin(BaseModel):
    """Mixin providing optional role and warehouse override fields.

    These fields allow overriding the default Snowflake role and warehouse
    for individual tool calls.
    """

    role: str | None = None
    """Snowflake role to use for this operation (overrides default)."""

    warehouse: str | None = None
    """Snowflake warehouse to use for this operation (overrides default)."""


# Shared inputSchema properties for role and warehouse
SESSION_OVERRIDE_PROPERTIES: dict[str, dict[str, Any]] = {
    "role": {
        "type": "string",
        "description": "Snowflake role to use for this operation (overrides default)",
    },
    "warehouse": {
        "type": "string",
        "description": "Snowflake warehouse to use for this operation (overrides default)",
    },
}
