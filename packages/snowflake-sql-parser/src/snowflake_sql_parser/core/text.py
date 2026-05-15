"""Shared text span models for Snowflake SQL analysis."""

import attrs


@attrs.define(frozen=True, slots=True)
class TextSpan:
    """A span in the original SQL text."""

    start: int
    end: int
