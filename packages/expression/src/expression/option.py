def unwrap_or[T](v: T | None, default: T) -> T:
    """Unwraps the value if it is not None, otherwise returns the default value.

    Returns
    -------
        T: The unwrapped value or the default value.

    Examples
    --------
    >>> unwrap_or(5, 0)
    5
    >>> unwrap_or(None, 0)
    0
    >>> unwrap_or("hello", "default")
    'hello'
    >>> unwrap_or(None, "default")
    'default'
    """
    if v is None:
        return default
    return v
