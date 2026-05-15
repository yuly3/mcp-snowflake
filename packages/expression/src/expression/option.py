from collections.abc import Callable


def map_[T, U](v: T | None, f: Callable[[T], U]) -> U | None:
    """Apply function to value if present.

    Parameters
    ----------
    v : T | None
        Value to transform or None.
    f : Callable[[T], U]
        Function to apply to the value.

    Returns
    -------
    U | None
        Transformed value or None if input is None.

    Examples
    --------
    >>> map_(5, lambda x: x * 2)
    10
    >>> map_(None, lambda x: x * 2)
    """
    return None if v is None else f(v)


def flat_map[T, U](v: T | None, f: Callable[[T], U | None]) -> U | None:
    """Apply function that returns optional value.

    Parameters
    ----------
    v : T | None
        Value to transform or None.
    f : Callable[[T], U | None]
        Function that returns transformed value or None.

    Returns
    -------
    U | None
        Transformed value or None if input is None or f returns None.

    Examples
    --------
    >>> flat_map(5, lambda x: x * 2 if x > 0 else None)
    10
    >>> flat_map(-5, lambda x: x * 2 if x > 0 else None)
    >>> flat_map(None, lambda x: x * 2)
    """
    return None if v is None else f(v)


def get_or[T](v: T | None, default: T) -> T:
    """Return value or default.

    Parameters
    ----------
    v : T | None
        Value to return or None.
    default : T
        Default value if input is None.

    Returns
    -------
    T
        Value or default.

    Examples
    --------
    >>> get_or(5, 10)
    5
    >>> get_or(None, 10)
    10
    """
    return default if v is None else v


def get_or_else[T](v: T | None, f: Callable[[], T]) -> T:
    """Return value or compute default.

    Parameters
    ----------
    v : T | None
        Value to return or None.
    f : Callable[[], T]
        Function to compute default value.

    Returns
    -------
    T
        Value or computed default.

    Examples
    --------
    >>> get_or_else(5, lambda: 10)
    5
    >>> get_or_else(None, lambda: 10)
    10
    """
    return f() if v is None else v


def map_or[T, U](v: T | None, default: U, f: Callable[[T], U]) -> U:
    """Transform value or return default.

    Parameters
    ----------
    v : T | None
        Value to transform or None.
    default : U
        Default value if input is None.
    f : Callable[[T], U]
        Function to apply to the value.

    Returns
    -------
    U
        Transformed value or default.

    Examples
    --------
    >>> map_or(5, 0, lambda x: x * 2)
    10
    >>> map_or(None, 0, lambda x: x * 2)
    0
    """
    return default if v is None else f(v)


def map_or_else[T, U](v: T | None, default_f: Callable[[], U], f: Callable[[T], U]) -> U:
    """Transform value or compute default.

    Parameters
    ----------
    v : T | None
        Value to transform or None.
    default_f : Callable[[], U]
        Function to compute default value.
    f : Callable[[T], U]
        Function to apply to the value.

    Returns
    -------
    U
        Transformed value or computed default.

    Examples
    --------
    >>> map_or_else(5, lambda: 0, lambda x: x * 2)
    10
    >>> map_or_else(None, lambda: 0, lambda x: x * 2)
    0
    """
    return default_f() if v is None else f(v)


def zip_[T, U](a: T | None, b: U | None) -> tuple[T, U] | None:
    """Combine two optional values into an optional tuple.

    Parameters
    ----------
    a : T | None
        First value.
    b : U | None
        Second value.

    Returns
    -------
    tuple[T, U] | None
        Tuple of both values if both are present, otherwise None.

    Examples
    --------
    >>> zip_(5, 'hello')
    (5, 'hello')
    >>> zip_(5, None)
    >>> zip_(None, 'hello')
    >>> zip_(None, None)
    """
    if a is not None and b is not None:
        return (a, b)
    return None
