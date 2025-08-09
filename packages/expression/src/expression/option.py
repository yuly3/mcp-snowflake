def unwrap_or[T](v: T | None, default: T) -> T:
    if v is None:
        return default
    return v
