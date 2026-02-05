import inspect
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, NoReturn, ParamSpec

P = ParamSpec("P")


class ContractViolationError(Exception):
    """Exception raised when a contract violation occurs.

    This exception is raised by the contract decorators when an unexpected
    exception occurs during function execution, unless the exception is
    explicitly listed in the known_err parameter.

    Attributes
    ----------
    function_name : str | None
        Name of the function where the contract violation occurred.
    original_exception : Exception | None
        The original exception that triggered the contract violation.
    context : dict[str, Any]
        Additional context information including function arguments.

    Examples
    --------
    >>> @contract()
    ... def divide(a: int, b: int) -> float:
    ...     return a / b
    >>> try:
    ...     divide(1, 0)
    ... except ContractViolationError as e:
    ...     print("Contract violation occurred")
    ...     print(f"Function: {e.function_name}")
    ...     print(f"Original error: {type(e.original_exception).__name__}")
    Contract violation occurred
    Function: divide
    Original error: ZeroDivisionError
    """

    def __init__(
        self,
        message: str = "contract violation",
        *,
        function_name: str | None = None,
        original_exception: Exception | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.function_name = function_name
        self.original_exception = original_exception
        self.context = context or {}

    def __str__(self) -> str:
        """Return enhanced string representation with context information."""
        base_msg = super().__str__()
        parts = [base_msg]

        if self.function_name:
            parts.append(f"in function '{self.function_name}'")

        if self.original_exception:
            parts.append(f"caused by {type(self.original_exception).__name__}: {self.original_exception}")

        return " ".join(parts)


def _default_map_err(
    err: Exception,
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> NoReturn:
    """Map an exception to ContractViolationError with enhanced context.

    Parameters
    ----------
    err : Exception
        The original exception that was caught.
    fn : Callable[..., Any]
        The function where the exception occurred.
    args : tuple[Any, ...]
        Positional arguments passed to the function.
    kwargs : dict[str, Any]
        Keyword arguments passed to the function.

    Raises
    ------
    ContractViolationError
        Always raises this exception with enhanced context including
        function name, original exception, and sanitized arguments.

    Examples
    --------
    >>> def test_func(x: int, password: str) -> int:
    ...     return x + len(password)
    >>> try:
    ...     _default_map_err(ValueError("test error"), test_func, (42, "secret"), {})
    ... except ContractViolationError as e:
    ...     print(f"Function: {e.function_name}")
    ...     print(f"Args: {e.context['args']}")
    ...     print(isinstance(e.original_exception, ValueError))
    Function: test_func
    Args: (42, '<REDACTED>')
    True
    """
    # Sanitize sensitive arguments
    sanitized_args = _sanitize_arguments(fn, args, kwargs)
    context = {
        "args": sanitized_args["args"],
        "kwargs": sanitized_args["kwargs"],
    }
    raise ContractViolationError(
        "contract violation",
        function_name=fn.__name__,
        original_exception=err,
        context=context,
    ) from err


def _sanitize_arguments(
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> dict[str, Any]:
    """Sanitize function arguments to remove sensitive information."""
    # List of sensitive parameter names that should be redacted
    sensitive_names = {
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
        "key",
        "api_key",
        "auth",
    }

    # Get function signature to map positional args to parameter names
    try:
        sig = inspect.signature(fn)
        param_names = list(sig.parameters.keys())
    except (ValueError, TypeError):
        # Fallback if signature inspection fails
        param_names = []

    # Sanitize positional arguments
    sanitized_args = []
    for i, arg in enumerate(args):
        if i < len(param_names) and param_names[i].lower() in sensitive_names:
            sanitized_args.append("<REDACTED>")
        else:
            sanitized_args.append(arg)

    # Sanitize keyword arguments
    sanitized_kwargs = {}
    for key, value in kwargs.items():
        if key.lower() in sensitive_names:
            sanitized_kwargs[key] = "<REDACTED>"
        else:
            sanitized_kwargs[key] = value

    return {"args": tuple(sanitized_args), "kwargs": sanitized_kwargs}


def contract[R, **P](
    *,
    map_err: Callable[
        [Exception, Callable[..., Any], tuple[Any, ...], dict[str, Any]],
        NoReturn,
    ] = _default_map_err,
    known_err: tuple[type[Exception], ...] = (),
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Enforce error handling contracts on functions.

    This decorator catches unexpected exceptions and maps them to contract
    violations, while allowing known exceptions to pass through unchanged.

    Parameters
    ----------
    map_err : Callable[[Exception, Callable[..., Any], tuple[Any, ...], dict[str, Any]], NoReturn], optional
        Function to map caught exceptions to contract violations.
        Receives the original exception, the function that failed,
        positional arguments, and keyword arguments.
        By default, raises ContractViolationError with enhanced context.
    known_err : tuple[type[Exception], ...], optional
        Tuple of exception types that should be allowed to pass through
        without being mapped to contract violations.

    Returns
    -------
    Callable[[Callable[P, R]], Callable[P, R]]
        Decorator function that wraps the target function with contract enforcement.

    Examples
    --------
    Basic usage with default error mapping:

    >>> @contract()
    ... def divide(a: int, b: int) -> float:
    ...     return a / b
    >>> try:
    ...     divide(1, 0)
    ... except ContractViolationError:
    ...     print("Contract violation")
    Contract violation

    Allow specific exceptions to pass through:

    >>> @contract(known_err=(ValueError,))
    ... def parse_int(s: str) -> int:
    ...     if not s.isdigit():
    ...         raise ValueError("Not a number")
    ...     return int(s)
    >>> try:
    ...     parse_int("abc")
    ... except ValueError as e:
    ...     print(f"Expected error: {e}")
    Expected error: Not a number

    Custom error mapping:

    >>> def custom_handler(err: Exception, fn: Callable, args: tuple, kwargs: dict) -> NoReturn:
    ...     raise RuntimeError(f"Custom contract violation in {fn.__name__}") from err
    >>> @contract(map_err=custom_handler)
    ... def failing_func() -> None:
    ...     raise ValueError("Something went wrong")
    >>> try:
    ...     failing_func()
    ... except RuntimeError as e:
    ...     print(f"Custom error: {e}")
    Custom error: Custom contract violation in failing_func
    """

    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        @wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            try:
                return fn(*args, **kwargs)
            except known_err:
                raise
            except Exception as e:
                map_err(e, fn, args, kwargs)

        return wrapper

    return decorator


def contract_async[R, **P](
    map_err: Callable[
        [Exception, Callable[..., Awaitable[Any]], tuple[Any, ...], dict[str, Any]],
        NoReturn,
    ] = _default_map_err,
    known_err: tuple[type[Exception], ...] = (),
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Enforce error handling contracts on async functions.

    This decorator is the async version of the contract decorator. It catches
    unexpected exceptions in async functions and maps them to contract
    violations, while allowing known exceptions to pass through unchanged.

    Parameters
    ----------
    map_err : Callable[[Exception, Callable[..., Awaitable[Any]], tuple[Any, ...], dict[str, Any]], NoReturn], optional
        Function to map caught exceptions to contract violations.
        Receives the original exception, the async function that failed,
        positional arguments, and keyword arguments.
        By default, raises ContractViolationError with enhanced context.
    known_err : tuple[type[Exception], ...], optional
        Tuple of exception types that should be allowed to pass through
        without being mapped to contract violations.

    Returns
    -------
    Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]
        Decorator function that wraps the target async function with contract enforcement.

    Examples
    --------
    Basic usage with default error mapping:

    >>> import asyncio
    >>> @contract_async()
    ... async def async_divide(a: int, b: int) -> float:
    ...     return a / b
    >>> async def test_basic():
    ...     try:
    ...         await async_divide(1, 0)
    ...     except ContractViolationError:
    ...         print("Contract violation")
    >>> asyncio.run(test_basic())
    Contract violation

    Allow specific exceptions to pass through:

    >>> @contract_async(known_err=(ValueError,))
    ... async def async_parse_int(s: str) -> int:
    ...     if not s.isdigit():
    ...         raise ValueError("Not a number")
    ...     return int(s)
    >>> async def test_known_err():
    ...     try:
    ...         await async_parse_int("abc")
    ...     except ValueError as e:
    ...         print(f"Expected error: {e}")
    >>> asyncio.run(test_known_err())
    Expected error: Not a number

    Custom error mapping:

    >>> def custom_async_handler(err: Exception, fn: Callable, args: tuple, kwargs: dict) -> NoReturn:
    ...     raise RuntimeError(f"Custom async contract violation in {fn.__name__}") from err
    >>> @contract_async(map_err=custom_async_handler)
    ... async def async_failing_func() -> None:
    ...     raise ValueError("Something went wrong")
    >>> async def test_custom():
    ...     try:
    ...         await async_failing_func()
    ...     except RuntimeError as e:
    ...         print(f"Custom error: {e}")
    >>> asyncio.run(test_custom())
    Custom error: Custom async contract violation in async_failing_func
    """

    def decorator(fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @wraps(fn)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            try:
                return await fn(*args, **kwargs)
            except known_err:
                raise
            except Exception as e:
                map_err(e, fn, args, kwargs)

        return wrapper

    return decorator
