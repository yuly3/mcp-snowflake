from collections.abc import Callable
from typing import Any, NoReturn

import pytest

from expression.contract import ContractViolationError, contract_async


def custom_map_err(
    err: Exception,
    fn: Callable[..., Any],
    args: tuple[Any, ...],  # noqa: ARG001
    kwargs: dict[str, Any],  # noqa: ARG001
) -> NoReturn:
    raise ContractViolationError(f"custom contract violation in {fn.__name__}") from err


class TestContractAsync:
    @pytest.mark.asyncio
    async def test_map_err_basic_async(self) -> None:
        """Test basic async contract decorator functionality with error mapping.

        Tests three main scenarios for async functions:
        1. Default error mapping (any exception -> ContractViolationError)
        2. Known errors passing through unchanged
        3. Custom error mapping function
        """

        @contract_async()
        async def raise_value_error(
            v: int,  # noqa: ARG001
        ) -> int:
            raise ValueError("This is an async value error")

        with pytest.raises(ContractViolationError):
            _ = await raise_value_error(42)

        @contract_async(known_err=(ValueError,))
        async def raise_value_error_known(
            v: int,  # noqa: ARG001
        ) -> int:
            raise ValueError("This is a known async value error")

        with pytest.raises(ValueError, match="This is a known async value error"):
            _ = await raise_value_error_known(42)

        @contract_async(map_err=custom_map_err)
        async def raise_value_error_custom(
            v: int,  # noqa: ARG001
        ) -> int:
            raise ValueError("This is a custom async value error")

        with pytest.raises(
            ContractViolationError,
            match="custom contract violation in raise_value_error_custom",
        ):
            _ = await raise_value_error_custom(42)

    @pytest.mark.asyncio
    async def test_success_cases_async(self) -> None:
        """Test that decorated async functions work normally when no exceptions occur."""

        @contract_async()
        async def add_async(a: int, b: int) -> int:
            return a + b

        result = await add_async(2, 3)
        assert result == 5

        @contract_async(known_err=(ValueError,))
        async def multiply_async(a: int, b: int) -> int:
            return a * b

        result = await multiply_async(4, 5)
        assert result == 20

    @pytest.mark.asyncio
    async def test_multiple_known_errors_async(self) -> None:
        """Test handling multiple exception types in known_err for async functions."""

        @contract_async(known_err=(ValueError, TypeError, KeyError))
        async def multi_error_func_async(error_type: str) -> int:
            if error_type == "value":
                raise ValueError("Async value error")
            if error_type == "type":
                raise TypeError("Async type error")
            if error_type == "key":
                raise KeyError("Async key error")
            if error_type == "runtime":
                raise RuntimeError("Async runtime error")
            return 42

        # Known errors should pass through
        with pytest.raises(ValueError, match="Async value error"):
            _ = await multi_error_func_async("value")

        with pytest.raises(TypeError, match="Async type error"):
            _ = await multi_error_func_async("type")

        with pytest.raises(KeyError, match="Async key error"):
            _ = await multi_error_func_async("key")

        # Unknown errors should be mapped to ContractViolationError
        with pytest.raises(ContractViolationError):
            _ = await multi_error_func_async("runtime")

    @pytest.mark.asyncio
    async def test_exception_chaining_async(self) -> None:
        """Test that original exceptions are properly chained as __cause__ for async functions."""

        @contract_async()
        async def raise_original_error_async() -> None:
            raise ValueError("Original async error message")

        with pytest.raises(ContractViolationError) as exc_info:
            await raise_original_error_async()

        # Check that the original exception is chained
        assert exc_info.value.__cause__ is not None
        assert isinstance(exc_info.value.__cause__, ValueError)
        assert str(exc_info.value.__cause__) == "Original async error message"

    @pytest.mark.asyncio
    async def test_complex_arguments_and_return_types_async(self) -> None:
        """Test that async decorators work with complex argument and return types."""

        @contract_async()
        async def process_data_async(
            data: dict[str, int],
            multiplier: int = 2,
            *args: str,
            **kwargs: bool,
        ) -> dict[str, int]:
            if not data:
                raise ValueError("Empty data")
            result = {k: v * multiplier for k, v in data.items()}
            if args:
                result["args_count"] = len(args)
            if kwargs:
                result["kwargs_count"] = len(kwargs)
            return result

        # Normal operation
        result = await process_data_async(
            {"a": 1, "b": 2},
            3,
            "arg1",
            "arg2",
            flag1=True,
            flag2=False,
        )
        expected = {"a": 3, "b": 6, "args_count": 2, "kwargs_count": 2}
        assert result == expected

        # Exception handling
        with pytest.raises(ContractViolationError):
            _ = await process_data_async({})

    @pytest.mark.asyncio
    async def test_empty_known_err_tuple_async(self) -> None:
        """Test that empty known_err tuple behaves like default for async functions."""

        @contract_async(known_err=())
        async def always_fails_async() -> None:
            raise ValueError("This should be mapped")

        with pytest.raises(ContractViolationError):
            await always_fails_async()


class TestMapErrEnhancedAsync:
    """Test enhanced map_err signature with async functions."""

    @pytest.mark.asyncio
    async def test_custom_map_err_with_function_context_async(self) -> None:
        """Test that custom map_err receives function name and arguments for async functions."""
        captured_context = {}

        def context_capturing_map_err(
            err: Exception,
            fn: Callable[..., Any],
            args: tuple[Any, ...],
            kwargs: dict[str, Any],
        ) -> NoReturn:
            captured_context["error_type"] = type(err).__name__
            captured_context["function_name"] = fn.__name__
            captured_context["args_count"] = len(args)
            captured_context["kwargs_count"] = len(kwargs)
            captured_context["first_arg"] = args[0] if args else None
            raise ContractViolationError("Custom async handler called") from err

        @contract_async(map_err=context_capturing_map_err)
        async def test_async_func(x: int, y: str, *, flag: bool = True) -> str:
            if x < 0:
                raise ValueError("Negative value")
            return f"{x}-{y}-{flag}"

        with pytest.raises(ContractViolationError, match="Custom async handler called"):
            _ = await test_async_func(-1, "test", flag=False)

        # Verify that context was captured correctly
        assert captured_context["error_type"] == "ValueError"
        assert captured_context["function_name"] == "test_async_func"
        assert captured_context["args_count"] == 2  # x, y
        assert captured_context["kwargs_count"] == 1  # flag
        assert captured_context["first_arg"] == -1

    @pytest.mark.asyncio
    async def test_custom_map_err_with_argument_inspection_async(self) -> None:
        """Test that custom map_err can inspect and use argument values for async functions."""

        def argument_aware_map_err(
            err: Exception,
            fn: Callable[..., Any],
            args: tuple[Any, ...],
            kwargs: dict[str, Any],
        ) -> NoReturn:
            # Create a detailed error message using the arguments
            args_info = f"args={args}" if args else "no args"
            kwargs_info = f"kwargs={kwargs}" if kwargs else "no kwargs"
            detailed_message = f"Async error in {fn.__name__}: {args_info}, {kwargs_info}"
            raise ContractViolationError(detailed_message) from err

        @contract_async(map_err=argument_aware_map_err)
        async def divide_async_with_context(
            a: int,
            b: int,
            precision: int = 2,
        ) -> float:
            if b == 0:
                raise ZeroDivisionError("Division by zero")
            result = a / b
            return round(result, precision)

        expected_pattern = r"Async error in divide_async_with_context: args=\(10, 0\), kwargs={'precision': 3}"
        with pytest.raises(ContractViolationError, match=expected_pattern):
            _ = await divide_async_with_context(10, 0, precision=3)

    @pytest.mark.asyncio
    async def test_async_function_signature_preservation(self) -> None:
        """Test that contract_async preserves the async nature of functions."""
        import inspect

        def logging_map_err(
            err: Exception,
            fn: Callable[..., Any],
            args: tuple[Any, ...],  # noqa: ARG001
            kwargs: dict[str, Any],  # noqa: ARG001
        ) -> NoReturn:
            # Verify that the function is indeed async
            assert inspect.iscoroutinefunction(fn), f"{fn.__name__} should be a coroutine function"
            raise ContractViolationError(f"Logged error in async {fn.__name__}") from err

        @contract_async(map_err=logging_map_err)
        async def async_function_with_error() -> int:
            raise RuntimeError("Test async error")

        # Verify the decorated function is still async
        assert inspect.iscoroutinefunction(async_function_with_error)

        with pytest.raises(
            ContractViolationError,
            match="Logged error in async async_function_with_error",
        ):
            _ = await async_function_with_error()
