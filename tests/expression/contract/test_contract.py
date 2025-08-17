from collections.abc import Callable
from typing import Any, NoReturn

import pytest

from expression.contract import ContractViolationError, contract


def custom_map_err(
    err: Exception,
    fn: Callable[..., Any],
    args: tuple[Any, ...],  # noqa: ARG001
    kwargs: dict[str, Any],  # noqa: ARG001
) -> NoReturn:
    raise ContractViolationError(f"custom contract violation in {fn.__name__}") from err


class TestContract:
    def test_map_err_basic(self) -> None:
        """Test basic contract decorator functionality with error mapping.

        Tests three main scenarios:
        1. Default error mapping (any exception -> ContractViolationError)
        2. Known errors passing through unchanged
        3. Custom error mapping function
        """

        @contract()
        def raise_value_error(
            v: int,  # noqa: ARG001
        ) -> int:
            raise ValueError("This is a value error")

        with pytest.raises(ContractViolationError):
            _ = raise_value_error(42)

        @contract(known_err=(ValueError,))
        def raise_value_error_known(
            v: int,  # noqa: ARG001
        ) -> int:
            raise ValueError("This is a known value error")

        with pytest.raises(ValueError, match="This is a known value error"):
            _ = raise_value_error_known(42)

        @contract(map_err=custom_map_err)
        def raise_value_error_custom(
            v: int,  # noqa: ARG001
        ) -> int:
            raise ValueError("This is a custom value error")

        with pytest.raises(
            ContractViolationError,
            match="custom contract violation in raise_value_error_custom",
        ):
            _ = raise_value_error_custom(42)

    def test_success_cases(self) -> None:
        """Test that decorated functions work normally when no exceptions occur."""

        @contract()
        def add(a: int, b: int) -> int:
            return a + b

        result = add(2, 3)
        assert result == 5

        @contract(known_err=(ValueError,))
        def multiply(a: int, b: int) -> int:
            return a * b

        result = multiply(4, 5)
        assert result == 20

    def test_multiple_known_errors(self) -> None:
        """Test handling multiple exception types in known_err."""

        @contract(known_err=(ValueError, TypeError, KeyError))
        def multi_error_func(error_type: str) -> int:
            if error_type == "value":
                raise ValueError("Value error")
            if error_type == "type":
                raise TypeError("Type error")
            if error_type == "key":
                raise KeyError("Key error")
            if error_type == "runtime":
                raise RuntimeError("Runtime error")
            return 42

        # Known errors should pass through
        with pytest.raises(ValueError, match="Value error"):
            _ = multi_error_func("value")

        with pytest.raises(TypeError, match="Type error"):
            _ = multi_error_func("type")

        with pytest.raises(KeyError, match="Key error"):
            _ = multi_error_func("key")

        # Unknown errors should be mapped to ContractViolationError
        with pytest.raises(ContractViolationError):
            _ = multi_error_func("runtime")

    def test_exception_chaining(self) -> None:
        """Test that original exceptions are properly chained as __cause__."""

        @contract()
        def raise_original_error() -> None:
            raise ValueError("Original error message")

        with pytest.raises(ContractViolationError) as exc_info:
            raise_original_error()

        # Check that the original exception is chained
        assert exc_info.value.__cause__ is not None
        assert isinstance(exc_info.value.__cause__, ValueError)
        assert str(exc_info.value.__cause__) == "Original error message"

    def test_complex_arguments_and_return_types(self) -> None:
        """Test that decorators work with complex argument and return types."""

        @contract()
        def process_data(
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
        result = process_data(
            {"a": 1, "b": 2}, 3, "arg1", "arg2", flag1=True, flag2=False
        )
        expected = {"a": 3, "b": 6, "args_count": 2, "kwargs_count": 2}
        assert result == expected

        # Exception handling
        with pytest.raises(ContractViolationError):
            _ = process_data({})

    def test_empty_known_err_tuple(self) -> None:
        """Test that empty known_err tuple behaves like default (no known errors)."""

        @contract(known_err=())
        def always_fails() -> None:
            raise ValueError("This should be mapped")

        with pytest.raises(ContractViolationError):
            always_fails()


class TestContractViolationErrorEnhanced:
    """Test enhanced ContractViolationError with detailed context."""

    def test_contract_violation_error_with_function_context(self) -> None:
        """Test ContractViolationError includes function name and original exception."""

        @contract()
        def failing_divide(a: int, b: int) -> float:
            return a / b

        with pytest.raises(ContractViolationError) as exc_info:
            _ = failing_divide(1, 0)

        error = exc_info.value

        # Should include function name
        assert error.function_name == "failing_divide"

        # Should include original exception
        assert error.original_exception is not None
        assert isinstance(error.original_exception, ZeroDivisionError)

        # Should maintain exception chaining
        assert error.__cause__ is not None
        assert isinstance(error.__cause__, ZeroDivisionError)

    def test_contract_violation_error_with_arguments_context(self) -> None:
        """Test ContractViolationError includes function arguments in context."""

        @contract()
        def failing_process(data: dict[str, int], multiplier: int) -> dict[str, int]:
            if not data:
                raise ValueError("Empty data not allowed")
            return {k: v * multiplier for k, v in data.items()}

        with pytest.raises(ContractViolationError) as exc_info:
            _ = failing_process({}, 5)

        error = exc_info.value

        # Should include function context
        assert error.function_name == "failing_process"
        assert error.original_exception is not None
        assert isinstance(error.original_exception, ValueError)

        # Should include arguments context
        assert "args" in error.context
        assert "kwargs" in error.context

        # Verify argument values (should be sanitized/safe to log)
        args_context = error.context["args"]
        kwargs_context = error.context["kwargs"]

        assert len(args_context) == 2  # data and multiplier
        assert args_context[0] == {}  # empty dict
        assert args_context[1] == 5  # multiplier
        assert kwargs_context == {}  # no keyword args

    def test_contract_violation_error_context_sanitization(self) -> None:
        """Test that sensitive information in arguments is sanitized."""

        @contract()
        def failing_with_sensitive_data(
            password: str,
            api_key: str,  # noqa: ARG001
            data: dict[str, str],
        ) -> str:
            if len(password) < 8:
                raise ValueError("Password too short")
            return f"Processing {data}"

        with pytest.raises(ContractViolationError) as exc_info:
            _ = failing_with_sensitive_data("123", "secret-api-key", {"user": "john"})

        error = exc_info.value

        # Should include function context
        assert error.function_name == "failing_with_sensitive_data"

        # Should sanitize sensitive arguments
        args_context = error.context["args"]
        assert args_context[0] == "<REDACTED>"  # password should be redacted
        assert args_context[1] == "<REDACTED>"  # api_key should be redacted
        assert args_context[2] == {"user": "john"}  # regular data should be preserved

    def test_contract_violation_error_str_representation(self) -> None:
        """Test enhanced string representation of ContractViolationError."""

        @contract()
        def failing_func(x: int) -> int:  # noqa: ARG001
            raise ValueError("Test error")

        with pytest.raises(ContractViolationError) as exc_info:
            _ = failing_func(42)

        error = exc_info.value
        error_str = str(error)

        # Should include function name in string representation
        assert "failing_func" in error_str

        # Should include original exception type
        assert "ValueError" in error_str


class TestMapErrEnhanced:
    """Test enhanced map_err signature with access to function context."""

    def test_custom_map_err_with_function_context(self) -> None:
        """Test that custom map_err receives function name and arguments."""
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
            raise ContractViolationError("Custom handler called") from err

        @contract(map_err=context_capturing_map_err)
        def test_func(x: int, y: str, *, flag: bool = True) -> str:
            if x < 0:
                raise ValueError("Negative value")
            return f"{x}-{y}-{flag}"

        with pytest.raises(ContractViolationError, match="Custom handler called"):
            _ = test_func(-1, "test", flag=False)

        # Verify that context was captured correctly
        assert captured_context["error_type"] == "ValueError"
        assert captured_context["function_name"] == "test_func"
        assert captured_context["args_count"] == 2  # x, y
        assert captured_context["kwargs_count"] == 1  # flag
        assert captured_context["first_arg"] == -1

    def test_custom_map_err_with_argument_inspection(self) -> None:
        """Test that custom map_err can inspect and use argument values."""

        def argument_aware_map_err(
            err: Exception,
            fn: Callable[..., Any],
            args: tuple[Any, ...],
            kwargs: dict[str, Any],
        ) -> NoReturn:
            # Create a detailed error message using the arguments
            args_info = f"args={args}" if args else "no args"
            kwargs_info = f"kwargs={kwargs}" if kwargs else "no kwargs"
            detailed_message = f"Error in {fn.__name__}: {args_info}, {kwargs_info}"
            raise ContractViolationError(detailed_message) from err

        @contract(map_err=argument_aware_map_err)
        def divide_with_context(a: int, b: int, precision: int = 2) -> float:
            if b == 0:
                raise ZeroDivisionError("Division by zero")
            result = a / b
            return round(result, precision)

        expected_pattern = (
            r"Error in divide_with_context: args=\(10, 0\), kwargs={'precision': 3}"
        )
        with pytest.raises(ContractViolationError, match=expected_pattern):
            _ = divide_with_context(10, 0, precision=3)
