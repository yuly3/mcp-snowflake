import pytest

from expression.contract import ContractViolationError


class TestContractViolationError:
    """Test the ContractViolationError exception class itself."""

    def test_contract_violation_error_inheritance(self) -> None:
        """Test that ContractViolationError is a proper Exception subclass."""
        error = ContractViolationError("test message")
        assert isinstance(error, Exception)
        assert str(error) == "test message"

    def test_contract_violation_error_with_cause(self) -> None:
        """Test that ContractViolationError can be raised with a cause."""
        original = ValueError("original error")
        with pytest.raises(ContractViolationError) as exc_info:
            raise ContractViolationError("contract violation") from original

        assert exc_info.value.__cause__ is original
        assert isinstance(exc_info.value.__cause__, ValueError)
