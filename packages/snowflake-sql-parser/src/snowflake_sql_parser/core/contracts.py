"""Package-local contract decorators for parser boundaries."""

from expression.contract import ContractViolationError, contract

from .errors import SQLAnalysisError

analysis_contract = contract(
    known_err=(SQLAnalysisError, ContractViolationError),
)
internal_contract = contract(known_err=(ContractViolationError,))
