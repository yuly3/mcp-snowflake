"""Package-local contract decorators for parser boundaries."""

from expression.contract import contract

from .errors import SQLAnalysisError

analysis_contract = contract(
    known_err=(SQLAnalysisError,),
)
