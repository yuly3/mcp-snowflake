import copy
from collections.abc import Callable
from typing import Any, Self, TypeGuard

from cattrs.converters import Converter


class ImmutableConverter[C: Converter, R]:
    """
    A converter wrapper that adds type validation to the conversion process.

    This class wraps a cattrs Converter and adds runtime type validation
    to ensure that unstructured values match the expected type R.

    Type Parameters
    ---------------
    C : Converter
        The type of the wrapped converter
    R : type
        The expected return type of unstructured values

    Parameters
    ----------
    converter : C
        The cattrs converter to wrap
    type_guard : Callable[[Any], TypeGuard[R]]
        A type guard function that validates if a value is of type R

    Examples
    --------
    >>> from cattrs import Converter
    >>> from typing import TypeGuard
    >>> def is_str(value) -> TypeGuard[str]:
    ...     return isinstance(value, str)
    >>> converter = Converter()
    >>> immutable_converter = ImmutableConverter(converter, is_str)
    >>> result = immutable_converter.unstructure("hello")
    >>> result
    'hello'
    """

    def __init__(
        self,
        converter: C,
        type_guard: Callable[[Any], TypeGuard[R]],
    ) -> None:
        self._converter = converter
        self._type_guard = type_guard

    def unstructure(self, value: Any) -> R:
        """
        Convert a structured value to an unstructured representation.

        Parameters
        ----------
        value : Any
            The value to unstructure

        Returns
        -------
        R
            The unstructured value, validated to be of type R

        Raises
        ------
        ValueError
            If the unstructured value doesn't match the expected type R
        """
        unstructured = self._converter.unstructure(value)
        if self._type_guard(unstructured):
            return unstructured
        raise ValueError(f"unstructured value is not of the expected type: {R}")

    def register_unstructure_hook[T](
        self,
        cls: type[T],
        hook: Callable[[T], R],
    ) -> Self:
        """
        Register a custom unstructure hook for a specific type.

        Creates a new instance of ImmutableConverter with the additional hook registered.
        The original converter remains unchanged.

        Parameters
        ----------
        cls : type[T]
            The type to register the hook for
        hook : Callable[[T], R]
            The function to use for unstructuring instances of cls

        Returns
        -------
        Self
            A new ImmutableConverter instance with the hook registered
        """
        newer_converter = self._converter.copy()
        newer_converter.register_unstructure_hook(cls, hook)

        newer = copy.copy(self)
        newer._converter = newer_converter  # noqa: SLF001
        return newer
