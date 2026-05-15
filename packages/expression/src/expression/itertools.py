from collections.abc import Callable, Iterable, Iterator
from functools import reduce
from heapq import nlargest
from itertools import chain, islice, takewhile
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from _typeshed import SupportsRichComparison


class Iter[T](Iterable[T]):
    """
    Wrapper class for Iterable to provide method chaining style utilities.
    """

    def __init__(self, iterable: Iterable[T]) -> None:
        self._iterable = iterable

    def __iter__(self) -> Iterator[T]:
        return iter(self._iterable)

    def map[R](self, func: Callable[[T], R]) -> "Iter[R]":
        """
        Apply func to every item of iterable and return a new Iter.

        >>> Iter([1, 2, 3]).map(lambda x: x * 2).to_list()
        [2, 4, 6]
        """
        return Iter(map(func, self._iterable))

    def filter(self, func: Callable[[T], bool] | None = None) -> "Iter[T]":
        """
        Filter items of iterable and return a new Iter.

        >>> Iter([1, 2, 3, 4]).filter(lambda x: x % 2 == 0).to_list()
        [2, 4]
        """
        return Iter(filter(func, self._iterable))

    def first(self) -> T | None:
        """
        Return the first item or None if empty.

        >>> Iter([1, 2, 3]).first()
        1
        >>> Iter([]).first() is None
        True
        """
        return next(iter(self._iterable), None)

    def reduce[R](self, func: Callable[[R, T], R], initial: R) -> R:
        """
        Apply a function of two arguments cumulatively to the items of iterable.

        >>> Iter([1, 2, 3, 4]).reduce(lambda x, y: x + y, 0)
        10
        """
        return reduce(func, self._iterable, initial)

    def to_list(self) -> list[T]:
        """
        Convert to list.

        >>> Iter([1, 2, 3]).to_list()
        [1, 2, 3]
        """
        return list(self._iterable)

    def to_set(self) -> set[T]:
        """
        Convert to set.

        >>> Iter([1, 2, 2, 3]).to_set()
        {1, 2, 3}
        """
        return set(self._iterable)

    def chain(self, *iterables: Iterable[T]) -> "Iter[T]":
        """
        Chain with other iterables.

        >>> Iter([1, 2]).chain([3, 4]).to_list()
        [1, 2, 3, 4]
        """
        return Iter(chain(self._iterable, *iterables))

    def take(self, n: int) -> "Iter[T]":
        """
        Return first n items.

        >>> Iter([1, 2, 3, 4]).take(2).to_list()
        [1, 2]
        """
        return Iter(islice(self._iterable, n))

    def skip(self, n: int) -> "Iter[T]":
        """
        Skip first n items.

        >>> Iter([1, 2, 3, 4]).skip(2).to_list()
        [3, 4]
        """
        return Iter(islice(self._iterable, n, None))

    def takewhile(self, predicate: Callable[[T], bool]) -> "Iter[T]":
        """
        Return items from the iterable as long as the predicate is true.

        >>> Iter([1, 4, 6, 4, 1]).takewhile(lambda x: x < 5).to_list()
        [1, 4]
        """
        return Iter(takewhile(predicate, self._iterable))

    def sort[T2: "SupportsRichComparison"](self: "Iter[T2]", *, reverse: bool = False) -> "Iter[T2]":
        """
        Return a new sorted Iter.

        >>> Iter([3, 1, 2]).sort().to_list()
        [1, 2, 3]
        >>> Iter([3, 1, 2]).sort(reverse=True).to_list()
        [3, 2, 1]
        """
        return Iter(sorted(self._iterable, reverse=reverse))

    def sort_by(self, *, key: "Callable[[T], SupportsRichComparison]", reverse: bool = False) -> "Iter[T]":
        """
        Return a new sorted Iter using a key function.

        >>> Iter(["a", "ccc", "bb"]).sort_by(key=len).to_list()
        ['a', 'bb', 'ccc']
        """
        return Iter(sorted(self._iterable, key=key, reverse=reverse))

    def top_k(self, k: int, *, key: "Callable[[T], SupportsRichComparison] | None" = None) -> "Iter[T]":
        """
        Return the k largest items, sorted in descending order.

        >>> Iter([1, 5, 2, 4, 3]).top_k(3).to_list()
        [5, 4, 3]
        """
        return Iter(nlargest(k, self._iterable, key=key))

    def enumerate(self, start: int = 0) -> "Iter[tuple[int, T]]":
        """
        Return enumerate object.

        >>> Iter(['a', 'b']).enumerate().to_list()
        [(0, 'a'), (1, 'b')]
        """
        return Iter(enumerate(self._iterable, start))
