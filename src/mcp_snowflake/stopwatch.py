import time
from typing import Self


class StopWatch:
    """A simple stopwatch class to measure execution time."""

    def __init__(self, start_time: int) -> None:
        self._start_time = start_time

    @classmethod
    def start(cls) -> Self:
        """Start the stopwatch."""
        return cls(time.perf_counter_ns())

    def elapsed_ns(self) -> int:
        """Get the elapsed time in nanoseconds."""
        return time.perf_counter_ns() - self._start_time

    def elapsed_us(self) -> float:
        """Get the elapsed time in microseconds."""
        return self.elapsed_ns() / 1_000

    def elapsed_ms(self) -> float:
        """Get the elapsed time in milliseconds."""
        return self.elapsed_ns() / 1_000_000
