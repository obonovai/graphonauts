"""High-precision execution time measurement for graph database query benchmarking.

This module provides a lightweight wall-clock timer built on ``time.perf_counter()``.
The choice of ``perf_counter`` is deliberate and motivated by three properties that
make it the most suitable clock source for benchmarking:

1. **Monotonicity.** Unlike ``time.time()``, ``perf_counter`` is guaranteed to never
   go backwards, even if the system clock is adjusted (e.g., by NTP synchronisation)
   during a benchmark run. This prevents negative or artificially inflated elapsed
   time readings.

2. **Highest available resolution.** On modern operating systems, ``perf_counter``
   provides sub-microsecond resolution (typically backed by the TSC or HPET hardware
   timer on x86 platforms, and ``mach_absolute_time`` on macOS). This resolution far
   exceeds what is needed for database query benchmarks (which typically take
   milliseconds to seconds) but ensures that no precision is lost for very fast
   queries.

3. **Inclusion of sleep time.** Unlike ``time.process_time()``, ``perf_counter``
   includes time spent sleeping or waiting for I/O. Since graph database queries
   involve network round-trips and disk I/O, wall-clock time (rather than CPU time)
   is the appropriate metric for end-to-end query latency measurement.

Usage::

    with TimeMonitor() as tm:
        await client.afetch(query)
    print(f"Elapsed: {tm.elapsed_time_s:.4f}s")
"""

import time


class TimeMonitor:
    """Context-manager-based wall-clock timer for measuring query execution latency.

    Uses ``time.perf_counter()`` to record start and end timestamps with the highest
    resolution available on the platform (sub-microsecond on modern hardware). The
    measured interval is monotonic and unaffected by system clock adjustments.

    The class supports three usage patterns:

    - **Context manager** (recommended): ``with TimeMonitor() as tm: ...`` automatically
      calls ``start()`` on entry and ``stop()`` on exit.
    - **Explicit start/stop**: call ``start()`` and ``stop()`` manually for more
      granular control.
    - **Live elapsed time**: access ``elapsed_time_s`` before calling ``stop()`` to
      obtain the running elapsed time relative to the start.

    Attributes:
        _start_time: The ``perf_counter`` reading at the moment ``start()`` was called,
            or ``None`` if the timer has not been started.
        _end_time: The ``perf_counter`` reading at the moment ``stop()`` was called,
            or ``None`` if the timer is still running or has not been started.
    """

    def __init__(self) -> None:
        self._start_time: float | None = None
        self._end_time: float | None = None

    @property
    def start_time_s(self) -> float:
        return self._start_time if self._start_time is not None else 0.0

    @property
    def elapsed_time_s(self) -> float:
        return self._get_elapsed_time()

    def start(self) -> None:
        """Record the start timestamp and clear any previous end timestamp.

        Captures the current ``perf_counter`` value and resets ``_end_time`` to
        ``None``, enabling the ``elapsed_time_s`` property to return the live
        running time until ``stop()`` is called.
        """
        self._start_time = time.perf_counter()
        self._end_time = None

    def stop(self) -> None:
        """Record the end timestamp, freezing the elapsed time measurement.

        After this call, ``elapsed_time_s`` returns the fixed interval between
        ``start()`` and ``stop()`` rather than the live running time.
        """
        self._end_time = time.perf_counter()

    def reset(self) -> None:
        """Clear both start and end timestamps, returning the timer to its initial state.

        After a reset, ``elapsed_time_s`` returns ``0.0`` until ``start()`` is called
        again.
        """
        self._start_time = None
        self._end_time = None

    def __enter__(self) -> "TimeMonitor":
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:  # type: ignore[no-untyped-def]
        self.stop()

    def _get_elapsed_time(self) -> float:
        if self._start_time is None:
            return 0.0
        elif self._end_time is None:
            return time.perf_counter() - self._start_time
        else:
            return self._end_time - self._start_time
