"""Docker container memory monitoring for graph database benchmark measurements.

This module provides real-time memory consumption tracking of a Docker container
during benchmark query execution. It is a pure measurement component --- it samples
memory and tracks baseline/peak values, but does not manage container lifecycle
(restart, health checking). Container lifecycle is handled by the separate
``ContainerManager`` class.

**Measurement approach.** The Docker Engine exposes a streaming statistics API
(``/containers/{id}/stats``) that reports cgroup-level resource usage at
approximately one-second intervals. This module consumes that stream in a
dedicated background daemon thread, recording timestamped memory samples while
the main thread executes the benchmark workload. This design ensures that
sampling overhead does not interfere with query execution timing.

**Cache-adjusted memory calculation.** Raw memory usage reported by Docker
includes the Linux page cache (filesystem cache), which can be substantial for
I/O-heavy graph databases. To obtain a measurement that reflects actual process
memory (heap, stack, memory-mapped data structures), the cache component is
subtracted. The implementation handles both cgroups v1 (which reports a ``cache``
field) and cgroups v2 (which reports ``inactive_file``) transparently.

**Thread safety.** Shared mutable state (``baseline_memory``, ``peak_memory``,
``data_points``) is protected by a ``threading.Lock`` to ensure correct
concurrent access between the monitoring thread and the main thread.

Usage::

    with MemoryMonitor(container_name="neo4j-graphonaut") as mm:
        await client.afetch(query)
    print(f"Peak: {mm.peak_mb:.2f}MB, Delta: {mm.memory_usage_mb:.2f}MB")
    mm.plot(save_path="memory.png")
"""

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any

import docker  # type: ignore
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)

# Maximum time (seconds) to wait for the monitor thread to finish after stop().
MONITOR_JOIN_TIMEOUT_S = 1.0


@dataclass
class MemoryDataPoint:
    """A single memory sample with timestamp relative to monitoring start."""

    timeline: float
    memory: int

    @property
    def memory_mb(self) -> float:
        return self.memory / (1024 * 1024)


def _extract_cache_adjusted_memory(stats: dict[str, Any]) -> int:
    """Extract cache-adjusted memory usage from Docker stats.

    Docker reports total memory usage including filesystem cache. For accurate
    process memory measurement, we subtract the cache component. Handles both
    cgroups v1 (reports 'cache') and v2 (reports 'inactive_file') layouts.

    Args:
        stats: The memory_stats dict from Docker container stats.

    Returns:
        Memory usage in bytes with filesystem cache subtracted.
    """
    usage = int(stats["usage"])
    cache = int(stats.get("stats", {}).get("inactive_file", 0)) or int(stats.get("stats", {}).get("cache", 0))
    return usage - cache


class MemoryMonitor:
    """Real-time Docker container memory monitor.

    This class implements memory measurement: baseline sampling, continuous
    background monitoring via the Docker stats streaming API, peak tracking,
    and post-hoc timeline visualisation. It is a pure measurement tool ---
    container lifecycle operations (restart, health checking) are handled
    externally by ``ContainerManager``.

    The class is designed to be used as a context manager. On ``__enter__``,
    a baseline memory sample is taken and the background monitoring thread is
    started. On ``__exit__``, the monitoring thread is stopped, a final memory
    sample is recorded, and peak memory is updated if necessary.

    Attributes:
        container_name: The Docker container name to monitor.
        baseline_memory: The memory reading (in bytes) taken at monitoring start,
            before the query executes.
        peak_memory: The highest memory reading (in bytes) observed during the
            entire monitoring window.
        data_points: Chronologically ordered list of ``MemoryDataPoint`` samples.
    """

    def __init__(self, container_name: str):
        self.container_name = container_name
        self.client = docker.from_env()

        self.baseline_memory: int | None = None
        self.peak_memory: int | None = None

        self.data_points: list[MemoryDataPoint] = []
        self.start_time: float | None = None

        self._monitoring = False
        self._monitor_thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def __enter__(self) -> "MemoryMonitor":
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:  # type: ignore[no-untyped-def]
        self.stop()

    def __repr__(self) -> str:
        return (
            f"MemoryMonitor(container={self.container_name}, "
            f"baseline={self.baseline_mb:.2f}MB, "
            f"peak={self.peak_mb:.2f}MB, "
            f"delta={self.memory_usage_mb:.2f}MB)"
        )

    @property
    def baseline_mb(self) -> float:
        with self._lock:
            if self.baseline_memory is None:
                return 0.0
            return self.baseline_memory / (1024 * 1024)

    @property
    def peak_mb(self) -> float:
        with self._lock:
            if self.peak_memory is None:
                return 0.0
            return self.peak_memory / (1024 * 1024)

    @property
    def memory_usage_mb(self) -> float:
        with self._lock:
            if self.baseline_memory is None or self.peak_memory is None:
                return 0
            return (self.peak_memory - self.baseline_memory) / (1024 * 1024)

    def start(self) -> None:
        """Begin memory monitoring.

        Records a baseline memory sample and spawns a daemon thread to
        continuously stream memory samples from the Docker stats API.

        This method is called automatically when the instance is used as a
        context manager.
        """
        self.start_time = time.time()
        self.baseline_memory = self._get_container_memory()
        self.peak_memory = self.baseline_memory
        self.data_points = [MemoryDataPoint(timeline=0.0, memory=self.baseline_memory)]

        self._monitoring = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()

    def stop(self) -> None:
        """Stop memory monitoring and record a final memory sample.

        Signals the background monitoring thread to terminate, waits for it to join
        (with a bounded timeout to avoid indefinite blocking), and takes one final
        memory reading. If this final reading exceeds the previously recorded peak,
        the peak is updated accordingly. This ensures that memory allocated during
        the very last moments of query execution is not missed due to sampling
        granularity.

        This method is called automatically when exiting the context manager.
        """
        self._monitoring = False
        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=MONITOR_JOIN_TIMEOUT_S)

        final_memory = self._get_container_memory()
        with self._lock:
            if self.peak_memory is None or final_memory > self.peak_memory:
                self.peak_memory = final_memory

            if self.start_time is not None:
                elapsed = time.time() - self.start_time
                self.data_points.append(MemoryDataPoint(timeline=elapsed, memory=final_memory))

    def plot(self, show: bool = False, save_path: str | None = None) -> None:
        """Render the memory timeline as a matplotlib line chart.

        Produces a time-series plot of memory usage (in megabytes) over elapsed
        seconds since monitoring began. The baseline is shown as a green dashed
        horizontal line, and the peak is marked with a red scatter point. The
        plot can be saved to a file, displayed interactively, or both.

        Args:
            show: If ``True``, display the plot in an interactive matplotlib window.
                Defaults to ``False`` (headless operation for automated benchmarks).
            save_path: If provided, the plot is saved to this filesystem path as a
                PNG image at 150 DPI. Parent directories must already exist.
        """
        if not self.data_points:
            print("No data to plot")
            return

        timestamps = [dp.timeline for dp in self.data_points]
        memory_mb = [dp.memory_mb for dp in self.data_points]

        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, memory_mb, linewidth=2, color="#2E86AB")

        if self.baseline_memory is not None:
            plt.axhline(
                y=self.baseline_mb,
                color="green",
                linestyle="--",
                linewidth=1,
                label=f"Baseline: {self.baseline_mb:.2f} MB",
            )

        if self.peak_memory is not None:
            peak_idx = memory_mb.index(max(memory_mb))
            plt.scatter(
                [timestamps[peak_idx]],
                [self.peak_mb],
                color="red",
                s=100,
                zorder=5,
                label=f"Peak: {self.peak_mb:.2f} MB",
            )

        plt.xlabel("Time (seconds)", fontsize=12)
        plt.ylabel("Memory Usage (MB)", fontsize=12)
        plt.title(f"Memory Usage - {self.container_name}", fontsize=14, fontweight="bold")
        plt.grid(True, alpha=0.3)
        plt.legend(loc="best")
        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches="tight")

        if show:
            plt.show()

        plt.close()

    def _get_container_memory(self) -> int:
        container = self.client.containers.get(self.container_name)
        stats = container.stats(stream=False)
        return _extract_cache_adjusted_memory(stats["memory_stats"])

    def _monitor_loop(self) -> None:
        try:
            container = self.client.containers.get(self.container_name)
            for stat in container.stats(stream=True, decode=True):
                if not self._monitoring:
                    break
                try:
                    current = _extract_cache_adjusted_memory(stat["memory_stats"])

                    with self._lock:
                        if self.start_time is not None:
                            elapsed = time.time() - self.start_time
                            self.data_points.append(MemoryDataPoint(timeline=elapsed, memory=current))

                        if self.peak_memory is None or current > self.peak_memory:
                            self.peak_memory = current
                except (KeyError, TypeError):
                    continue
        except Exception:
            logger.debug("Monitor thread stopped", exc_info=True)
