"""Result dataclasses for the three benchmark phases: load, memory, and time.

This module defines the data structures that capture, persist, and aggregate
benchmark measurements. The framework produces three distinct types of results,
each corresponding to a phase in the benchmark lifecycle:

1. **Load results** (``LoadResult``): Capture the wall-clock duration and memory
   footprint of the initial TPC-H data ingestion into the database.

2. **Memory results** (``MemoryResult``): Record the memory consumption of
   individual query executions, measured after a container restart to isolate
   the query's memory impact from prior state.

3. **Time results** (``TimeResult``, ``TimeSummary``): Record individual query
   execution times across multiple runs and compute descriptive statistics.

All result types implement a uniform serialisation interface: ``save()`` writes
the result as a JSON file to a structured directory hierarchy, and ``load()``
reconstructs the object from a JSON file. The directory hierarchy follows the
convention::

    benchmarks/
        load/{database}.json
        memory/{database}/{category}/{variant}/memory.json
        time/{database}/{category}/{variant}/run_{NNN}.json
        time/{database}/{category}/{variant}/summary.json

This hierarchical layout enables both programmatic aggregation (e.g., by the
``compare`` module) and manual inspection of individual results.
"""

import json
import statistics
from dataclasses import asdict, dataclass, field
from pathlib import Path


@dataclass
class LoadResult:
    """Result of loading TPC-H data into a graph database.

    Captures both temporal and spatial metrics for the data ingestion phase.
    The memory measurements are obtained via the Docker stats API, sampling
    container memory usage at regular intervals throughout the load process.

    Attributes:
        database: Identifier of the database under test (e.g., ``"neo4j"``).
        timestamp: Unix epoch timestamp when the load measurement was taken,
            enabling chronological ordering of repeated experiments.
        total_time_s: Wall-clock duration of the complete data load, in seconds.
        memory_baseline_mb: Container memory usage in megabytes before the load
            process begins, representing the database engine's idle footprint.
        memory_peak_mb: Maximum container memory usage in megabytes observed
            during the load process.
        memory_usage_mb: Net memory consumption (``memory_peak_mb`` minus
            ``memory_baseline_mb``), representing the additional memory
            required for data ingestion.
    """

    database: str
    timestamp: float
    total_time_s: float
    memory_baseline_mb: float
    memory_peak_mb: float
    memory_usage_mb: float

    def save(self, path: Path) -> None:
        """Persist the load result as a JSON file.

        Args:
            path: Directory in which to save the result. The file is named
                ``{database}.json``. Parent directories are created if needed.
        """
        path.mkdir(parents=True, exist_ok=True)
        filepath = path / f"{self.database}.json"
        with open(filepath, "w") as f:
            json.dump(asdict(self), f, indent=2)

    @classmethod
    def load(cls, path: Path) -> "LoadResult":
        """Reconstruct a ``LoadResult`` from a previously saved JSON file.

        Args:
            path: Path to the JSON file.

        Returns:
            A ``LoadResult`` instance with values restored from the file.
        """
        with open(path) as f:
            return cls(**json.load(f))


@dataclass
class MemoryResult:
    """Result of a memory benchmark measurement for a single query.

    Memory benchmarks follow a rigorous isolation protocol: the database
    container is restarted before each query measurement to flush all
    in-memory caches and buffers. The baseline measurement is taken after
    the restart and health check confirm the database is ready. The peak
    measurement is captured during and immediately after query execution.
    The delta (``memory_usage_mb = memory_peak_mb - memory_baseline_mb``)
    represents the additional memory the database required to process the
    query, isolating the query's memory impact from the engine's static
    footprint.

    Attributes:
        database: Identifier of the database under test.
        query_category: String category of the benchmark query (e.g., ``"selection"``, ``"join"``).
        query_variant: Integer variant within the query category.
        description: Human-readable description of the query being measured.
        timestamp: Unix epoch timestamp of the measurement.
        memory_baseline_mb: Container memory usage in megabytes after the
            restart, before query execution begins.
        memory_peak_mb: Maximum container memory usage in megabytes observed
            during and after query execution.
        memory_usage_mb: Net memory consumption attributable to the query
            (``memory_peak_mb - memory_baseline_mb``).
    """

    database: str
    query_category: str
    query_variant: int
    description: str
    timestamp: float
    memory_baseline_mb: float
    memory_peak_mb: float
    memory_usage_mb: float

    def save(self, directory: Path) -> None:
        """Persist the memory result as ``memory.json`` in the given directory.

        Args:
            directory: Target directory. Created if it does not exist.
        """
        directory.mkdir(parents=True, exist_ok=True)
        with open(directory / "memory.json", "w") as f:
            json.dump(asdict(self), f, indent=2)

    @classmethod
    def load(cls, path: Path) -> "MemoryResult":
        """Reconstruct a ``MemoryResult`` from a previously saved JSON file.

        Args:
            path: Path to the JSON file.

        Returns:
            A ``MemoryResult`` instance with values restored from the file.
        """
        with open(path) as f:
            return cls(**json.load(f))


@dataclass
class TimeResult:
    """Result of a single execution time measurement for one benchmark run.

    Time benchmarks execute each query multiple times (default: 50 iterations)
    without container restarts between runs, reflecting a warm-cache scenario
    that is representative of production workloads. Each iteration produces
    one ``TimeResult`` instance, persisted as ``run_001.json``, ``run_002.json``,
    and so forth. Retaining individual run data enables post-hoc analysis of
    run-to-run variance, outlier identification, and distribution fitting.

    Attributes:
        database: Identifier of the database under test.
        query_category: String category of the benchmark query (e.g., ``"selection"``, ``"join"``).
        query_variant: Integer variant within the query category.
        description: Human-readable description of the query being measured.
        run_number: One-based index of this run within the measurement series.
        timestamp: Unix epoch timestamp of this specific run.
        execution_time_s: Wall-clock query execution time in seconds, measured
            using ``time.perf_counter`` for sub-microsecond resolution.
    """

    database: str
    query_category: str
    query_variant: int
    description: str
    run_number: int
    timestamp: float
    execution_time_s: float

    def save(self, directory: Path) -> None:
        """Persist the time result as ``run_{NNN}.json`` in the given directory.

        The run number is zero-padded to three digits for lexicographic
        sorting consistency.

        Args:
            directory: Target directory. Created if it does not exist.
        """
        directory.mkdir(parents=True, exist_ok=True)
        with open(directory / f"run_{self.run_number:03d}.json", "w") as f:
            json.dump(asdict(self), f, indent=2)

    @classmethod
    def load(cls, path: Path) -> "TimeResult":
        """Reconstruct a ``TimeResult`` from a previously saved JSON file.

        Args:
            path: Path to the JSON file.

        Returns:
            A ``TimeResult`` instance with values restored from the file.
        """
        with open(path) as f:
            return cls(**json.load(f))


@dataclass
class TimeSummary:
    """Aggregated descriptive statistics from multiple time benchmark runs.

    This class computes and stores summary statistics for a single query's
    time benchmark series. The statistical methodology is as follows:

    - **Mean** (arithmetic): Computed via ``statistics.mean()``. Provides the
      expected value of execution time but is sensitive to outliers.
    - **Median**: Computed via ``statistics.median()``. A robust measure of
      central tendency that is less affected by extreme values.
    - **Standard deviation** (sample): Computed via ``statistics.stdev()``
      using Bessel's correction (dividing by ``n - 1``). Quantifies
      run-to-run variability. Set to 0.0 for single-run series where
      variance is undefined.
    - **Minimum and maximum**: The extreme values in the sorted run series.
    - **5th and 95th percentiles**: Computed using linear interpolation between
      adjacent order statistics (see ``_percentile``). Together they define
      the central 90% range, providing a robust characterisation of the
      execution time distribution that excludes the most extreme 10% of
      observations. This is more informative than min/max for identifying
      typical performance bounds.

    All time values are rounded to microsecond precision (6 decimal places)
    to avoid false precision from floating-point arithmetic while retaining
    sufficient granularity for sub-millisecond queries.

    The summary is computed from the individual ``run_NNN.json`` files via
    ``from_results()`` or ``from_directory()``, and persisted as
    ``summary.json`` alongside the run files.

    Attributes:
        database: Identifier of the database under test.
        query_category: String category of the benchmark query (e.g., ``"selection"``, ``"join"``).
        query_variant: Integer variant within the query category.
        description: Human-readable description of the query.
        num_runs: Total number of execution time measurements aggregated.
        mean_s: Arithmetic mean execution time in seconds.
        median_s: Median execution time in seconds.
        stdev_s: Sample standard deviation of execution times in seconds.
        min_s: Minimum observed execution time in seconds.
        max_s: Maximum observed execution time in seconds.
        percentile_5_s: 5th percentile execution time in seconds.
        percentile_95_s: 95th percentile execution time in seconds.
        all_times_s: Complete sorted list of individual execution times in
            seconds, retained for potential post-hoc analysis such as
            distribution fitting or outlier detection.
    """

    database: str
    query_category: str
    query_variant: int
    description: str
    num_runs: int
    mean_s: float
    median_s: float
    stdev_s: float
    min_s: float
    max_s: float
    percentile_5_s: float
    percentile_95_s: float
    all_times_s: list[float] = field(default_factory=list)

    def save(self, directory: Path) -> None:
        """Persist the summary as ``summary.json`` in the given directory.

        Args:
            directory: Target directory. Created if it does not exist.
        """
        directory.mkdir(parents=True, exist_ok=True)
        with open(directory / "summary.json", "w") as f:
            json.dump(asdict(self), f, indent=2)

    @classmethod
    def load(cls, path: Path) -> "TimeSummary":
        """Reconstruct a ``TimeSummary`` from a previously saved JSON file.

        Args:
            path: Path to the JSON file.

        Returns:
            A ``TimeSummary`` instance with values restored from the file.
        """
        with open(path) as f:
            return cls(**json.load(f))

    @classmethod
    def from_results(cls, results: list[TimeResult]) -> "TimeSummary":
        """Compute summary statistics from a list of ``TimeResult`` objects.

        The computation proceeds by extracting and sorting all execution times,
        then applying the standard descriptive statistics functions from
        Python's ``statistics`` module. Percentiles are computed via the
        ``_percentile`` helper using linear interpolation.

        Args:
            results: A non-empty list of ``TimeResult`` instances, typically
                from multiple runs of the same query.

        Returns:
            A ``TimeSummary`` with all statistical fields populated.

        Raises:
            ValueError: If the input list is empty, as statistical aggregation
                over zero observations is undefined.
        """
        if not results:
            raise ValueError("Cannot summarize empty results list")

        times = sorted(r.execution_time_s for r in results)
        first = results[0]

        return cls(
            database=first.database,
            query_category=first.query_category,
            query_variant=first.query_variant,
            description=first.description,
            num_runs=len(times),
            mean_s=round(statistics.mean(times), 6),
            median_s=round(statistics.median(times), 6),
            stdev_s=round(statistics.stdev(times), 6) if len(times) >= 2 else 0.0,
            min_s=round(times[0], 6),
            max_s=round(times[-1], 6),
            percentile_5_s=round(_percentile(times, 5), 6),
            percentile_95_s=round(_percentile(times, 95), 6),
            all_times_s=[round(t, 6) for t in times],
        )

    @classmethod
    def from_directory(cls, directory: Path) -> "TimeSummary":
        """Load all individual run files from a directory and compute summary statistics.

        This convenience method discovers all ``run_*.json`` files in the
        specified directory, deserialises them into ``TimeResult`` instances,
        and delegates to ``from_results()`` for statistical aggregation.

        Args:
            directory: Path to a directory containing ``run_NNN.json`` files.

        Returns:
            A ``TimeSummary`` computed from all discovered run files.

        Raises:
            FileNotFoundError: If no run files are found in the directory.
        """
        run_files = sorted(directory.glob("run_*.json"))
        if not run_files:
            raise FileNotFoundError(f"No run files found in {directory}")

        results = [TimeResult.load(f) for f in run_files]
        return cls.from_results(results)


def _percentile(sorted_data: list[float], p: float) -> float:
    """Compute the p-th percentile from pre-sorted data using linear interpolation.

    This function implements the linear interpolation method for percentile
    estimation, which is equivalent to NumPy's ``np.percentile`` with the
    default ``interpolation='linear'`` setting. The method is defined as
    follows:

    1. Compute the fractional rank: ``k = (p / 100) * (n - 1)``, where ``n``
       is the number of observations and ``p`` is the desired percentile.
    2. Decompose ``k`` into its integer part (``lower``) and fractional part
       (``fraction``).
    3. Interpolate between the two adjacent order statistics:
       ``result = data[lower] + fraction * (data[upper] - data[lower])``.

    This approach avoids the discontinuities inherent in nearest-rank methods
    and provides a smooth estimate that is well-suited for the relatively
    small sample sizes typical in benchmark experiments (e.g., 50 runs).

    Args:
        sorted_data: A list of observations sorted in ascending order. The
            caller is responsible for ensuring the data is pre-sorted.
        p: The desired percentile, expressed as a value between 0 and 100
            (inclusive). For example, ``p=5`` computes the 5th percentile.

    Returns:
        The interpolated p-th percentile value. For single-element inputs,
        returns the sole element regardless of the requested percentile.
    """
    n = len(sorted_data)
    if n == 1:
        return sorted_data[0]
    k = (p / 100) * (n - 1)
    lower = int(k)
    upper = lower + 1
    if upper >= n:
        return sorted_data[-1]
    fraction = k - lower
    return sorted_data[lower] + fraction * (sorted_data[upper] - sorted_data[lower])
