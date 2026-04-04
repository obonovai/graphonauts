"""Docker container lifecycle management for benchmark isolation.

This module provides the ``ContainerManager`` class, which encapsulates the
container restart and health-check logic required by the Graphonauts benchmarking
framework. It is a standalone lifecycle component, deliberately separated from
the measurement utilities (``TimeMonitor``, ``MemoryMonitor``) to maintain a
clean separation of concerns:

- **``ContainerManager``** handles *when* and *how* the database container is
  restarted and verified as healthy.
- **``MemoryMonitor``** and **``TimeMonitor``** handle *what* is measured during
  query execution.

This separation allows both memory and time benchmark phases to share the same
restart infrastructure without duplicating code or conflating measurement with
lifecycle management.

**Restart protocol.** A container restart proceeds as follows:

1. The Docker SDK ``restart()`` API is invoked on the target container.
2. A fixed settle period (``WAIT_AFTER_RESTART_S``) elapses to allow the
   container's main process to begin initialisation.
3. The container logs are polled at ``HEALTH_CHECK_POLL_S`` intervals for
   database-specific readiness indicators (e.g., ``"Started."`` for Neo4j,
   ``"Bolt connection"`` for Memgraph).
4. Once all indicators are found, an additional settle period
   (``HEALTH_CHECK_SETTLE_S``) is observed to allow background initialisation
   (recovery log replay, cache warming) to complete.
5. The host OS page cache is dropped by running a privileged Alpine container
   that writes ``3`` to ``/proc/sys/vm/drop_caches``. This is done *after*
   the health check because the container restart itself loads database files
   into the host page cache during recovery --- dropping caches before the
   restart would be ineffective.

If the readiness indicators do not appear within the configured timeout, a
``TimeoutError`` is raised. If the container enters an unexpected state (e.g.,
``exited``), a ``RuntimeError`` is raised.
"""

import logging
import time

import docker  # type: ignore
from docker.models.containers import Container  # type: ignore

logger = logging.getLogger(__name__)

# Time (seconds) to wait after issuing container restart before checking logs.
# The container needs time to start its main process before readiness indicators appear.
WAIT_AFTER_RESTART_S = 3.0

# Extra wait (seconds) after a readiness indicator is found in container logs.
# Ensures the database is fully initialized (not just logging readiness) before
# the benchmark begins.
HEALTH_CHECK_SETTLE_S = 2.0

# Polling interval (seconds) between health check log inspections.
HEALTH_CHECK_POLL_S = 0.5


class ContainerManager:
    """Manages Docker container lifecycle operations for benchmark isolation.

    Provides container restart with health-check polling, used by both memory
    and time benchmark phases to establish a known container state before
    measurement begins.

    The class is instantiated with the same parameters that ``DatabaseConfig``
    provides (container name, readiness indicators, timeout), making it easy
    to construct from any database's configuration.

    Attributes:
        container_name: The Docker container name to manage.
        health_check_indicators: Log substrings indicating database readiness.
        health_check_timeout: Maximum seconds to wait for container readiness.
    """

    def __init__(
        self,
        container_name: str,
        health_check_indicators: list[str] | None = None,
        health_check_timeout: float = 60.0,
    ):
        self.container_name = container_name
        self.health_check_indicators = health_check_indicators or []
        self.health_check_timeout = health_check_timeout
        self.client = docker.from_env()

    def restart_and_wait(self) -> None:
        """Restart the Docker container, wait for health check, and drop host page caches.

        Issues a restart command via the Docker SDK, pauses for a fixed settle
        period (``WAIT_AFTER_RESTART_S``) to allow the container's main process
        to begin initialisation, and then polls the container logs for readiness
        indicators. The restart timestamp is recorded so that only log entries
        produced after the restart are examined, avoiding false positives from
        stale log output.

        After all readiness indicators are found, an additional settle period
        (``HEALTH_CHECK_SETTLE_S``) is observed. Finally, the host OS page cache
        is dropped to ensure that subsequent query execution starts with a true
        cold cache --- the container restart loads database files into the host
        page cache during recovery, so caches must be dropped *after* the
        database is healthy.

        Raises:
            TimeoutError: If the readiness indicators do not appear within
                ``health_check_timeout`` seconds.
            RuntimeError: If the container enters an unexpected state.
        """
        container = self.client.containers.get(self.container_name)
        restart_time = int(time.time())
        container.restart()
        time.sleep(WAIT_AFTER_RESTART_S)
        self._wait_for_healthy(container, since=restart_time)
        self._drop_host_caches()

    def _drop_host_caches(self) -> None:
        """Drop the host OS page cache, dentries, and inodes.

        Runs a privileged Alpine container that writes ``3`` to
        ``/proc/sys/vm/drop_caches``, which instructs the Linux kernel to free
        all reclaimable slab objects (dentries and inodes) and all page cache
        pages. Because Docker containers share the host kernel, the database
        container's file I/O benefits from the host page cache --- without
        dropping it, the first query after a container restart would read data
        from warm host cache rather than from disk, defeating the purpose of
        cold-cache benchmarking.
        """
        self.client.containers.run(
            "alpine",
            command='sh -c "echo 3 > /proc/sys/vm/drop_caches"',
            privileged=True,
            remove=True,
        )

    def _wait_for_healthy(self, container: Container, since: int | None = None) -> None:
        """Block until the container's logs contain a readiness indicator or the timeout expires.

        Polls the container logs at ``HEALTH_CHECK_POLL_S`` intervals, searching for
        any of the configured ``health_check_indicators`` substrings. When a match is
        found, an additional settle period (``HEALTH_CHECK_SETTLE_S``) is observed
        before returning, to allow background database initialisation (e.g., recovery
        log replay, cache warming) to complete.

        Args:
            container: The Docker container object to monitor.
            since: A Unix timestamp; if provided, only log entries produced after this
                time are examined. This prevents false-positive matches against stale
                log output from a previous container lifecycle.

        Raises:
            TimeoutError: If no readiness indicator is found within
                ``health_check_timeout`` seconds.
            RuntimeError: If the container enters an unexpected state (e.g., ``exited``).
        """
        start_time = time.time()

        while time.time() - start_time < self.health_check_timeout:
            try:
                container.reload()

                status = container.status
                if status not in ["running", "restarting"]:
                    raise RuntimeError(f"Container status is '{status}', expected 'running'")

                if since is not None:
                    logs = container.logs(since=since).decode("utf-8", errors="ignore")
                else:
                    logs = container.logs(tail=50).decode("utf-8", errors="ignore")

                if any(indicator in logs for indicator in self.health_check_indicators):
                    time.sleep(HEALTH_CHECK_SETTLE_S)
                    return

                time.sleep(HEALTH_CHECK_POLL_S)

            except Exception as e:
                print(f"Warning during health check: {e}")
                time.sleep(HEALTH_CHECK_POLL_S)

        raise TimeoutError(
            f"Container '{self.container_name}' did not become healthy within {self.health_check_timeout}s"
        )
