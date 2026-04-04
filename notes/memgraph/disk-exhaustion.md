# Memgraph Disk Exhaustion During Benchmark Restarts

## 1. Problem Statement

During extended benchmark runs against the Memgraph database, the container terminated unexpectedly with a
critical assertion failure. The error manifested during the snapshot creation phase of a graceful shutdown,
triggered by the benchmark framework's container restart protocol:

```
[memgraph_log] [critical] Assertion failed in file /home/mg/memgraph/src/utils/file.cpp at line 876.
Expression: 'written > 0'
Message: 'while trying to write to
/var/lib/memgraph/snapshots/20260407115136892758_timestamp_26819 an error occurred:
No space left on device (28). Possibly 262048 bytes of data were lost from this
call and possibly 110100601 bytes were lost from previous calls.'
```

Upon restart, Memgraph's internal RocksDB instance (used for persistent settings storage) also failed to
initialise due to the same underlying condition:

```
RocksDB couldn't be initialized inside /var/lib/memgraph/settings -- IO error: No
space left on device: While appending to file:
/var/lib/memgraph/settings/000156.sst: No space left on device
```

The container entered an `exited` state instead of `running`, which the benchmark framework's health check
detected as an unrecoverable failure:

```
Warning during health check: Container status is 'exited', expected 'running'.
```

## 2. Memgraph Storage and Persistence Model

Memgraph is an in-memory graph database: all node, relationship, and property data reside in RAM during
normal operation. However, Memgraph provides optional durability through two complementary mechanisms:

1. **Snapshots.** A snapshot is a full serialisation of the in-memory graph state to a binary file on disk.
   By default, Memgraph creates a snapshot on every graceful shutdown (`--storage-snapshot-on-exit=true`)
   and optionally at periodic intervals (`--storage-snapshot-interval-sec`). Snapshots are written to the
   directory `/var/lib/memgraph/snapshots/` and are retained according to a configurable retention count
   (`--storage-snapshot-retention-count`, default 3). On startup, Memgraph recovers the most recent
   snapshot to reconstruct the in-memory state.

2. **Write-Ahead Log (WAL).** The WAL records individual write operations (node/relationship
   creation, property updates, deletions) as a sequential log. On startup, after loading the latest
   snapshot, Memgraph replays any WAL entries that postdate the snapshot to recover operations that
   occurred between the last snapshot and the shutdown. WAL files accumulate in
   `/var/lib/memgraph/wal/` and are pruned after a new snapshot supersedes them.

3. **RocksDB settings store.** Memgraph uses an embedded RocksDB instance at
   `/var/lib/memgraph/settings/` for internal configuration persistence. This store grows incrementally
   through SST file compaction.

In the benchmarking deployment, Memgraph's data directory (`/var/lib/memgraph`) is backed by a Docker
named volume (`memgraph_data`), which resides on the Docker Desktop virtual machine's filesystem. The
total available disk space is therefore bounded by the Docker Desktop VM disk allocation, which is shared
across all containers and volumes on the host.

## 3. Root Cause Analysis

The benchmark framework's memory measurement protocol requires a container restart before each query
execution to establish a clean memory baseline. The restart sequence proceeds as follows:

1. The Docker SDK `restart()` API is invoked, which sends `SIGTERM` to the Memgraph process.
2. Memgraph performs a graceful shutdown, during which it serialises the entire in-memory graph to a new
   snapshot file in `/var/lib/memgraph/snapshots/`.
3. After the container process terminates, Docker restarts it. On startup, Memgraph loads the most recent
   snapshot to restore the graph state.

With the TPC-H Scale Factor 1 dataset loaded (approximately 8.6 million nodes and 17.1 million
relationships), each snapshot file is on the order of 100--200 MB. The benchmark suite comprises 19
queries, and the default execution mode runs both memory and time benchmarks for each query. Memory
benchmarks perform one container restart per query, and time benchmarks perform one additional restart per
query. This yields approximately 38 restarts per full benchmark session.

Under the default Memgraph configuration, each of these restarts triggers a new snapshot on shutdown. Even
with the default retention count of 3, the concurrent presence of the active snapshot plus up to 2 older
snapshots awaiting cleanup, combined with WAL files accumulated between snapshots and RocksDB SST files,
produces a cumulative disk footprint that grows with each restart cycle. On a Docker Desktop installation
with a constrained virtual disk, this accumulation eventually exhausts the available space.

The failure cascade is as follows:

1. During graceful shutdown, the snapshot writer encounters `ENOSPC` (error 28: No space left on device)
   mid-write, resulting in a partial and corrupt snapshot file.
2. Memgraph raises a critical assertion failure and terminates abnormally.
3. On the subsequent restart attempt, RocksDB cannot write to its settings directory (also affected by
   disk exhaustion), causing Memgraph to fail during initialisation.
4. The container enters the `exited` state. The benchmark framework's health check polling loop detects
   this status and raises an error, halting the benchmark run.

## 4. Solution: Controlled Persistence Strategy

The benchmarking workload is fundamentally read-only after the initial data load. No benchmark query
modifies the graph structure (even the "modification" category queries perform only sorting and
deduplication, not writes). This characteristic allows a minimal persistence strategy: a single snapshot
created once after data loading, with all automatic persistence mechanisms disabled.

The following Docker Compose command flags suppress automatic disk writes:

```yaml
command: [
  "--log-level=INFO",
  "--also-log-to-stderr=true",
  "--storage-snapshot-on-exit=false",
  "--storage-wal-enabled=false",
  "--storage-snapshot-interval-sec=0",
]
```

| Flag | Default | Modified | Effect |
|------|---------|----------|--------|
| `--storage-snapshot-on-exit` | `true` | `false` | Suppresses snapshot creation during graceful shutdown |
| `--storage-wal-enabled` | `true` | `false` | Disables write-ahead logging entirely |
| `--storage-snapshot-interval-sec` | `300` | `0` | Disables periodic snapshot creation |

With these flags, no automatic disk writes occur during the benchmark lifecycle. Data persistence is
instead achieved through a single manual snapshot, created programmatically at the end of the data loading
phase:

```cypher
CREATE SNAPSHOT
```

This command is executed by the Memgraph loader (`_acreate_snapshot()` method) after all TPC-H entities
have been loaded and temporary loading indexes have been dropped. The resulting snapshot is the sole
persistence artefact on disk. On each subsequent container restart during benchmarking, Memgraph recovers
from this single snapshot without creating any new files.

The disk footprint thus remains constant at approximately the size of one snapshot (~100--200 MB for
TPC-H SF1), regardless of how many benchmark restarts are performed.

## 5. Recovery Procedure

If the disk exhaustion has already occurred, the following steps restore the environment:

```bash
# 1. Remove the container and its volumes
poe memgraph-down -v

# 2. Start a fresh container (with updated storage flags)
poe memgraph-up

# 3. Reload TPC-H data (creates the single manual snapshot)
python -m graphonauts load memgraph
```

After this procedure, the benchmark suite can be executed without risk of disk exhaustion:

```bash
python -m graphonauts benchmark memgraph --runs 50
```
