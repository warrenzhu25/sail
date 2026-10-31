# 10 · Shuffle and Streams: Moving Data Between Tasks

> **You'll learn:** how tasks in different stages exchange Arrow data, what a
> stream key is, how shuffle write and read operators work, how in-memory and
> on-disk streams differ, how workers fetch data from each other over Arrow
> Flight, and where the disk-based (blocking) shuffle stands.
>
> **Crate:** `sail-execution` (`plan/` shuffle operators, `stream/`,
> `stream_manager/`, `stream_accessor/`, `stream_service/`)

---

## 1. What This Component Does

When the job graph cuts a plan into stages (see
[09](09-local-and-cluster-execution.md)), data has to flow from upstream tasks
to downstream tasks. This component is the **data plane** that moves it.

```text
 Stage 0 task (partition p)                         Stage 1 task (partition q)
 ┌───────────────────────────┐                      ┌────────────────────────────┐
 │ … plan fragment …         │                      │ ShuffleReadExec             │
 │ ShuffleWriteExec          │  channel q of p      │   reads channel q from      │
 │   hash(k) → channels 0..N ├────────────────────► │   every upstream partition  │
 └───────────────────────────┘  (memory or Flight)  │ … plan fragment …           │
                                                    └────────────────────────────┘
```

Every piece of exchanged data is a **stream** of Arrow record batches,
identified by a **stream key**:

> (job, stage, partition, attempt, channel)

So "channel 3 of attempt 0 of partition 5 of stage 2 of job 17" is exactly one
stream. Including the attempt in the key means a retried task never mixes its
output with a failed attempt's.

---

## 2. Why It's Built This Way

**Arrow all the way.** Data is never converted to rows between stages. Batches
move as Arrow arrays in memory, or as Arrow IPC over Arrow Flight between
machines, so no serialization format has to be designed and parsing costs are
minimal.

**Streams, not files, by default.** In the default pipelined mode, downstream
tasks read upstream output **while it's being produced**. That gives low
latency for interactive queries and avoids writing intermediate data to disk.
The cost is that producers and consumers must run at the same time, and losing a
task means rerunning its whole connected region.

**Backpressure is built in.** Each in-memory stream is a bounded channel with
`cluster.task_stream_buffer` batches (16 by default). If a consumer is slow, the
producer waits instead of filling memory.

**One abstraction, several storage kinds.** Shuffle operators write to and read
from *locations*. Where the bytes actually live (memory on this machine, disk on
this machine, another worker) is decided by the location. That leaves room for a
disk-based shuffle and, in the future, remote storage.

---

## 3. How It Works

### 3.1 Write side: `ShuffleWriteExec`

At the root of every upstream task, the task runner places a **shuffle write**
operator. For the task's partition, it has one **write location per output
channel**. When executed:

1. It opens a sink for every channel location.
2. It reads batches from the plan fragment below it.
3. It splits each batch by the stage's **output distribution**, using
   DataFusion's batch partitioner: **hash** of the keys modulo the channel count,
   or **round-robin**.
4. It writes each slice to its channel's sink. A sink can report *closed*, for
   example when a downstream consumer only needed part of the data (such as a
   `LIMIT`). That channel is then skipped, and when every sink is closed, writing
   stops early.
5. At the end, it closes every sink and produces an empty result. The real output
   is in the streams.

### 3.2 Read side: `ShuffleReadExec`

Stage-input placeholders in a downstream plan are replaced with **shuffle read**
operators. For each partition, a shuffle read has a list of **read
locations**, which depends on the input mode:

| Input mode | Partition *q* reads |
|------------|---------------------|
| Forward | All channels of upstream partition *q* |
| Shuffle | Channel *q* of **every** upstream partition |
| Merge | All channels of every upstream partition (for an order-preserving merge above) |
| Broadcast | Everything, from every upstream partition (replicated) |

It opens all its locations at once and **merges** the streams into one, taking
batches as they arrive from any upstream.

### 3.3 Locations

| Write location | Read location | Meaning |
|----------------|---------------|---------|
| Local (memory, *N* replicas) | Driver / Worker *(id)* | Kept in this process's stream manager. Readers on the same process read it directly. Readers on other workers fetch it through Flight. |
| Local (disk) | Driver / Worker *(id)* | Written to a file under the shuffle directory, so it can be read later, more than once, even after the producer finishes (blocking mode, see 3.7). |
| Remote *(URI)* | Remote *(URI)* | Reserved for external storage such as object stores. **Not implemented**: it returns "not implemented: remote stream". |

**Replicas.** A memory stream can be read a fixed number of times. The job
graph calculates how many readers each stage output has (one per consumer for
forward and shuffle inputs, one per downstream partition for merge and broadcast
inputs), and the stream keeps that many replicas. Because Arrow batches are
reference-counted, a replica costs a pointer copy, not a data copy.

### 3.4 The stream manager

Each driver and worker has a **stream manager** that maps stream keys to local
streams. A stream can be in one of three states:

| State | Meaning |
|-------|---------|
| **Pending** | A reader asked for the stream before the producer created it. The reader gets a channel immediately, and its sender is parked here. |
| **Created** | The producer has published the stream. Parked readers are connected, and new readers subscribe directly. |
| **Failed** | The producer failed. Readers receive the error. |

**Readers can arrive before writers.** In pipelined mode, downstream tasks may
start before upstream tasks. When a reader asks for a stream that doesn't exist
yet, the manager parks it and schedules a check after
`cluster.task_stream_creation_timeout_secs`. If the producer still hasn't
appeared by then, the reader fails instead of waiting forever.

### 3.5 Between machines: the stream service

Every worker (and the driver) runs a small **Arrow Flight** service. To read a
stream that lives on another worker:

1. The reading task's location says "worker *W*, key *K*".
2. The local actor uses its peer client for worker *W* (it learns peer addresses
   from the driver) and calls Flight `DoGet` with a ticket encoding *K*.
3. Worker *W*'s Flight service asks its stream manager for *K*, which may wait
   in the pending state, and streams the batches back as Arrow IPC messages.
4. On the reading side, the Flight stream becomes an ordinary record batch
   stream and is merged with the other inputs.

The same mechanism lets the driver read the final stage's output from workers
(see [09](09-local-and-cluster-execution.md), job output).

### 3.6 Cleanup

When a job ends, or a stage's consumers are all finished, the driver tells
workers to **clean up** the job's streams. Their stream managers drop the
entries, freeing memory (and deleting files for disk streams).

### 3.7 Blocking (disk) shuffle: status

Sail also has a **blocking** output mode: upstream tasks write each channel to
disk and exit, and downstream stages start only after the upstream stage
finishes. It's a prerequisite for adaptive partition coalescing and for
recovering from a lost worker by recomputing only the lost output.

| On this branch | Details |
|----------------|---------|
| Disk stream (write to a temp file, rename into place, small text index with byte and row counts) | Implemented and unit-tested |
| Adaptive coalescing from index statistics | Implemented, used in tests |
| Turning blocking mode on from configuration | **Not reachable.** The driver always uses pipelined mode. |

The follow-up `disk-shuffle-hardening` branch makes blocking shuffle usable:
`cluster.shuffle_mode`, `cluster.shuffle_dir`, adaptive coalescing settings,
worker-reported shuffle statistics, stage retry on lost workers and fetch
failures, per-process shuffle directories, and buffered I/O. See
`docs/concepts/architecture-deep-dive.md` (*Shuffle Modes, Adaptive
Coalescing, And Fault Tolerance*) and
`docs/concepts/architecture/adaptive-disk-shuffle-design.md`.

---

## 4. Example: Hash Shuffle With 3 Producers and 2 Consumers

```python
# In Sail, the shuffle partition count comes from execution.default_parallelism.
df = spark.range(0, 9, numPartitions=3).selectExpr("id % 2 AS k", "id")
df.groupBy("k").count().collect()
```

Suppose stage 0 has 3 partitions (P0, P1, P2), and stage 1 has 2 partitions
(Q0, Q1), hashing on `k` into 2 channels.

| Producer | Rows | Channel 0 (`k` hashes to 0) | Channel 1 |
|----------|------|-----------------------------|-----------|
| P0 | id 0,1,2 | partial counts for one `k` | partial counts for the other `k` |
| P1 | id 3,4,5 | … | … |
| P2 | id 6,7,8 | … | … |

- Q0 reads **channel 0 of P0, P1, and P2**: three streams merged, then the final
  aggregate.
- Q1 reads **channel 1 of P0, P1, and P2**.
- That's 6 streams in total, with keys `(job, stage 0, p, attempt 0, c)` for
  p ∈ {0,1,2} and c ∈ {0,1}.
- If P1 and Q0 run on the same worker, Q0 reads P1's stream directly from memory.
  P0's and P2's streams come over Flight from their workers.

---

## 5. End-to-End: Life of One Batch

| # | Where | What happens |
|---|-------|--------------|
| 1 | Producer task (worker A) | The plan fragment emits a batch. |
| 2 | Shuffle write | Splits the batch by hash into per-channel slices. |
| 3 | Stream manager A | The slice for channel 1 is sent into the memory stream for key (J, S, P, a, 1). The stream holds up to 16 batches. |
| 4 | Consumer task (worker B) | Its shuffle read's location for (J, S, P, a, 1) says "worker A". |
| 5 | Worker B → worker A | A Flight `DoGet` with that key as the ticket. |
| 6 | Worker A | The stream service subscribes to the memory stream (parking the request if the stream isn't created yet) and streams batches as Arrow IPC. |
| 7 | Worker B | The batch arrives, is merged with other inputs, and flows into the consumer's operators. |
| 8 | End | The producer closes its sinks, streams finish, the consumer completes, and the driver later cleans up the job's streams. |

---

## 6. Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| Reader fails after waiting on a pending stream | The upstream task never started or crashed. Check its status and worker logs. Raise `cluster.task_stream_creation_timeout_secs` if workers start slowly. |
| "not implemented: remote stream" | Something requested remote (object store) stream storage, which isn't implemented. |
| Memory grows on a worker during a large shuffle | Pipelined streams hold data until consumers read it. Fewer partitions, a bounded memory pool, or the disk shuffle (hardening branch) help. |
| Retried job produces duplicates | Shouldn't happen, because the attempt is part of the stream key. Report it with the job graph and task attempts (`system.execution.tasks`). |

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Shuffle write and read operators, stage input placeholder | `crates/sail-execution/src/plan/` (`shuffle_write.rs`, `shuffle_read.rs`, `stage_input.rs`) |
| Stream keys | `crates/sail-execution/src/id.rs` |
| Read and write locations, merged stream | `crates/sail-execution/src/stream/` (`reader.rs`, `writer.rs`, `merge.rs`) |
| Stream manager (pending, created, failed) | `crates/sail-execution/src/stream_manager/core.rs` |
| Memory and disk streams | `crates/sail-execution/src/stream_manager/local.rs` |
| Choosing local or remote access | `crates/sail-execution/src/stream_accessor/` |
| Arrow Flight stream service | `crates/sail-execution/src/stream_service/` |
| Input and output locators in task definitions | `crates/sail-execution/src/task/definition.rs` |
| Disk shuffle design | `docs/concepts/architecture/adaptive-disk-shuffle-design.md` |
