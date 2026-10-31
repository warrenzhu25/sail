# 09 · Local and Cluster Execution

> **You'll learn:** how a physical plan actually runs, in-process (local mode)
> or across a driver and workers (cluster mode); how a plan is cut into stages
> and tasks; how tasks are scheduled into worker slots; how workers are started,
> monitored, and stopped; and how failures are retried.
>
> **Crate:** `sail-execution` (job runners, driver, job graph, scheduler, task
> assigner, worker pool, worker, task runner, worker managers, plan codec)
>
> The data exchange between tasks (streams and shuffle) is covered in
> [10 · Shuffle and Streams](10-shuffle-and-streams.md).


## In One Minute

- **Local mode** runs the physical plan in-process with DataFusion.
  **Cluster mode** sends it to a **driver**, which runs it on **workers**.
- The plan is cut into **stages** wherever data must be exchanged (repartition,
  coalesce, sort-preserving merge). Each stage has one **task** per partition.
- In the default pipelined mode, connected stages run **together** as one
  **region**. A region needs about as many slots as its widest stage.
- Workers start on demand (4 initially, then as needed) and stop after 60
  seconds idle. Lost workers are detected by missing heartbeats (120 seconds).
- A failed task reruns its whole region, up to 3 attempts.

---

## 1. What This Component Does

Everything up to this point produces one thing: a DataFusion **physical plan**.
This component runs it and returns a stream of Arrow record batches. It has a
deliberately small interface, the **job runner**:

> *Give me a physical plan and a session, and I'll give you a stream of
> results.* (Plus *stop*, which returns the history of what ran.)

There are two implementations:

| Job runner | Mode | How it runs a plan |
|------------|------|--------------------|
| **Local** | `local` | Wraps each operator with tracing and calls DataFusion's `execute_stream` in the same process, using all CPU cores. |
| **Cluster** | `local-cluster`, `kubernetes-cluster` | Sends the plan to a **driver** actor, which splits it into **stages** and **tasks**, runs the tasks on **workers** (and a few on the driver itself), and streams the final stage's output back. |

Protocol handlers can't tell which one they're using. The same query works in
both modes.

---

## 2. Why It's Built This Way

**DataFusion is already a great single-node engine.** Local mode adds almost
nothing: tracing and a job ID. All the engineering effort for distribution goes
into cluster mode.

**Stages are cut where data is exchanged.** DataFusion's physical plans already
mark the points where data must be redistributed: repartition operators,
"coalesce to one partition" operators, and order-preserving merges. Those are
exactly the places where a distributed engine needs a shuffle. Sail's job graph
builder cuts the plan there, so the stages follow from the optimizer's
decisions.

**Actors on both sides.** The driver and each worker are actors (see
[02](02-entry-points-and-server.md)). Job, stage, task, worker, and stream state
changes concurrently. Handling every change as a message, one at a time, keeps
this manageable.

**Pipelined regions are gang-scheduled.** In the default pipelined mode,
producer and consumer tasks run **at the same time** and stream data to each
other in memory. A consumer can't start without its producers, and producers
can't finish without consumers reading. So all the tasks in such a connected
**region** must get slots together. Sail schedules regions as a whole, in the
order they were queued.

**Workers are started on demand.** The driver requests workers when queued
work needs more slots than are free, up to `cluster.worker_max_count`, and
stops idle workers after `cluster.worker_max_idle_time_secs`. In Kubernetes,
this is elastic: pods appear for a big query and go away afterwards.

---

## 3. How It Works

### 3.1 Vocabulary

| Term | Meaning |
|------|---------|
| **Job** | One execution of a physical plan, such as one `collect()` |
| **Stage** | A piece of the plan between exchanges. Has N **partitions**. |
| **Task** | One partition of one stage. Can have several **attempts**. |
| **Channel** | One slice of a task's output, for example one hash bucket of a shuffle |
| **Region** | A group of tasks that must be scheduled together (see 3.4) |
| **Task set** | Tasks from different stages that share **one slot**. Stages in the same *slot-sharing group* (by default, all stages) put partition *p* in the same task set. |
| **Slot** | A unit of worker capacity. Each worker has `cluster.worker_task_slots` slots (8 by default). |

### 3.2 From physical plan to job graph

The **job graph builder** walks the physical plan bottom-up:

| Operator found | What happens |
|----------------|--------------|
| **Repartition** (hash or round-robin) | Cut here. Everything below becomes a stage whose output is split into channels (hash or round-robin). Above, a *shuffle read* operator reads one channel from every upstream partition. |
| **Coalesce partitions** (N → 1) | Cut here. Upstream output goes to one channel, and a single downstream partition reads all of it. A `fetch` limit becomes a global limit above it. |
| **Sort-preserving merge** | Cut here, using **merge** input mode: the downstream task reads every upstream partition and merge-sorts them. |
| **Build side of a collect-left hash join**, nested-loop, cross, or piecewise merge join | Marked *shared*. Every downstream partition needs the whole build side, which is a **broadcast** input. |
| **Catalog command**, **system table scan** | Placed in a stage that runs **on the driver**, because only the driver has the catalog and session state. |
| Anything else | Stays inside the current stage. |

Before building, two safety rewrites run:

- A **global limit** must see a single input partition, so one is enforced.
- A **collect-left hash join** whose build side can emit unmatched rows (right,
  full, and anti-style joins) is turned into a **partitioned** hash join.
  Otherwise each worker would emit the unmatched build-side rows again,
  producing duplicates.

Each stage records:

| Field | Meaning |
|-------|---------|
| Inputs | Which upstream stages, and in what **input mode**: *forward* (same partition), *shuffle* (one channel from all partitions), *merge* (all channels from all partitions), *broadcast* (everything, to every partition) |
| Plan | The stage's physical plan fragment, with stage-input placeholders |
| Mode | **Pipelined** (stream in memory, the default) or **Blocking** (materialize to disk). On this branch, blocking mode isn't reachable from configuration, see [10](10-shuffle-and-streams.md). |
| Distribution | Hash (keys, number of channels) or round-robin (number of channels) |
| Placement | **Worker** or **Driver** |
| Group | Slot-sharing group name |

### 3.3 The driver

The driver actor holds four main components:

| Component | Owns |
|-----------|------|
| **Job scheduler** | Jobs, stages, tasks, attempts, and their states. Decides what's ready to run, and produces the task definition (serialized plan, input locations, output location) for each task. |
| **Task assigner** | The queue of regions waiting for slots, the driver's own slots, each worker's slots, and which task runs where. Calculates how many workers to request. |
| **Worker pool** | Worker lifecycles: starting through the worker manager, registration, heartbeats, idle and lost detection, stopping. Also gRPC clients to each worker. |
| **Task runner** | Runs driver-placed tasks (catalog commands, system tables) inside the driver. |

**States:**

- Job: *running* → *draining* → *succeeded*, *failed*, or *canceled*
- Stage: *active* (tasks may still run or be read) → *inactive*
- Task attempt: *created* → *scheduled* → *running* → *succeeded*, *failed*, or *canceled*

**Messages the driver handles:** server ready, register worker, worker
heartbeat, worker known peers, probe pending, idle, or lost worker, execute job,
clean up job, update task, probe pending task, create or fetch local, remote,
driver, or worker streams, observe state, and shutdown.

### 3.4 Regions: what gets scheduled together

The scheduler groups tasks into **regions**:

- In pipelined mode, all partitions of a **connected group of pipelined stages**
  form one region, because they must run at the same time.
- When every link inside the group is *forward* (partition *p* feeds partition
  *p*), the region is split **per partition**, so each partition chain can be
  scheduled on its own.
- Regions across **blocking** boundaries are independent and depend on each
  other. A downstream region becomes eligible only when its upstream regions
  succeed.

**If any task in a region fails, the whole region is rescheduled** (new
attempts), up to `cluster.task_max_attempts` (3 by default). After that the
region, and the job, fail.

### 3.5 Assigning tasks to slots

1. When a region becomes eligible, its task sets are added to the **queue**.
2. The assigner takes regions **from the front** of the queue and tries to place
   **all** of the region's task sets into vacant slots: driver-placed sets on the
   driver, worker-placed sets on workers. Because stages in the same
   slot-sharing group share task sets (partition *p* of every stage goes into
   bucket *p*), a region needs about as many slots as its **widest stage**, not
   its total number of tasks.
3. If the region doesn't fit, it goes **back to the front** and assignment stops
   for now. This keeps strict queue order. A large region can block smaller ones
   behind it (head-of-line blocking). A task that waits longer than
   `cluster.task_launch_timeout_secs` (120 s) for a slot fails with "task
   scheduling timeout".
4. For each placed task, the scheduler creates the **task definition** and sends
   it to the worker (*run task*) or the driver's own task runner.
5. **Scaling:** when the driver starts (with its session), it launches
   `cluster.worker_initial_count` workers (4 by default). After that, the
   assigner counts the slots needed by queued regions, minus vacant slots, and
   divides by slots per worker. The worker pool starts that many new workers,
   capped by `cluster.worker_max_count` (0 means no limit).

### 3.6 The worker

A worker is an actor with its own gRPC server and its own stream manager:

1. **Start.** It starts its server, registers with the driver (its host and
   port), and begins sending heartbeats every
   `cluster.worker_heartbeat_interval_secs`.
2. **Run task.** It decodes the plan and hands it to its task runner.
3. **Execute.** The task runner:
   - decodes the protobuf plan with Sail's **plan codec**, which knows how to
     serialize every Sail operator, UDF, and data source,
   - adapts Parquet scans (for Delta column mapping and schema evolution),
   - replaces stage-input placeholders with **shuffle read** operators pointing at
     the upstream tasks, and wraps the root with a **shuffle write** operator for
     the task's output channels (see [10](10-shuffle-and-streams.md)),
   - wraps operators with tracing,
   - executes its partition and starts a **task monitor** that drives the stream to
     completion and reports *running*, *succeeded*, or *failed* (with an error
     cause) to the driver.
4. **Report.** Status updates carry a sequence number, and the driver ignores
   outdated ones.
5. **Clean up and stop.** The driver tells the worker to clean up a job's streams,
   and stops idle workers.

### 3.7 Worker lifecycle and failure detection

| Event | Driver behavior |
|-------|-----------------|
| Worker requested | The worker manager starts it: an in-process actor in `local-cluster`, a pod running `sail worker` in Kubernetes |
| Not registered within `worker_launch_timeout_secs` (120 s) | Treated as failed to start, and the request count is reduced |
| No heartbeat for `worker_heartbeat_timeout_secs` (120 s) | Worker **lost**: stopped, its tasks marked failed ("task failed for lost worker"), jobs refreshed so regions are rescheduled, and new workers requested |
| No tasks for `worker_max_idle_time_secs` (60 s) | Worker stopped to free resources |
| Task still waiting for a slot after `task_launch_timeout_secs` (120 s) | Task marked failed with "task scheduling timeout", which fails and retries its region |

### 3.8 The job output

The final stage's output is the job's result. The driver opens streams to the
final tasks' outputs, through local streams on the driver or by fetching from
workers, and merges them into the single stream returned to the caller. If the
consumer goes away (the client cancels), the driver notices the dropped receiver
and stops forwarding output.

### 3.9 Kubernetes specifics

The Kubernetes worker manager creates one pod per worker from
`kubernetes.image` in `kubernetes.namespace`. The pods are named with
`kubernetes.worker_pod_name_prefix`, owned by the driver pod (so they're
garbage-collected with it), and optionally customized with
`kubernetes.worker_pod_template` (resources, node selectors, volumes, …). Each
pod runs `sail worker` with environment variables carrying its worker ID and the
driver's external address.

---

## 4. Examples

### 4.1 Run a local cluster

```bash
SAIL_MODE=local-cluster \
SAIL_CLUSTER__WORKER_INITIAL_COUNT=2 \
SAIL_CLUSTER__WORKER_TASK_SLOTS=4 \
sail spark server
```

```python
df = spark.range(10_000_000).selectExpr("id % 100 AS k", "id AS v")
df.groupBy("k").sum("v").collect()
```

### 4.2 The job graph for that query

Physical plan (simplified):

```text
AggregateExec(final, k)
  RepartitionExec(hash(k), 8)          ← stage boundary
    AggregateExec(partial, k)
      RangeExec(0..1e7, 8 partitions)
```

Job graph:

```text
=== stage 0 ===  mode=Pipelined partitions=8 distribution=Hash(k, 8 channels) placement=Worker
  AggregateExec(partial) ← RangeExec
=== stage 1 ===  inputs=[StageInput(stage=0, mode=Shuffle)] partitions=8 placement=Worker
  AggregateExec(final) ← ShuffleRead(stage 0, channel = my partition)
```

Because stage 0 → stage 1 is pipelined, both stages (16 tasks) form **one
region**. Both stages are in the default slot-sharing group, so partition *p* of
stage 0 and partition *p* of stage 1 share a slot: the region needs **8 slots**
at once. Two workers × 4 slots = 8 slots, so it fits without starting more
workers. With `WORKER_TASK_SLOTS=2`, the assigner would ask for two more workers.

### 4.3 Watching it

```python
spark.sql("SELECT * FROM system.execution.stages").show(truncate=False)
spark.sql("SELECT * FROM system.cluster.workers").show()
```

### 4.4 A failure, step by step

A two-stage pipelined job is running when task (stage 1, partition 3) fails
with an error:

```text
t0  task (s1, p3, attempt 0) reports FAILED
t0  scheduler: region R0 has a failed task with 1 attempt < 3  → region not failed yet
t0  scheduler: create attempt 1 for every task in R0 (both stages, all partitions)
t0  assigner: queue R0's task sets again with the new attempts
t1  new attempts run; streams use new keys (attempt = 1), so no data mixes with attempt 0
t2  all tasks of attempt 1 succeed → region succeeded → job succeeded
```

If the same task fails on attempts 0, 1, and 2, the region is marked failed and
the job fails with the last error.

### 4.5 A lost worker, step by step

```text
t=0s     worker W3 sends its last heartbeat, then its pod is evicted
t=120s   heartbeat timeout probe fires: no heartbeat since t=0s → W3 is LOST
         - W3 is stopped and removed from the slot pool
         - every task assigned to W3 is marked FAILED ("task failed for lost worker")
         - affected jobs are refreshed → their regions are rescheduled with new attempts
         - the assigner asks for a replacement worker if slots are now short
```

On this branch, a lost worker counts as a normal task failure (toward the 3
attempts). The `disk-shuffle-hardening` branch treats it as recoverable instead
(see [10](10-shuffle-and-streams.md)).

---

## 5. End-to-End: One Query in Cluster Mode

| # | Component | What happens |
|---|-----------|--------------|
| 1 | Cluster job runner | Sends *execute job (plan, task context)* to the driver and waits for a stream. |
| 2 | Job scheduler | Builds the job graph (stages, inputs, distributions, placements), builds the topology (regions and dependencies), and registers the job. |
| 3 | Job scheduler | Creates attempt #0 for the tasks of eligible regions and queues them. |
| 4 | Task assigner | Tries to place the first region into free slots. The driver already started `worker_initial_count` workers when it was created. If the region doesn't fit, it computes how many more workers are needed. |
| 5 | Worker pool | Starts extra workers if needed. They register and start heartbeats. Each registration triggers another assignment pass. |
| 6 | Driver | For each placed task, builds the task definition (serialized plan, input locators, output kind) and sends *run task* to the worker. |
| 7 | Workers | Decode and rewrite the plan, start the tasks, and report *running*. Upstream tasks write channels to their stream manager. Downstream tasks fetch them (see [10](10-shuffle-and-streams.md)). |
| 8 | Driver | Opens the final stage's outputs and returns the merged stream to the job runner, which returns it to the protocol layer. |
| 9 | Workers | Report *succeeded*. The region and job succeed. Stages whose consumers are done are cleaned up. |
| 10 | Idle workers | Stopped after 60 seconds without tasks. |
| On failure | | A failed task fails its region. The region is rescheduled with new attempts, up to 3. A lost worker fails all of its tasks. |

---

## 6. Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Job fails with "task scheduling timeout" | A region needs more slots than the cluster can provide within 120 seconds (`worker_max_count × worker_task_slots`, or workers start too slowly) | Raise `worker_max_count` or `worker_task_slots`, reduce partitions (`execution.default_parallelism`), or raise `cluster.task_launch_timeout_secs`. |
| Small queries wait behind a big one | Head-of-line blocking in the region queue | Run big jobs in separate sessions, which get separate drivers, or give the cluster more slots. |
| "task failed for lost worker" | Heartbeats stopped: pod killed, out of memory, network | Check the pod logs and memory. Bound the memory pool (see [06](06-session-and-runtime.md)). |
| "repartition is order-preserving …" error | An order-preserving repartition can't be distributed yet | Rewrite the query, or run locally. |
| Works in `local`, fails in `local-cluster` | An operator or UDF can't be serialized by the plan codec, or a function is missing in the worker session | Add codec support. Check the worker session's functions. |

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Job runners (local and cluster) | `crates/sail-execution/src/job_runner.rs` |
| Job graph and builder | `crates/sail-execution/src/job_graph/` (`mod.rs`, `planner.rs`) |
| Driver actor and messages | `crates/sail-execution/src/driver/actor/`, `driver/event.rs` |
| Job scheduler, topology, state | `crates/sail-execution/src/driver/job_scheduler/` |
| Task assigner (slots, scaling) | `crates/sail-execution/src/driver/task_assigner/` |
| Worker pool | `crates/sail-execution/src/driver/worker_pool/` |
| Job output stream | `crates/sail-execution/src/driver/output.rs` |
| Worker actor | `crates/sail-execution/src/worker/` |
| Task runner and monitor | `crates/sail-execution/src/task_runner/` |
| Task definitions and regions | `crates/sail-execution/src/task/` (`definition.rs`, `scheduling.rs`) |
| Worker managers (local, Kubernetes) | `crates/sail-execution/src/worker_manager/` |
| Plan codec (operators, UDFs, sources ↔ protobuf) | `crates/sail-execution/src/codec.rs`, `proto/` |
| gRPC services and clients | `crates/sail-execution/src/driver/server.rs`, `client.rs`, `worker/`, `rpc.rs` |
