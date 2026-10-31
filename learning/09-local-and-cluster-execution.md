# 09 · Local and Cluster Execution

Everything before this point produces one thing: a DataFusion physical plan.
This doc is about running it. Sail can run a plan inside a single process or
spread it across a driver and many workers, and the code that asked for the
result can't tell the difference. Here's how both modes work, how a plan is cut
into stages and tasks, how tasks get scheduled onto workers, and what happens
when things fail.

The data exchange between tasks (streams and shuffles) gets its own doc,
[10](10-shuffle-and-streams.md).

---

## One small interface: the job runner

The boundary between planning and execution is deliberately tiny. A **job
runner** takes a physical plan and a session and returns a stream of Arrow record
batches. It can also be stopped, at which point it hands back a history of what
it ran. That's the whole contract.

There are two job runners. The **local** runner wraps every operator in the plan
with lightweight tracing (so execution time and row counts reach telemetry) and
asks DataFusion to execute it in the current process, using all CPU cores.
DataFusion is already an excellent single-machine engine, so local mode adds
almost nothing on top.

The **cluster** runner sends the plan to a **driver**, which cuts it into pieces,
runs those pieces on **workers**, and streams the final result back. Because
both runners return the same kind of stream, the Spark Connect and Flight SQL
front ends never need to know which mode is in use. Moving from a laptop to a
cluster is a configuration change.

## Cutting a plan into stages

A distributed engine has to move data between machines wherever a computation
needs rows that live elsewhere. DataFusion's physical plans already mark exactly
those places, because they need them for multi-threaded execution: a
**repartition** (redistribute rows by hash or round-robin), a **coalesce** (merge
many partitions into one), and an **order-preserving merge** (combine sorted
partitions into one sorted stream). Sail's job graph builder walks the plan and
cuts it at those operators. Each piece becomes a **stage**, and each stage runs
as one **task** per partition. A task's output is divided into **channels**, one
per downstream consumer partition.

Take a simple grouped count:

```text
Aggregate (final) [k]
  Repartition hash(k, 8)                ← cut here
    Aggregate (partial) [k]
      Range 0..10,000,000 (8 partitions)
```

The builder produces two stages. Stage 0 contains the range and the partial
aggregation, with its output hashed by k into 8 channels. Stage 1 contains the
final aggregation and reads, for each of its 8 partitions, the matching channel
from every stage-0 task.

How a stage reads its input depends on the operator that caused the cut. After
a hash or round-robin repartition, each downstream partition reads one channel
from every upstream partition, which is a classic shuffle. After an
order-preserving merge, the downstream task reads everything from every upstream
partition and merge-sorts it. And when a join needs a complete copy of one side
on every partition, as the build side of a "collect left" hash join or a
nested-loop join does, that side is *broadcast*.

Some work must happen on the driver itself, because only the driver has the
session's catalogs and state. Catalog commands and system table scans are placed
in stages that run on the driver.

Before building stages, two safety rewrites run. A global LIMIT must see a
single input partition, so the builder ensures one. And a collect-left hash join
whose build side can produce unmatched rows (right, full, and similar joins) is
turned into a partitioned join. Otherwise, every worker holding a copy of the
build side would emit those unmatched rows again, and the result would contain
duplicates.

## The driver and its helpers

The driver is an actor (see [02](02-entry-points-and-server.md)) with four
helpers. The **job scheduler** tracks jobs, stages, tasks, and attempts, decides
what is ready to run, and prepares each task's definition: the serialized plan
fragment, where its inputs come from, and where its output goes. The **task
assigner** keeps the queue of work waiting for capacity, knows which slots are
free on the driver and on each worker, and calculates how many workers to ask
for. The **worker pool** manages worker lifecycles: starting them, registering
them, watching heartbeats, and stopping them. A **task runner** inside the driver
runs the few tasks placed on the driver.

Every change, such as a new job, a worker registering, a heartbeat, or a task
reporting its status, arrives as a message to the driver, and the driver handles
them one at a time. Tasks move from created to scheduled to running to succeeded
(or failed or canceled). Stages are active while their tasks run or their output
is still being read, and inactive afterwards. Jobs run, drain, and end as
succeeded, failed, or canceled.

## Why connected stages are scheduled together

In the default **pipelined** mode, stages don't take turns. A downstream task
reads its inputs while upstream tasks are still producing them, streaming data
through memory (see [10](10-shuffle-and-streams.md)). That gives low latency, but
it means a consumer can't make progress without its producers, and producers
can't finish without consumers reading. So all the tasks of a connected group of
pipelined stages must start together. The scheduler calls such a group a
**region**.

Capacity is measured in **slots**: each worker offers cluster.worker_task_slots
of them, eight by default. Tasks from different stages of a region can share a
slot. By default, partition 3 of stage 0 and partition 3 of stage 1 go into the
same slot. A region therefore needs about as many slots as its *widest* stage,
not its total number of tasks. The grouped count above has 16 tasks but needs
only 8 slots.

When the stages in a region are linked one-to-one (partition p feeds partition p,
with no shuffle in between), the scheduler goes further and splits the region
per partition, so each chain can be scheduled on its own.

## Assigning work to slots

When a region becomes ready, its tasks join a queue. The assigner takes regions
from the front of the queue and tries to place **all** of a region's tasks into
free slots at once. If the region doesn't fit, it goes back to the front of the
queue and assignment pauses until more capacity appears. Keeping strict queue
order prevents starvation, at the price that one large region can hold up
smaller ones behind it. A task that waits longer than
cluster.task_launch_timeout_secs, 120 seconds by default, fails with "task
scheduling timeout".

Capacity grows on demand. When a session's driver starts, it immediately asks
for cluster.worker_initial_count workers, four by default. After that, whenever
queued work needs more slots than are free, the assigner divides the shortfall
by the slots per worker and asks the worker pool for that many more, up to
cluster.worker_max_count (zero means no limit). Workers that have had nothing to
do for cluster.worker_max_idle_time_secs, 60 seconds by default, are stopped. On
Kubernetes this means pods appear when a big query arrives and disappear
afterwards.

## What a worker does

A worker is also an actor, with its own gRPC server and its own stream manager.
When it starts, it registers with the driver and begins sending heartbeats.
When the driver sends it a task, the worker's task runner decodes the plan
fragment using Sail's plan codec, which knows how to serialize every Sail
operator, function, and data source. It adjusts Parquet scans so they handle
Delta column mapping and schema changes, and replaces the stage-input
placeholders with *shuffle read* operators pointing at the upstream tasks. It adds
a *shuffle write* operator at the root to send output to the right channels,
wraps everything with tracing, and starts executing. A small monitor drives the
task to completion and reports running, succeeded, or failed (with the error)
back to the driver. Status updates carry sequence numbers, so the driver can
ignore stale ones.

Workers are started by a *worker manager*. In local-cluster mode, it runs worker
actors inside the same process. In Kubernetes mode, it creates one pod per worker
from kubernetes.image in kubernetes.namespace, named with a configurable prefix
and owned by the driver's pod, so Kubernetes cleans them up if the driver goes
away. An optional pod template customizes resources, node selectors, and
volumes. Each pod runs `sail worker`, with environment variables telling it its
ID and how to reach the driver.

## Returning the result

The final stage's output is the job's result. The driver opens streams to the
final tasks' outputs, reading locally or fetching from workers, and merges them
into the single stream returned to the job runner. If the client goes away, the
driver notices that nobody is reading and stops forwarding output.

## When things fail

Failures are handled at the level of regions. If any task in a region fails, the
whole region is rescheduled with new attempts, up to cluster.task_max_attempts
(three by default). New attempts write to new stream keys, because the attempt
number is part of every stream's name, so their data never mixes with the failed
attempt's. Here's a failure that recovers:

```text
t0  task (stage 1, partition 3, attempt 0) reports FAILED
t0  region has a failed task, but fewer than 3 attempts → schedule attempt 1 for every task in the region
t0  the region's tasks are queued again with their new attempt numbers
t1  attempt 1 runs, reading and writing streams named with attempt = 1
t2  every task succeeds → the region succeeds → the job succeeds
```

If the same task fails on all three attempts, the region fails, and the job fails
with the last error.

Losing a worker looks different, because the worker can't report anything.
Instead, the driver notices the silence. If a worker hasn't sent a heartbeat for
cluster.worker_heartbeat_timeout_secs, 120 seconds by default, it's declared
lost. The driver stops it, marks every task that was assigned to it as failed
("task failed for lost worker"), refreshes the affected jobs so their regions are
rescheduled, and asks for a replacement worker if capacity is now short. A worker
that never registers within cluster.worker_launch_timeout_secs is treated as
having failed to start.

On this branch, a lost worker counts as an ordinary task failure toward the three
attempts. The disk-shuffle-hardening branch treats lost workers and lost shuffle
data as recoverable instead, and recomputes only what was lost (see
[10](10-shuffle-and-streams.md)).

## Following one job

Start a local cluster with two workers of four slots each, then run the grouped
count:

```bash
SAIL_MODE=local-cluster SAIL_CLUSTER__WORKER_INITIAL_COUNT=2 \
SAIL_CLUSTER__WORKER_TASK_SLOTS=4 sail spark server
```

```python
spark.range(10_000_000).selectExpr("id % 100 AS k", "id AS v").groupBy("k").sum("v").collect()
```

The cluster runner sends the plan to the driver. The scheduler builds the two
stages described earlier, sees that they're pipelined, and forms one region of 16
tasks needing 8 slots. The two workers started with the session offer exactly 8
slots, so the region fits. The driver sends each worker its task definitions. The
workers decode the fragments, start the stage-0 tasks writing hashed channels and
the stage-1 tasks reading them, and report progress. The driver merges the
stage-1 outputs into the result stream, the client receives its 100 rows, the
tasks report success, and the job ends. A minute later, the idle workers are
stopped. While this runs, `SELECT * FROM system.execution.stages` and
system.cluster.workers show what's happening.

---

## Common problems

"Task scheduling timeout" means a region couldn't get enough slots within 120
seconds, either because the cluster can never be big enough (raise
cluster.worker_max_count or cluster.worker_task_slots, or reduce
execution.default_parallelism) or because workers start slowly (raise the
timeout). Small queries waiting behind a big one are seeing head-of-line
blocking in the queue; running big jobs in separate sessions gives them separate
drivers. "Task failed for lost worker" means heartbeats stopped: check the pod's
logs and memory, and bound the memory pool (see [06](06-session-and-runtime.md)).
An error about an order-preserving repartition means the plan contains one, which
can't be distributed yet. And a query that works locally but fails in
local-cluster mode usually contains an operator or function the plan codec can't
serialize, or a function missing from the worker session.

## Where to look in the code

Everything here lives in crates/sail-execution. The job runners are in
src/job_runner.rs, and the job graph and its builder in src/job_graph/ (mod.rs and
planner.rs). The driver actor and its messages are under src/driver/actor/ and
src/driver/event.rs, the scheduler with its topology and state under
src/driver/job_scheduler/, the slot assigner under src/driver/task_assigner/, the
worker pool under src/driver/worker_pool/, and the job output stream in
src/driver/output.rs. The worker actor is under src/worker/, the task runner and
monitor under src/task_runner/, and task definitions and regions under src/task/.
Worker managers for local and Kubernetes modes are in src/worker_manager/. The
plan codec, which serializes operators, functions, and data sources, is
src/codec.rs, with its protobuf definitions under proto/.
