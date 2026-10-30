---
title: Architecture Deep Dive
rank: 3
---

# Sail Architecture Deep Dive

This guide explains how Sail works inside: what happens to a query from the
moment a client sends it until results come back, and why each piece is designed
the way it is. It's written for engineers who want to evaluate, debug, or extend
Sail.

Each section explains the idea in plain language first. File and type names
appear only in the **Where to look** table at the end of each section, so you
can read the explanations without knowing the code, then jump into the code
when you need to.

For a shorter, diagram-based overview, see [Architecture Overview](./architecture/).

## Sail In One Page

Sail is a query engine written in Rust that speaks Spark's language. A PySpark
program, a SQL client, or an Arrow Flight SQL client can connect to Sail as if
it were Spark. Sail executes the work on Apache DataFusion, a fast columnar
query engine, instead of the JVM.

Every query goes through the same five steps:

1. **Receive.** A protocol front end (Spark Connect, SQL, or Flight SQL) accepts
   the request.
2. **Translate.** The request is turned into Sail's own neutral description of
   the query, called the **spec plan**. SQL text and Spark DataFrame calls end
   up in the same form.
3. **Resolve.** The spec plan is checked against the session: tables are looked
   up in catalogs, columns are matched to schemas, and functions are found. The
   result is a DataFusion **logical plan**, which says *what* to compute.
4. **Plan.** The logical plan is optimized and turned into a **physical plan**,
   which says *how* to compute it: which operators, in which order, with how
   many partitions.
5. **Execute.** A **job runner** runs the physical plan, either inside the same
   process (local mode) or spread across a driver and many workers (cluster
   mode). Results stream back to the client as Arrow record batches.

The rest of this guide walks through those steps in order, then covers the
supporting systems: catalogs, table formats, storage, functions, and
observability.

### Key Terms

| Term | Meaning |
|------|---------|
| Session | A client's working context: its configuration, temporary views, catalogs, and the job runner it uses. |
| Spec plan | Sail's protocol-neutral description of a query or command. It still contains unresolved names. |
| Logical plan | A DataFusion plan with every name resolved. It describes what to compute. |
| Physical plan | A DataFusion plan of concrete operators. It describes how to compute. |
| Extension node | A Sail-specific operator for behavior DataFusion doesn't have built in. |
| Job runner | The component that executes a physical plan, either locally or on a cluster. |
| Driver and worker | In cluster mode, the driver coordinates and workers execute. |
| Stage | A part of a distributed job that can run without exchanging data. Stages are separated by shuffles. |
| Shuffle | Redistributing rows between stages, usually by hashing a key. |
| Actor | A component that owns its state and changes it only in response to messages, one at a time. |

## 1. How Requests Enter Sail

### The Program Entry Point

Sail ships as a single binary. When it starts, it reads its configuration and
decides what role to play: server, worker, or something else.

The binary has one unusual job: it can also behave like a **Python
interpreter**. Python user-defined functions sometimes need to start child
Python processes, and many Python libraries find "the Python executable" by
looking at the current program. So before Sail starts Python, it sets an
environment variable. If a child process starts with that variable set, the
Sail binary hands control straight to Python's own main function. To the
Python libraries, Sail looks like an ordinary Python executable.

Apart from that, startup is kept deliberately thin. It optionally installs a
faster memory allocator (mimalloc), then hands off to library code, where all
server and worker behavior lives.

### Spark Connect

Spark Connect is the protocol modern PySpark clients use to talk to a Spark
server over gRPC. Sail implements the server side, and its handlers are split by
purpose:

- **Execution requests** run a query or command and stream results back.
- **Analysis requests** ask questions about a plan without running it: its
  schema, its explain string, whether it's streaming, and so on.
- **Configuration and artifact requests** manage session settings and uploaded
  files.

When a client asks Sail to run a query:

1. The handler looks up the client's session and its job runner.
2. It converts the Spark request into a spec plan. Each request type has its own
   converter. A SQL string that turns out to be a command (such as
   `CREATE TABLE`) is executed right away, and its output is returned as a small
   in-memory table, which is how Spark behaves.
3. It resolves and plans the spec plan into a physical plan.
4. It hands the physical plan to the job runner, which returns a stream of
   results.
5. A response adapter wraps each result batch in the message format Spark
   Connect expects, adding session and operation IDs. For queries, batches are
   produced lazily as the client reads them. For commands, the stream is
   drained up front so the side effect is guaranteed to happen.

That last step is deliberately the only place that knows about Spark Connect's
message format. The execution engine underneath produces plain result batches
and could serve a different protocol with a different adapter.

Analysis requests follow the first part of the same path. They resolve the plan
just far enough to answer the question, for example to compute the output
schema, and then stop. A few Spark analysis features aren't supported yet:
`persist` and `unpersist` do nothing and log a warning, and some
semantic-comparison requests return "not implemented".

### Arrow Flight SQL

Flight SQL is an Arrow-native protocol for SQL clients. Sail runs a separate
Flight SQL service, but it reuses the same session, planning, and execution
machinery as Spark Connect. Adding a protocol means adding a front end, not a
second engine.

### Where to look

| Topic | Location |
|-------|----------|
| Binary entry point, embedded Python | `crates/sail-cli/src/main.rs` |
| Spark Connect handlers | `crates/sail-spark-connect/src/service/` (`plan_executor.rs`, `plan_analyzer.rs`) |
| Response adapter | `ExecutePlanResponseStream` in `plan_executor.rs` |
| Flight SQL server | `crates/sail-flight/src/lib.rs` |

## 2. Sessions And The Runtime Environment

### What a Session Contains

A session is everything Sail needs to serve one client. Sail builds on
DataFusion's session object and attaches its own pieces to it as
**extensions**: small, typed attachments that any code holding the session can
retrieve. The important ones are:

- the **Spark session** information (session ID, Spark settings),
- the **job service**, which provides the job runner,
- the **catalog manager**, which knows about databases, tables, and views,
- the **table format registry**, which knows how to read and write Parquet,
  Delta, Iceberg, and other formats.

Attaching these as extensions means Sail doesn't have to fork or wrap
DataFusion's types. Any code that has the session can find what it needs.

### Two Kinds of Sessions

Sessions are created by **session factories**, and there are two kinds:

| | Server session | Worker session |
|---|---|---|
| Who uses it | Client-facing requests | Tasks running on cluster workers |
| Holds user state (temporary views, settings) | Yes | No |
| Can submit jobs | Yes | No, it only runs tasks it's given |
| Resolves spec plans | Yes | No, it receives plans that are already planned |
| Needs storage, functions, and table formats | Yes | Yes |

If something works locally but fails only in cluster mode, a common cause is
that a feature was installed in the server session but not in the worker
session.

### The Runtime Environment

Each session also has a DataFusion **runtime environment**. It controls:

- **Memory.** The pool can be unbounded (the default), greedy (first come,
  first served up to a limit), or fair (a limit shared so no single operator
  takes everything). Bounded pools let operators spill to disk instead of
  running out of memory.
- **Temporary files.** These are the directories operators spill to.
  Configuring a maximum size of zero turns spilling off.
- **Object storage access**, through a registry that creates storage clients on
  demand (see [Object Storage](#7-object-storage)).
- **Caches** for file listings, file metadata, and file statistics.

Each cache can be turned off, shared across all sessions (global), or kept
per session:

| Cache scope | Good for | Trade-off |
|-------------|----------|-----------|
| Global | Many sessions reading the same stable datasets | Memory grows across sessions, and you need to think about stale entries. |
| Per session | Isolation, frequently changing data | Each new session fetches the same metadata again. |
| Off | Data that changes constantly | Every access goes back to storage, which is slow for large file sets. |

The factory takes an optional final adjustment step, so server and worker
sessions can share one set of defaults while still customizing details.

### Session Lifecycle

A **session manager** creates sessions on first use, returns the existing one
on later requests, cleans up idle sessions, and records history when sessions
end. It's an actor, so two requests racing to create or delete the same
session can't step on each other. If you need per-session cleanup that runs
exactly once, it belongs here.

### Where to look

| Topic | Location |
|-------|----------|
| Session factories | `crates/sail-session/src/session_factory/` (`server.rs`, `worker.rs`) |
| Runtime environment (memory, caches, spilling) | `crates/sail-session/src/runtime.rs` |
| Session lifecycle | `crates/sail-session/src/session_manager/` |
| Memory configuration | `runtime.memory_pool.*` and `runtime.temporary_files.*` in the application config |

## 3. From Request To Spec Plan

### Why Sail Has Its Own Plan Model

Before anything touches DataFusion, every request becomes a **spec plan**. The
extra step is there for five reasons:

1. **One path for every protocol.** SQL text and Spark DataFrame calls both
   become spec plans, so the complicated resolution logic is written once.
2. **Resolve later.** A spec plan records what the user asked for without
   needing a live catalog. Table names, column names, and function names stay
   as plain names until resolution.
3. **Easy to inspect.** Spec plans can be printed and compared, which makes it
   easy to tell whether a bug is in translation or in resolution.
4. **One shape for one meaning.** A SQL `WHERE x > 5` and a DataFrame
   `.filter(col("x") > 5)` produce the same spec node.
5. **Room for Spark-only features.** Pandas UDFs, streaming operations, and
   other Spark-specific constructs can be represented even when DataFusion has
   no equivalent.

A spec plan is either a **query**, which returns rows, or a **command**, which
has a side effect, such as creating a table, writing data, or registering a
function.

### How SQL Becomes a Spec Plan

SQL goes through two crates:

1. **Parsing.** The SQL parser splits the text into tokens and builds a syntax
   tree. The tree describes the *structure* of the SQL and nothing more. It
   doesn't know whether a table exists.
2. **Analysis.** The SQL analyzer walks the syntax tree and produces a spec
   plan. Statements are handled at the top level (queries, DDL, DML, utility
   commands), and query clauses and expressions are converted by their own
   modules.

The analyzer intentionally **doesn't resolve anything**. A column reference
`t.id` stays the name "t.id", `COUNT(*)` stays a call to a function named
"count", and `BETWEEN` stays a between-expression. Resolution needs things the
analyzer doesn't have, such as the current catalog, registered functions, and
table schemas. Keeping the analyzer free of those makes it simpler to test and
puts all resolution errors in one place.

When you add support for a Spark expression, make sure the SQL path and the
Spark Connect path produce the **same** spec expression. Otherwise the two
front ends will behave differently.

### Where to look

| Topic | Location |
|-------|----------|
| Spec plan model | `crates/sail-common/src/spec/` |
| SQL tokenizer and parser | `crates/sail-sql-parser/` |
| SQL entry helpers | `crates/sail-sql-analyzer/src/parser.rs` |
| Statements, queries, expressions | `crates/sail-sql-analyzer/src/` (`statement.rs`, `query.rs`, `expression.rs`) |

## 4. Resolution And Planning

### Resolution: Giving Names Meaning

The **plan resolver** turns a spec plan into a DataFusion logical plan. It
walks the plan node by node and:

- looks up tables through the catalog manager and table format registry,
- matches column references to the columns actually available at that point,
- finds functions in the function registry,
- converts Spark data types to Arrow data types and inserts casts,
- turns commands into Sail extension nodes that perform the side effect.

The resolver carries state as it goes. It tracks which internal column names
map to which user-visible names, the schemas of outer queries (for correlated
subqueries), common table expressions, and which columns are hidden.

**Internal names and hidden columns.** During resolution Sail gives columns
unique internal names, so that two columns both called `id` (from a self-join,
say) can never be confused. It also sometimes adds **hidden columns** to carry
extra information through the plan, for example which side of an outer join
produced a row, the position inside an exploded array, or a row's source file.
At the end:

- hidden columns are removed from the output, and
- the original user-facing names are restored.

The result of resolution is a **named plan**: the logical plan plus the list of
names clients should see. If a client gets the wrong column names back, start
by checking this name mapping.

As a safety net, after resolving each query node the resolver checks that every
output column is one it knows about. If a new feature adds a column without
registering it, resolution fails right away instead of producing confusing
results later.

### Optimization

After resolution, DataFusion's logical optimizer rewrites the plan, for example
by pushing filters down and removing unused columns. Sail adds a few rules of its
own where Spark compatibility or Sail's extension nodes require it. This step
is intentionally small. Most meaning is settled during resolution, and most
performance work happens in physical optimization.

### Physical Planning

DataFusion knows how to plan standard operators such as scans, filters, joins,
and aggregates. For Sail's extension nodes, Sail registers its own planners,
which DataFusion asks in this order:

1. **Lakehouse planners** for Delta Lake and Iceberg operations, such as
   row-level `DELETE`, `UPDATE`, and `MERGE`. They go first because these
   operations need format-specific plans.
2. **The system table planner**, for tables that expose Sail's own runtime
   state.
3. **The general extension planner** for everything else Sail adds: range
   generation, `show()` output, Python map-partitions, monotonically increasing
   IDs, Spark partition IDs, file writes and deletes, streaming operators,
   catalog commands, and barriers.

Each planner either handles a node or passes it on. If nobody handles a node,
planning fails. So **every new extension node needs a physical planning case**.

### Why Sail Needs Extension Nodes

DataFusion's built-in operators don't cover everything Spark does:

- **Spark-specific behavior.** `range` with a set partition count,
  `monotonically_increasing_id()`, and `spark_partition_id()` behave in ways
  defined by Spark.
- **Format-aware writes.** Writes have to respect partitioning, bucketing, sort
  order, and each format's commit protocol.
- **Python execution.** Running Python over batches of Arrow data needs a
  custom operator.
- **Streaming.** Spark Structured Streaming sources and sinks don't fit
  DataFusion's batch model.
- **Catalog commands.** `CREATE TABLE` and similar commands change the catalog
  rather than return data.

Because these are extension nodes, they still go through DataFusion's
optimizer like any other operator.

### Physical Optimization

Last, physical optimizer rules adjust the plan for execution: repartitioning,
sort requirements, join strategy, pushing projections and limits down, and
constraints specific to cluster execution. Sail also includes **cost-based join
reordering**, which picks a join order that keeps intermediate results small.
This matters a lot for complex SQL.

### Where to look

| Topic | Location |
|-------|----------|
| Plan resolver | `crates/sail-plan/src/resolver/` (`mod.rs`, `plan.rs`, `query/`, `expression/`, `command/`) |
| Physical planner registration | `crates/sail-session/src/planner.rs` |
| Sail logical and physical extension nodes | `crates/sail-logical-plan/`, `crates/sail-physical-plan/` |
| Optimizer rules | `crates/sail-logical-optimizer/`, `crates/sail-physical-optimizer/` (including `join_reorder/`) |

## 5. Execution

### Job Runners: One Interface, Two Modes

Planning ends with a physical plan, which is handed to a **job runner**. A job
runner has one job: take a physical plan and return a stream of results. There
are two:

- **The local job runner** runs the plan in the current process using
  DataFusion's normal multi-threaded execution. It first wraps every operator
  with lightweight tracing, so execution time and row counts reach Sail's
  telemetry.
- **The cluster job runner** sends the plan to a **driver**, which splits it into
  stages, runs the stages on **workers**, and returns the final results as a
  stream.

Both return the same kind of stream, so protocol handlers never need to know
which mode is in use. Switching between local and cluster mode is a
configuration change, not a code change.

### Cluster Mode Components

Cluster mode is built from actors. Each component owns its state and changes it
only while handling one message at a time. That removes the need for locks
spread across the code, makes the set of possible events explicit, and keeps
shutdown orderly.

| Component | Role |
|-----------|------|
| Driver | Coordinates the cluster. It receives jobs, tracks job, stage, and task state, and assembles the result stream. |
| Job graph | A fixed description of a job: which stages exist, how they depend on each other, and how data moves between them. It's built once from the physical plan. |
| Job scheduler | Tracks progress through the job graph: which stages are ready, running, finished, or failed. It also makes the runtime decisions described in the next section. |
| Task assigner | Chooses which worker runs each ready task. |
| Worker pool | The driver's view of the workers: which have registered, which are healthy, and which have been lost. |
| Worker | Runs the tasks it's given and exchanges data with other workers. |
| Task runner | Inside a worker, runs a single task's piece of the plan and reports when it succeeds or fails. |
| Streams | The data layer that moves Arrow batches between tasks and back to the driver. |
| Worker manager | Starts workers, either as local processes or as Kubernetes pods. |

The **job graph** is the plan of a job (the *what*) and the **job scheduler**
is its progress tracker (the *when*). Keeping them separate lets the same job
graph support different scheduling policies.

A task can be ready while no worker is free, and a worker can be free while no
task is ready. Because task state and worker state are tracked separately, it's
easier to tell which situation you're in.

### Shuffle Modes, Adaptive Coalescing, And Fault Tolerance

::: warning Branch scope
This section describes the `disk-shuffle-hardening` branch (commits `e6a7bc25`
to `7de2cc6c`). Without those commits, the disk-based mode can't be turned on,
the `cluster.shuffle_*` settings don't exist, and a lost worker isn't recovered.
See
[Implementation Status](./architecture/adaptive-disk-shuffle-design#_2-implementation-status-read-this-first).
:::

#### Two ways to pass data between stages

When one stage's output feeds the next, Sail can move the data in two ways. You
choose with the `cluster.shuffle_mode` setting. The setting is experimental.

- **Pipelined (the default).** Both stages run at the same time, and data flows
  from producer to consumer in memory and over the network as it's produced.
  This gives the lowest latency, but every stage in the pipeline has to be
  running and holding memory together. If one task fails, the whole connected
  pipeline has to restart.
- **Blocking.** Each producer task writes its output to local disk and exits.
  The next stage starts only after every producer has finished. This adds disk
  I/O, but it frees memory and task slots between stages. It also unlocks two
  features: resizing the next stage based on how much data was actually
  produced, and recovering from a lost worker by recomputing only what was lost.

Pipelined suits interactive, small-to-medium queries. Blocking suits large
batch jobs, memory-constrained clusters, and any job that has to survive losing
a worker.

#### How shuffle data is stored on disk

In blocking mode, every task attempt writes **one file per output partition**
(called a *channel*). Each file has a tiny companion index file that records
how many bytes and rows it holds.

Files are written under temporary names and renamed into place only when
complete, so a reader never sees a half-written file. A partition that receives
no rows gets an index file but no data file. When a downstream task finds
**neither** file, it reports an error instead of silently reading nothing. That
error is how the driver learns the output was lost and needs to be recomputed.

Every driver and worker writes into its own uniquely named directory under
`cluster.shuffle_dir`, which defaults to the system temporary directory, so two
Sail processes on the same machine can never clash. A stage's files are deleted
as soon as every stage that reads them has finished, the job's files are deleted
when the job ends, and each process deletes its whole directory when it stops.

#### Adaptive coalescing: right-sizing the next stage

The number of partitions is chosen when the query is planned, before Sail
knows how much data each partition will actually get. After a selective filter,
you can end up with hundreds of nearly empty partitions, each costing a task
launch.

In blocking mode, the driver fixes this at runtime. When a task finishes, its
worker reports how many bytes and rows it wrote to each partition. Before the
next stage starts, the driver adds these numbers up and **merges neighboring
small partitions** into fewer, larger ones, aiming for about 64 MiB each. You
can change the target with `cluster.adaptive_shuffle_target_partition_bytes`.

Two safeguards keep this from going too far:

- **Keep the cluster busy.** The driver won't create fewer partitions than the
  cluster has task slots, unless that would make partitions smaller than 1 MiB.
  Merging everything into a few big partitions would leave most of the cluster
  idle on CPU-heavy work. Spark's adaptive execution behaves the same way in its
  "parallelism first" mode.
- **Only merge when it's safe.** The driver skips merging when the stage reads
  its input in a way tied to the original partitioning, when any statistics are
  missing, or when the inputs don't line up. Inputs that are broadcast to every
  task aren't affected. The decision is made once, before the stage starts, and
  is never revisited.

Sail also **detects skew**: a partition more than five times the median size
and at least 128 MiB. Today it only logs skewed partitions. It doesn't yet split
them or switch join strategies.

#### Surviving failures

| What went wrong | How Sail notices | What Sail does |
|-----------------|------------------|----------------|
| A task fails with an error | The worker reports the failure | Retries the task, up to `cluster.task_max_attempts` times (3 by default). |
| A worker dies or stops responding | The worker can't start a task, or misses heartbeats for 2 minutes | Marks the worker lost. Reruns its running tasks, **and** reruns finished tasks whose output was stored on that worker. |
| A task can't read its input | The read fails with a *shuffle fetch error* that names the producer task and worker | Reruns the producer, then retries the reader. If the producer's stage had already been cleaned up, the whole stage is rerun. |
| The final stage is retried while results are streaming | Errors in the result stream | Waits for the retried stage to provide a new stream instead of failing the job. |

Failures caused by a lost worker or lost data aren't the task's fault, so they
don't count against the normal retry limit. They have their own, higher cap of
16 attempts, so a job can't keep retrying forever.

### Where to look

| Topic | Location |
|-------|----------|
| Job runners | `crates/sail-execution/src/job_runner.rs` |
| Driver, scheduler, assigner, worker pool | `crates/sail-execution/src/driver/` |
| Coalescing logic | `try_coalesce_stage` in `driver/job_scheduler/core.rs`, `driver/job_scheduler/adaptive.rs` |
| Workers and task runners | `crates/sail-execution/src/worker/`, `task_runner/` |
| Shuffle files on disk | `DiskStream` in `crates/sail-execution/src/stream_manager/local.rs` |
| Shuffle statistics | `crates/sail-execution/src/task/stats.rs` |
| Shuffle fetch errors | `ShuffleFetchError` in `crates/sail-common-datafusion/src/error.rs` |
| Worker provisioning | `crates/sail-execution/src/worker_manager/` (local, Kubernetes) |
| Settings | `cluster.*` in `crates/sail-common/src/config/application.yaml` |

## 6. Catalogs

### What a Catalog Does

A catalog answers questions like "which databases exist?" and "where is table
`sales.orders` stored, and what's its schema?" Sail supports several catalog
back ends: an in-memory catalog, a system catalog, AWS Glue, Hive Metastore,
Iceberg REST, Unity Catalog, and Microsoft OneLake.

Every back end implements the same small interface: create, get, list, alter,
and drop for databases, tables, and views. Back ends only translate these calls
to their own API. They don't decide how names are interpreted.

### Name Resolution Lives in One Place

The **catalog manager** in each session interprets names. A one-part name like
`orders` means "the default catalog and default database". A longer name like
`glue.sales.orders` uses the first part as a catalog if a catalog with that name
is registered. Databases can have multi-level names, which some catalogs
support.

The catalog manager also keeps things external catalogs can't store: temporary
views, and functions or plans that a Spark Connect client registers at runtime
and refers to later by ID.

### Layered Behavior

Two features apply to many back ends, so they're added as **wrappers** around a
back end instead of being built into each one:

- a **caching layer**, which remembers database, table, and view lists to avoid
  repeated remote calls,
- a **runtime layer**, which runs the back end's network calls in the right
  execution context.

A typical remote catalog is set up as *caching → runtime → Glue (or another
back end)*. Each back end stays focused on its own API, each layer can be
tested on its own, and the configuration decides which layers to use. An
in-memory catalog doesn't need either one.

### Where to look

| Topic | Location |
|-------|----------|
| Catalog interface | `crates/sail-catalog/src/provider/mod.rs` |
| Name resolution, temporary objects | `crates/sail-catalog/src/manager/mod.rs` |
| Caching and runtime wrappers | `crates/sail-catalog/src/provider/` (`cache.rs`, `runtime.rs`) |
| Back ends | `crates/sail-catalog-*` (memory, system, glue, hms, iceberg, unity, onelake) |

## 7. Data Sources, Table Formats, And Storage

### One Interface for Every Format

Reading and writing go through a common **table format** interface. A format
has a name and knows how to create a reader and a writer. It can also support
row-level changes (`DELETE`, `UPDATE`, `MERGE`) and table alterations. Formats
are kept in a registry by name, so the planner can ask for "parquet" or "delta"
without the code having to list every format.

A read is described by its paths, optional schema, partitioning, bucketing,
sort order, and options. A write is described by its input plan, mode
(append, overwrite, and so on), partitioning, and options.

**Options keep track of where they came from.** An option can come from table
properties, from the query (`OPTIONS (...)`), from the table location, or from
time-travel settings. Sail keeps these as separate layers instead of merging
them into one map early, so a format can tell a table-level setting apart from
a per-query override when that matters.

### Built-in File Formats

Sail includes Arrow, Avro, binary, CSV, JSON, Parquet, and text files, plus
streaming-oriented sources and sinks (console, rate, socket) and a Python data
source. For plain files, a **listing** step finds which files belong to a scan,
using cached directory listings where possible.

### Lakehouse Formats: Delta Lake and Iceberg

Delta Lake and Iceberg aren't just "Parquet with extras", so each has its own
crate. They need:

- **A metadata log.** Which files belong to the table is recorded in a
  transaction log or in manifests, not found by listing a directory.
- **Atomic commits.** A write must create its data files *and* commit a log
  entry, and has to cope with concurrent writers.
- **Schema evolution.** The table's schema can change over time, and old data
  must still be readable.
- **Row-level changes.** Deletes and updates either rewrite files or record
  deletion vectors.
- **Time travel.** You can read the table as of an earlier version or point in
  time.

Each lakehouse crate is split into a read side (building scans from snapshots),
an operations side (writes and commits), and the snapshot and transaction
mechanics. The lakehouse planners mentioned in
[Physical Planning](#physical-planning) connect these to query planning.

During row-level operations, Sail carries a few internal columns through the
plan, such as each row's source file, its position in the file, and the kind of
change. That's how writers know exactly which rows to rewrite or mark as
deleted.

### Object Storage

Sail reads and writes local files, S3, Azure, GCS, HTTP, HDFS, and other
storage through one **dynamic registry**. You don't configure every bucket ahead
of time. When a plan touches a URL such as `s3://analytics/...`, the registry
checks whether it already has a client for that scheme and bucket. If it does,
it reuses the client. If not, it creates one from configuration and keeps it for
later.

That means one query can read from S3 and write to GCS without extra setup.
Shared behavior such as logging or lazy start-up is added as layers around the
storage clients, the same way catalogs are layered.

### Where to look

| Topic | Location |
|-------|----------|
| Table format interface, read and write descriptions, option layers | `crates/sail-common-datafusion/src/datasource.rs` |
| Built-in formats and listing | `crates/sail-data-source/src/` (`formats/`, `listing/`, `options/`) |
| Delta Lake | `crates/sail-delta-lake/src/` (`datasource/`, `operations/`, `kernel/`) |
| Iceberg | `crates/sail-iceberg/src/` (`datasource/`, `operations/`) |
| Lakehouse planners | `crates/sail-plan-lakehouse/` |
| Object storage | `crates/sail-object-store/` |

## 8. Functions And Python

### Built-in Functions

Sail implements Spark's built-in scalar, aggregate, window, and table functions.
They're grouped by area: strings, dates and times, arrays, maps, math, JSON,
CSV, XML, URLs, variants, geospatial, and more. They're registered when a
session is created, and resolution finds them by name. To add a Spark function,
implement it and make sure the resolver can find it under Spark's name. The
compatibility data under `python/pysail/data/compatibility/functions` records
which functions are supported.

### Python UDFs

PySpark users can register Python functions at runtime. Sail records them
through the catalog manager. During resolution, Python functions that work on
one value at a time (and pandas UDFs) become function expressions. Functions
that work on whole partitions, such as `mapInPandas` or `mapInArrow`, become
the **map-partitions** operator. Either way, Sail calls into an embedded Python
interpreter and passes data as Arrow batches. This is why the Sail binary can
act as a Python interpreter (see
[The Program Entry Point](#the-program-entry-point)).

There are two separate Python crates:

- the **Python UDF** crate runs Python code inside query plans,
- the **Python bindings** crate lets Python programs use Sail as a library
  (`pysail`).

### Where to look

| Topic | Location |
|-------|----------|
| Built-in functions | `crates/sail-function/` |
| Python UDF execution | `crates/sail-python-udf/`, `crates/sail-plan/src/resolver/query/udf.rs` |
| Python bindings | `crates/sail-python/` |

## 9. Telemetry And Server Infrastructure

Spark Connect, Flight SQL, the driver, the workers, and the session manager all
share one set of gRPC server and actor building blocks, so each service doesn't
reinvent them.

Telemetry uses OpenTelemetry. Traces, metrics, and logs can be exported to any
OTLP endpoint, and each one can be turned on separately. In local mode, every
physical operator is wrapped with tracing that records the job, stage, attempt,
and operator, so you can see where time goes.

| Topic | Location |
|-------|----------|
| Shared server and actor framework | `crates/sail-server/` |
| Telemetry | `crates/sail-telemetry/`, `telemetry.*` settings |

## 10. A Query, End To End

Here's what happens when a PySpark client runs
`spark.table("sales.orders").filter("amount > 100").groupBy("region").count()`
against a Sail cluster:

| Step | What happens |
|------|--------------|
| 1. Receive | The Spark Connect handler gets the request and finds the client's session. |
| 2. Translate | The Spark request becomes a spec plan: *read table "sales.orders"*, *filter*, *group by region*, *count*. Nothing has been looked up yet. |
| 3. Resolve | The catalog manager resolves `sales.orders` to a catalog, database, and table. The table format registry creates a reader for its format. `amount` and `region` are matched to real columns, and `count` is found in the function registry. |
| 4. Optimize | The filter is pushed down toward the scan, and unused columns are dropped. |
| 5. Physical plan | A scan, filter, partial aggregate, shuffle by `region`, and final aggregate. |
| 6. Execute | The cluster job runner sends the plan to the driver. The driver splits it at the shuffle into two stages. Stage 1 (scan, filter, partial count) runs on many workers. Stage 2 (final count) reads Stage 1's output. In blocking mode, Stage 2 may first be coalesced into fewer partitions. |
| 7. Respond | The final result batches stream back through the driver. The response adapter wraps them as Spark Connect messages and restores the user-facing column names. |

A SQL query follows the same path from step 3 on. Only steps 1 and 2 differ:
the SQL text is parsed and analyzed into the same kind of spec plan.

## 11. Extending Sail

**Adding a catalog back end.** Implement the catalog interface for your back
end, translating its databases, tables, and views into Sail's catalog types. Add
configuration and register it in session setup, and decide whether it needs the
caching and runtime wrappers. Don't put name resolution in the back end. It
receives names that the catalog manager has already split up.

**Adding a table format.** Implement the table format interface: a name, a
reader, and a writer. Add row-level writes, merge support, or table alterations
only if the format supports them. Register the format during session setup. Use
the shared read and write descriptions and option layers instead of building
your own way of handling options.

**Adding a new kind of operation.** Define a logical extension node, make the
resolver produce it from the right spec plan, add a physical planning case, and
implement the physical operator. Make sure the resolver registers every new
output column. Otherwise its safety check will fail.

**Changing cluster behavior.** First decide which layer owns the change: the job
graph, the scheduler, task assignment, the worker pool, workers, task running,
or data exchange. Keep the job runner interface the same, so protocol handlers
never need to know whether a plan runs locally or on a cluster.

## 12. Debugging Guide

| Symptom | Where to look |
|---------|---------------|
| SQL fails to parse | The SQL parser and the parser helpers in the SQL analyzer |
| SQL parses but means the wrong thing | The SQL analyzer's statement, query, and expression conversion |
| A DataFrame call behaves differently from the same SQL | Compare the spec plans the two produce. They should match. |
| Table or column not found | Catalog manager name resolution, the catalog back end, resolver state |
| Table found but returns the wrong data | The table format's read path, the object storage registry |
| Logical plan is fine but physical planning fails | Missing physical planning case for an extension node |
| Fails only in cluster mode | Whether the worker session has the same extensions as the server session, and whether every operator in the plan can be serialized |
| Client sees wrong column names | The named plan's user-facing names, hidden column removal |
| Logs show "shuffle fetch failed" | A producer's output couldn't be read. Check whether its worker was lost or its files were deleted. Sail retries automatically (up to 16 times). |
| Logs show "shuffle output not found" | Shuffle files were deleted or lived on a lost worker. Confirm that the stage was rerun. |
| A blocking stage wasn't coalesced | Turn on debug logs for the job scheduler and look for "skip adaptive coalescing" (missing statistics) or input modes that block merging. |
| Too few, slow tasks after coalescing | Lower `cluster.adaptive_shuffle_target_partition_bytes`, or add task slots per worker. |
| Disk fills up during large shuffles | Point `cluster.shuffle_dir` at a large local SSD (on Kubernetes, a properly sized `emptyDir`). |
| Worker killed for running out of memory | The memory pool defaults to unbounded. Use the `fair` pool with an explicit maximum so operators spill. |

**General tips:**

- Run `EXPLAIN` to see the logical and physical plans and find where the plan
  differs from what you expected.
- Log the spec plan to tell a translation bug from a resolution bug.
- In cluster mode, check in this order: did the driver build the stages you
  expected, did workers receive the tasks, and did workers connect to each other
  to exchange data?

## 13. Production Readiness Roadmap

This section looks at where Sail stands for running production batch workloads
in cluster mode, and lays out a plan to close the gaps.

### What "production ready" means

1. **Correct.** Results match Spark. If a feature isn't supported, Sail fails
   with a clear message instead of returning a different answer.
2. **Resilient.** Losing a worker, having a pod preempted, or filling a disk
   slows a job down but doesn't fail it.
3. **Bounded.** Memory and disk use stay within configured limits, and operators
   spill instead of crashing the process.
4. **Operable.** Operators can see what a job is doing, why it's slow, and why
   it failed, without reading the source code.

### Where Sail is today

| Area | Today | Biggest gap |
|------|-------|-------------|
| Spark compatibility | Most tracked Spark functions are supported. | About 50 functions are planned or only partially supported, and there's no ongoing automated comparison against Spark. |
| Shuffle | In-memory (pipelined) by default. Disk-based (blocking) mode is experimental. | Blocking mode writes one file per producer task per partition, and output is lost along with its worker. |
| Adaptive execution | Merges small partitions, with safeguards. | Skew is only detected, not fixed. Join strategy doesn't change at runtime. |
| Fault tolerance | Task retry, recovery from lost workers, and recompute of lost output. | No driver failover, no speculative execution, and pipelined mode restarts whole pipelines. |
| Memory | Bounded memory pools are available. | The default pool is unbounded and isn't sized from the container, so one large query can take down a worker. |
| Kubernetes | Workers run as pods, with templates and TLS. | Shuffle data lives on pod-local storage. There's no defined behavior for pod disruption and no autoscaling. |
| Observability | OpenTelemetry traces, metrics, and logs. | No job or stage UI. Shuffle, spill, retry, and coalescing activity aren't exported as metrics. |

### Phase 1: Safe by default

| Work | Done when |
|------|-----------|
| **Bounded memory by default.** Use the fair memory pool in cluster mode, sized from the container's memory limit, with spilling for sort, aggregate, and join. | TPC-DS at scale factor 1000 finishes with no worker killed for running out of memory, at default settings. |
| **Promote blocking shuffle.** Make it the default for multi-stage batch jobs, keeping pipelined for interactive queries. | Blocking mode passes the full test suite and is within 10% of pipelined on TPC-H and TPC-DS. |
| **Disk management.** Support several shuffle directories, check free space, clean up directories left by crashed processes, and fail clearly when the disk is full. | A test that fills the disk ends in a clean failure or retry, never a hang. |
| **Automated correctness checks.** Run the PySpark and Spark SQL test suites, plus a nightly random-query comparison against Spark, covering NULLs, decimals, time zones, and ANSI mode. | 30 days without an unexplained mismatch. |

### Phase 2: Scale and resilience

| Work | Done when |
|------|-----------|
| **Fewer shuffle files.** Write one data file plus an offset index per producer task instead of one file per partition. | A 10,000 × 10,000 partition shuffle runs without running out of file handles. |
| **Shuffle that survives pod loss.** Add a per-node shuffle service that outlives worker pods, or support writing shuffle data to object storage. | Preempting 20% of worker pods mid-job doesn't rerun stages that already finished. |
| **Skew handling.** Split oversized partitions across several tasks. | A join where one key holds half the rows finishes within 2× the time of an evenly distributed join. |
| **Runtime join selection.** Switch to a broadcast join when the runtime statistics show one side is small. | Measurable speedup on TPC-DS queries with selective filters. |
| **Speculative execution.** Start backup copies of straggler tasks. | Lower tail latency when slow workers are injected. |
| **Driver resilience.** At minimum, a driver restart fails in-flight jobs cleanly and releases their workers. Failover can come later. | No leftover worker pods or shuffle files after killing the driver. |

### Phase 3: Operability

| Work | Done when |
|------|-----------|
| **Execution metrics.** Per-job, per-stage, and per-task shuffle bytes, spill bytes, retries by cause, coalescing decisions, and lost-worker events. | A slow job can be explained from dashboards alone. |
| **Job and stage UI.** A web UI or Spark History Server–compatible event log with the stage graph, task timelines, and plans annotated with runtime numbers. | Users can debug a slow query without access to server logs. |
| **Configuration hardening.** Move stable settings out of experimental, validate them at startup, and publish tuning guides. | Every production setting is documented and validated. |
| **Rollout tooling.** Shadow runs (run on both Spark and Sail and compare results on a sample), per-tenant canaries, and automatic fallback to Spark for unsupported plans. | A documented playbook, tried on a real workload. |

### Milestones

| Milestone | Scope | Outcome |
|-----------|-------|---------|
| M1 | Phase 1 | Safe to try on real batch jobs (beta). |
| M2 | Phase 2, first four items | Handles large, skewed, preemptible workloads (GA candidate). |
| M3 | The rest of Phase 2, plus Phase 3 | Operable at fleet scale (GA). |

## Further Reading

- [Architecture Overview](./architecture/) for the high-level picture with
  diagrams.
- [Query Planning](./query-planning/) for more on plan optimization.
- [Adaptive Disk-Based Shuffle Design](./architecture/adaptive-disk-shuffle-design)
  for the full design, history, and tests of the disk-based shuffle.
- [Configuration](../guide/configuration/) for every runtime and deployment
  setting.
- [Deployment](../guide/deployment/) for Docker and Kubernetes.
