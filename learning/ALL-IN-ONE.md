# Learning Sail: Complete Guide (All Documents in One)

This file merges every document in `learning/` into one page, for reading
from start to finish or searching everything at once. The separate files are
the source. Regenerate this file after editing them.

## Contents

- [Learning Sail: An Internal Guide](#doc-readme)
- [01 · End-to-End: One Query Through All of Sail](#doc-01-end-to-end-workflow)
- [02 · Entry Points, Configuration, and the Server Framework](#doc-02-entry-points-and-server)
- [03 · Spark Connect: The Front Door](#doc-03-spark-connect)
- [04 · SQL Parser and Analyzer](#doc-04-sql-parser-and-analyzer)
- [05 · The Spec Plan Model (and Shared Foundations)](#doc-05-spec-plan-and-config)
- [06 · Sessions and the Runtime Environment](#doc-06-session-and-runtime)
- [07 · The Plan Resolver: From Intent to a DataFusion Plan](#doc-07-plan-resolver)
- [08 · Logical and Physical Plans, Extension Nodes, and Optimizers](#doc-08-logical-physical-plans-and-optimizers)
- [09 · Local and Cluster Execution](#doc-09-local-and-cluster-execution)
- [10 · Shuffle and Streams: Moving Data Between Tasks](#doc-10-shuffle-and-streams)
- [11 · Catalogs: Finding Tables](#doc-11-catalogs)
- [12 · Data Sources and File Formats](#doc-12-data-sources-and-formats)
- [13 · Lakehouse Formats: Delta Lake and Apache Iceberg](#doc-13-lakehouse-delta-and-iceberg)
- [14 · Object Storage: Reaching S3, GCS, Azure, HDFS, HTTP, and More](#doc-14-object-storage)
- [15 · Functions and Python UDFs](#doc-15-functions-and-python-udfs)
- [16 · Arrow Flight SQL, Telemetry, and Observability](#doc-16-flight-sql-and-telemetry)
- [17 · Testing and Development Workflow](#doc-17-testing-and-development)


---

<a id="doc-readme"></a>

# Learning Sail: An Internal Guide

These notes explain **how Sail works inside**, one component at a time. Each
document answers three questions: **what** the component does, **why** it's
designed that way, and **how** it works. Each also includes examples, an
end-to-end walkthrough, common problems, and a *Where to Look* table that maps
concepts to code.

Sail is a Spark-compatible query engine written in Rust on top of Apache Arrow
and Apache DataFusion. PySpark (through Spark Connect), SQL, and Arrow Flight SQL
clients talk to it. It runs queries in one process or across a driver and many
workers.

> These notes describe the `learning-docs` branch, which is based on `deepdive`.
> Where the `disk-shuffle-hardening` branch changes behavior (blocking shuffle,
> stage retry), the docs say so explicitly.

---

## Reading Order

| # | Document | Read it to understand |
|---|----------|------------------------|
| 01 | [End-to-End Workflow](#doc-01-end-to-end-workflow) | **Start here.** One query through every component. |
| 02 | [Entry Points, Configuration, and the Server Framework](#doc-02-entry-points-and-server) | The `sail` binary and its roles, configuration loading, actors, gRPC servers |
| 03 | [Spark Connect](#doc-03-spark-connect) | How PySpark talks to Sail: RPCs, sessions, streaming responses, errors |
| 04 | [SQL Parser and Analyzer](#doc-04-sql-parser-and-analyzer) | SQL text → tokens → AST → spec plan |
| 05 | [The Spec Plan Model](#doc-05-spec-plan-and-config) | Sail's protocol-neutral intermediate representation |
| 06 | [Sessions and the Runtime Environment](#doc-06-session-and-runtime) | Session contents and lifecycle, memory pools, spilling, caches, system tables |
| 07 | [The Plan Resolver](#doc-07-plan-resolver) | Spec plan → DataFusion plan: names, columns, functions, tables, commands |
| 08 | [Logical and Physical Plans, Extension Nodes, and Optimizers](#doc-08-logical-physical-plans-and-optimizers) | Sail's custom operators, optimizer rules, join reordering |
| 09 | [Local and Cluster Execution](#doc-09-local-and-cluster-execution) | Job runners, driver, stages, tasks, slots, workers, failures |
| 10 | [Shuffle and Streams](#doc-10-shuffle-and-streams) | How data moves between tasks: memory, disk, Arrow Flight |
| 11 | [Catalogs](#doc-11-catalogs) | Name resolution, temporary views, Glue, HMS, Iceberg REST, Unity, OneLake |
| 12 | [Data Sources and File Formats](#doc-12-data-sources-and-formats) | Parquet, CSV, JSON, …: the table format interface, options, listing, Python data sources |
| 13 | [Lakehouse Formats: Delta Lake and Iceberg](#doc-13-lakehouse-delta-and-iceberg) | Snapshots, logs, manifests, commits, deletion vectors, `MERGE` |
| 14 | [Object Storage](#doc-14-object-storage) | S3, GCS, Azure, HDFS, HTTP, Hugging Face |
| 15 | [Functions and Python UDFs](#doc-15-functions-and-python-udfs) | Built-in Spark functions and embedded Python execution |
| 16 | [Flight SQL, Telemetry, and Observability](#doc-16-flight-sql-and-telemetry) | The Flight SQL front end, logs, traces, metrics |
| 17 | [Testing and Development Workflow](#doc-17-testing-and-development) | Building, gold data, Python tests, the Spark test suite |

**Suggested paths:**

- *New to Sail:* 01 → 02 → 03 → 05 → 07 → 09
- *Working on SQL or Spark compatibility:* 04 → 05 → 07 → 15 → 17
- *Working on distributed execution:* 09 → 10 → 06 → 08
- *Working on storage, formats, or catalogs:* 11 → 12 → 13 → 14

---

## Architecture at a Glance

```text
 Clients:  PySpark (Spark Connect)        SQL tools (Flight SQL)        pysail (Python library)
               │                                  │                           │
               ▼                                  ▼                           ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ Front ends      sail-spark-connect          sail-flight            sail-python     │  03, 16, 02
 │                 protobuf → spec             SQL → spec                             │
 ├───────────────────────────────────────────────────────────────────────────────────┤
 │ SQL             sail-sql-parser + sail-sql-macro → sail-sql-analyzer               │  04
 │ Plan model      sail-common (spec, config, runtime)                                │  05
 │ Sessions        sail-session, sail-cache, sail-server (actors, gRPC)               │  06, 02
 ├───────────────────────────────────────────────────────────────────────────────────┤
 │ Planning        sail-plan (resolver, functions) → DataFusion optimizer             │  07
 │                 sail-logical-plan / sail-physical-plan / *-optimizer               │  08
 ├───────────────────────────────────────────────────────────────────────────────────┤
 │ Execution       sail-execution: local runner │ driver ↔ workers, shuffle, Flight   │  09, 10
 ├───────────────────────────────────────────────────────────────────────────────────┤
 │ Metadata        sail-catalog + providers (memory, system, glue, hms, iceberg, …)   │  11
 │ Data            sail-data-source (files, Python), sail-delta-lake, sail-iceberg    │  12, 13
 │ Storage         sail-object-store (S3, GCS, Azure, HDFS, HTTP, HF)                 │  14
 │ Functions       sail-function, sail-python-udf                                     │  15
 │ Observability   sail-telemetry                                                     │  16
 └───────────────────────────────────────────────────────────────────────────────────┘
                 Apache DataFusion  ·  Apache Arrow  ·  Tokio  ·  tonic (gRPC)  ·  PyO3
```

---

## Crate Map

| Crate | Approx. size (lines) | Doc |
|-------|----------------------|-----|
| `sail-cli` | 0.8k | 02 |
| `sail-server` | 0.6k | 02 |
| `sail-python` | 0.5k | 02, 15 |
| `sail-common` | 4k | 02, 05 |
| `sail-spark-connect` | 9.5k | 03 |
| `sail-sql-parser`, `sail-sql-macro`, `sail-sql-analyzer` | 6k + 0.8k + 6k | 04 |
| `sail-session`, `sail-cache` | 2k + 0.6k | 06 |
| `sail-common-datafusion` | 6k | 06, 12 (shared interfaces) |
| `sail-plan` | 24k | 07, 15 |
| `sail-logical-plan`, `sail-physical-plan` | 3.7k + 2.7k | 08 |
| `sail-logical-optimizer`, `sail-physical-optimizer` | 0.2k + 8.5k | 08 |
| `sail-execution` | 15.5k | 09, 10 |
| `sail-catalog` + 6 providers | 3.7k + ≈26k | 11 |
| `sail-data-source` | 11.7k | 12 |
| `sail-delta-lake`, `sail-iceberg`, `sail-plan-lakehouse` | 37k + 21k + 0.6k | 13 |
| `sail-object-store` | 1.6k | 14 |
| `sail-function`, `sail-python-udf` | 37k + 2.7k | 15 |
| `sail-flight`, `sail-telemetry` | 0.6k + 2.5k | 16 |
| `sail-gold-test`, `sail-build-scripts` | 0.5k + 0.5k | 17 |

---

## Glossary

| Term | Meaning | Doc |
|------|---------|-----|
| Actor | A component that owns its state and processes one message at a time | 02 |
| Barrier | A node that runs precondition plans (such as creating a table) before the main plan | 08 |
| Catalog manager | The per-session component that resolves names and manages temporary views | 11 |
| Channel | One slice of a task's output, such as one hash bucket of a shuffle | 10 |
| Driver | The cluster-mode coordinator actor (one per session) | 09 |
| Extension node | A Sail-specific logical or physical operator plugged into DataFusion | 08 |
| Gold data | JSON files recording expected outputs, regenerated with `SAIL_UPDATE_GOLD_DATA=1` | 17 |
| Job / stage / task / attempt | One execution, a piece between exchanges, one partition of a stage, one try of a task | 09 |
| Job runner | The interface that executes a physical plan, locally or on a cluster | 09 |
| Named plan | A resolved logical plan plus its user-facing column names | 07 |
| Option layer | One ordered source of format options (table properties, query options, time travel) | 12 |
| Pipelined / blocking | Stream between running stages in memory, or materialize to disk | 09, 10 |
| Plan config | Session settings that change how a query is resolved (time zone, ANSI, …) | 03, 07 |
| Region | Tasks that must be scheduled together | 09 |
| Session extension | A typed service attached to a DataFusion session (catalogs, job service, …) | 06 |
| Slot | A unit of worker capacity for running a task set | 09 |
| Spec plan | Sail's protocol-neutral, unresolved intermediate plan | 05 |
| Stream key | (job, stage, partition, attempt, channel), identifying one data stream | 10 |
| Table format | A plugin that knows how to read and write a format (Parquet, Delta, …) | 12 |

---

## How These Docs Were Written

Every behavior described here was checked against the code on this branch.
Default values come from `crates/sail-common/src/config/application.yaml`. If
the code changes, the *Where to Look* tables point at the files to re-check.
Corrections are welcome: edit the relevant document and note what changed in
the commit message.


---

<a id="doc-01-end-to-end-workflow"></a>

# 01 · End-to-End: One Query Through All of Sail

> **You'll learn:** the complete journey of a single PySpark query, from the
> client to Parquet files on S3 and back, touching every component once. Read
> this first, then dive into the component docs it links to.

---

## The Scenario

A data analyst runs this against a Sail server in **cluster mode**
(`SAIL_MODE=kubernetes-cluster`):

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.remote("sc://sail.example.com:50051").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

orders = spark.table("glue.sales.orders")                 # Parquet table in AWS Glue
result = (orders
    .filter(F.col("amount") > 100)
    .groupBy("region")
    .agg(F.count("*").alias("n"), F.sum("amount").alias("total"))
    .orderBy(F.desc("total")))

result.show()
```

Here's everything that happens, in order.

---

## Stage 0: Before the Query (server startup)

| What | Where |
|------|-------|
| `sail spark server` loads its configuration (YAML defaults + `SAIL_*` environment variables), creates the **primary** and **io** runtimes, sets up telemetry, and serves the Spark Connect gRPC service through the shared server builder (health, reflection, tracing). | [02](#doc-02-entry-points-and-server) |

## Stage 1: Connecting and Setting Configuration

| # | What happens | Where |
|---|--------------|-------|
| 1 | `getOrCreate()` generates a session ID. The first RPC reaches the server. | [03](#doc-03-spark-connect) |
| 2 | The **session manager actor** finds no session with that ID, so it asks the **server session factory** to build one: table format registry, **catalog manager** (with `glue` and the reserved `system` catalog), activity tracker, a **cluster job runner** (with its own **driver** actor), system tables, and the runtime environment (memory pool, spill directories, file caches, **dynamic object store registry**). The Spark front end adds its Spark session extension. | [06](#doc-06-session-and-runtime), [11](#doc-11-catalogs) |
| 3 | `spark.conf.set(...)` is a `Config` RPC. The value is stored in the session's Spark configuration and later becomes the **plan config**'s `session_timezone`. | [03](#doc-03-spark-connect) |

## Stage 2: Building the DataFrame (no execution yet)

| # | What happens | Where |
|---|--------------|-------|
| 4 | `spark.table`, `filter`, `groupBy`, `agg`, and `orderBy` only build a relation tree **in the client**. Nothing is sent until an action runs. | |

## Stage 3: `show()` Sends the Plan

| # | What happens | Where |
|---|--------------|-------|
| 5 | `show()` sends `ExecutePlan` with a root relation: ShowString(Sort(Aggregate(Filter(Read("glue.sales.orders"))))), a new operation ID, and the *reattachable* option. | [03](#doc-03-spark-connect) |
| 6 | The relation is translated into a **spec plan**: a show-string command over a sort, an aggregate (grouping `region`, aggregates `count(*) AS n` and `sum(amount) AS total`), a filter with the unresolved function `>` applied to `amount` and the literal `100`, and a read of the named table `["glue","sales","orders"]`. Names are still unresolved. | [05](#doc-05-spec-plan-and-config) |

## Stage 4: Resolving

| # | What happens | Where |
|---|--------------|-------|
| 7 | The **resolver** starts with an empty state. For the read, the catalog manager resolves `glue.sales.orders` to catalog `glue`, namespace `sales`, table `orders`. The cache wrapper misses, the runtime wrapper runs the call on the io runtime, and the **Glue provider** returns a table status: format `parquet`, location `s3://lake/sales/orders/`, columns, partitioned by `dt`. | [07](#doc-07-plan-resolver), [11](#doc-11-catalogs) |
| 8 | The resolver asks the **Parquet table format** for a source. It parses the location, lists files through the **S3 store** (created lazily on the io runtime, with AWS credentials and the bucket region), skips hidden files, and builds a listing table with partition column `dt`. | [12](#doc-12-data-sources-and-formats), [14](#doc-14-object-storage) |
| 9 | Columns get internal IDs (`#0` order_id, `#1` region, `#2` amount, `#3` dt). `amount > 100` resolves to `#2 > 100`, with the literal coerced to `amount`'s type. `count(*)` and `sum` are found in the **aggregate table**. `desc(total)` resolves against the aggregate's output. The result is a **named plan**: the DataFusion logical plan plus user names `[region, n, total]`, wrapped in a show-string node. | [07](#doc-07-plan-resolver), [15](#doc-15-functions-and-python-udfs) |

## Stage 5: Optimizing and Physical Planning

| # | What happens | Where |
|---|--------------|-------|
| 10 | **Logical optimization:** the filter is pushed into the scan and only `region` and `amount` are read. | [08](#doc-08-logical-physical-plans-and-optimizers) |
| 11 | **Physical planning:** the extension planner maps show-string to its operator. DataFusion plans the scan (Parquet with pruning), a **partial aggregate**, a **hash repartition on `region`**, a **final aggregate**, a **sort**, and a **sort-preserving merge** to a single output. | [08](#doc-08-logical-physical-plans-and-optimizers) |
| 12 | **Physical optimization:** enforce distribution and sorting, pushdowns, and Sail's checks. Output columns are renamed from IDs to `region`, `n`, `total`. | [08](#doc-08-logical-physical-plans-and-optimizers), [07](#doc-07-plan-resolver) |

The plan now looks roughly like this:

```text
ShowStringExec
  SortPreservingMergeExec [total DESC]            ← stage boundary (merge)
    SortExec [total DESC]
      AggregateExec (final) [region]
        RepartitionExec hash(region, 8)           ← stage boundary (shuffle)
          AggregateExec (partial) [region]
            DataSourceExec parquet (filter amount>100, projection [region, amount])
```

## Stage 6: Distributed Execution

| # | What happens | Where |
|---|--------------|-------|
| 13 | The **cluster job runner** sends *execute job* to the session's **driver**. | [09](#doc-09-local-and-cluster-execution) |
| 14 | The **job graph builder** cuts the plan at the repartition and the merge: **Stage 0** (scan → partial aggregate, output hashed by `region` into 8 channels), **Stage 1** (shuffle read → final aggregate → sort, 8 partitions), **Stage 2** (merge read → show string, 1 partition). | [09](#doc-09-local-and-cluster-execution) |
| 15 | When the session's driver was created (step 2), it already asked the **Kubernetes worker manager** for `worker_initial_count` (4) pods running `sail worker`. All links are pipelined, so the three stages form **one region**. They share slots by partition, so the region needs about 8 slots (the widest stage). | [09](#doc-09-local-and-cluster-execution) |
| 16 | The workers start, create **worker sessions**, register with the driver, and begin heartbeats. As soon as enough slots are free, the region is placed, and each task's **definition** (plan serialized by the codec, input locators, output locators) is sent to its worker. More workers are requested only if the region doesn't fit. | [09](#doc-09-local-and-cluster-execution), [06](#doc-06-session-and-runtime) |
| 17 | On each worker, the **task runner** decodes the plan, adds **shuffle write** at the root of each Stage 0 task and **shuffle read** where Stage 1 and 2 read their inputs, wraps operators with tracing, executes, and reports *running*. | [09](#doc-09-local-and-cluster-execution), [16](#doc-16-flight-sql-and-telemetry) |
| 18 | **Stage 0 tasks** read their Parquet files directly from S3 (row groups pruned by `amount` statistics), compute partial counts and sums, hash rows by `region`, and push each slice into an **in-memory stream** keyed (job, 0, p, 0, channel). | [12](#doc-12-data-sources-and-formats), [10](#doc-10-shuffle-and-streams) |
| 19 | **Stage 1 task q** opens channel *q* of every Stage 0 task: local streams directly, remote ones through **Arrow Flight `DoGet`** to peer workers. It merges them, computes final aggregates for its regions, and sorts. | [10](#doc-10-shuffle-and-streams) |
| 20 | **Stage 2** reads all Stage 1 outputs, merge-sorts by `total`, and formats the top 20 rows as a text table. | [10](#doc-10-shuffle-and-streams), [08](#doc-08-logical-physical-plans-and-optimizers) |
| 21 | The driver opens the final stage's output stream and returns it as the job's result stream. | [09](#doc-09-local-and-cluster-execution) |

## Stage 7: Streaming the Response

| # | What happens | Where |
|---|--------------|-------|
| 22 | The Spark Connect **executor** sends a schema message, the Arrow batch containing the formatted table, and "complete", buffering each by response ID (with empty heartbeat batches every 15 seconds if needed). PySpark prints the table. | [03](#doc-03-spark-connect) |
| 23 | Tasks report *succeeded*. The region and job succeed. Streams are cleaned up. Telemetry records spans and metrics for every operator, labeled by job, stage, partition, and attempt. `system.execution.jobs` now lists the job. | [09](#doc-09-local-and-cluster-execution), [16](#doc-16-flight-sql-and-telemetry) |
| 24 | After 60 seconds without tasks, idle workers are stopped. After 15 minutes without requests, the session is removed, its driver shuts down, and its history is kept. | [09](#doc-09-local-and-cluster-execution), [06](#doc-06-session-and-runtime) |

---

## The Same Query in Local Mode

With `SAIL_MODE=local` (the default), steps 1–12 are identical. In stage 6 the
**local job runner** wraps the plan with tracing and runs it with DataFusion's
multi-threaded engine in the server process. Repartitions are in-memory
DataFusion exchanges, and there are no stages, workers, or Flight streams.

## The Same Query as SQL

```sql
SELECT region, count(*) AS n, sum(amount) AS total
FROM glue.sales.orders WHERE amount > 100
GROUP BY region ORDER BY total DESC
```

Only step 6 changes: the SQL is tokenized, parsed into an AST, and analyzed into
an equivalent spec plan ([04](#doc-04-sql-parser-and-analyzer)). Everything after
that is the same.

---

## What Could Go Wrong, and Where to Look

| Failure | Component |
|---------|-----------|
| `ParseException` | [04 · SQL parser](#doc-04-sql-parser-and-analyzer) |
| `TABLE_OR_VIEW_NOT_FOUND` | [11 · Catalogs](#doc-11-catalogs) |
| `cannot resolve attribute` / `unknown function` | [07 · Resolver](#doc-07-plan-resolver), [15 · Functions](#doc-15-functions-and-python-udfs) |
| Access denied on S3 | [14 · Object storage](#doc-14-object-storage) |
| "task scheduling timeout" or a lost worker | [09 · Execution](#doc-09-local-and-cluster-execution) |
| Reader stuck on a pending stream | [10 · Shuffle and streams](#doc-10-shuffle-and-streams) |
| Worker out of memory | [06 · Sessions and runtime](#doc-06-session-and-runtime) (memory pool) |
| Long query drops behind a proxy | [03 · Spark Connect](#doc-03-spark-connect) (heartbeats and reattach) |


---

<a id="doc-02-entry-points-and-server"></a>

# 02 · Entry Points, Configuration, and the Server Framework

> **You'll learn:** how Sail starts, the roles one binary can play, how
> configuration is loaded, and the actor and gRPC building blocks every service
> uses.
>
> **Crates:** `sail-cli`, `sail-python`, `sail-server`, `sail-common`
> (configuration and runtime)

---

## 1. What This Component Does

Everything in Sail starts in a single binary called `sail`. Depending on its
arguments it can act as:

| Role | Command | Who uses it |
|------|---------|-------------|
| Spark Connect server | `sail spark server` | PySpark clients (`SparkSession.builder.remote("sc://…")`) |
| Interactive shell | `sail spark shell` | A developer who wants a PySpark REPL backed by Sail |
| Script runner | `sail spark run -f job.py` | Batch scripts. Starts a server, runs the script, and exits. |
| MCP server | `sail spark mcp-server` | AI assistants that speak the Model Context Protocol |
| Flight SQL server | `sail flight server` | Arrow Flight SQL clients (JDBC/ADBC-style tools) |
| Cluster worker | `sail worker` | Started automatically by the driver in Kubernetes mode. Internal only. |
| Python interpreter | (hidden) | Child Python processes started by PySpark or UDF code |

The same engine is also packaged as the Python library **`pysail`**, which lets
a Python program start a Spark Connect server inside its own process.

---

## 2. Why It's Built This Way

**One binary for everything.** A single artifact is easier to ship (one
container image, one wheel) and guarantees the server and its workers run
exactly the same code. Kubernetes worker pods run the same image as the
driver, just with the `worker` argument.

**The binary can pretend to be Python.** Sail runs Python UDFs in an embedded
interpreter. Python libraries (multiprocessing, some PySpark helpers) start
child processes by re-running `sys.executable`. Inside Sail, that's the `sail`
binary, not `python`. To make this work, Sail sets the internal environment
variable `SAIL_INTERNAL__RUN_PYTHON` before it starts Python. When a child
process starts with that variable set, `main` skips the CLI completely and
calls Python's own `Py_Main`. To the Python library, the child behaves exactly
like a Python interpreter.

**The shell and runner start a private server.** `sail spark shell` and
`sail spark run` don't need you to start a server first. They bind a server to
`127.0.0.1` on a random port, run the PySpark shell or script against
`sc://127.0.0.1:<port>`, and shut the server down when Python exits. Listening
only on loopback keeps the private server off the network.

**Two thread pools.** Sail creates two Tokio runtimes: a **primary** runtime
for query execution and RPC handling, and an **io** runtime for object storage
and other blocking-prone I/O. Keeping them apart means a burst of slow storage
calls can't starve query execution of threads.

---

## 3. How It Works

### 3.1 Startup sequence for `sail spark server`

1. **Native entry point.** The program checks for the embedded-Python variable.
   If it isn't set, it optionally installs the `mimalloc` allocator, then calls
   the CLI runner.
2. **Crypto provider.** The runner installs the AWS-LC TLS crypto provider
   (needed for HTTPS object stores and TLS between cluster nodes).
3. **Argument parsing.** `clap` parses the subcommand: `spark server`, `--ip`
   (default `127.0.0.1`), `--port` (default `50051`), and `-C <dir>` to change
   directory first.
4. **Configuration.** The application configuration is loaded (see 3.2).
5. **Runtimes.** The primary and io Tokio runtimes are created, with the thread
   stack size taken from `runtime.stack_size`.
6. **Telemetry.** OpenTelemetry is set up with the resource kind `server` (see
   [16 · Flight SQL, Telemetry, and Observability](#doc-16-flight-sql-and-telemetry)).
   A guard object shuts telemetry down cleanly on exit.
7. **Listen.** A TCP listener binds to the address, and the Spark Connect
   service is served on it (see [03 · Spark Connect](#doc-03-spark-connect)).
8. **Wait for shutdown.** The server runs until `Ctrl+C`. Sail captures the
   signal itself in Tokio, because the embedded Python would otherwise swallow
   it.

### 3.2 Configuration loading

All settings are defined in one YAML file compiled into the binary
(`crates/sail-common/src/config/application.yaml`). Each entry has a key, a
type, a default, a description, and sometimes an `experimental: true` flag.
At startup:

1. The YAML file supplies the **defaults**.
2. **Environment variables** starting with `SAIL_` override them. Double
   underscores become dots, so `SAIL_CLUSTER__WORKER_MAX_COUNT=8` sets
   `cluster.worker_max_count`.
3. The result is checked against typed Rust structs. Unknown fields are
   rejected, so a typo fails loudly instead of being silently ignored.

Variables starting with `SAIL_INTERNAL__` are reserved for Sail's own use
(such as the embedded-Python switch) and are ignored by application
configuration.

The configuration is grouped into sections:

| Section | Controls | Examples |
|---------|----------|----------|
| `mode` | Where queries run | `local`, `local-cluster`, `kubernetes-cluster` |
| `runtime` | Threads, memory, spilling | `runtime.memory_pool.type`, `runtime.temporary_files.paths` |
| `cluster` | Driver, workers, tasks, RPC retries | `cluster.worker_task_slots`, `cluster.task_max_attempts` |
| `execution` | DataFusion execution | `execution.batch_size`, `execution.default_parallelism` |
| `parquet` | Parquet read and write | `parquet.pushdown_filters`, `parquet.compression` |
| `kubernetes` | Worker pods | `kubernetes.image`, `kubernetes.namespace`, `kubernetes.worker_pod_template` |
| `catalog` | Catalogs and defaults | `catalog.list`, `catalog.default_catalog` |
| `optimizer` | Optimizer switches | `optimizer.enable_join_reorder` |
| `spark`, `flight` | Protocol sessions | `spark.session_timeout_secs` |
| `telemetry` | OpenTelemetry export | `telemetry.otlp_endpoint`, `telemetry.export_traces` |
| `python` | Python data sources | `python.data_source_write_channel_capacity` |

### 3.3 Execution modes

The `mode` setting decides which job runner each session gets (see
[09 · Local and Cluster Execution](#doc-09-local-and-cluster-execution)):

| Mode | What happens | When to use |
|------|--------------|-------------|
| `local` (default) | The whole query runs in the server process with DataFusion's multi-threaded engine. | Laptops, single machines, development. |
| `local-cluster` | A driver and several worker **actors** run inside the same process and talk over gRPC on localhost. | Testing distributed behavior without Kubernetes. |
| `kubernetes-cluster` | The driver runs in the server and starts worker **pods** running `sail worker`. | Real distributed jobs. |

### 3.4 The worker role

`sail worker` is only started by the Kubernetes worker manager. It loads the
same configuration (the pod receives its worker ID and driver address through
`SAIL_CLUSTER__*` variables), creates a **worker session** (an execution
environment without user state, see
[06 · Sessions and Runtime](#doc-06-session-and-runtime)), sets up telemetry
with the resource kind `worker`, registers with the driver, and runs tasks
until it's told to stop.

### 3.5 The actor framework

The driver, workers, and session manager are **actors**. An actor:

- owns its state privately,
- receives messages through a small bounded mailbox (8 messages),
- handles **one message at a time**, so its state never needs locks,
- never blocks inside a handler. Slow work is spawned as a background task that
  sends a message back when it's done,
- can send itself a message after a delay. This is how timeouts and heartbeats
  are implemented.

Each handler returns either *continue* or *stop*. Background tasks belong to the
actor and are cancelled when it stops. An actor system keeps track of all
actors so the process can wait for them to finish on shutdown.

**Why actors?** A cluster driver tracks jobs, stages, tasks, attempts,
workers, and streams, all changing concurrently. Handling each change as a
message, one at a time, rules out whole classes of race conditions and makes
the set of things that can happen explicit: it's exactly the list of message
types.

### 3.6 The gRPC server builder

Spark Connect, Flight SQL, the driver, and each worker all serve gRPC through
one shared builder. It provides:

- TCP no-delay and keep-alive, plus HTTP/2 keep-alive pings (every 60 seconds,
  with a 10-second timeout) and adaptive flow-control windows,
- a standard **health service**, which reports each added service as serving,
- **reflection**, so tools like `grpcurl` can discover the API,
- a tracing layer that attaches OpenTelemetry spans to every request,
- graceful shutdown when a signal future completes.

There's also a shared **retry strategy** (fixed delay or exponential backoff),
configured by `cluster.rpc_retry_strategy.*`, for RPCs between the driver and
workers.

---

## 4. Examples

### 4.1 Run a server and connect from PySpark

```bash
sail spark server --port 50051
```

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql("SELECT 1 + 1 AS two").show()
```

### 4.2 Start a server from Python

```python
from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

server = SparkConnectServer()          # 127.0.0.1, random port
server.start(background=True)
_, port = server.listening_address
spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
spark.range(10).show()
server.stop()
```

### 4.3 Run a local cluster with a bounded memory pool

```bash
export SAIL_MODE=local-cluster
export SAIL_CLUSTER__WORKER_INITIAL_COUNT=4
export SAIL_CLUSTER__WORKER_TASK_SLOTS=8
export SAIL_RUNTIME__MEMORY_POOL__TYPE=fair
export SAIL_RUNTIME__MEMORY_POOL__FAIR__MAX_SIZE=8589934592   # 8 GiB
sail spark server
```

### 4.4 Run a script in one step

```bash
sail spark run -f etl_job.py
```

---

## 5. End-to-End: What Happens When You Type `sail spark shell`

| Step | What happens |
|------|--------------|
| 1 | `main` sees no embedded-Python variable and calls the CLI runner. |
| 2 | The runner parses `spark shell`. |
| 3 | Configuration is loaded from YAML plus `SAIL_*` variables. Runtimes and telemetry are created. |
| 4 | A Spark Connect server binds to `127.0.0.1:0`, so the OS picks a free port. |
| 5 | The server task starts in the background on the primary runtime. |
| 6 | Sail starts its embedded Python and calls the bundled shell module with the chosen port. The module creates a PySpark session connected to `sc://127.0.0.1:<port>` and starts an interactive console. |
| 7 | Each command you type becomes a Spark Connect request to the in-process server. |
| 8 | When you exit Python, a shutdown signal fires, the server stops gracefully, and telemetry is flushed. |

---

## 6. Common Problems

| Symptom | Cause | Fix |
|---------|-------|-----|
| Startup fails with "duplicate field" or "unknown field" | A misspelled or conflicting `SAIL_*` variable | Check the spelling against `application.yaml`. Remember `__` stands for `.`. |
| `Ctrl+C` doesn't stop the server | The signal was captured elsewhere | Use `sail spark server` rather than a custom Python wrapper, or stop the `pysail` server explicitly. |
| Worker pods start and immediately exit | Driver address or worker ID not passed through | Check the pod template and `cluster.driver_external_host` and `cluster.driver_external_port`. |
| Clients on other machines can't connect | Server bound to loopback | Pass `--ip 0.0.0.0`. TLS is usually handled by a gateway in front of the server. |

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Binary entry point and embedded Python | `crates/sail-cli/src/main.rs`, `crates/sail-cli/src/python.rs` |
| Subcommands | `crates/sail-cli/src/runner.rs` |
| Server, shell, script runner, MCP | `crates/sail-cli/src/spark/` |
| Worker entry point | `crates/sail-cli/src/worker/entrypoint.rs` |
| Python bindings (`pysail._native`) | `crates/sail-python/src/` |
| Python-facing server class | `python/pysail/spark/__init__.py` |
| Configuration schema and loader | `crates/sail-common/src/config/` (`application.yaml`, `application.rs`) |
| Primary and io runtimes | `crates/sail-common/src/runtime.rs` |
| Actor framework | `crates/sail-server/src/actor.rs` |
| gRPC server builder, retry strategy | `crates/sail-server/src/builder.rs`, `retry.rs` |


---

<a id="doc-03-spark-connect"></a>

# 03 · Spark Connect: The Front Door

> **You'll learn:** how a PySpark client talks to Sail, how Spark protobuf
> messages become Sail plans, how results stream back, and how sessions,
> configuration, errors, reconnection, and cancellation work.
>
> **Crate:** `sail-spark-connect`

---

## 1. What This Component Does

**Spark Connect** is the client/server protocol introduced in Spark 3.4. A
PySpark program using Spark Connect doesn't run a JVM. It builds a description
of the query (a tree of protobuf *relations*) and sends it over gRPC to a
server. Sail implements that server, so **unchanged PySpark code** can run on
Sail just by pointing the client at Sail's address:

```python
spark = SparkSession.builder.remote("sc://sail-host:50051").getOrCreate()
```

This crate is responsible for:

1. serving the Spark Connect gRPC API,
2. finding or creating the client's session,
3. **translating** Spark's protobuf plans into Sail's protocol-neutral spec plan
   (see [05 · The Spec Plan Model](#doc-05-spec-plan-and-config)),
4. handing the plan to the planner and job runner,
5. **streaming** results back as Arrow batches, with heartbeats, reconnection,
   and cancellation,
6. translating Sail errors into the exception classes PySpark expects.

---

## 2. Why It's Built This Way

**Compatibility at the wire, not in the code.** Sail doesn't reimplement
PySpark. It reimplements the protocol PySpark speaks. Everything the client
library does (DataFrame building, pandas conversion, error display) keeps
working because Sail sends exactly the messages a Spark server would.

**Translate once, at the edge.** Spark's protobuf has roughly 60 relation types
and many commands. This crate converts them into the spec plan **right away**.
Nothing deeper in Sail sees Spark protobuf, so the planner is shared with SQL
and Flight SQL, and protocol quirks stay in one crate.

**Results are buffered for reconnection.** Long-running queries over unreliable
networks drop connections. Spark Connect lets a client **reattach** to a running
operation and continue from the last response it received. Sail supports this
by buffering each response, with an ID, until the client confirms it has
received it.

**Heartbeats keep idle streams alive.** A query can take minutes before
producing its first row. Proxies and load balancers often close connections
that are silent for too long. Sail sends an empty batch every 15 seconds
(`spark.execution_heartbeat_interval_secs`) while it waits.

---

## 3. How It Works

### 3.1 The RPC surface

| RPC | Purpose | Sail behavior |
|-----|---------|---------------|
| `ExecutePlan` | Run a query (`Root` relation) or a command | Main path, see 3.3. Compressed plans aren't supported. |
| `AnalyzePlan` | Ask about a plan without running it | Schema, explain, tree string, is-local, is-streaming, Spark version, DDL parsing, JSON-to-DDL, storage level. `persist`/`unpersist` are accepted but do nothing. Input files, same-semantics, and semantic-hash return "not implemented". |
| `Config` | Get, set, or unset session settings | Stored per session. Some settings are read-only, and removed settings produce warnings. |
| `AddArtifacts`, `ArtifactStatus` | Upload Python files, JARs, and other artifacts | Handled by the artifact manager. |
| `Interrupt` | Cancel operations | By all, by tag, or by operation ID. |
| `ReattachExecute` | Reconnect to a running operation | Replays buffered responses after the last one the client saw. |
| `ReleaseExecute` | Tell the server the client has received responses | Frees buffered responses up to an ID, or all of them. |
| `ReleaseSession` | End a session | Removes the session from the session manager. |
| `FetchErrorDetails`, `CloneSession` | Error details, session cloning | Limited support. |

The service accepts and sends **gzip** and **zstd** compressed messages and
uses Spark's default maximum inbound message size.

### 3.2 Sessions

Each request carries a **session ID** and a **user ID**. The session manager
(see [06 · Sessions and Runtime](#doc-06-session-and-runtime)) returns the
existing session or creates one through a Spark-specific factory. That factory:

- attaches a **Spark session** extension holding the session and user IDs, the
  Spark configuration, running operations ("executors"), and streaming queries,
- sets `spark.sql.session.timeZone` to the server's system time zone by default,
- sets an idle timeout of 15 minutes (`spark.session_timeout_secs`), after
  which an unused session is removed.

**Spark settings that change planning.** When a plan is resolved, the session's
Spark settings are turned into a **plan config**, the set of settings that
affect how a query is interpreted:

| Spark setting | Effect in Sail |
|---------------|----------------|
| `spark.sql.session.timeZone` | Time zone for timestamp functions and casts |
| `spark.sql.ansi.enabled` | ANSI SQL mode: errors instead of NULLs on overflow and invalid casts |
| `spark.sql.timestampType` | Whether `TIMESTAMP` means `TIMESTAMP_LTZ` (default) or `TIMESTAMP_NTZ` |
| `spark.sql.sources.default` | Default file format for tables |
| `spark.sql.warehouse.dir` | Where managed tables are stored |
| `spark.sql.crossJoin.enabled` | Whether implicit cross joins are allowed |
| `spark.sql.execution.arrow.useLargeVarTypes` | Use 64-bit-offset Arrow strings and binaries |
| Pandas UDF settings (assign columns by name, safe conversion, …) | Passed to the Python UDF runtime |

### 3.3 Running a query, step by step

1. **Find the session** for the session ID and user ID.
2. **Pick a handler.** A `Root` relation is a query. A `Command` is dispatched
   by type: register function, write (v1 and v2), create view, SQL command,
   streaming start and management, register table function, register data
   source, checkpoint, merge into table, and so on. Unsupported commands
   (ML, pipelines, resource profiles, extensions) return "not implemented".
3. **Translate to a spec plan.** The relation tree is converted node by node.
   Expressions, literals, and data types each have their own converters.
4. **Resolve and plan.** The spec plan and the session's plan config go to the
   planner, which returns a physical plan (see
   [07 · The Plan Resolver](#doc-07-plan-resolver)).
5. **Execute.** The physical plan goes to the session's job runner, which returns
   a stream of Arrow batches (see
   [09 · Local and Cluster Execution](#doc-09-local-and-cluster-execution)).
6. **Stream the response** in one of two modes:
   - **Lazy** (queries): an *executor* task reads the stream and sends a
     **schema** message, then **Arrow batch** messages, then **complete**. If the
     result is empty, it still sends one empty batch so the client learns the
     schema. Each message gets a UUID and is kept in the buffer until released.
   - **Eager silent** (most commands): the stream is read to the end right away,
     which makes the side effect happen, then only "complete" is sent.
7. **Wrap each message** as an `ExecutePlanResponse` with the session ID,
   operation ID, and response ID.

### 3.4 SQL commands vs. SQL queries

`spark.sql("…")` arrives as a **SQL command**. Sail first translates it:

- If it's a **query** (`SELECT …`), Sail doesn't run it yet. It returns the
  relation to the client, which runs it later when you call `.show()` or
  `.collect()`. This matches Spark's lazy DataFrames.
- If it's a **command** (`CREATE TABLE`, `INSERT`, `SET`, …), Sail runs it
  **immediately**, collects the output into one batch, and returns it as a local
  relation. This matches Spark, where `spark.sql("CREATE TABLE …")` takes effect
  before the call returns.

### 3.5 Reattach and release

When the client sets the *reattachable* option (PySpark does by default):

- Every response is buffered with its ID.
- If the connection drops, the client calls `ReattachExecute` with the last
  response ID it received. Sail pauses the executor task, replays buffered
  responses after that ID, and resumes reading the stream.
- As the client processes responses, it calls `ReleaseExecute` so Sail can drop
  them from the buffer and keep memory bounded.

### 3.6 Interrupts

`Interrupt` can target every operation in the session, operations with a given
**tag** (set by the client with `spark.addTag`), or one **operation ID**. The
matching executors are stopped, and the response lists the IDs that were
interrupted.

### 3.7 Streaming queries

`writeStream.start()` starts a streaming plan. Its stream is attached to a
**streaming query** object in the session. Later commands, such as status, stop,
await termination, and the query manager's list and get, work on these objects.
The client-side listener bus isn't supported yet.

### 3.8 Errors

Sail errors are mapped to Spark exception classes so PySpark raises the right
Python exception:

| Kind of problem | Spark class the client sees |
|-----------------|-----------------------------|
| SQL syntax error | `ParseException` |
| Unknown table, column, or function; type mismatch | `AnalysisException` |
| Failure while running | `QueryExecutionException`, `SparkRuntimeException` |
| Bad number or argument | `NumberFormatException`, `IllegalArgumentException` |
| Overflow or division error (ANSI mode) | `ArithmeticException` |
| Date and time parsing | `DateTimeException` |
| Python UDF error | `PythonException` |
| Not supported yet | `UnsupportedOperationException` |

Like Spark, every exception is sent with gRPC status `INTERNAL`, plus error
details naming the class. Messages are cut off at about 3.7 KB because gRPC
limits total header size to 16 KB, and long Python tracebacks would otherwise
break the response. Cancellations use the `CANCELLED` status.

---

## 4. Examples

### 4.1 A DataFrame query

```python
df = (spark.read.parquet("s3://bucket/orders")
        .filter("amount > 100")
        .groupBy("region").count())
df.show()
```

What Sail receives when `.show()` runs: a `Root` relation that is roughly
*ShowString(Aggregate(Filter(Read(parquet, s3://bucket/orders))))*. The
translator produces a spec query plan with a read node, a filter node holding
the unresolved expression `amount > 100`, an aggregate node grouping by the
unresolved column `region`, and a show-string node. The result streams back as
one Arrow batch containing the formatted table text.

### 4.2 A SQL command

```python
spark.sql("CREATE TABLE t (id INT) USING parquet")   # runs now
df = spark.sql("SELECT * FROM t")                    # doesn't run yet
df.collect()                                         # runs now
```

### 4.3 Session settings

```python
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.ansi.enabled", "true")
spark.sql("SELECT CAST('abc' AS INT)").show()  # raises an error in ANSI mode
```

### 4.4 Cancelling with tags

```python
spark.addTag("nightly")
# ... start long-running queries in other threads ...
spark.interruptTag("nightly")
```

---

## 5. End-to-End: One `df.collect()`

| # | Where | What happens |
|---|-------|--------------|
| 1 | PySpark | Serializes the DataFrame's relation tree and calls `ExecutePlan` with a new operation ID and the reattachable option. |
| 2 | Server | Reads the session ID and user ID, and gets or creates the session. |
| 3 | Server | Sees a `Root` relation and calls the relation handler. |
| 4 | Translator | Converts the relation tree to a spec query plan. |
| 5 | Planner | Builds a plan config from the Spark settings, resolves the spec plan, and optimizes it into a physical plan. |
| 6 | Job runner | Starts execution and returns a stream. |
| 7 | Executor | Sends the schema, then each Arrow batch (or an empty one every 15 seconds while waiting), then "complete", buffering each by ID. |
| 8 | PySpark | Receives batches, calls `ReleaseExecute` as it goes, and builds Python rows (or a pandas DataFrame for `toPandas`). |
| 9 | If the network drops | PySpark calls `ReattachExecute` with the last response ID, and Sail replays the rest. |

---

## 6. Extending and Debugging

**Adding support for a new relation or command.** Find its `TODO` branch in the
relation or command converter. Add a matching spec node if one doesn't exist
(see [05 · The Spec Plan Model](#doc-05-spec-plan-and-config)), convert the
protobuf fields into it, then teach the resolver to handle it (see
[07 · The Plan Resolver](#doc-07-plan-resolver)).

**Debugging a wrong result.** Turn on debug logging for this crate. Every
request and response is logged at debug level. Compare the incoming relation
with the spec plan it was translated to. If the spec plan already looks wrong,
the bug is in this crate. If not, look at resolution.

| Symptom | Likely cause |
|---------|--------------|
| `UnsupportedOperationException: … not implemented` | The relation, command, or analysis type isn't implemented yet. |
| A DataFrame call and the same SQL return different results | The two translation paths produce different spec plans. |
| Long queries fail behind a proxy | Heartbeat interval longer than the proxy's idle timeout. Lower `spark.execution_heartbeat_interval_secs`. |
| Client error messages end in "[truncated]" | The message was longer than the gRPC header limit. Check the server log for the full message. |
| Session settings "forgotten" | The session was idle for more than 15 minutes and was removed. Raise `spark.session_timeout_secs`. |

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Server start | `crates/sail-spark-connect/src/entrypoint.rs` |
| RPC implementations | `crates/sail-spark-connect/src/server.rs` |
| Execution and analysis handlers | `crates/sail-spark-connect/src/service/` (`plan_executor.rs`, `plan_analyzer.rs`, `config_manager.rs`, `artifact_manager.rs`) |
| Streaming responses, buffering, heartbeats | `crates/sail-spark-connect/src/executor.rs` |
| Protobuf → spec conversion | `crates/sail-spark-connect/src/proto/` (`plan.rs`, `expression.rs`, `literal.rs`, `data_type*.rs`) |
| Spark session extension | `crates/sail-spark-connect/src/session.rs` |
| Session factory and timeout | `crates/sail-spark-connect/src/session_manager.rs` |
| Spark settings → plan config | `crates/sail-spark-connect/src/config.rs` |
| Error mapping | `crates/sail-spark-connect/src/error.rs` |
| Streaming queries | `crates/sail-spark-connect/src/streaming.rs` |


---

<a id="doc-04-sql-parser-and-analyzer"></a>

# 04 · SQL Parser and Analyzer

> **You'll learn:** how a SQL string becomes a syntax tree, then a spec plan;
> how Sail's parser is generated from Rust types; which SQL is supported; and
> how to add new syntax.
>
> **Crates:** `sail-sql-parser` (lexer, AST, parser), `sail-sql-macro`
> (derive macros that generate the parser), `sail-sql-analyzer` (AST → spec
> plan)

---

## 1. What This Component Does

SQL reaches Sail from several places:

- `spark.sql("…")` from PySpark (through Spark Connect),
- SQL statements from Flight SQL clients,
- view definitions, which are stored as SQL text and re-parsed when a view is
  read,
- smaller snippets: a data type string (`"struct<a:int>"`), an expression string
  (`df.selectExpr("a + 1")`), a table name (`"db.t"`), or a date or interval
  literal.

This component turns that text into Sail's **spec plan** (see
[05 · The Spec Plan Model](#doc-05-spec-plan-and-config)) in three steps:

```
"SELECT region, count(*) FROM sales.orders WHERE amount > 100 GROUP BY region"
   │  1. Lexer        → tokens:  SELECT · region · , · count · ( · * · ) · FROM · …
   │  2. Parser       → AST:     Query { select: [...], from: [...], where: ..., group_by: [...] }
   │  3. Analyzer     → spec:    Aggregate(Filter(Read("sales.orders"), amount > 100), [region], [count(*)])
   ▼
spec::Plan::Query(...)
```

---

## 2. Why It's Built This Way

**Sail has its own Spark SQL parser.** Generic SQL parsers don't match Spark's
dialect closely enough. Spark has backtick identifiers, `LATERAL VIEW`,
`DISTRIBUTE BY`/`CLUSTER BY`/`SORT BY`, `INSERT OVERWRITE DIRECTORY`, typed
literals like `INTERVAL '1' DAY` and `X'0A'`, the `<=>` operator, `DIV`, and
many statement forms. Owning the parser lets Sail match Spark's grammar and
error behavior exactly.

**The grammar is written as Rust types.** Each AST node is an ordinary Rust
struct or enum, and a derive macro generates its parser:

- a **struct** means "these parts in this order",
- an **enum** means "one of these alternatives",
- `Option<T>` means "optional", and `Vec<T>` or `Sequence<T, Comma>` means
  "repeated" or "separated by commas".

So the AST *is* the grammar. Adding syntax usually means adding a field or a
variant, and the parser follows automatically. The same derive also produces a
**syntax descriptor** (useful for generating grammar documentation) and a
**text** form that turns the AST back into SQL.

**Parsing and meaning are separate steps.** The parser only checks shape. The
analyzer maps shape to meaning but still doesn't look anything up: no catalogs,
no function registry, no schemas. All of that happens later, during resolution
(see [07 · The Plan Resolver](#doc-07-plan-resolver)). Keeping these steps
separate makes each one simple, easy to test, and reusable. The Spark Connect
and Flight SQL front ends both call the same two functions.

**Errors that point at the problem.** The parser is built on **chumsky**, a
parser combinator library that tracks spans (where each token came from) and
labels (what kind of thing was expected). A syntax error can say "expected an
expression at line 3, column 17" instead of just "parse error".

---

## 3. How It Works

### 3.1 Keywords are generated at build time

`data/keywords.txt` lists about 360 Spark SQL keywords in alphabetical order
(ADD, AFTER, ALL, …). A build script reads the file and generates:

- a `Keyword` enum with one variant per keyword (`SELECT` → `Select`,
  `SYSTEM_TIME` → `SystemTime`),
- a fast lookup table from text to keyword,
- one small parser type per keyword, so the AST can write `pub select: Select`.

To add a keyword, add a line to the file in sorted order.

### 3.2 The lexer: text → tokens

The lexer splits the input into tokens and keeps the original text and position
of each one:

| Token | Examples | Notes |
|-------|----------|-------|
| Word | `SELECT`, `orders`, `123`, `1e5` | Records whether the word is a keyword. Numbers start as words and are interpreted later. |
| String | `'abc'`, `"abc"`, `` `col name` ``, `X'0A'`, `r'\d+'` | Records the quote style and any prefix. Escape sequences aren't processed yet. |
| Whitespace | spaces, tabs, newlines | Kept as tokens so spans stay exact, then skipped by the parser. |
| Comment | `-- …`, `/* … */` | Block comments can be nested. |
| Punctuation | `(`, `)`, `,`, `.`, `+`, `<`, `=`, … | Multi-character operators (`<=>`, `>>>`, `::`) are assembled by the parser. |

Three **parser options** control dialect details: whether a quote can be
escaped by doubling it (`'it''s'`), whether triple-quoted strings are allowed,
and whether double-quoted text is an identifier or a string. Sail uses Spark's
defaults.

### 3.3 The parser: tokens → AST

Most AST types use the derived parser. Some can't be derived and are written by
hand:

- **Terminals**: keywords, literals, operators, identifiers.
- **Recursive types**: a query contains subqueries, and an expression contains
  expressions. These are declared first and defined afterwards, which the
  macro's `dependency` attribute supports.
- **Operator precedence**: expressions use a Pratt parser, which applies binding
  strength explicitly.

**Operator precedence**, from tightest to loosest binding, matching Spark:

| Level | Operators |
|-------|-----------|
| Postfix | field access `a.b`, subscript `a[0]`, `::` cast |
| Unary | `+a`, `-a`, `~a` |
| 24 | `*`, `/`, `%`, `DIV` |
| 23 | `+`, `-`, `\|\|` (string concatenation) |
| 22 | `<<`, `>>`, `>>>` |
| 21 | `&` |
| 20 | `^` |
| 19 | `\|` |
| 18 | `=`, `==`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `<=>`, `!<`, `!>` |
| Postfix | `IS [NOT] NULL`, `[NOT] BETWEEN`, `[NOT] IN`, `[NOT] LIKE`, `RLIKE`, … |
| Prefix | `NOT`, `!` |
| 14 | `AND` |
| 13 | `OR` |

So `a + b * c > 10 AND NOT d OR e` parses as
`((a + (b * c)) > 10 AND (NOT d)) OR e`.

**What a query looks like in the AST.** A query has an optional `WITH` clause,
a body, and modifiers (`WINDOW`, `ORDER BY`, `SORT BY`, `CLUSTER BY`,
`DISTRIBUTE BY`, `LIMIT`, `OFFSET`). The body is either a single `SELECT` or a
set operation (`UNION`, `INTERSECT`, `EXCEPT`/`MINUS`, with `ALL` or
`DISTINCT`). A `FROM` item can be a table name, a subquery, `VALUES`, a nested
join, or a table function. Every Spark join type is supported: inner, cross,
left/right/full outer, left/right semi, left/right anti, natural, and `USING`.

**Statements.** About 45 statement kinds are recognized:

| Group | Statements |
|-------|------------|
| Queries | `SELECT …`, `WITH …`, `VALUES …`, `TABLE t` |
| Catalogs and databases | `USE`, `SET CATALOG`, `CREATE`/`ALTER`/`DROP DATABASE`, `SHOW DATABASES`, `SHOW CATALOGS` |
| Tables | `CREATE TABLE`, `CREATE TABLE … AS SELECT`, `REPLACE TABLE`, `ALTER TABLE`, `DROP TABLE`, `REFRESH TABLE`, `SHOW TABLES`, `SHOW TABLE EXTENDED`, `SHOW CREATE TABLE`, `SHOW COLUMNS`, `DESCRIBE` |
| Views | `CREATE`/`ALTER`/`DROP VIEW`, `SHOW VIEWS` |
| Functions | `REFRESH FUNCTION`, `DROP FUNCTION`, `SHOW FUNCTIONS` |
| Writes | `INSERT INTO`, `INSERT OVERWRITE`, `INSERT … REPLACE WHERE`, `INSERT OVERWRITE DIRECTORY`, `MERGE INTO`, `UPDATE`, `DELETE`, `LOAD DATA` |
| Caching | `CACHE TABLE`, `UNCACHE TABLE`, `CLEAR CACHE` |
| Session | `SET key = value`, `SET TIME ZONE` |
| Other | `EXPLAIN`, `ANALYZE TABLE[S]`, `COMMENT ON …` |

### 3.4 The analyzer: AST → spec plan

The analyzer walks the AST and builds spec plan nodes. It's split by kind of
node:

- **Statements** decide between a query and a command. A query becomes
  `spec::Plan::Query`. `CREATE TABLE` becomes a create-table command node with a
  table definition (columns, format, partitioning, location, options), `USE db`
  becomes a set-current-database command, and so on.
- **Queries** become relational nodes: `FROM` becomes a read of a named table,
  `WHERE` a filter, the `SELECT` list a projection, `GROUP BY` an aggregate,
  `ORDER BY` a sort, and `LIMIT` a limit. `WITH` clauses become named
  subqueries. `LATERAL VIEW explode(…)` becomes a generator.
- **Expressions** become spec expressions. Names become *unresolved
  attributes*, function calls become *unresolved functions*, `*` becomes an
  *unresolved star*, and `CASE`, `CAST`, `EXTRACT`, `SUBSTRING`, lambdas,
  subqueries, and `EXISTS` each map to their spec form.
- **Literals** are parsed fully: numbers (integer, decimal, and float, including
  Spark suffixes like `10L`, `1.5BD`, `2D`), strings with escapes, binary
  (`X'…'`), dates, timestamps (with and without time zone), and intervals
  (`INTERVAL '1-2' YEAR TO MONTH`, `INTERVAL 3 DAYS 4 HOURS`).
- **Data types** map Spark type names (`INT`, `BIGINT`, `DECIMAL(10,2)`,
  `STRING`, `VARCHAR(10)`, `ARRAY<…>`, `MAP<…>`, `STRUCT<…>`, `TIMESTAMP_NTZ`, …)
  to spec types.

Nothing is looked up. A query that mentions a table that doesn't exist still
analyzes fine. The error comes later, during resolution.

### 3.5 Parameters

`spark.sql("SELECT * FROM t WHERE id = ?", args=[42])` and the named form
`:id` are supported. The Spark Connect layer converts the arguments into
literal or expression values and attaches them to the plan. Placeholders in the
SQL become *placeholder* expressions that the resolver fills in.

---

## 4. Examples

### 4.1 Walking through one query

```sql
SELECT region, count(*) AS n
FROM sales.orders
WHERE amount > 100
GROUP BY region
ORDER BY n DESC
LIMIT 10
```

| Step | Result |
|------|--------|
| Tokens | `SELECT`(keyword) · `region`(word) · `,` · `count` · `(` · `*` · `)` · `AS`(keyword) · `n` · `FROM`(keyword) · `sales` · `.` · `orders` · `WHERE` · `amount` · `>` · `100` · … |
| AST | A query whose body is a select with two projection items, `FROM` as the table name `sales.orders`, `WHERE` as a binary expression (`amount` `>` `100`), and `GROUP BY` `region`. Its modifiers are `ORDER BY` `n DESC` and `LIMIT 10`. |
| Spec | Limit(10, Sort([n DESC], Aggregate(group=[region], agg=[region, count(*) AS n], Filter(amount > 100, Read(table "sales.orders"))))) |

Every name in the spec (`region`, `amount`, `n`, `count`, `sales.orders`) is
still unresolved.

### 4.2 Snippet parsing from DataFrames

```python
df.selectExpr("upper(name) AS u", "price * qty AS total")   # parse_expression
df.filter("amount > 100")                                    # parse_expression
spark.createDataFrame(rows, "id INT, tags ARRAY<STRING>")    # parse_data_type
spark.table("db.t")                                          # parse_object_name
```

### 4.3 Seeing the parse

`EXPLAIN` and the analyzer's tests print the AST and spec plans. Many tests
check a **round trip**: parse the SQL, turn it back into text, and compare it
with the expected normalized form.

---

## 5. End-to-End: `spark.sql("CREATE TABLE t (id INT) USING parquet")`

| # | What happens |
|---|--------------|
| 1 | Spark Connect receives a SQL command and calls `parse_one_statement`. |
| 2 | The lexer produces tokens: `CREATE`, `TABLE`, `t`, `(`, `id`, `INT`, `)`, `USING`, `parquet`. |
| 3 | The parser tries each statement form and matches *create table*: name `t`, a column list with `id` of type `INT`, and a `USING parquet` clause. |
| 4 | The analyzer turns this into a create-table command with a table definition: the column `id` with type Int32, format `parquet`, and no location (so it's a managed table). |
| 5 | Because it's a command, Spark Connect runs it immediately. The resolver turns it into a catalog command operator, the catalog creates the table, and an empty result is returned (see [03 · Spark Connect](#doc-03-spark-connect)). |

---

## 6. Extending the SQL Grammar

**To add a new clause or statement:**

1. Add any new keywords to `data/keywords.txt`, in sorted order.
2. Add fields or variants to the AST type in `src/ast/` (`statement.rs`,
   `query.rs`, or `expression.rs`). For example, a new statement is a new
   `Statement` variant whose fields are the keyword tokens and parts in order.
3. If the new part is recursive, set the `dependency` and `function` attributes
   so it receives the already-declared query or expression parser.
4. Map the new AST node to spec nodes in `sail-sql-analyzer`. If no spec node
   fits, add one (see [05](#doc-05-spec-plan-and-config)) and handle it in the
   resolver (see [07](#doc-07-plan-resolver)).
5. Add parser tests (round trip) and an end-to-end test. Gold-data tests in the
   repository compare results with real Spark (see
   [17 · Testing and Development](#doc-17-testing-and-development)).

**Pitfalls:**

- Derived parsers can't handle **left recursion**. Write such rules by hand
  (as with expression operators).
- Alternatives are tried **in order**. Put longer or more specific forms first,
  for example `LEFT SEMI` before `LEFT`.
- New keywords can break queries that use those words as identifiers. Check
  how Spark treats the keyword (reserved or not), and add tests that use it as
  a column name.

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Keyword list and generator | `crates/sail-sql-parser/data/keywords.txt`, `crates/sail-sql-parser/build.rs` |
| Lexer and tokens | `crates/sail-sql-parser/src/lexer.rs`, `token.rs`, `string.rs` |
| AST (the grammar) | `crates/sail-sql-parser/src/ast/` (`statement.rs`, `query.rs`, `expression.rs`, `data_type.rs`, `literal.rs`, `identifier.rs`) |
| Parser options | `crates/sail-sql-parser/src/options.rs` |
| Derive macros (`TreeParser`, `TreeSyntax`, `TreeText`) | `crates/sail-sql-macro/src/` |
| Entry functions (`parse_one_statement`, `parse_expression`, …) | `crates/sail-sql-analyzer/src/parser.rs` |
| AST → spec | `crates/sail-sql-analyzer/src/` (`statement.rs`, `query.rs`, `expression.rs`, `data_type.rs`, `literal/`) |
| Callers | Spark Connect (`proto/plan.rs`), Flight SQL (`service.rs`), view expansion (`sail-plan/src/resolver/query/read.rs`) |


---

<a id="doc-05-spec-plan-and-config"></a>

# 05 · The Spec Plan Model (and Shared Foundations)

> **You'll learn:** what Sail's intermediate representation, the *spec plan*,
> looks like, why it exists, how its pieces fit together, and what else lives in
> the shared `sail-common` crate.
>
> **Crate:** `sail-common` (`spec`, `config`, `error`, `runtime`, `object`,
> `datetime`, `string`)

---

## 1. What This Component Does

The spec plan is the **common language** between Sail's front ends and its
planner. Every request ends up as a spec plan:

```
PySpark DataFrame ──(Spark Connect translator)──┐
spark.sql("…")    ──(SQL parser + analyzer)─────┼──► spec::Plan ──► Plan resolver ──► DataFusion
Flight SQL query  ──(SQL parser + analyzer)─────┘
```

A spec plan is a tree of plain Rust data (enums and structs) that can be
serialized with serde. It records **what the user asked for**, with names still
unresolved:

- Table names are just names (`["sales", "orders"]`), not table handles.
- Columns are *unresolved attributes*: a name and, optionally, the ID of the
  DataFrame it came from.
- Functions are *unresolved functions*: a name, arguments, and flags such as
  `DISTINCT` or `IGNORE NULLS`.
- Types are Sail's own data types, which closely follow Arrow types plus a few
  Spark-only ones.

---

## 2. Why It Exists

| Reason | What it means in practice |
|--------|---------------------------|
| **One resolver for every protocol** | SQL and DataFrame code both become spec plans, so the complicated resolution logic is written once. Adding a protocol means writing one translator. |
| **Resolve later** | Translators don't need a session, catalogs, or functions. They can be pure and simple to test. |
| **Normalization** | `df.filter(col("x") > 5)` and `WHERE x > 5` become the same filter node. The resolver handles one form, not two. |
| **Spark-only features** | Pandas UDFs, `mapInPandas`, streaming watermarks, `na.fill`, `stat.crosstab`, and similar features have spec nodes even though DataFusion has no equivalent. |
| **Inspectable** | Spec plans implement `Debug` and serde. When a result is wrong, print the spec plan to see whether the bug is in translation or later. |

---

## 3. What's in a Spec Plan

### 3.1 Top level

A **plan** is either:

- a **query plan**, which produces rows, or
- a **command plan**, which has a side effect and may produce a small result.

Both carry an optional **plan ID**. Spark Connect assigns an ID to each
DataFrame. When you write `df1["id"]`, the column reference carries `df1`'s
plan ID, so the resolver can tell `df1.id` from `df2.id` in a self-join, even
though both are named `id`.

### 3.2 Query nodes

About 70 query node types, grouped by purpose:

| Group | Nodes |
|-------|-------|
| Reading | Read (named table, data source with format/paths/options, table function, dynamic table), LocalRelation (inline data), Values, Range, Empty, CachedLocalRelation |
| Relational | Project, Filter, Join, LateralJoin, Aggregate, Sort, Limit (with optional offset), Tail, Deduplicate, SetOperation (union, intersect, except), SubqueryAlias, TableAlias |
| Columns | WithColumns, WithColumnsRenamed, Drop, ToDf, ToSchema |
| Reshaping | Pivot, Unpivot, LateralView (generators like `explode`) |
| Partitioning | Repartition, RepartitionByExpression, Hint |
| Sampling | Sample, TableSample |
| Python | MapPartitions (`mapInPandas`/`mapInArrow`), GroupMap (`applyInPandas`), CoGroupMap, ApplyInPandasWithState, user-defined table functions |
| Streaming | WithWatermark |
| DataFrame helpers | FillNa, DropNa, Replace, StatSummary, StatDescribe, StatCrosstab, StatCov, StatCorr, StatApproxQuantile, StatFreqItems, StatSampleBy, CollectMetrics, Parse (`from_json`/`from_csv` on a DataFrame) |
| SQL structure | WithCtes (`WITH`), WithParameters (bound `?`/`:name` parameters), WithRelations |

### 3.3 Command nodes

About 60 command types:

| Group | Commands |
|-------|----------|
| Catalogs and databases | CurrentCatalog, SetCurrentCatalog, CreateCatalog, CurrentDatabase, SetCurrentDatabase, CreateDatabase, DropDatabase, ListCatalogs, ListDatabases, GetDatabase, DatabaseExists, DescribeCatalog, DescribeDatabase |
| Tables | CreateTable, CreateTableAsSelect, AlterTable, DropTable, RefreshTable, RefreshByPath, RecoverPartitions, ListTables, ShowTables, ShowTableExtended, GetTable, TableExists, ListColumns, DescribeTable, AnalyzeTable(s) |
| Views | CreateView, AlterView, DropView, CreateTemporaryView, DropTemporaryView, ListViews |
| Functions | RegisterFunction, RegisterTableFunction, RefreshFunction, DropFunction, ListFunctions, GetFunction, FunctionExists, DescribeFunction |
| Writes | Write (v1 `df.write`), WriteTo (v2 `df.writeTo`), WriteStream, InsertInto, InsertOverwriteDirectory, LoadData |
| Row-level | MergeInto, Update, Delete |
| Caching | CacheTable, UncacheTable, ClearCache, IsCached |
| Display | ShowString (`df.show()`), HtmlString (notebooks), Explain, DescribeQuery |
| Other | SetVariable, CommentOn… |

### 3.4 Expressions

| Expression | Meaning | Example source |
|------------|---------|----------------|
| Literal | A typed constant | `5`, `'abc'`, `DATE '2024-01-01'` |
| UnresolvedAttribute | A column reference by name (plus an optional plan ID, and whether it's a metadata column) | `amount`, `t.amount`, `df["amount"]` |
| UnresolvedFunction | A function call by name, with positional and named arguments, `DISTINCT`, `IGNORE NULLS`, `FILTER (WHERE …)`, and `ORDER BY` inside aggregates | `count(DISTINCT x)`, `percentile_cont(0.5) WITHIN GROUP (ORDER BY x)` |
| UnresolvedStar | `*` or `t.*`, optionally with exclusions | `SELECT * EXCEPT (a)` |
| UnresolvedRegex | Columns selected by regular expression | `` df.colRegex("`a.*`") `` |
| UnresolvedExtractValue | Field, index, or key access | `s.field`, `arr[0]`, `m['k']` |
| Alias, Cast, SortOrder, Window | What the names say | `x AS y`, `CAST(x AS INT)`, `ORDER BY x DESC NULLS LAST`, `OVER (…)` |
| LambdaFunction, UnresolvedNamedLambdaVariable | Higher-order functions | `transform(arr, x -> x + 1)` |
| UpdateFields | Change struct fields | `withField`, `dropFields` |
| Subqueries | ScalarSubquery, InSubquery, Exists | `WHERE x IN (SELECT …)` |
| Grouping | Rollup, Cube, GroupingSets | `GROUP BY ROLLUP(a, b)` |
| CommonInlineUserDefinedFunction | An inline Python UDF definition | `@udf` functions |
| Placeholder, NamedArgument, CallFunction | Parameters, `f(arg => value)`, `call_function("name", …)` | |

### 3.5 Data types

Spec data types follow Arrow closely: integers (signed and unsigned, 8–64
bits), floats (16, 32, 64), decimals (128 and 256), dates, times, timestamps,
durations, intervals, binary and string types (regular, large, and view
variants), lists, structs, maps, unions, and dictionaries. A few types aren't
in Arrow:

| Type | Why |
|------|-----|
| **ConfiguredUtf8 / ConfiguredBinary** | Spark's `STRING` and `BINARY`. Whether they become 32-bit or 64-bit-offset Arrow types depends on `spark.sql.execution.arrow.useLargeVarTypes`, which isn't known until resolution. |
| **Timestamp with "configured" time zone** | Spark's plain `TIMESTAMP` means LTZ or NTZ depending on `spark.sql.timestampType`, also decided at resolution. |
| **UserDefined** | Spark UDTs, which keep their JVM/Python class names and an underlying SQL type. |
| **Variant** | Spark 4's semi-structured `VARIANT`. |
| **Geometry / Geography** | Spatial types with a spatial reference ID (Spark 4.1). |

Deciding these "configured" types late is a good example of why resolution is
a separate step: the same SQL text can produce different Arrow types in
different sessions.

### 3.6 Literals

Literals carry their exact type: every integer width, floats, decimals with
precision and scale, timestamps at each precision (second to nanosecond) with an
optional time zone, dates, times, intervals (year-month, day-time,
month-day-nanosecond), binary, strings, and nested arrays, maps, and structs.

---

## 4. Other Shared Foundations in `sail-common`

| Module | What it provides |
|--------|------------------|
| `config` | The application configuration: a YAML schema compiled into the binary, plus `SAIL_*` environment variable overrides (see [02](#doc-02-entry-points-and-server)). Also the internal CLI configuration. |
| `runtime` | The primary and io Tokio runtimes and the handle passed around to reach them. |
| `error` | Common error kinds: missing argument, invalid argument, not supported, internal error. Each crate wraps these in its own error type. |
| `object` | Helpers that let trait objects be compared, hashed, and ordered. DataFusion needs this for Sail's custom plan nodes and UDFs. |
| `datetime` | Time zone and timestamp helpers shared by functions and planning. |
| `string` | String helpers, such as case-insensitive comparison. |

---

## 5. Example: The Same Query From Two Front Ends

**DataFrame:**

```python
orders = spark.table("sales.orders")
orders.filter(orders["amount"] > 100).select("region", "amount").limit(5)
```

**SQL:**

```sql
SELECT region, amount FROM sales.orders WHERE amount > 100 LIMIT 5
```

Both produce essentially this spec plan (shown as an indented tree):

```text
Query: Limit(5)
  Project [ UnresolvedAttribute(region), UnresolvedAttribute(amount) ]
    Filter UnresolvedFunction(">", [UnresolvedAttribute(amount), Literal(Int32 100)])
      Read NamedTable(name = ["sales", "orders"])
```

The only difference: in the DataFrame version, the `amount` reference in the
filter carries the plan ID of `orders`. Note that `>` is just a function named
`>` at this stage. Operators become real comparison expressions during
resolution.

---

## 6. End-to-End: Life of a Spec Plan

| # | Stage | What happens to the spec plan |
|---|-------|-------------------------------|
| 1 | Front end | Built from protobuf or SQL. Pure data, no session needed. |
| 2 | Debugging (optional) | Can be printed (`{:?}`) or serialized. |
| 3 | Resolver | Walked top-down and bottom-up. Names are looked up, types are fixed using the session's plan config, and the result is a DataFusion logical plan (see [07](#doc-07-plan-resolver)). |
| 4 | After resolution | No longer used. Everything downstream works on DataFusion plans. |

---

## 7. Extending the Spec

Add a new spec node when a feature can't be expressed with existing nodes:

1. Add the variant to the query node, command node, or expression enum, with
   fields that capture **intent**, not implementation. Use spec expressions and
   object names, never DataFusion types.
2. Produce it from every front end that supports the feature: Spark Connect
   (`proto/plan.rs`) and, if there's SQL syntax for it, the SQL analyzer.
3. Resolve it in `sail-plan` (see [07](#doc-07-plan-resolver)).
4. Keep the field order and serde names stable. The spec is used in tests and
   logs.

---

## 8. Where to Look

| Topic | Location |
|-------|----------|
| Plans, query nodes, command nodes | `crates/sail-common/src/spec/plan.rs` |
| Expressions, object names, identifiers | `crates/sail-common/src/spec/expression.rs` |
| Data types | `crates/sail-common/src/spec/data_type.rs` |
| Literals | `crates/sail-common/src/spec/literal.rs` |
| Configuration | `crates/sail-common/src/config/` |
| Runtimes | `crates/sail-common/src/runtime.rs` |
| Errors | `crates/sail-common/src/error.rs` |
| Settings that affect resolution (plan config) | `crates/sail-plan/src/config.rs` |


---

<a id="doc-06-session-and-runtime"></a>

# 06 · Sessions and the Runtime Environment

> **You'll learn:** what a Sail session contains, how it's assembled, how
> sessions are created, tracked, timed out, and deleted, and how memory,
> spilling, caches, and storage access are configured for each session.
>
> **Crates:** `sail-session`, `sail-cache`, and the session parts of
> `sail-common-datafusion`

---

## 1. What This Component Does

Sail builds on Apache DataFusion, whose central object is the
**`SessionContext`**: configuration, a runtime environment (memory, disk,
storage), optimizer rules, a query planner, and registered functions. Sail
creates one per client session and customizes it heavily.

This component:

1. **assembles** a session context with everything Sail needs (catalogs, table
   formats, the job runner, the planner, optimizer rules, and Spark-compatible
   settings),
2. **manages** session lifecycles (create on first use, reuse, time out, delete,
   and remember history),
3. **builds the runtime environment**: the memory pool, temporary-file
   (spill) directories, file metadata caches, and the object store registry.

---

## 2. Why It's Built This Way

**Extensions instead of wrappers.** DataFusion's session configuration can hold
arbitrary typed **extensions**. Sail attaches its own services there, and any
code holding the session can get one by type:
`ctx.extension::<CatalogManager>()`. That means Sail doesn't have to wrap or
fork DataFusion's types, and new services can be added without changing
function signatures across the codebase.

**Sail turns off DataFusion's defaults.** Server sessions are created
*without* DataFusion's default catalog, information schema, built-in table
formats, or built-in functions. Sail manages all of these itself, for Spark
compatibility. For example, Spark's `concat` with a NULL argument returns NULL,
unlike DataFusion's version. One DataFusion function, `first_value`, is
registered explicitly because one of DataFusion's optimizer rules quietly
depends on it.

**Two factories for two jobs.** Client-facing sessions and worker sessions need
different things. A worker only has to *run* plan fragments, so it doesn't need
catalogs, user state, or a job runner, but it does need every function and
format available to decode the plan it receives.

**One job runner per session.** Each server session creates its own job runner.
In cluster mode, that means **each session gets its own driver**, with its own
workers. Sessions are isolated: one user's job can't take over another
session's workers.

**An actor for the session lifecycle.** Requests for the same session can
arrive concurrently. Creation, idle checks, and deletion are messages handled
one at a time by a session manager actor, so they can't race.

---

## 3. How It Works

### 3.1 What a server session contains

| Extension | Purpose | Read more |
|-----------|---------|-----------|
| Table format registry | Parquet, CSV, JSON, Avro, Arrow, text, binary, socket, rate, console, Delta, Iceberg, and Python data sources | [12](#doc-12-data-sources-and-formats), [13](#doc-13-lakehouse-delta-and-iceberg) |
| Catalog manager | Catalogs, databases, tables, views, temporary objects, name resolution | [11](#doc-11-catalogs) |
| Activity tracker | Time of last activity, used for idle timeout | below |
| Job service | Wraps the job runner (local or cluster) | [09](#doc-09-local-and-cluster-execution) |
| System table service | Lets `system.*` tables see sessions, jobs, stages, tasks, and workers | below |
| Delta table cache | Caches loaded Delta table state within the session | [13](#doc-13-lakehouse-delta-and-iceberg) |
| Spark session | Added by the Spark Connect front end: IDs, Spark settings, executors, streaming queries | [03](#doc-03-spark-connect) |

The session state also includes:

- **analyzer and optimizer rules**: Sail's logical rules. Lakehouse rules (such
  as expanding row-level operations) run **first**, ahead of DataFusion's
  built-in rules,
- **physical optimizer rules**: Sail's list, with optional cost-based join
  reordering (`optimizer.enable_join_reorder`, off by default) (see
  [08](#doc-08-logical-physical-plans-and-optimizers)),
- **the query planner**: Sail's extension planner, which knows how to plan Sail's
  custom nodes,
- **DataFusion execution settings** copied from Sail configuration: batch size
  (8,192 rows by default), target partitions (`execution.default_parallelism`;
  0 means one per CPU core), statistics collection, and about 30 Parquet read and
  write settings (page index, pruning, filter pushdown, compression, row group
  size, bloom filters, and so on).

### 3.2 The runtime environment

The runtime environment controls the physical resources a session uses:

| Resource | Setting | Behavior |
|----------|---------|----------|
| **Memory pool** | `runtime.memory_pool.type` | `unbounded` (default): no limit. `greedy`: first come, first served up to `max_size`. `fair`: `max_size` shared fairly among operators that can spill. Only bounded pools make operators **spill** instead of growing without limit. |
| **Spill files** | `runtime.temporary_files.paths`, `.max_size` | A max size of 0 turns spilling off. No paths means the OS temporary directory. Otherwise spills go to the listed directories. The default limit is 64 GiB. |
| **Object stores** | (see [14](#doc-14-object-storage)) | A dynamic registry that creates S3, GCS, Azure, HTTP, HDFS, or local clients on demand. |
| **File listing cache** | `execution.file_listing_cache.*` | Caches directory listings. Off by default. |
| **File statistics cache** | `parquet.file_statistics_cache.*` | Caches per-file statistics. Global by default. |
| **File metadata cache** | `parquet.file_metadata_cache.*` | Caches Parquet footers. Global by default. |

Each cache has a **scope**: `none`, `global` (one cache shared by every
session in the process), or `session` (a new cache per session). Caches are
built on `moka` with a time-to-live and a size or entry limit.

| Scope | Good for | Watch out for |
|-------|----------|---------------|
| Global | Many sessions reading the same tables | Changes made outside Sail are seen only after the TTL expires. |
| Session | Isolation between users | Each session pays to warm its own cache. |
| None | Data that changes constantly | Every query lists files and reads footers again. |

### 3.3 Worker sessions

A worker session is minimal: a runtime environment (the same memory, spill, and
storage setup), a Delta table cache, and DataFusion's **default features**
(built-in functions and formats). The defaults are needed because when a worker
decodes a plan fragment, every function the plan refers to must be resolvable
by name.

### 3.4 Session lifecycle

```text
                GetOrCreateSession
   (none) ────────────────────────────► Running ──DeleteSession──► Deleting ──► Deleted (history kept)
                                          │
                                          └── creation or stop failed ──► Failed
```

1. **Get or create.** The front end sends *get or create (session ID, user ID)*.
   If the session is running, the existing context is returned. Otherwise the
   factory builds a new one.
2. **Track activity and schedule a probe.** Every *get or create* call records
   "active now" in the session's activity tracker and schedules a *probe idle
   session* message, carrying that timestamp, for after the timeout
   (`spark.session_timeout_secs`, 15 minutes by default, or
   `flight.session_timeout_secs`, 1 hour, for Flight SQL).
3. **Check the probe.** When a probe arrives, the actor compares the session's
   last activity with the probe's timestamp. If nothing has happened since,
   the session is idle and is deleted. If there was activity, that later
   request already scheduled its own probe, so this one is ignored.
4. **Delete.** The job runner is stopped. In cluster mode this shuts down the
   session's driver and its workers. The job runner returns a **history**
   (jobs, stages, tasks, workers), which is kept in the *Deleted* state so
   system tables can still report on finished sessions.

### 3.5 System tables

Sail exposes its own runtime state as read-only SQL tables in a catalog named
`system`:

| Table | Shows |
|-------|-------|
| `system.session.sessions` | Sessions and their status |
| `system.session.options` | Session settings |
| `system.execution.jobs` | Jobs per session: ID, status, start and stop times |
| `system.execution.stages` | Stages: partitions, inputs, mode, distribution, placement, status |
| `system.execution.tasks` | Tasks: stage, partition, attempt, status, times |
| `system.cluster.workers` | Workers: ID, host, port, status, times |

The table definitions are generated at build time from a YAML file. Queries on
them are answered by asking the session manager actor, which asks each session's
job runner (or its saved history).

---

## 4. Examples

### 4.1 Bound memory and send spills to a fast disk

```bash
export SAIL_RUNTIME__MEMORY_POOL__TYPE=fair
export SAIL_RUNTIME__MEMORY_POOL__FAIR__MAX_SIZE=17179869184      # 16 GiB
export SAIL_RUNTIME__TEMPORARY_FILES__PATHS='["/mnt/nvme/sail-spill"]'
sail spark server
```

A large sort or hash aggregation now writes sorted runs to `/mnt/nvme/sail-spill`
once it reaches its share of the 16 GiB, instead of growing until the operating
system kills the process.

### 4.2 Cache directory listings for a read-heavy dashboard

```bash
export SAIL_EXECUTION__FILE_LISTING_CACHE__TYPE=global
export SAIL_EXECUTION__FILE_LISTING_CACHE__TTL=300   # seconds
```

### 4.3 See what's running

```python
spark.sql("SELECT * FROM system.execution.jobs").show()
spark.sql("SELECT stage, status, count(*) FROM system.execution.tasks GROUP BY 1, 2").show()
```

---

## 5. End-to-End: A New PySpark Client Connects

| # | What happens |
|---|--------------|
| 1 | The first request arrives with a new session ID. |
| 2 | The front end asks the session manager actor to *get or create* it. |
| 3 | The actor calls the server session factory, which builds the extensions (table formats including Python data sources, catalogs from `catalog.list`, activity tracker, a new job runner for `mode`, system tables, Delta cache). |
| 4 | It builds the runtime environment: memory pool, disk manager, caches, and object store registry. |
| 5 | It builds the session state without DataFusion's defaults, adding Sail's analyzer, optimizer, and physical rules and the extension planner. It registers `first_value`. |
| 6 | The Spark Connect hook adds the Spark session extension and sets the default time zone. |
| 7 | The actor stores the session as *Running*, records the activity time, and schedules an idle probe. |
| 8 | Later requests reuse the context. Each one updates the activity time and schedules its own probe. |
| 9 | When a probe finds no activity since it was scheduled (15 minutes by default), the session is deleted, its job runner is stopped, and its history is kept. |

---

## 6. Common Problems

| Symptom | Cause | Fix |
|---------|-------|-----|
| Process killed for running out of memory on big joins or sorts | Default `unbounded` memory pool | Use `fair` with a `max_size` below the container limit. |
| "Resources exhausted" errors | Pool too small, or the operator can't spill | Raise `max_size`, or make sure spilling isn't disabled (`temporary_files.max_size` > 0). |
| New files in a table aren't picked up | A global listing or statistics cache with a long TTL | Lower the TTL, use `session` scope, or run `REFRESH TABLE`. |
| A function works locally but fails on workers | The function isn't available in the worker session | Check that it's registered somewhere the worker can decode it. |
| Temporary views disappear | Session idle timeout | Raise `spark.session_timeout_secs`. |

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Server session factory | `crates/sail-session/src/session_factory/server.rs` |
| Worker session factory | `crates/sail-session/src/session_factory/worker.rs` |
| Runtime environment (memory, spill, caches) | `crates/sail-session/src/runtime.rs` |
| Caches | `crates/sail-cache/src/` |
| Session manager actor | `crates/sail-session/src/session_manager/` |
| Table format registration | `crates/sail-session/src/formats.rs` |
| Catalog setup | `crates/sail-session/src/catalog.rs` |
| Optimizer rule lists | `crates/sail-session/src/optimizer.rs` |
| Extension planner | `crates/sail-session/src/planner.rs` |
| Extension trait, job runner trait, activity tracker | `crates/sail-common-datafusion/src/extension.rs`, `session/` |
| System table definitions | `crates/sail-common-datafusion/data/system/databases.yaml`, `crates/sail-catalog-system/` |


---

<a id="doc-07-plan-resolver"></a>

# 07 · The Plan Resolver: From Intent to a DataFusion Plan

> **You'll learn:** how Sail turns a spec plan into an optimized DataFusion
> physical plan; how names, columns, functions, tables, views, and commands are
> resolved; and why Sail gives every column an internal ID.
>
> **Crate:** `sail-plan` (resolver, function mapping, catalog command node,
> explain, result formatting, streaming rewrite)

---

## 1. What This Component Does

The resolver is the **brain of Sail's Spark compatibility**. It takes the
protocol-neutral spec plan (see [05](#doc-05-spec-plan-and-config)) and the
session's plan config (time zone, ANSI mode, timestamp type, …) and produces
DataFusion plans that behave like Spark.

It owns the whole planning pipeline:

```text
spec::Plan
  │ 1. Resolve       → DataFusion LogicalPlan (with internal column IDs) + user-facing names
  │ 2. Analyze       → DataFusion analyzer rules (type coercion, …)
  │ 3. Optimize      → Sail and DataFusion logical optimizer rules
  │ 4. Stream rewrite→ adjustments for streaming plans (if the plan is streaming)
  │ 5. Physical plan → the extension planner turns logical nodes into operators, then physical optimizer rules run
  │ 6. Rename        → internal column IDs replaced with the user-facing names
  ▼
Arc<dyn ExecutionPlan>  (ready for a job runner)
```

The entry point is `resolve_and_execute_plan(session, plan_config, spec_plan)`.
It returns the physical plan plus text renderings of the initial logical,
final logical, and final physical plans, which `EXPLAIN` uses.

---

## 2. Why It's Built This Way

**Spark semantics differ from DataFusion's in many small ways.** Name matching
is case-insensitive. Duplicate column names are allowed (`SELECT a, a`). Join
outputs keep both sides' columns even when they share a name. `GROUP BY` can
refer to aliases. `HAVING` and `ORDER BY` can refer to columns that aren't in the
`SELECT` list. Hundreds of functions have Spark-specific behavior. The resolver
handles all of this **before** DataFusion sees the plan, so DataFusion only has
to execute well-formed plans.

**Every column gets an internal ID.** When the resolver produces a column, it
names it with an opaque ID like `#7` and records `#7 → "amount"` in its state.
This solves several problems:

- **Duplicates and ambiguity.** `SELECT a, a` or a self-join produces two columns
  named `a`. In DataFusion they'd clash. As `#3` and `#9` they don't.
- **DataFrame references.** A Spark Connect column reference can carry the plan
  ID of the DataFrame it came from (`df1["id"]`). Each ID records which plan IDs
  it belongs to, so `df1.id` and `df2.id` resolve to different columns.
- **Hidden columns.** Some operations need extra columns internally, such as the
  join keys of both sides of an outer join, or columns used only by `ORDER BY`.
  These are marked *hidden* and removed from the final output.

At the end, a **named plan** carries the logical plan and the list of
user-facing names. After physical planning, the output columns are renamed
from IDs back to those names.

**Functions are a lookup table of builders.** Sail doesn't register its Spark
functions in DataFusion's registry. It keeps a table that maps each function name
to a small builder that receives already-resolved arguments and the session
context, and returns a DataFusion expression. A builder can map directly to a
DataFusion function (`sqrt`), wrap it with casts, call a Sail-specific UDF
(`concat` uses Sail's Spark-compatible concat), or do custom logic
(`upper`, `element_at`). Behavior that depends on the session, such as ANSI mode
or time zone, is handled here.

---

## 3. How It Works

### 3.1 Resolver state

One state object is threaded through the whole resolution of a plan:

| Field | Purpose |
|-------|---------|
| Field registry | Maps each internal ID (`#0`, `#1`, …) to a user name, the plan IDs that refer to it, and whether it's hidden |
| Outer query schema | The schema visible to a correlated subquery from its enclosing query |
| Aggregate state | Whether we're resolving `GROUP BY` or `HAVING`, and the projections and grouping expressions available, so `HAVING count(*) > 1` can reuse aggregate results |
| CTEs | `WITH` definitions visible to the current query |
| Subquery references | Relations attached through Spark Connect's "with relations" mechanism |
| Parameter values | Bound `?` and `:name` values, so `IDENTIFIER(:tbl)` can be evaluated |

### 3.2 Resolving queries

The query resolver is a big dispatch over spec query nodes. Each node type has a
focused module: read, project, filter, join, aggregate, sort, limit, set
operations, pivot, sample, values, lateral views, lateral joins, column operations
(`withColumns`, rename, drop), NA functions, statistics, UDF and UDTF nodes, time
travel, and so on.

After resolving each node, the resolver **checks that every output column is
registered** in its state. If a new code path forgets to register a column, it
fails right away with a clear error instead of producing a plan with
unexplained names.

### 3.3 Resolving a named table

When a query reads `sales.orders`, the resolver tries these in order:

1. **`format.path` shortcut.** If the name has two parts and the first is a
   registered format, as in `` parquet.`s3://bucket/path` ``, it's a direct file
   read.
2. **CTE.** If a `WITH` clause defined that name, its plan is used.
3. **Catalog lookup.** The catalog manager (see [11](#doc-11-catalogs)) returns
   what the name refers to:
   - **Table**: its columns, format, location, partitioning, bucketing, sort
     order, constraints, and properties. The resolver builds a *source info*
     (paths, schema, partitioning, and layered options: table properties, then
     query options, then time-travel options) and asks the **table format** for a
     table source (see [12](#doc-12-data-sources-and-formats)).
   - **View**: the stored SQL text is **parsed again** and resolved, so the view
     sees current table definitions.
   - **Temporary or global temporary view**: the stored logical plan is reused,
     with fresh column IDs.
4. **Table sample** (`TABLESAMPLE`) and **time travel** (`VERSION AS OF`,
   `TIMESTAMP AS OF`) are applied where supported. Time travel isn't allowed on
   CTEs or views.

### 3.4 Resolving columns

To resolve a column reference like `t.amount` or `df["amount"]`:

1. Generate candidate interpretations. `a.b.c` could be table `a`, column `b`,
   field `c`, or column `a` with nested fields `b.c`, and so on.
2. Look for matching fields in the current input schema, comparing names
   case-insensitively and checking the plan ID if there is one.
3. In an aggregate context, also look in the aggregate's outputs.
4. Look at **hidden** fields (for example, sorting by a column that was
   projected away).
5. If still not found and we're inside a subquery, look in the **outer**
   query's schema. That makes it a correlated reference.
6. Zero matches gives *cannot resolve attribute*. More than one gives
   *ambiguous attribute*.

### 3.5 Resolving functions

For each unresolved function:

1. Resolve the arguments first.
2. Check the session's registered **user functions** (Python UDFs, see
   [15](#doc-15-functions-and-python-udfs)).
3. Look the name up in the **built-in tables**: scalar, generator (`explode`,
   `posexplode`, `inline`, `stack`, …), aggregate, window, and table-valued
   functions (`range`, …).
4. Call the builder with the arguments and a function context (session time
   zone, ANSI mode, …). It returns a DataFusion expression.
5. Unknown names produce *unknown function*, which the client sees as an
   `AnalysisException`.

Special cases include `count(*)` (rewritten to count a constant), `DISTINCT`
and `FILTER (WHERE …)` on aggregates, `IGNORE NULLS`, ordered-set aggregates
(`percentile_cont … WITHIN GROUP`), named arguments, and lambda functions
(`transform`, `filter`, `aggregate`).

### 3.6 Aggregation

Resolving an aggregate query happens in three steps:

1. Resolve the **grouping expressions** (which may refer to `SELECT` aliases)
   and the **projections**, pulling out the aggregate calls.
2. Resolve `HAVING` with access to both, so `HAVING count(*) > 10` reuses the
   same aggregate.
3. Rewrite into DataFusion's form: an aggregate node with grouping and
   aggregate expressions, an optional filter for `HAVING`, and a projection on
   top. `ROLLUP`, `CUBE`, and `GROUPING SETS` become grouping-set expressions,
   with `grouping()` and `grouping_id()` support.

### 3.7 Resolving commands

Commands become logical plans too, so they go through the same pipeline and job
runner as queries:

| Command kind | Logical plan produced | Executed by |
|--------------|-----------------------|-------------|
| Catalog commands (create, drop, alter, list, describe, `USE`, …) | A **catalog command node** wrapping a catalog command | A catalog command operator that calls the catalog manager and returns a small result table |
| Writes (`df.write`, `writeTo`, `INSERT`, CTAS, `INSERT OVERWRITE DIRECTORY`) | A **file write node** with sink info (mode, partitioning, bucketing, options) | The table format's writer (see [12](#doc-12-data-sources-and-formats)) |
| `DELETE`, `UPDATE`, `MERGE INTO` | **File delete** and **merge into** nodes | Lakehouse planners (see [13](#doc-13-lakehouse-delta-and-iceberg)) |
| `df.show()`, notebook HTML | A **show string** node | Formats rows as a text table, Spark style |
| `EXPLAIN` | Runs the pipeline in explain mode (simple, extended, codegen, cost, formatted, verbose, analyze) | Returns the plan text |
| Register UDF/UDTF, `SET` variable | Updates the session | Returns an empty result |
| Streaming write | A streaming plan, rewritten for streaming execution | A streaming query in the session |

Other Sail logical nodes the resolver produces: range, monotonically increasing
ID, Spark partition ID, explicit repartition, sort within partitions, map
partitions (Python), and barrier. See
[08](#doc-08-logical-physical-plans-and-optimizers).

---

## 4. Example: Resolving a Query Step by Step

```sql
SELECT region, count(*) AS n
FROM sales.orders
WHERE amount > 100
GROUP BY region
```

**Spec plan** (from [04](#doc-04-sql-parser-and-analyzer)):
Aggregate(group=[region], proj=[region, count(*) AS n], input=Filter(amount > 100, Read("sales.orders"))).

**Resolution:**

| Step | Action | State after |
|------|--------|-------------|
| Read | The catalog says `sales.orders` is a Parquet table at `s3://lake/orders` with columns `order_id` (bigint), `region` (string), and `amount` (double). The Parquet format returns a table source. Columns are registered. | `#0→order_id`, `#1→region`, `#2→amount` |
| Filter | `amount` resolves to `#2`. `>` with the integer literal `100` becomes `#2 > 100.0` after type coercion. | unchanged |
| Aggregate | `region` resolves to `#1`. `count(*)` becomes `count(1)`. The alias `n` registers a new field. | `#3→region` (group output), `#4→n` |
| Named plan | Logical plan plus user names `["region", "n"]` | |

**DataFusion logical plan** (simplified):

```text
Projection: #3, #4
  Aggregate: groupBy=[[#1 AS #3]], aggr=[[count(Int64(1)) AS #4]]
    Filter: #2 > Float64(100)
      TableScan: sales.orders projection=[#1, #2]
```

**After optimization and physical planning:** the filter is pushed into the
Parquet scan (row-group pruning and page-index filtering), the aggregate is
split into partial and final stages around a hash repartition on `region`, and
the output columns are renamed to `region` and `n`.

---

## 5. End-to-End: Where the Resolver Sits

| # | Caller | Resolver's role |
|---|--------|-----------------|
| 1 | Spark Connect or Flight SQL hands over a spec plan and plan config | |
| 2 | | Builds a resolver bound to the session |
| 3 | | Resolves the named plan (table lookups through the catalog manager and table formats, functions through the builder tables) |
| 4 | | Runs DataFusion's analyzer and optimizer through the session state (Sail rules included) |
| 5 | | Applies the streaming rewrite if the plan reads from a streaming source |
| 6 | | Asks the session's query planner for a physical plan (Sail extension planners, then physical optimizer rules) |
| 7 | | Renames output columns to user-facing names |
| 8 | Job runner receives the physical plan (see [09](#doc-09-local-and-cluster-execution)) | |

---

## 6. Extending and Debugging

**Adding a Spark function:**

1. Implement it in `sail-function` if DataFusion has no equivalent (see
   [15](#doc-15-functions-and-python-udfs)).
2. Add `("name", builder)` to the matching list in `sail-plan/src/function/`
   (scalar by category, aggregate, window, generator, table).
3. Pick the builder style: `unary` or `binary` for direct mappings, `udf` for a
   scalar UDF, or `custom` when you need the function context or argument
   rewriting.
4. Add a test and update the compatibility data (see
   [17](#doc-17-testing-and-development)).

**Adding a new query node:** add a resolver function in `resolver/query/`, make
sure every new output column is registered, and if no DataFusion node fits,
create a Sail logical extension node (see
[08](#doc-08-logical-physical-plans-and-optimizers)).

| Symptom | Where to look |
|---------|---------------|
| `cannot resolve attribute` | Column resolution. Check case, qualifiers, plan IDs, and whether the column was projected away before it's used. |
| `ambiguous attribute` | A join of two inputs that share a name. Qualify it, or use `df1["col"]`. |
| `unknown function: x` | Missing from the built-in tables, or the UDF isn't registered in this session |
| Wrong column names in results | The named plan's user-facing names, or the final rename |
| Different result from Spark for a function | That function's builder. Check casts, NULL handling, and ANSI behavior. |
| "attribute … is missing from the schema" from a new feature | A new column wasn't registered in the resolver state |

`EXPLAIN EXTENDED` shows the initial logical, final logical, and physical plans
that this pipeline records.

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Pipeline entry point (`resolve_and_execute_plan`) | `crates/sail-plan/src/lib.rs` |
| Resolver and named plan | `crates/sail-plan/src/resolver/mod.rs`, `plan.rs` |
| Resolver state (column IDs, hidden columns, CTEs) | `crates/sail-plan/src/resolver/state.rs` |
| Query nodes | `crates/sail-plan/src/resolver/query/` (`read.rs`, `aggregate.rs`, `join.rs`, `project.rs`, …) |
| Expressions | `crates/sail-plan/src/resolver/expression/` (`attribute.rs`, `function.rs`, `window.rs`, `subquery.rs`, `wildcard.rs`, `udf.rs`, …) |
| Commands | `crates/sail-plan/src/resolver/command/` (`write.rs`, `merge.rs`, `catalog/`, …) |
| Function builder tables | `crates/sail-plan/src/function/` (`scalar/`, `aggregate.rs`, `window.rs`, `generator.rs`, `table/`, `common.rs`) |
| Catalog command node | `crates/sail-plan/src/catalog.rs` |
| `EXPLAIN` | `crates/sail-plan/src/explain.rs` |
| `show()` formatting | `crates/sail-plan/src/formatter.rs` |
| Settings that affect resolution | `crates/sail-plan/src/config.rs` |
| Streaming rewrite | `crates/sail-plan/src/streaming/` |


---

<a id="doc-08-logical-physical-plans-and-optimizers"></a>

# 08 · Logical and Physical Plans, Extension Nodes, and Optimizers

> **You'll learn:** which operators Sail adds to DataFusion and why; how custom
> logical nodes become physical operators; which optimizer rules run and in
> what order; and how cost-based join reordering works.
>
> **Crates:** `sail-logical-plan`, `sail-physical-plan`,
> `sail-logical-optimizer`, `sail-physical-optimizer`, plus the extension
> planner in `sail-session/src/planner.rs`

---

## 1. What This Component Does

DataFusion has a rich set of built-in operators: scans, filters, projections,
joins, aggregates, sorts, windows, unions, and repartitioning. Spark needs a few
more. Sail fills the gap in two layers:

| Layer | What Sail adds |
|-------|----------------|
| **Logical** (what to compute) | About 15 **extension nodes** that represent Spark-only operations |
| **Physical** (how to compute) | A matching **execution operator** for each, plus a few physical-only helpers |
| **Planning** | An **extension planner** that maps each logical node to its operator |
| **Optimization** | A curated list of DataFusion's logical and physical rules plus Sail's own rules, including **cost-based join reordering** |

---

## 2. Why It's Built This Way

**Use DataFusion's extension points, don't fork it.** DataFusion lets
applications define their own logical nodes (`UserDefinedLogicalNode`) and
physical operators (`ExecutionPlan`), and plug in a planner for them. Sail's
Spark-specific behavior lives entirely in these extensions, so Sail can keep up
with DataFusion releases.

**The resolver has no side effects.** Resolving a plan must not change anything.
For example, `CREATE TABLE AS SELECT` can't create the table while planning,
because `EXPLAIN` of that statement must not create the table either. Side
effects are expressed as nodes and happen only during **execution**. The
**barrier** node exists for this: "run these preconditions (such as a catalog
change) first, then the main plan."

**Keep every DataFusion rule, add a few.** Sail's physical rule list is checked
by a test: it must contain **all** of DataFusion's default physical rules, **in
the same order**, with Sail's rules inserted around them. This guards against
accidentally dropping an optimization when upgrading DataFusion.

---

## 3. How It Works

### 3.1 Sail's extension nodes

| Logical node | Physical operator | Spark feature | What it does |
|--------------|-------------------|---------------|--------------|
| Range | RangeExec | `spark.range(…)`, `range()` table function | Generates numbers in parallel, split across a set number of partitions |
| ShowString | ShowStringExec | `df.show()`, notebook HTML | Formats rows as a text or HTML table, with Spark's truncation rules |
| MapPartitions | MapPartitionsExec | `mapInPandas`, `mapInArrow`, Python UDTFs | Streams each partition through a Python function (see [15](#doc-15-functions-and-python-udfs)) |
| MonotonicId | MonotonicIdExec | `monotonically_increasing_id()` | Computes `(partition_id << 33) + row_number_within_partition`, exactly like Spark |
| SparkPartitionId | SparkPartitionIdExec | `spark_partition_id()` | Returns the current partition number |
| SortWithinPartitions | a sort that keeps partitioning | `sortWithinPartitions`, `SORT BY` | Sorts each partition separately |
| ExplicitRepartition | ExplicitRepartitionExec → rewritten to a repartition | `repartition(n)`, `repartition(n, cols)`, `DISTRIBUTE BY` | Records a user-requested repartition so the optimizer keeps it |
| SchemaPivot | SchemaPivotExec | (internal helper) | Collects its input and outputs a single string column built from the given column names. The current resolver doesn't produce this node. |
| FileWrite | the table format's writer | `df.write`, `INSERT`, CTAS | Writes data through the format's sink (see [12](#doc-12-data-sources-and-formats)) |
| FileDelete | the format's row-level writer | `DELETE FROM` | Deletes rows in lakehouse tables (see [13](#doc-13-lakehouse-delta-and-iceberg)) |
| MergeInto | the lakehouse merge plan, with a cardinality check | `MERGE INTO` | Runs merge actions and enforces that each target row matches at most one source row |
| CatalogCommand (in `sail-plan`) | CatalogCommandExec | DDL, `SHOW`, `DESCRIBE`, `USE`, … | Calls the catalog manager at execution time and returns a result table |
| Barrier | BarrierExec | Every write | Wraps each file write. Runs precondition plans (such as the catalog command that creates the target table) to completion before the write starts. |
| Stream source wrapper, adapter, filter, limit, collector | Stream* operators | Structured Streaming | Run a plan over unbounded input and produce updates (see 3.5) |

### 3.2 The extension planner

When DataFusion finds a logical node it doesn't know, it asks the registered
extension planners **in order**:

1. **Lakehouse planners**: Delta and Iceberg row-level operations, such as
   expanding `DELETE`, `UPDATE`, and `MERGE` into scan, rewrite, and commit plans.
2. **System table planner**: the `system.*` tables.
3. **Sail's general planner**: everything in the table above.

The first planner that recognizes the node produces the operator. If none does,
planning fails, so **every new logical node needs a planner case**.

### 3.3 Logical optimizer rules

Sail takes DataFusion's default analyzer rules (type coercion and so on) and
default optimizer rules, with these changes:

| Change | Why |
|--------|-----|
| Lakehouse rules run **first** (for example, *expand row-level operation*) | `DELETE`, `UPDATE`, and `MERGE` must become ordinary scans and joins before the standard rules optimize them |
| **Decorrelate lateral projection** is added before DataFusion's lateral-join decorrelation | Handles the common case `LATERAL (SELECT t1.a + 1)` by turning it into a cross join plus a projection |
| *Push down leaf projections* is removed | Doesn't fit how Sail uses projections |

Everything else (filter pushdown, projection pruning, constant folding, common
subexpression elimination, subquery decorrelation, limit pushdown, …) is standard
DataFusion.

### 3.4 Physical optimizer rules

The rules run in this order. Sail's additions are in **bold**.

| # | Rule | Purpose |
|---|------|---------|
| 1 | Output requirements (add) | Remember required ordering and distribution at the root |
| 2 | Aggregate statistics | Answer `count(*)`, `min`, `max` from statistics when possible |
| 3 | **Join reorder** (if `optimizer.enable_join_reorder`) | Cost-based join order, see 3.6 |
| 4 | Join selection | Choose hash, sort-merge, or nested-loop join, build side, and partitioned vs. collect-left |
| 5 | Limited distinct aggregation | `SELECT DISTINCT … LIMIT n` stops early |
| 6 | Filter pushdown | Push filters into scans and through joins |
| 7 | Enforce distribution | Insert repartitions so joins and aggregates get the partitioning they need |
| 8 | Combine partial/final aggregate | Merge two aggregation steps when there's no repartition between them |
| 9 | Enforce sorting | Insert or remove sorts |
| 10 | Optimize aggregate order | Use existing order for aggregates |
| 11 | Projection pushdown | Drop unused columns early |
| 12 | Output requirements (remove) | |
| 13 | Top-K aggregation | `ORDER BY agg LIMIT k` |
| 14 | Limit push past windows, limit pushdown | Push limits down |
| 15 | Projection pushdown (again), push down sort | |
| 16 | Ensure cooperative | Make long-running operators yield to the async scheduler |
| 17 | Filter pushdown (post-optimization) | Dynamic filters, for example from hash join build sides |
| 18 | **Rewrite explicit repartition** | Turn the user's `repartition()` node into a real repartition operator |
| 19 | **Rewrite collect-left hash join** | Safety net: make sure the build side of a collect-left hash join has exactly one partition, even after reordering |
| 20 | **Enforce barrier partitioning** | Make sure all barrier preconditions finish before the main plan starts |
| 21 | Sanity check | Check that distribution and ordering requirements are met |

### 3.5 Streaming plans

If a plan reads from a streaming source (rate, socket, Kafka-style formats, …),
a rewrite step after logical optimization turns it into a **streaming plan**.
Every operator except the sink carries a *flow event* schema: the data columns
plus extra fields that mark whether a row is being added or retracted. A
**collector** node at the top can turn the stream of retractable updates back
into final results, as long as the query is bounded.

### 3.6 Cost-based join reordering

When a query joins many tables (star and snowflake schemas, TPC-DS style), the
order of the joins can change runtime by orders of magnitude. Sail's join
reorder rule (off by default) works on physical plans:

1. **Find reorderable regions.** Walk the plan bottom-up, collecting maximal
   groups of inner equi-joins (plus filters and projections), and treat
   everything else as a leaf relation.
2. **Build a join graph.** Relations are vertices, and join conditions are
   edges. Columns joined by equality are placed in **equivalence sets**, which
   share a single estimate of distinct values.
3. **Estimate cardinalities** from scan statistics: row counts, distinct counts,
   and filter selectivity (with fixed guesses for non-equality filters and a
   deliberately pessimistic guess for theta joins, to avoid near-cross-products).
4. **Enumerate join orders** with dynamic programming over connected subsets
   (the DPhyp approach). The cost of joining A and B is cost(A) + cost(B) +
   cardinality(A ⋈ B). The search falls back to a **greedy** algorithm when more
   than 10,000 candidate plans would be generated, and prunes more aggressively
   above 10 relations.
5. **Protect the fact table.** In star schemas with one dominant "fact" table
   (at least 5 relations, with the fact table holding most of the rows), joins
   that avoid the fact table and have low-confidence estimates get a penalty,
   so dimensions aren't joined to each other first.
6. **Rebuild the plan** in the chosen order, restoring the original column order
   and any filters and projections.

---

## 4. Examples

### 4.1 `monotonically_increasing_id()`

```python
spark.range(0, 10, numPartitions=2).withColumn("id2", F.monotonically_increasing_id()).show()
```

Partition 0 produces IDs 0–4. Partition 1 produces `1<<33` (8589934592) through
8589934596. Those are the same values Spark produces.

### 4.2 `repartition` stays where you put it

```python
df.repartition(8, "customer_id").write.parquet("out/")
```

The explicit repartition node survives logical optimization, and the late
physical rule turns it into a hash repartition into 8 partitions. That gives 8
output file groups, as in Spark.

### 4.3 CTAS uses a barrier

```sql
CREATE TABLE top_customers AS
SELECT customer_id, sum(amount) AS total FROM orders GROUP BY 1 ORDER BY total DESC LIMIT 100
```

The plan is roughly *Barrier(precondition = create-table catalog command,
main = FileWrite(query))*. The table is created during execution, never during
planning, and the write starts only after the create succeeds.

### 4.4 Turning on join reordering

```bash
export SAIL_OPTIMIZER__ENABLE_JOIN_REORDER=true
```

Then compare `EXPLAIN` output for a five-table star join with the setting off
and on.

---

## 5. End-to-End: A Logical Node's Journey

Take `spark.range(1_000_000).selectExpr("id % 10 AS k").groupBy("k").count().show()`:

| # | Stage | Plan shape |
|---|-------|------------|
| 1 | Resolver | ShowString( Aggregate( Projection( **RangeNode**(0..1e6) ) ) ) |
| 2 | Logical optimizer | Projection simplified. ShowString and Range stay as extension nodes. |
| 3 | Extension planner | RangeNode → RangeExec (split across target partitions). ShowString → ShowStringExec. Aggregate → partial and final AggregateExec. |
| 4 | Physical optimizer | Enforce distribution inserts a hash repartition on `k` between the partial and final aggregates. Sail's rules check the plan. |
| 5 | Job runner | Executes. ShowStringExec returns one batch with the formatted table. |

---

## 6. Adding a New Operator

1. **Logical node** in `sail-logical-plan`: implement DataFusion's user-defined
   logical node interface (name, inputs, schema, expressions, and how to rebuild
   with new inputs). Keep it free of side effects.
2. **Physical operator** in `sail-physical-plan`: implement `ExecutionPlan`
   (properties such as partitioning and ordering, `execute` returning a stream).
3. **Planner case** in `sail-session/src/planner.rs`: map node → operator.
4. **Resolver** in `sail-plan`: produce the node from the right spec plan
   (see [07](#doc-07-plan-resolver)).
5. **Cluster support**: if the operator must run on workers, add it to the plan
   codec so it can be serialized and sent to workers (see
   [09](#doc-09-local-and-cluster-execution)).
6. **Optimizer interaction**: if the operator has partitioning or ordering
   requirements, declare them so the distribution and sorting rules work around
   it.

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Logical extension nodes | `crates/sail-logical-plan/src/` |
| Physical operators | `crates/sail-physical-plan/src/` |
| Catalog command node and operator | `crates/sail-plan/src/catalog.rs`, `crates/sail-physical-plan/src/catalog_command.rs` |
| Extension planner (node → operator) | `crates/sail-session/src/planner.rs` |
| Logical rule list | `crates/sail-logical-optimizer/src/lib.rs`, `crates/sail-session/src/optimizer.rs` |
| Physical rule list | `crates/sail-physical-optimizer/src/lib.rs` |
| Join reordering | `crates/sail-physical-optimizer/src/join_reorder/` (`graph.rs`, `cardinality_estimator.rs`, `enumerator.rs`, `cost_model.rs`, `reconstructor.rs`) |
| Barrier and collect-left rules | `crates/sail-physical-optimizer/src/barrier.rs`, `collect_left.rs` |
| Streaming rewrite | `crates/sail-plan/src/streaming/rewriter.rs`, `crates/sail-logical-plan/src/streaming/` |


---

<a id="doc-09-local-and-cluster-execution"></a>

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
> [10 · Shuffle and Streams](#doc-10-shuffle-and-streams).

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
[02](#doc-02-entry-points-and-server)). Job, stage, task, worker, and stream state
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
| Mode | **Pipelined** (stream in memory, the default) or **Blocking** (materialize to disk). On this branch, blocking mode isn't reachable from configuration, see [10](#doc-10-shuffle-and-streams). |
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
     the task's output channels (see [10](#doc-10-shuffle-and-streams)),
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
| 7 | Workers | Decode and rewrite the plan, start the tasks, and report *running*. Upstream tasks write channels to their stream manager. Downstream tasks fetch them (see [10](#doc-10-shuffle-and-streams)). |
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
| "task failed for lost worker" | Heartbeats stopped: pod killed, out of memory, network | Check the pod logs and memory. Bound the memory pool (see [06](#doc-06-session-and-runtime)). |
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


---

<a id="doc-10-shuffle-and-streams"></a>

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
[09](#doc-09-local-and-cluster-execution)), data has to flow from upstream tasks
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
(see [09](#doc-09-local-and-cluster-execution), job output).

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


---

<a id="doc-11-catalogs"></a>

# 11 · Catalogs: Finding Tables

> **You'll learn:** how Sail turns a name like `prod.sales.orders` into a table
> with a schema, format, and location; how multiple catalogs coexist; how
> temporary views work; which metastores Sail supports and how to configure
> them; and how caching and runtime isolation are layered on.
>
> **Crates:** `sail-catalog` (interface, manager, commands, caching and runtime
> wrappers), `sail-catalog-memory`, `sail-catalog-system`, `sail-catalog-glue`,
> `sail-catalog-hms`, `sail-catalog-iceberg`, `sail-catalog-unity`,
> `sail-catalog-onelake`

---

## 1. What This Component Does

A **catalog** answers metadata questions: *Which databases exist? What tables
are in `sales`? What are the columns, format, location, and partitioning of
`sales.orders`?* It also carries out DDL: create, alter, and drop databases,
tables, and views.

Sail separates two jobs:

| Job | Who | What |
|-----|-----|------|
| **Interpreting names** | The **catalog manager** (one per session) | Decides which catalog and database a name refers to, handles temporary views, keeps the current catalog and database, and runs catalog commands |
| **Storing metadata** | **Catalog providers** (one per configured catalog) | Talk to a metastore: in-memory, AWS Glue, Hive Metastore, Iceberg REST, Unity Catalog, or Microsoft OneLake |

---

## 2. Why It's Built This Way

**One small interface for every metastore.** Every provider implements the same
dozen operations: create, get, list, and drop databases; create, get, list,
alter, and drop tables; and create, get, list, and drop views. Adding a metastore
means implementing those and translating between its types and Sail's.

**Name resolution in one place.** Spark's rules for one-, two-, and three-part
names, the current catalog and database, and the global temporary database are
subtle. If each provider implemented them, the providers would drift apart. The
manager implements them once, and providers only see "catalog, namespace,
object".

**Multi-level namespaces.** Some catalogs (Iceberg REST, Unity) have nested
namespaces. Sail represents a database as a **namespace**, a non-empty list of
parts, rather than a single string.

**Layers for cross-cutting behavior.** Two things apply to every remote
catalog, so they're added as **wrappers** around providers:

- a **runtime wrapper**, which runs every provider call on Sail's **io**
  runtime. A slow metastore can't block query execution threads, and client
  libraries that expect a particular Tokio runtime get one.
- a **caching wrapper**, which caches database, table, and view lookups (with
  size and TTL limits) and invalidates entries on DDL.

A typical remote catalog looks like *cache → runtime → provider*.

---

## 3. How It Works

### 3.1 Configuration

Catalogs are declared in `catalog.list`, a list of inline tables (TOML-style)
in an environment variable:

```bash
export SAIL_CATALOG__LIST='[
  {name="sail", type="memory", initial_database=["default"]},
  {name="glue", type="glue", region="us-east-1"},
  {name="ice",  type="iceberg-rest", uri="https://rest.example.com", warehouse="s3://lake/wh"}
]'
export SAIL_CATALOG__DEFAULT_CATALOG=sail
export SAIL_CATALOG__DEFAULT_DATABASE='["default"]'
```

| Type | Required settings | Optional settings |
|------|-------------------|-------------------|
| `memory` | `name`, `initial_database` | `initial_database_comment` |
| `glue` | `name` | `region`, `endpoint_url` |
| `hive_metastore` (`hms`) | `name`, `uris` | `thrift_transport`, `auth` (for example Kerberos), `kerberos_service_principal`, `min_sasl_qop`, `connect_timeout_secs` |
| `iceberg_rest` | `name`, `uri` | `warehouse`, `prefix`, `oauth_access_token`, `bearer_access_token` |
| `unity` | `name` | `uri`, `default_catalog`, `token` |
| `onelake` | `name`, `url` | `bearer_token` (otherwise uses the Azure CLI) |

Every remote type also accepts **cache settings**: a cache type, size, and TTL
for databases, tables, and views. Secrets are kept as secret strings and never
printed.

**Defaults:** the default configuration has one in-memory catalog named `sail`
with a database named `default`. If `catalog.default_catalog` is empty and
exactly one catalog is defined, that catalog becomes the default. With several
catalogs you must choose one. A catalog named **`system`** is always added
(see [06](#doc-06-session-and-runtime)), and that name is reserved.

### 3.2 Name resolution

The manager keeps a **current catalog**, a **current database**, and the name
of the **global temporary database** (`global_temp`).

**Database references:**

| Reference | Resolves to |
|-----------|-------------|
| `[db]` | current catalog, database `db` |
| `[c, db]`, if `c` is a registered catalog | catalog `c`, database `db` |
| `[c, ns1, ns2]`, if `c` is a registered catalog | catalog `c`, namespace `ns1.ns2` |
| `[ns1, ns2]`, if `ns1` isn't a catalog | current catalog, namespace `ns1.ns2` |

**Table and view references:** the last part is the object name. The rest is
resolved as a database reference, or means the current database if there's only
one part.

**Lookup order** for "get table or view", which is what reads use:

1. A **one-part** name: check **temporary views** first.
2. If the qualifier is the global temporary database (`global_temp.v`), check
   **global temporary views**.
3. Ask the provider for a **table**.
4. Ask the provider for a **view**.
5. If nothing is found: `[TABLE_OR_VIEW_NOT_FOUND] Table or view not found: …`.

### 3.3 What a table lookup returns

A lookup returns a **table status** describing the object:

| Kind | Contents |
|------|----------|
| **Table** | Columns (name, type, nullable, comment, default, generated expression, partition, bucket, and cluster flags), constraints, format (for example `parquet`, `delta`, `iceberg`), location, partition columns, sort order, bucketing, properties, and whether it's external |
| **View** | The SQL definition text, columns, and properties |
| **Temporary view** / **global temporary view** | A logical plan |

The resolver uses the format and location to ask the matching table format for
a reader (see [07](#doc-07-plan-resolver) and
[12](#doc-12-data-sources-and-formats)).

### 3.4 Temporary views and tracked objects

- `df.createOrReplaceTempView("v")` stores the DataFrame's **logical plan** in
  the session's temporary view manager. Global temporary views live under
  `global_temp`.
- Spark Connect clients can register functions and plans that later requests
  refer to by ID. The manager's **tracker** gives them internal IDs.
- Registered **Python UDFs** are kept as session functions, which the resolver
  checks before built-in functions (see [15](#doc-15-functions-and-python-udfs)).

### 3.5 Catalog commands

DDL and metadata commands are resolved into a **catalog command** (about 60
kinds) wrapped in a logical node (see [07](#doc-07-plan-resolver)). When the plan
runs, the operator calls the command's `execute`, which uses the manager and
providers and returns a small Arrow table formatted like Spark's output for
`SHOW TABLES`, `DESCRIBE`, and so on. Commands run on the **driver** in cluster
mode, because only the driver has the session's catalog state.

### 3.6 Providers in brief

| Provider | Notes |
|----------|-------|
| **Memory** | Kept inside the session's catalog manager. Metadata is **per session** and lost when the session ends (the data files themselves stay). Good for development and tests. |
| **System** | Read-only `system.*` tables generated from Sail's runtime state |
| **AWS Glue** | Uses the AWS SDK. Maps Glue databases and tables to Sail status (Hive-style formats, Iceberg tables). Credentials come from the standard AWS chain. |
| **Hive Metastore** | A Thrift client, with optional SASL/Kerberos (GSSAPI). Maps Hive storage descriptors (input/output formats, SerDe) to and from Sail formats. |
| **Iceberg REST** | A client for the Iceberg REST catalog API (separate `apis` and `models` modules), with nested namespaces and OAuth or bearer tokens |
| **Unity Catalog** | A REST client with token or credential configuration. `default_catalog` selects the Unity catalog. |
| **OneLake** | Microsoft Fabric's OneLake Delta table API. Uses a bearer token or gets one from the Azure CLI. |

---

## 4. Examples

### 4.1 Working with multiple catalogs

```python
spark.sql("SHOW CATALOGS").show()                    # sail, glue, ice, system
spark.sql("USE glue.analytics")
spark.table("events").limit(5).show()                # glue.analytics.events
spark.table("ice.marketing.campaigns").show()        # three-part name
spark.sql("CREATE TABLE sail.default.tmp USING parquet AS SELECT 1 AS x")
```

### 4.2 Temporary views

```python
df.createOrReplaceTempView("recent")
spark.sql("SELECT count(*) FROM recent").show()      # temporary view wins for one-part names
df.createGlobalTempView("shared")
spark.sql("SELECT * FROM global_temp.shared").show()
```

### 4.3 Caching a slow metastore

```bash
export SAIL_CATALOG__LIST='[{name="hms", type="hms", uris=["thrift://metastore:9083"],
  table_cache_type="global", table_cache_ttl_secs=60}]'
```

---

## 5. End-to-End: `spark.table("glue.analytics.events")`

| # | Component | What happens |
|---|-----------|--------------|
| 1 | Spark Connect | Creates a read of the named table `["glue", "analytics", "events"]`. |
| 2 | Resolver | Isn't a `format.path` shortcut or a CTE, so it asks the catalog manager for the table or view. |
| 3 | Catalog manager | Three parts, so it isn't a temporary view. `glue` is a registered catalog, so catalog = `glue`, namespace = `analytics`, object = `events`. |
| 4 | Cache wrapper | Checks the table cache. On a miss, it calls the wrapped provider. |
| 5 | Runtime wrapper | Runs the call on the io runtime. |
| 6 | Glue provider | Calls Glue `GetTable` and converts columns, format (for example Parquet), location, and partition keys into a table status. |
| 7 | Resolver | Builds source info and asks the Parquet table format for a source (see [12](#doc-12-data-sources-and-formats)). |
| 8 | Planner and execution | Scan, prune, and read (see [07](#doc-07-plan-resolver), [09](#doc-09-local-and-cluster-execution)). |

---

## 6. Common Problems

| Symptom | Cause | Fix |
|---------|-------|-----|
| "cannot infer default catalog when multiple catalogs are defined" | Several catalogs without `catalog.default_catalog` | Set `SAIL_CATALOG__DEFAULT_CATALOG`. |
| `TABLE_OR_VIEW_NOT_FOUND` for a table you just created | Different session (memory catalogs are per session), or a stale cache | Use a persistent catalog, or lower cache TTLs. |
| "cannot define catalog with reserved name: system" | A catalog named `system` in `catalog.list` | Rename it. |
| Name resolves to the wrong catalog | The first name part matches a catalog name | Fully qualify the name, or rename the catalog or database. |
| Queries stall on metadata calls | A slow metastore | Turn on catalog caching. Check network and authentication. |

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Provider interface | `crates/sail-catalog/src/provider/mod.rs` |
| Namespaces, options | `crates/sail-catalog/src/provider/namespace.rs`, `options.rs` |
| Caching and runtime wrappers | `crates/sail-catalog/src/provider/cache.rs`, `runtime.rs` |
| Catalog manager (name resolution) | `crates/sail-catalog/src/manager/` (`mod.rs`, `table.rs`, `view.rs`, `database.rs`, `function.rs`, `tracker.rs`) |
| Temporary views | `crates/sail-catalog/src/temp_view.rs` |
| Catalog commands | `crates/sail-catalog/src/command.rs` |
| Hive storage formats | `crates/sail-catalog/src/hive_format.rs` |
| Table status types | `crates/sail-common-datafusion/src/catalog/` |
| Wiring catalogs into sessions | `crates/sail-session/src/catalog.rs` |
| Catalog configuration | `crates/sail-common/src/config/application.rs` (catalog types), `application.yaml` (`catalog.*`) |
| Providers | `crates/sail-catalog-{memory,system,glue,hms,iceberg,unity,onelake}/` |


---

<a id="doc-12-data-sources-and-formats"></a>

# 12 · Data Sources and File Formats

> **You'll learn:** how Sail reads and writes files (Parquet, CSV, JSON, Avro,
> Arrow, text, binary) and streaming test sources; the table format interface
> that all formats implement; how options are defined, layered, and checked;
> how paths, globs, and partition directories are discovered; and how Python
> data sources plug in.
>
> **Crates:** `sail-data-source` (built-in formats, listing, options, URLs,
> Python data sources), the table format interface in
> `sail-common-datafusion/src/datasource.rs`
>
> Lakehouse formats (Delta Lake, Iceberg) are in
> [13](#doc-13-lakehouse-delta-and-iceberg), and storage access (S3, GCS, …) is in
> [14](#doc-14-object-storage).

---

## 1. What This Component Does

Whenever Sail reads data, through `spark.read…`, `spark.table(…)` on a file
table, or `SELECT * FROM parquet.`path``, or writes it, through `df.write…`,
`INSERT`, or CTAS, it goes through a **table format**. A table format is a
named plugin that knows how to:

| Capability | Called for |
|------------|------------|
| **Create a source** | Reads: returns a DataFusion table source the planner can scan (with projection, filter, and limit pushdown) |
| **Infer a schema** | Planning when no schema was given |
| **Create a writer** | Writes: returns a physical plan that consumes the input and writes files |
| Create a row-level writer (optional) | `DELETE`, `UPDATE`, `MERGE` (lakehouse formats only) |
| Alter table properties or column types (optional) | `ALTER TABLE` on formats that support it |
| Merge strategy (optional) | Whether row-level changes are copy-on-write (*eager*) or merge-on-read |

All formats live in a **registry** keyed by lowercase name and attached to each
session (see [06](#doc-06-session-and-runtime)):

| Name | Kind | Read | Write |
|------|------|------|-------|
| `parquet` | File | Yes, with row-group and page pruning, filter pushdown, bloom filters | Yes |
| `csv` | File | Yes, with header handling and schema inference | Yes |
| `json` | File | Yes (newline-delimited) | Yes |
| `avro` | File | Yes | Yes |
| `arrow` | File (Arrow IPC) | Yes | Yes |
| `text` | File (one string column per line) | Yes | Yes |
| `binary` | File (path, modification time, length, content) | Yes | No |
| `delta`, `iceberg` | Lakehouse | See [13](#doc-13-lakehouse-delta-and-iceberg) | |
| `rate`, `socket` | Streaming test sources | Yes | No |
| `console` | Streaming sink | No | Yes |
| *Python data sources* | User-defined, in Python | Yes | Yes |

---

## 2. Why It's Built This Way

**One interface for every format.** The resolver doesn't know whether it's
reading Parquet or Iceberg. It builds a neutral description (paths, schema,
partitioning, options) and asks the registry. Adding a format means
implementing the interface and registering it. Nothing else in the planner
changes.

**Reuse DataFusion's file machinery.** File formats are built on DataFusion's
**listing table**, which already handles splitting files into partitions,
parallel scans, statistics, projection and filter pushdown, partition-column
pruning, and parallel writes. Sail adds Spark's behavior on top: option names
and defaults, Hadoop-style globs, partition inference rules, hidden-file
handling, and output file naming.

**Spark-compatible options, defined as data.** Spark has dozens of options per
format, with aliases (`sep`/`delimiter`), table-property forms
(`option.delimiter`), defaults, and scopes (read, write, or both). Sail defines
each format's options in a **YAML file**, about 250 options in total, and a
build script generates typed Rust structs and parsers from them. Option
handling stays consistent and self-documenting, and an option marked
unsupported can produce a clear error instead of being silently ignored.

**Options keep track of where they came from.** An option can come from catalog
table properties, the operation (`.option(…)`, `OPTIONS (…)`), the table
location, or time travel. These are passed as ordered **option layers** (later
layers override earlier ones), so a format can tell a table default apart from a
per-query override.

---

## 3. How It Works

### 3.1 Describing a read and a write

**Read description (source info):**

| Field | Meaning |
|-------|---------|
| Paths | One or more paths or URLs, which may contain globs |
| Schema | Optional user or catalog schema, including partition columns |
| Constraints | Primary key and similar constraints, if any |
| Partition by | Partition column names (Hive-style `col=value` directories) |
| Bucket by, sort order | Bucketing and sort metadata from the catalog |
| Options | Option layers |

**Write description (sink info):**

| Field | Meaning |
|-------|---------|
| Input | The physical plan producing the rows |
| Mode | Error if exists, ignore if exists, append, overwrite, overwrite where *condition*, overwrite partitions |
| Partition by | Partition columns (with optional transforms, for lakehouse formats) |
| Bucket by, sort order | |
| Options | Option layers. The output path is the `path` option. |
| Logical schema | The input schema with field metadata, for example Delta generated-column expressions |

### 3.2 Paths, globs, and listing

1. **Parse the URL.** Sail's own URL parser understands **Hadoop glob syntax**,
   which differs from typical shell globs: `{a,b}` alternation, `[^x]` negated
   classes, `*`, `?`. Each glob is split into a fixed base path and a pattern,
   and alternations are expanded.
2. **List files** through the object store registry (see
   [14](#doc-14-object-storage)), optionally using the file listing cache. Hidden
   files and directories (starting with `_` or `.`, such as `_SUCCESS` or
   `.crc`) are excluded by default, as in Spark.
3. **Infer partitions.** If no partition columns were given, `col=value`
   directory names are discovered from the paths and become columns. Their types
   are inferred.
4. **Infer the schema** if none was given, using the format's inference: Parquet
   footers, sampling CSV and JSON rows, Avro schemas, and so on. Some formats
   handle options such as CSV `inferSchema=false` (all columns become strings).
5. **Build a listing table** with the files, schema, partition columns, target
   partitions (`execution.default_parallelism`), and constraints, then return it
   as a table source.

### 3.3 Reading

During planning and execution, the listing table:

- groups files (and Parquet row groups) into **partitions** to read in parallel,
- applies **projection pushdown** (read only the needed columns),
- applies **filter pushdown**: for Parquet, row-group statistics, page indexes,
  bloom filters, and late materialization (`parquet.pushdown_filters`,
  `parquet.pruning`, `parquet.enable_page_index`, …), plus partition pruning
  from directory values,
- uses the **file statistics and metadata caches** (see
  [06](#doc-06-session-and-runtime)) to avoid rereading Parquet footers.

In cluster mode the scan is serialized into the task plan (see
[09](#doc-09-local-and-cluster-execution)), and each worker reads its assigned
files directly from storage.

### 3.4 Writing

For plain file formats, the writer:

1. checks the input isn't a streaming plan (streams can't be written to plain
   file tables),
2. rejects features plain files don't support: **bucketing** and **partition
   transforms**,
3. treats the path as a **directory** and always writes multiple files,
4. picks the file extension, including the compression codec for Parquet (for
   example `.zstd.parquet`),
5. builds DataFusion's file sink with Hive-style **partition directories**
   (partition columns aren't stored inside the files) and runs it in parallel.

At the writer level, plain file formats only **append**: the sink mode is
currently ignored by the listing writer, which is marked as a TODO in the code.
Table-level checks (does the table exist, create it if needed, error or ignore
modes) are done by the resolver as **preconditions** in the barrier node (see
[07](#doc-07-plan-resolver), [08](#doc-08-logical-physical-plans-and-optimizers)).
Lakehouse formats implement the full set of modes transactionally.

Parquet writing honors about 20 `parquet.*` settings: compression, dictionary,
statistics, row group size, page size, bloom filters, writer version, and so
on.

### 3.5 Options

Each format's options are listed in `src/options/data/<format>.yaml`:

```yaml
- key: delimiter
  description: Sets a single character separator for each field and value.
  default: { value: ",", parser: parse_char }
  supported: true
  rust_type: char
  scopes: [read, write]
  origins:
    - type: option          # .option("sep", ";") or .option("delimiter", ";")
      keys: [delimiter, sep]
    - type: table_property  # TBLPROPERTIES ('option.sep' = ';')
      keys: [option.delimiter, option.sep]
```

At build time this becomes a typed struct per format and scope (for example,
CSV read options) with parsers. At run time, option layers are applied in order
and each key is matched case-insensitively against its aliases.

| Format | Approximate number of options |
|--------|-------------------------------|
| CSV | 42 |
| Parquet | 34 |
| JSON | 27 |
| Delta | 37 |
| Iceberg | 103 |
| Text, socket, rate, binary | A few each |

### 3.6 Streaming test sources and sinks

- **`rate`**: generates `(timestamp, value)` rows at a configured rate.
- **`socket`**: reads text lines from a TCP socket.
- **`console`**: prints micro-batch results.

These support Structured Streaming examples and tests (see the streaming notes
in [08](#doc-08-logical-physical-plans-and-optimizers)).

### 3.7 Python data sources

PySpark 4's **Python Data Source API** lets users write readers and writers in
Python. Sail supports it in two ways:

- **Session registration**: `spark.dataSource.register(MySource)` sends the
  pickled class to Sail, which creates a Python table format for that session.
- **Entry-point discovery**: at startup, Sail looks for installed Python
  packages that advertise data sources through `importlib.metadata` entry
  points and registers them globally.

When read, Sail calls the source's `schema()`, plans **partitions** with the
reader's `partitions()`, and runs `read(partition)` for each one in its embedded
Python interpreter, converting the results to Arrow batches. Simple filters can
be pushed into the reader. Writes run the Python writer for each partition, then
a **commit** step that calls the writer's commit or abort with the per-partition
messages. Settings `python.data_source_*` control the write channel capacity
and slow-operation warnings.

---

## 4. Examples

```python
# Parquet with partition discovery: /data/events/date=2024-01-01/part-0.parquet …
df = spark.read.parquet("s3://lake/events/")            # gains a `date` column
df.filter("date = '2024-01-01'").count()                # prunes other date directories

# CSV with options
spark.read.option("header", True).option("sep", ";").csv("/data/*.csv")

# Hadoop-style glob with alternation
spark.read.json("gs://bucket/logs/{2024,2025}/*/app-[^t]*.json")

# Write partitioned Parquet
df.write.partitionBy("country").parquet("/out/by_country/")

# Query a path directly in SQL
spark.sql("SELECT count(*) FROM parquet.`s3://lake/events/`").show()

# Python data source
from pyspark.sql.datasource import DataSource, DataSourceReader
class Counter(DataSource):
    @classmethod
    def name(cls): return "counter"
    def schema(self): return "n INT"
    def reader(self, schema): return CounterReader()
class CounterReader(DataSourceReader):
    def read(self, partition):
        for i in range(3): yield (i,)
spark.dataSource.register(Counter)
spark.read.format("counter").load().show()
```

---

## 5. End-to-End: `spark.read.parquet("s3://lake/events/").filter("date='2024-01-01'").count()`

| # | Component | What happens |
|---|-----------|--------------|
| 1 | Spark Connect | A data source read: format `parquet`, paths `["s3://lake/events/"]`, no schema. |
| 2 | Resolver | Asks the registry for `parquet` and passes source info with an empty schema and the option layers. |
| 3 | URL resolution | Parses the path, finds no globs, and lists files under `s3://lake/events/` through the S3 client from the object store registry (see [14](#doc-14-object-storage)). Skips `_SUCCESS`. |
| 4 | Partition inference | Finds `date=…` directories and adds a `date` partition column. |
| 5 | Schema inference | Reads one Parquet footer, which may be cached. The schema is the file schema plus `date`. |
| 6 | Listing table | Created with files, schema, partitions, and target partitions. |
| 7 | Optimizer | The filter `date = '2024-01-01'` becomes a **partition filter**. `count(*)` needs no columns, so it may be answered from statistics. |
| 8 | Execution | Only files under `date=2024-01-01/` are opened. Row counts come from Parquet metadata where possible. |

---

## 6. Common Problems

| Symptom | Cause |
|---------|-------|
| "bucketing for writing listing table format" not implemented | `bucketBy` with a plain file format isn't supported |
| "cannot write streaming data to listing table" | A streaming query writing to a plain file path. Use a supported sink. |
| A write to an existing path adds files instead of replacing them | The listing writer appends. Use a table, a lakehouse format, or clear the path first. |
| Unexpected extra column from directories | Partition inference found `col=value` directories. Pass an explicit schema to control it. |
| Option has no effect | Misspelled, not supported, or only valid for the other scope (read vs. write). Check `options/data/<format>.yaml`. |
| Glob doesn't match | Hadoop glob syntax: use `[^x]`, not `[!x]`, and `{a,b}` for alternatives |

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Table format interface, source and sink info, option layers, sink modes | `crates/sail-common-datafusion/src/datasource.rs` |
| Format registration | `crates/sail-session/src/formats.rs` |
| Listing-based table format (read and write) | `crates/sail-data-source/src/listing/` |
| Built-in formats | `crates/sail-data-source/src/formats/` (`parquet/`, `csv/`, `json/`, `avro/`, `arrow/`, `text/`, `binary/`, `rate/`, `socket/`, `console/`) |
| Option definitions (YAML) and parsers | `crates/sail-data-source/src/options/` (`data/*.yaml`, `parsers.rs`), `build.rs` |
| URL and glob parsing | `crates/sail-data-source/src/url.rs` |
| Python data sources | `crates/sail-data-source/src/formats/python/` |
| File write planning | `crates/sail-physical-plan/src/file_write.rs` |


---

<a id="doc-13-lakehouse-delta-and-iceberg"></a>

# 13 · Lakehouse Formats: Delta Lake and Apache Iceberg

> **You'll learn:** how Sail reads and writes Delta Lake and Iceberg tables
> natively in Rust; how snapshots, transaction logs, and manifests turn into
> file lists; how pruning, deletion vectors, and delete files work; how commits
> and conflict checks keep writes safe; and how `DELETE`, `UPDATE`, and `MERGE`
> are planned.
>
> **Crates:** `sail-delta-lake`, `sail-iceberg`, `sail-plan-lakehouse`

---

## 1. What This Component Does

Plain Parquet directories can't do atomic writes, time travel, schema
evolution, or row-level updates. **Lakehouse table formats** add a metadata
layer that records *exactly which files make up each version of a table*:

| | Delta Lake | Apache Iceberg |
|-|-----------|----------------|
| Metadata | `_delta_log/` with JSON commit files (`00000000000000000042.json`) and Parquet **checkpoints** | `metadata/` with table metadata JSON, **manifest lists**, and **manifests** (Avro) |
| A version is | a log version number | a **snapshot** ID (with optional branches and tags) |
| Row-level deletes | Copy-on-write rewrites, or **deletion vectors** (merge-on-read) | Copy-on-write, or **position and equality delete files** |

Sail implements both formats **natively in Rust**, on top of DataFusion and
Sail's table format interface (see [12](#doc-12-data-sources-and-formats)). Both
are registered as table formats named `delta` and `iceberg`.

---

## 2. Why It's Built This Way

**Native implementations, designed for distributed execution.** Reading a big
table's metadata can itself be a large job: thousands of log files or
manifests, millions of file entries. Sail expresses metadata work as **physical
plans** (scan the log, replay it, prune files, then scan data), so metadata
processing benefits from the same parallel engine and can run on workers in
cluster mode.

**Logical planning stays format-neutral.** The resolver produces generic nodes
for writes, deletes, and merges. A lakehouse optimizer rule (*expand row-level
operation*) and the lakehouse planners turn them into format-specific physical
plans. Each format decides *how* to delete or merge (rewrite files, or write
deletion vectors or delete files), behind the same interface.

**Correctness first.** Commits are atomic: write data files first, then publish
a new version in a single operation. Concurrent writers are detected, and Delta
runs a **conflict checker** that decides whether a losing commit can be retried
on top of the winning one.

---

## 3. Delta Lake in Sail

### 3.1 Reading: from log to files

1. **Find the log segment.** List `_delta_log/` and choose the latest
   checkpoint (classic single-file, multi-part, or V2 with sidecars) plus the
   JSON commits after it, up to the requested version (`versionAsOf`) or
   timestamp (`timestampAsOf`).
2. **Load protocol and metadata.** The schema, partition columns, table
   properties, and required reader features (column mapping, deletion vectors,
   timestamp without time zone, type widening, …).
3. **Replay the log as a plan.**
   - A *log scan* reads checkpoint and commit files as Arrow rows of actions.
   - **Log replay** groups actions by file key (path plus deletion-vector ID) and
     keeps the **newest** action per key. An `add` that's later `remove`d drops
     out. The result is the active file set for the snapshot. For very large
     logs, the replay strategy is configurable (`deltaLogReplayStrategy`,
     `deltaLogReplayHashThreshold`).
   - Per-file **statistics** (row counts, min, max, null counts) are decoded from
     each `add` action's stats JSON.
4. **Prune files.** Filters are checked against partition values and file
   statistics. Files that can't match are skipped before any data is read.
5. **Scan data.** The remaining files are read as Parquet, with:
   - an **expression adapter** for column mapping (physical vs. logical names),
     type widening, and schema evolution (missing columns become NULL),
   - **deletion vectors**: for files that have one, a row index is tracked while
     scanning and rows whose positions are set in the deletion vector's roaring
     bitmap are filtered out.

### 3.2 Writing

1. **Plan the write.** Validate the input against the table schema. Options such
   as `mergeSchema` and `overwriteSchema` allow evolution. Generated columns are
   computed from their expressions, and invariants are enforced.
2. **Write data files.** Parquet files are written (optionally partitioned),
   collecting statistics for each file. `targetFileSize` and `writeBatchSize`
   control the files (file compression comes from the `parquet.*` settings).
3. **Commit.** A commit operator gathers the `add` (and for overwrites, `remove`)
   actions, plus `metaData` and `protocol` changes if needed, and attempts to
   write the next log version **atomically** (put-if-absent).
4. **On conflict**, if another writer created that version first, the conflict
   checker compares what this transaction read and wrote with the winning
   commit. If they don't overlap in a way that matters, the commit is retried at
   the next version. Otherwise it fails with a concurrency error.
5. **Checkpoint and clean up.** Checkpoints are written periodically to keep
   log replay fast. Log cleanup handles expired files and V2 checkpoint
   compatibility.

**Write modes:** append, overwrite, `replaceWhere` (overwrite rows matching a
condition), error if exists, and ignore if exists. Many Spark Delta options are
defined but marked unsupported, for example `partitionOverwriteMode`,
`maxRecordsPerFile`, change data feed reads, and streaming-source options. Each
option's `supported` flag in `delta.yaml` is authoritative.

### 3.3 Row-level operations

`DELETE`, `UPDATE`, and `MERGE` are first expanded by the lakehouse optimizer
rule (it runs before DataFusion's own rules), then planned by Delta:

| Operation | Copy-on-write (default) | Merge-on-read (when the table has deletion vectors enabled) |
|-----------|-------------------------|-------------------------------------------------------------|
| `DELETE` | Find files containing matching rows using metadata. Rewrite those files without the matching rows. Commit `remove` for the old files and `add` for the new ones. | Find affected files. Scan them to find matching **row positions**. Write deletion vector files (bitmaps). Commit `remove(old)` and `add(same file, new DV)`. No data is rewritten. |
| `UPDATE` | Rewrite affected files with updated rows | Not implemented yet |
| `MERGE` | Join source and target, apply `WHEN MATCHED` / `NOT MATCHED` / `NOT MATCHED BY SOURCE` clauses, rewrite touched files, and enforce that each target row matches at most one source row | Target-side deletes use deletion vectors. Inserted and updated rows are written as new files. |

To find exactly which rows to change, Sail carries internal **metadata
columns** through these plans (the source file path, the row index within the
file, and the operation type). They're stripped before data is written.

---

## 4. Iceberg in Sail

### 4.1 Reading: from snapshot to files

1. **Load table metadata.** Find the current metadata JSON (from the catalog or
   the table location). Pick the snapshot: current, `VERSION AS OF` (snapshot
   ID), `TIMESTAMP AS OF`, or a branch or tag name.
2. **Read the manifest list** for the snapshot, then the **manifests**. A
   manifest scan operator turns manifest entries into rows of data-file metadata:
   path, partition tuple, record count, column bounds, and so on.
3. **Prune** manifests and files using partition values (with Iceberg partition
   **transforms**: identity, bucket, truncate, year, month, day, hour) and
   column statistics.
4. **Scan data files** (Parquet), with **name mapping** and **schema
   evolution** (field IDs map files written with older schemas to the current
   schema).
5. **Apply deletes** written by other engines: **position deletes** (file path
   plus row position) and **equality deletes** (rows matching key values), using
   sequence numbers to decide which deletes apply to which files.

### 4.2 Writing

- **Append** and **overwrite**: data files are written with partition transforms
  applied, new manifests and a manifest list are created, and a new snapshot is
  committed by writing a new metadata JSON file (and `metadata/version-hint.text`)
  next to the table, keeping external catalogs in sync where configured (see
  [11](#doc-11-catalogs)).
- **Not supported yet:** bucketing, predicate overwrite and partition overwrite,
  and row-level `DELETE`, `UPDATE`, `MERGE` writers for Iceberg.
- **Table properties** can be set and unset (`ALTER TABLE … SET TBLPROPERTIES`).
- About 100 Iceberg read and write options are defined (see
  `sail-data-source/src/options/data/iceberg.yaml`). Check each one's
  `supported` flag.

---

## 5. Examples

```python
# Delta: create, append, time travel
df.write.format("delta").save("s3://lake/orders_delta")
more.write.format("delta").mode("append").save("s3://lake/orders_delta")
spark.read.format("delta").option("versionAsOf", 0).load("s3://lake/orders_delta").count()

# Delta: schema evolution
new_df.write.format("delta").mode("append").option("mergeSchema", True).save("s3://lake/orders_delta")

# Delta: replaceWhere
fixes.write.format("delta").mode("overwrite") \
     .option("replaceWhere", "date = '2024-01-01'").save("s3://lake/orders_delta")
```

```sql
-- Row-level operations on a Delta table registered in a catalog
DELETE FROM sales.orders WHERE status = 'cancelled';
UPDATE sales.orders SET amount = amount * 1.1 WHERE region = 'EU';
MERGE INTO sales.orders t
USING updates s ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Iceberg time travel
SELECT * FROM ice.db.events VERSION AS OF 123456789;
SELECT * FROM ice.db.events TIMESTAMP AS OF '2024-06-01 00:00:00';
```

---

## 6. End-to-End: `DELETE FROM sales.orders WHERE status = 'cancelled'` (Delta)

| # | Component | What happens |
|---|-----------|--------------|
| 1 | SQL analyzer | Produces a delete command with table `sales.orders` and the condition. |
| 2 | Resolver | Looks up the table (format `delta`, location `s3://lake/orders`) and produces a **file delete** node. |
| 3 | Lakehouse optimizer rule | Expands it into a format-neutral **row-level write** node (Delete). |
| 4 | Delta table format | Checks table properties to choose the strategy: copy-on-write, or merge-on-read if deletion vectors are enabled. |
| 5 | Metadata pipeline | Log scan → replay → statistics → prune: finds candidate files whose `status` statistics could include `'cancelled'`. |
| 6a | Copy-on-write | Scans candidate files, writes new files containing only rows where `NOT (status = 'cancelled')`, and emits `remove` and `add` actions. |
| 6b | Merge-on-read | Scans candidate files with row indexes, collects matching positions into bitmaps, writes deletion vector files, and emits `remove(old)` and `add(old path, new DV)`. |
| 7 | Commit | Writes log version *N+1* atomically. On a race, runs the conflict checker and retries or fails. |
| 8 | Result | Returns operation metrics (rows deleted, files added and removed). |

---

## 7. Common Problems

| Symptom | Cause |
|---------|-------|
| "Merge-on-Read strategy for UPDATE is not yet implemented for Delta Lake" | `UPDATE` on a table with deletion vectors enabled |
| "predicate or partition overwrite for Iceberg" not implemented | `replaceWhere` or dynamic overwrite on Iceberg |
| Concurrent write fails with a conflict | Two writers changed overlapping files or metadata. Retry, or serialize the writers. |
| Slow first query on a huge Delta table | Log replay over many commits. Make sure checkpoints are written. Tune the replay strategy options. |
| Schema mismatch on append | Use `mergeSchema` (Delta), or evolve the schema explicitly. |

---

## 8. Where to Look

| Topic | Location |
|-------|----------|
| Delta table format (entry points for read, write, row-level, alter) | `crates/sail-delta-lake/src/table_format.rs` |
| Delta log listing, segments, replay, cleanup | `crates/sail-delta-lake/src/delta_log/` |
| Delta snapshots, checkpoints, transactions, conflict checker | `crates/sail-delta-lake/src/kernel/` |
| Delta read pruning and scan | `crates/sail-delta-lake/src/datasource/` |
| Delta physical operators (log replay, scan by adds, writer, commit, DV writer, remove actions) | `crates/sail-delta-lake/src/physical_plan/` |
| Delta operation planners (write, delete, update, merge, commit) | `crates/sail-delta-lake/src/physical_plan/planner/` |
| Delta protocol types (actions, schema, statistics, properties) | `crates/sail-delta-lake/src/spec/` |
| Deletion vectors | `crates/sail-delta-lake/src/deletion_vector/` |
| Iceberg table format | `crates/sail-iceberg/src/table_format.rs` |
| Iceberg spec (metadata, manifests, partition transforms, types, delete index) | `crates/sail-iceberg/src/spec/` |
| Iceberg read (provider, pruning) | `crates/sail-iceberg/src/datasource/` |
| Iceberg operators (manifest scan, scan by data files, delete apply, writer, commit) | `crates/sail-iceberg/src/physical_plan/` |
| Iceberg write operations (append, overwrite, snapshot) | `crates/sail-iceberg/src/operations/` |
| Row-level expansion rule and lakehouse planners | `crates/sail-plan-lakehouse/src/` |
| Delta and Iceberg options | `crates/sail-data-source/src/options/data/{delta,iceberg}.yaml` |


---

<a id="doc-14-object-storage"></a>

# 14 · Object Storage: Reaching S3, GCS, Azure, HDFS, HTTP, and More

> **You'll learn:** how Sail turns a URL like `s3://bucket/path` into a storage
> client, which schemes are supported, where credentials and regions come from,
> and how laziness, logging, and runtime isolation are layered around every
> store.
>
> **Crate:** `sail-object-store`

---

## 1. What This Component Does

All file I/O in Sail goes through the Apache Arrow `object_store` interface:
listing, reading byte ranges of Parquet files, writing output files, committing
Delta logs, and reading Iceberg metadata. Sail provides a **dynamic object
store registry** that DataFusion consults whenever it sees a URL:

| URL scheme | Backend | Notes |
|------------|---------|-------|
| `file://` or plain paths | Local file system | Always registered |
| `memory://` | In-memory store | Tests |
| `s3://`, `s3a://`, `https://…amazonaws.com/…` | Amazon S3 (and S3-compatible stores) | Credentials from the AWS default chain. The region is detected automatically. |
| `gs://` | Google Cloud Storage | Configured from environment variables |
| `az://`, `abfs://`, `abfss://`, `adl://`, `https://…blob.core.windows.net/…` | Azure Blob and ADLS Gen2 | Configured from environment variables |
| `http://`, `https://` (other hosts) | Generic HTTP or WebDAV | Read-only in practice |
| `hf://datasets/<user>/<dataset>[@rev]/<path>` | Hugging Face datasets | Only the `datasets` repository type |
| `hdfs://` | HDFS | Only when built with the `hdfs` feature |

---

## 2. Why It's Built This Way

**No up-front registration.** Spark users expect `spark.read.parquet("s3://any-bucket/…")`
to just work. DataFusion's default registry needs every bucket registered
before use. Sail's registry **creates a store on demand** the first time a
`(scheme, authority)` pair is seen (for example `s3` + `my-bucket`), then
caches it. A single query can read from one bucket and write to another, or
cross clouds, with no setup.

**Clients are created lazily.** Building an S3 client may load AWS
configuration, refresh credentials, or make a network call to find the bucket's
region. Sail wraps cloud stores in a **lazy** layer, so this work happens on
first use, not when a plan merely mentions a path. Planning stays fast, and
unused paths cost nothing.

**Storage I/O runs on its own runtime.** Each store is wrapped in a **runtime
layer** that creates the client and runs its I/O on Sail's dedicated **io**
Tokio runtime (see [02](#doc-02-entry-points-and-server)). HTTP clients are tied
to the runtime that created them, and slow storage shouldn't starve query
threads.

**Every call is logged.** A **logging layer** records object store operations
(get, put, list, …) at debug level, which is very useful when tracing what a
query actually read.

---

## 3. How It Works

### 3.1 Getting a store

```text
DataFusion needs url  ─►  registry.get_store(url)
                              │ key = (scheme, authority)
                              ├─ in cache? → return it
                              └─ no → build:  Runtime layer ( Logging layer ( Lazy layer ( real client ) ) )
                                     insert into cache (atomically) → return
```

The cache is a concurrent map, and get-or-insert is atomic, so two tasks asking
for the same bucket at the same moment get the same store.

### 3.2 Amazon S3

1. Start from environment-based settings (`AWS_*` variables such as
   `AWS_ENDPOINT`, `AWS_ALLOW_HTTP`, `AWS_ACCESS_KEY_ID`, …).
2. Load the **AWS SDK default configuration** once per process: profiles, SSO,
   web identity, instance metadata, and so on. If it provides credentials, use
   them through a caching credential provider so they refresh automatically.
3. **Parse the URL** to find the bucket. Supported forms include `s3://bucket/key`,
   `s3a://bucket/key`, virtual-hosted `https://bucket.s3.region.amazonaws.com/key`,
   and path-style `https://s3.region.amazonaws.com/bucket/key`.
4. **Find the region**: from the URL or environment, else from the AWS
   configuration, else by asking S3 where the bucket lives.

This works with S3-compatible services such as MinIO or R2 when you set the
endpoint (and `AWS_ALLOW_HTTP=true` for plain HTTP).

### 3.3 Google Cloud Storage, Azure, and HTTP

These are built from environment variables supported by the `object_store`
crate, for example `GOOGLE_SERVICE_ACCOUNT` / `GOOGLE_APPLICATION_CREDENTIALS`
for GCS, `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`, or
`AZURE_STORAGE_SAS_KEY` for Azure, plus the URL. HTTP stores read client
options from matching environment variables.

### 3.4 Hugging Face

`hf://datasets/user/dataset@revision/path/*.parquet` is parsed into user,
dataset, optional revision, and path. Listing and reading go through the
Hugging Face Hub API, so public datasets can be queried directly.

### 3.5 Where the registry is used

- **Scans and writes** in every table format (see [12](#doc-12-data-sources-and-formats), [13](#doc-13-lakehouse-delta-and-iceberg))
- **Delta logs and Iceberg metadata**, which use the same stores
- **Workers**: each worker session builds its own registry (see
  [06](#doc-06-session-and-runtime)), so workers read data directly from storage
  rather than through the driver.

---

## 4. Examples

```bash
# Static credentials and a custom S3-compatible endpoint (for example MinIO)
export AWS_ACCESS_KEY_ID=minio AWS_SECRET_ACCESS_KEY=minio123
export AWS_ENDPOINT=http://minio:9000 AWS_ALLOW_HTTP=true AWS_REGION=us-east-1
sail spark server
```

```python
spark.read.parquet("s3://analytics/events/2024/").count()
spark.read.csv("gs://public-bucket/data/*.csv", header=True).show()
spark.read.parquet("abfss://container@account.dfs.core.windows.net/path/").show()
spark.read.parquet("hf://datasets/user/dataset/data/*.parquet").show()
df.write.parquet("s3://output-bucket/results/")    # a different bucket, no extra setup
```

---

## 5. End-to-End: First Read of `s3://analytics/events/`

| # | What happens |
|---|--------------|
| 1 | The listing step asks the registry for a store for `s3://analytics`. |
| 2 | Cache miss. The registry builds runtime → logging → lazy(S3) and caches it under `(s3, analytics)`. No network calls yet. |
| 3 | The first `list` call triggers the lazy initializer on the io runtime: load the AWS configuration, set up the credential provider, parse the URL, and resolve the region if needed. |
| 4 | Listing returns object metadata. Parquet footers are read with ranged GETs. Each call is logged at debug level. |
| 5 | Later queries, and other sessions in the same process, reuse the cached store and credentials. |

---

## 6. Common Problems

| Symptom | Likely cause and fix |
|---------|----------------------|
| "S3 bucket name must be specified in url" | Malformed URL. Use `s3://bucket/key`. |
| Access denied on S3 | Credentials not found by the AWS chain. Check `AWS_PROFILE`, environment variables, and the instance role. |
| Very slow first access to a bucket | Region detection or credential refresh. Set `AWS_REGION` to skip detection. |
| "unsupported object store URL" | The scheme isn't supported, or HDFS support wasn't compiled in. |
| "unsupported repository type" with `hf://` | Only `hf://datasets/...` is supported. |
| Need to see what was read | Turn on debug logs for the object store crate (the logging layer). |

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Dynamic registry and scheme dispatch | `crates/sail-object-store/src/registry.rs` |
| S3 (credentials, URL parsing, region detection) | `crates/sail-object-store/src/s3.rs` |
| Hugging Face store | `crates/sail-object-store/src/hugging_face.rs` |
| Lazy, logging, and runtime layers | `crates/sail-object-store/src/layers/` |
| Where the registry is created for each session | `crates/sail-session/src/runtime.rs` |


---

<a id="doc-15-functions-and-python-udfs"></a>

# 15 · Functions and Python UDFs

> **You'll learn:** how Sail provides about 500 Spark-compatible built-in
> functions, how a function name in a query becomes executable code, and how
> Python UDFs, pandas UDFs, UDAFs, UDTFs, `mapInPandas`, and `applyInPandas`
> run inside Sail's embedded Python interpreter.
>
> **Crates:** `sail-function` (Spark-compatible function implementations),
> `sail-plan/src/function` (the name → builder tables), `sail-python-udf`
> (Python UDF execution), `sail-python` (the `pysail` bindings)

---

## 1. What This Component Does

| Part | Responsibility |
|------|----------------|
| **Function tables** (in `sail-plan`) | Map each Spark function name to a *builder* that produces a DataFusion expression from resolved arguments |
| **Function implementations** (`sail-function`) | Spark-compatible scalar, aggregate, and window functions that DataFusion doesn't provide, or provides with different semantics |
| **Python UDF runtime** (`sail-python-udf`) | Runs user Python code over Arrow data inside the Sail process, through PyO3 |

Approximate counts in the function tables:

| Kind | Entries |
|------|---------|
| Scalar | ≈ 360: string (74), math (64), datetime (54), misc (31), array (25), conversion (14), lambda (12), conditional (11), predicate (11), collection, map, xml, variant, json, hash, bitwise, url, geo, csv, struct |
| Aggregate | ≈ 64 |
| Window | ≈ 70 entries (ranking and offset functions, plus aggregates usable as window functions) |
| Generators | 6 (`explode`, `explode_outer`, `posexplode`, `posexplode_outer`, `inline`, `inline_outer`, …) |
| Table-valued | `range`, … |

The repository also tracks Spark compatibility per function in
`python/pysail/data/compatibility/functions/` (status: supported, partially
supported, planned, or not supported).

---

## 2. Why It's Built This Way

**DataFusion first, Spark semantics where needed.** Many Spark functions map
directly onto DataFusion functions (`sqrt`, `abs`, `lower`). Others differ in
small but important ways:

- `concat` returns NULL if any argument is NULL in Spark.
- Date formats use Java patterns (`yyyy-MM-dd`), not strftime.
- ANSI mode makes overflows and invalid casts raise errors.
- Division by zero returns NULL (non-ANSI).
- Array indexes start at 1 in `element_at`.

For these, `sail-function` provides Spark-specific implementations
(`SparkConcat`, `spark_date_part`, `spark_to_number`, …), and the table points
at those instead.

**Builders, not a flat registry.** A builder gets the **resolved arguments**
and a **function context** (session time zone, ANSI mode, plan config). It can
inspect argument types, insert casts, rewrite into several expressions, or pick
different implementations depending on the session. DataFusion's plain function
registry can't express that.

**Python runs in-process.** Spark runs Python UDFs in separate worker processes
and moves data through sockets. Sail **embeds** CPython (PyO3) and passes Arrow
data to it directly. That removes inter-process serialization and process
management. The function is unpickled lazily, once, and reused.

---

## 3. How It Works

### 3.1 Resolving a function call

When the resolver meets an unresolved function `f(args…)` (see
[07](#doc-07-plan-resolver)):

1. Resolve the arguments.
2. If the session has a **user function** named `f` (a registered Python UDF),
   use it.
3. Else, if `f` is in the **scalar or generator table**, call its builder.
4. Else, if `f` is in the **aggregate table**, build an aggregate expression,
   applying `DISTINCT`, `FILTER`, `ORDER BY`, and `IGNORE NULLS`.
5. Window functions in `OVER (…)` use the **window table**.
6. Otherwise: *unknown function*.

**Builder styles** (examples from the code):

| Style | Example | Meaning |
|-------|---------|---------|
| `unary` / `binary` | `("sqrt", F::unary(double(expr_fn::sqrt)))` | Fixed arity, with a cast wrapper |
| `udf` | `("concat", F::udf(SparkConcat::new()))` | Use a Sail scalar UDF |
| `custom` | `("upper", F::custom(upper))`, `("try_element_at", F::custom(\|i\| element_at(i, true)))` | Arbitrary logic with access to the function context |
| `nullary` | `current_date()`-style functions | No arguments |

### 3.2 Built-in implementations

`sail-function` is organized by area:

- `scalar/`: array, collection, csv, datetime (Spark date parts, intervals,
  `make_timestamp`, `unix_timestamp`, `convert_tz`, Java-format → chrono format,
  …), geo, hash (Spark-compatible `hash`, `xxhash64`, …), json, map, math, misc,
  predicate, string (`format_number`, `levenshtein`, `soundex`, `mask`,
  `regexp_extract_all`, `sentences`, `split`, `to_number`, …), url, variant, xml,
  struct helpers, `explode`
- `aggregate/`: `mode`, `percentile`, `percentile_disc`, `kurtosis`,
  `skewness`, `product`, `max_by` / `min_by`, `histogram_numeric`, `try_avg`,
  bitmap aggregates, `schema_of_variant_agg`
- `window/`: Spark's `ntile`

Each is a DataFusion UDF, UDAF, or UDWF. That means it's vectorized over Arrow
arrays, works in both local and cluster mode (the plan codec serializes it by
name), and takes part in DataFusion's optimizations.

### 3.3 Python UDF kinds

| PySpark API | Sail kind | How it runs |
|-------------|-----------|-------------|
| `@udf` (row-at-a-time) | Batch scalar UDF | Arrow arrays are converted to Python values, the function is called per row, and results are converted back |
| `@udf(useArrow=True)` | Arrow-batched scalar UDF | Python receives Arrow-backed batches |
| `@pandas_udf` (scalar), iterator-of-series | Scalar pandas, scalar pandas iterator | Columns are passed as `pandas.Series` per batch |
| Spark 4 Arrow scalar UDFs | Scalar Arrow, scalar Arrow iterator | Columns are passed as `pyarrow` arrays |
| `@pandas_udf` grouped aggregate | UDAF | Each group's rows are collected, and the function returns one value |
| `df.groupBy(…).applyInPandas(f, schema)` | Group map | Batches are collected per group into a list of structs, then `f(pandas.DataFrame)` returns rows |
| `df.groupBy(…).cogroup(…).applyInPandas` | Cogroup map | Two groups go to the function together |
| `df.mapInPandas(f, schema)`, `mapInArrow` | Map-partitions iterator | The **map partitions** operator streams each partition's batches through `f` (see [08](#doc-08-logical-physical-plans-and-optimizers)) |
| `@udtf` | Python UDTF | A table function that can produce many rows per input row |
| `spark.udf.register("name", f)` | Any of the above | Registered in the session, so it's callable from SQL |

### 3.4 Execution details

1. **Registration.** The client sends the function's pickled bytes
   (cloudpickle), return type, eval type, and whether it's deterministic. Sail
   stores it as a session function (see [11](#doc-11-catalogs)).
2. **Planning.** The resolver creates the right expression or operator for the
   kind, with the input and output Arrow types and the UDF configuration
   (session time zone, pandas conversion settings from
   `spark.sql.execution.pandas.*`).
3. **First call.** Sail takes Python's interpreter lock, unpickles the function,
   and caches the Python object.
4. **Each batch.** Arrow arrays are handed to Python. A bundled Python helper
   module converts between Arrow and PySpark or pandas values, using PySpark's
   own serializers where possible. The function runs, and results are converted
   back to Arrow with the declared return type.
5. **Errors.** Python exceptions are captured with their traceback and appear to
   the client as `PythonException` (see [03](#doc-03-spark-connect)).

**Performance:**

- Plain `@udf` is the slowest: one Python call per row. Prefer `pandas_udf` or
  Arrow UDFs, which do one call per batch.
- All Python code in a process shares the **interpreter lock**. Pure-Python work
  from parallel partitions takes turns. Vectorized pandas or NumPy code releases
  the lock for much of its work.
- In cluster mode, each worker runs its own embedded Python, so the same Python
  packages must be installed on every worker image.

---

## 4. Examples

```python
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, pandas_udf
import pandas as pd

# Built-ins
df.select(F.date_format("ts", "yyyy-MM-dd"), F.element_at("arr", 1), F.concat("a", "b"))

# Row-at-a-time UDF
@udf("int")
def plus_one(x): return x + 1

# Vectorized pandas UDF
@pandas_udf("double")
def zscore(v: pd.Series) -> pd.Series: return (v - v.mean()) / v.std()

# Grouped map
def normalize(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf["v"] = pdf["v"] - pdf["v"].mean(); return pdf
df.groupBy("k").applyInPandas(normalize, schema="k long, v double")

# mapInPandas
def keep_positive(it):
    for pdf in it: yield pdf[pdf.v > 0]
df.mapInPandas(keep_positive, schema=df.schema)

# Register for SQL
spark.udf.register("plus_one", plus_one)
spark.sql("SELECT plus_one(id) FROM range(3)").show()
```

---

## 5. End-to-End: `df.select(zscore("v"))`

| # | Component | What happens |
|---|-----------|--------------|
| 1 | PySpark | Pickles `zscore` and sends a projection whose expression is an inline UDF (eval type: scalar pandas, return type `double`). |
| 2 | Spark Connect | Converts it to a spec inline UDF expression. |
| 3 | Resolver | Builds a Python scalar UDF expression (kind: scalar pandas) with the input type of `v` and the UDF settings. |
| 4 | Physical planning | A projection operator evaluates the UDF expression. In cluster mode the pickled bytes go into the task plan. |
| 5 | Execution | For each batch: take the interpreter lock, get the cached function, convert column `v` to a `pandas.Series`, call `zscore`, convert the result to an Arrow `Float64` array, and continue. |
| 6 | Result | Arrow batches flow on to the client. |

---

## 6. Adding a Built-in Function

1. Check whether DataFusion already has it with the right semantics. If so,
   just add a table entry.
2. If not, implement a UDF, UDAF, or UDWF in `sail-function` under the right
   area: its signature, return type logic, and an `invoke` that works on Arrow
   arrays and handles NULLs and ANSI mode.
3. Add `("spark_name", builder)` to the matching list in
   `sail-plan/src/function/`.
4. If it's a new UDF type that must run on workers, make sure the plan codec can
   serialize it (see [09](#doc-09-local-and-cluster-execution)).
5. Add tests (gold data or Spark comparison, see
   [17](#doc-17-testing-and-development)) and update the compatibility JSON.

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Function tables and builders | `crates/sail-plan/src/function/` (`common.rs`, `scalar/`, `aggregate.rs`, `window.rs`, `generator.rs`, `table/`) |
| Function lookup during resolution | `crates/sail-plan/src/resolver/expression/function.rs` |
| Spark-compatible implementations | `crates/sail-function/src/` (`scalar/`, `aggregate/`, `window/`) |
| Python UDF kinds | `crates/sail-python-udf/src/udf/` |
| Arrow ↔ Python conversion | `crates/sail-python-udf/src/conversion.rs`, `python/spark.py` |
| Pickled payload formats | `crates/sail-python-udf/src/cereal/` |
| UDF settings | `crates/sail-python-udf/src/config.rs` |
| Python bindings (`pysail`) | `crates/sail-python/` |
| Compatibility data | `python/pysail/data/compatibility/functions/` |


---

<a id="doc-16-flight-sql-and-telemetry"></a>

# 16 · Arrow Flight SQL, Telemetry, and Observability

> **You'll learn:** how SQL clients connect to Sail through Arrow Flight SQL, and
> how Sail produces logs, traces, and metrics (with OpenTelemetry) so you can see
> what a query did.
>
> **Crates:** `sail-flight`, `sail-telemetry`, plus the system tables from
> [06](#doc-06-session-and-runtime)

---

## Part A: Arrow Flight SQL

### A.1 What it is

**Arrow Flight SQL** is a protocol for running SQL over gRPC that returns
results as Arrow record batches. Clients include ADBC and JDBC Flight SQL
drivers, DBeaver, Python's `adbc_driver_flightsql`, and others. Sail runs a
Flight SQL server with:

```bash
sail flight server --port 32010
```

### A.2 Why it exists

Spark Connect is great for PySpark, but many tools only speak SQL. Flight SQL
gives them a fast, columnar way in. Results are already Arrow, so there's no row
conversion. Sail reuses **everything below the front end**: the SQL parser and
analyzer, the resolver, sessions, job runners, catalogs, and formats.

### A.3 How it works

Flight SQL uses two steps:

| Step | RPC | Sail behavior |
|------|-----|---------------|
| 1 | `GetFlightInfo` (statement query) | Parse the SQL ([04](#doc-04-sql-parser-and-analyzer)) into a spec plan. Get the session. Resolve and plan ([07](#doc-07-plan-resolver)). **Start execution** on the job runner. Store the result stream under a new **query handle**. Return a `FlightInfo` with the schema and a ticket containing the handle. |
| 2 | `DoGet` (ticket) | Look up the stream by handle, **remove it** (each result can be read once), and stream Arrow batches to the client. |

Details:

- **Commands** (`CREATE TABLE`, `INSERT`, …) are run to completion in step 1 and
  their (small) result is kept in memory. **Queries** stream lazily in step 2.
- **Sessions:** all Flight SQL clients currently share **one** session
  (`flight-default`), created through the same session manager as Spark Connect.
  Temporary views and settings are therefore shared across Flight clients. The
  idle timeout is `flight.session_timeout_secs` (1 hour).
- **Handshake** is accepted but no authentication is done. Put the server behind
  an authenticating gateway in production.
- **Metrics:** when telemetry metrics are on, Flight records statement counts,
  active statements, duration, and rows, labeled by statement type (query or
  command) and status.
- Catalog-metadata RPCs (`GetTables`, `GetSchemas`, …) and prepared statements
  are minimal or not implemented. Use SQL (`SHOW TABLES`, `DESCRIBE`) instead.

### A.4 Example

```python
import adbc_driver_flightsql.dbapi as flight_sql
conn = flight_sql.connect("grpc://localhost:32010")
cur = conn.cursor()
cur.execute("SELECT region, count(*) FROM parquet.`/data/orders` GROUP BY region")
print(cur.fetch_arrow_table())
```

---

## Part B: Telemetry and Observability

### B.1 What you get

| Signal | Source | Where it goes |
|--------|--------|---------------|
| **Logs** | Rust `log` macros throughout Sail | stderr (filtered by `RUST_LOG`, default `info`). Also attached to the current trace span as events, and optionally exported over OTLP. |
| **Traces** | `fastrace` spans: gRPC requests, actor messages, planning steps, job execution, **every physical operator** | OTLP collector (Jaeger, Tempo, …) when `telemetry.export_traces=true` |
| **Metrics** | Operator metrics from DataFusion, plus Flight statement metrics | OTLP when `telemetry.export_metrics=true` |
| **System tables** | Live job, stage, task, worker, and session state | `SELECT * FROM system.execution.jobs`, … (see [06](#doc-06-session-and-runtime)) |
| **EXPLAIN** | Logical and physical plans, optionally with runtime metrics (`EXPLAIN ANALYZE`) | The query result |

### B.2 Why it's built this way

- **One instrumentation pass.** Instead of adding timing code to every operator,
  Sail **wraps the optimized physical plan** just before execution: every
  operator is wrapped in a tracing node that creates spans and turns DataFusion's
  operator metrics into OpenTelemetry metrics. Local job runners and cluster task
  runners both do this.
- **Consistent attributes.** Every metric and span carries the same labels: job
  ID, stage, partition, attempt, operator ID and name (and expression ID and name
  for expression timing). That makes it easy to slice by query or stage. The list
  of metrics and attributes is defined in YAML (`metrics/data/registry.yaml`,
  `attributes.yaml`), and code is generated from it.
- **Off by default, cheap when off.** All exporting is disabled unless you turn
  it on, so local development has no setup.
- **Context crosses process boundaries.** gRPC client and server layers pass the
  trace context (W3C `traceparent`), and Kubernetes worker pods receive it at
  startup, so driver and worker spans join one trace.

### B.3 Metrics available

| Group | Metrics |
|-------|---------|
| Output | `execution.output_size`, `execution.output_batch_count`, `execution.output_row_count` |
| CPU and memory | `execution.elapsed_compute_time`, `execution.memory_used` |
| Spilling | `execution.spill_count`, `execution.spill_size`, `execution.spill_row_count` |
| Expressions and filters | `execution.expression_evaluation_time`, `execution.filter.input_row_count` / `output_row_count` |
| Joins | build-side time, batches, rows, matches, memory; probe-side batches, rows, matched rows; operation time; output rows; candidates; left and right inputs; memory |
| Flight SQL | `flight.statement.total_count`, `active_count`, `duration`, `row_count` |
| Bookkeeping | `execution.unknown_metric_count` (DataFusion metrics without a mapping) |

### B.4 Configuration

| Setting | Default | Meaning |
|---------|---------|---------|
| `telemetry.export_traces` | false | Export traces over OTLP |
| `telemetry.export_metrics` | false | Export metrics over OTLP |
| `telemetry.export_logs` | false | Export logs over OTLP |
| `telemetry.otlp_endpoint` | `http://localhost:4317` | Collector address |
| `telemetry.otlp_protocol` | `grpc` | `grpc` or HTTP variants |
| `telemetry.*_interval_secs`, `logs_export_*` | | Export intervals, batch sizes, and queue sizes |

The resource kind is `server` for the Spark Connect or Flight server process and
`worker` for worker processes, so you can filter by role in your observability
backend.

### B.5 Example: traces into a local Jaeger

```bash
docker run -d -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one
export SAIL_TELEMETRY__EXPORT_TRACES=true
export SAIL_TELEMETRY__OTLP_ENDPOINT=http://localhost:4317
RUST_LOG=info sail spark server
```

Run a query, then open Jaeger at `http://localhost:16686`. You'll see a trace
for the Spark Connect request containing planning spans, then a job-execution
span with child spans for each operator (scan, filter, aggregate, repartition,
…). In cluster mode, spans from workers appear in the same trace.

### B.6 End-to-end: what gets recorded for one query

| # | Recorded |
|---|----------|
| 1 | A server span for the gRPC request (Spark Connect `ExecutePlan` or Flight `GetFlightInfo`) |
| 2 | Spans for resolving the plan and starting the job runner |
| 3 | In cluster mode: driver actor message spans (execute job, update task, …), and gRPC client spans to workers |
| 4 | For each operator in each task: a span plus metrics (rows, batches, bytes, compute time, memory, spills) with job, stage, partition, attempt, and operator labels |
| 5 | Log lines emitted along the way, attached as span events |
| 6 | Afterwards, `system.execution.*` tables show the job, stages, and task attempts with status and times |

---

## Where to Look

| Topic | Location |
|-------|----------|
| Flight SQL server entry | `crates/sail-flight/src/lib.rs`, `crates/sail-cli/src/flight/server.rs` |
| Flight SQL service (statement query, do-get) | `crates/sail-flight/src/service.rs` |
| Query handles and stored streams | `crates/sail-flight/src/state.rs` |
| Flight metrics | `crates/sail-flight/src/metrics.rs` |
| Telemetry setup and shutdown | `crates/sail-telemetry/src/telemetry.rs` |
| Physical plan tracing wrapper | `crates/sail-telemetry/src/execution/physical_plan.rs` |
| gRPC tracing layers | `crates/sail-telemetry/src/layers/` |
| Loggers (stderr plus span events) | `crates/sail-telemetry/src/loggers/` |
| Metric and attribute definitions | `crates/sail-telemetry/src/metrics/data/` (`registry.yaml`, `attributes.yaml`) |
| Telemetry settings | `telemetry.*` in `crates/sail-common/src/config/application.yaml` |


---

<a id="doc-17-testing-and-development"></a>

# 17 · Testing and Development Workflow

> **You'll learn:** how the repository is laid out, how to build Sail (Rust and
> Python), the kinds of tests and when to use each, how gold data and snapshot
> tests work, how the PySpark test suite is run against Sail, and a practical
> workflow for making a change.

---

## 1. Repository Map

| Path | Contents |
|------|----------|
| `crates/` | All Rust crates (about 35). See the crate map in [README](#doc-readme). |
| `python/pysail/` | The `pysail` Python package: server wrapper, CLI shims, examples, compatibility data, and **Python tests** |
| `docs/` | The public documentation site (VitePress) |
| `scripts/` | Spark test harness, gold data bootstrap, build helpers |
| `docker/`, `k8s/`, `deployments/`, `compose.yml` | Container images, Kubernetes manifests, local service stacks (for example for HMS or MinIO) |
| `pyproject.toml` | Python build (maturin) and `hatch` environments for development and testing |
| `Cargo.toml` | Rust workspace |

---

## 2. Building

| Goal | Command |
|------|---------|
| Build Rust crates | `cargo build` |
| Build the CLI binary (release) | `cargo build -r -p sail-cli --bins` |
| Build and install `pysail` into the dev environment | `hatch run maturin develop` |
| Build a wheel | `hatch run maturin build` (add `--release`) |
| Install a built wheel into the test environment | `hatch run install-pysail` |
| Format and lint | `cargo +nightly fmt`, `cargo clippy --all-targets --all-features`, `hatch fmt` |

The `pysail` package is a Rust extension (built with PyO3 and maturin) plus
Python modules. The `sail` binary and `pysail` share all of the engine code.

See `docs/development/` (setup, build, recipes) for prerequisites, faster builds,
debugging, and cross-compilation.

---

## 3. Kinds of Tests

| Kind | Where | Run with | Good for |
|------|-------|----------|----------|
| **Rust unit tests** | `#[cfg(test)]` modules in each crate | `cargo test -p <crate>` | Algorithms, small components (coalescing, parsers, codecs, actors) |
| **Gold data tests** | `crates/*/tests/gold_data/*.json` | `cargo test` | Locking in exact outputs for SQL syntax, expressions, functions, data types, and plans |
| **Python integration tests** | `python/pysail/tests/` | `hatch run pytest` | End-to-end PySpark behavior against a real Sail server |
| **BDD feature tests** | `*.feature` files with pytest-bdd | `hatch run pytest` | Readable scenario tables (for example function behaviors, Iceberg and Delta features) |
| **Snapshot tests** | `__snapshots__/` (syrupy) | `hatch run pytest`, with `--snapshot-update` to refresh | Plan and output snapshots for features |
| **Spark's own test suite** | Patched PySpark test suite run against Sail | `scripts/spark-tests/*.sh` | Broad compatibility tracking |
| **Ibis tests** | The Ibis backend test suite against Sail | `hatch run test-ibis:…` | Third-party DataFrame library compatibility |
| **Ignored integration tests** | For example HMS with Kerberos | `cargo test … -- --ignored` | Tests that need external services (containers) |

### 3.1 Gold data tests

**Gold data** records the **actual** output of many inputs (SQL statements,
expressions, function calls) in JSON files that are reviewed like code:

```json
{ "input":  { "query": "SELECT map(1.0, '2', 3.0, '4');",
              "result": ["{1.0:\"2\",3.0:\"4\"}"],
              "schema": { "type": "struct", "fields": [ … ] } },
  "output": { "success": "ok" } }
```

- **Inputs** are collected once from the Spark project's own tests by a bootstrap
  script (`scripts/spark-gold-data/bootstrap.sh`, with a Spark patch). This is
  rare, mainly when Spark is upgraded.
- **Outputs** are what Sail produces: success, a result, or an error message.
- When you change behavior, regenerate and **commit** the gold data so reviewers
  see the difference:

```bash
env SAIL_UPDATE_GOLD_DATA=1 cargo test     # rewrite outputs
cargo test                                  # verify they're stable
```

Gold data lives in, for example, `crates/sail-sql-parser/tests/gold_data/`
(syntax) and `crates/sail-spark-connect/tests/gold_data/` (data types,
expressions, functions by category, plans, table schemas).

### 3.2 Python tests

Python tests start a Sail Spark Connect server, usually in-process through
`pysail`, and run PySpark code against it. Folders mirror the components:
`aggregate`, `catalog`, `catalog_integration`, `datasource`, `delta`,
`iceberg`, `execution`, `function`, `glue`, `hms`, `literal`, `optimizer`,
`streaming`, and more. Some tests run against a real Spark as well, to compare
results.

```bash
hatch run maturin develop          # build pysail into the env
hatch run pytest                   # all Python tests
hatch run pytest python/pysail/tests/spark/delta -k merge
env SPARK_REMOTE="sc://localhost:50051" hatch run pytest   # against an already-running server
```

### 3.3 Running Spark's test suite

`scripts/spark-tests/` builds a patched PySpark (3.5 or 4.1), starts a Sail
server (`run-server.sh`), runs PySpark's tests against it (`run-tests.sh`), and
summarizes the results (`generate-test-report.sh`, plus `jq` scripts to list
passed and failed tests and count errors). CI uses this to track compatibility
over time. See `docs/development/spark-tests/`.

---

## 4. A Practical Workflow for a Change

**Example: fix a Spark function's behavior.**

1. **Reproduce.** Write a failing PySpark snippet against a dev server:
   `hatch run maturin develop`, then a quick script or a new test in
   `python/pysail/tests/spark/function/`.
2. **Locate.** Find the function in `sail-plan/src/function/…` (the builder) and,
   if needed, `sail-function/src/…` (the implementation). See
   [15](#doc-15-functions-and-python-udfs).
3. **Fix**, and add a Rust unit test if the logic is self-contained.
4. **Update gold data:** `env SAIL_UPDATE_GOLD_DATA=1 cargo test`. Review the JSON
   diff and make sure only the intended outputs changed.
5. **Run Python tests** for the area, and update snapshots if needed
   (`--snapshot-update`).
6. **Update compatibility data** in `python/pysail/data/compatibility/` if
   support status changed.
7. **Format and lint:** `cargo +nightly fmt`, `cargo clippy …`, `hatch fmt`.

**Example: debug a cluster-only failure.**

1. Reproduce with `SAIL_MODE=local-cluster` (see
   [09](#doc-09-local-and-cluster-execution)).
2. Turn on logs: `RUST_LOG=sail_execution=debug,info`. The task runner logs each
   task's rewritten physical plan, and the driver logs state transitions.
3. Look at `system.execution.stages` and `system.execution.tasks`.
4. If an operator fails to serialize, look at the plan codec
   (`sail-execution/src/codec.rs`).

---

## 5. Tips

| Tip | Why |
|-----|-----|
| Use `cargo test -p <crate>` | Rebuilding and testing the whole workspace is slow |
| Read `docs/development/recipes/reducing-build-time.md` | Linker and profile settings that speed up builds a lot |
| Prefer gold data or feature files over ad-hoc asserts for Spark behavior | Reviewers see the exact behavior change, and outputs are easy to regenerate |
| Use `RUST_LOG=<crate>=debug` | Every Spark Connect request and response, task plan, and object store call is logged at debug level |
| Use the system tables | Fastest way to see what the cluster is doing |

---

## 6. Where to Look

| Topic | Location |
|-------|----------|
| Development docs | `docs/development/` (`setup/`, `build/`, `recipes/`, `spark-tests/`) |
| Contributor guide | `CONTRIBUTING.md`, `docs/development/architecture-contributor-guide.md` |
| Gold data helpers | `crates/sail-common/src/tests.rs` (`SAIL_UPDATE_GOLD_DATA`), `crates/sail-gold-test/` |
| Gold data files | `crates/*/tests/gold_data/` |
| Gold data bootstrap | `scripts/spark-gold-data/` |
| Spark test harness | `scripts/spark-tests/` |
| Python tests | `python/pysail/tests/` |
| Hatch environments | `pyproject.toml` (`[tool.hatch.envs.*]`) |

