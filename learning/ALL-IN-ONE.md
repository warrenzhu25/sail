# Learning Sail: Complete Guide (All Documents in One)

This file merges every document in `learning/` into one page, for reading
from start to finish or searching everything at once. The separate files are
the source. Regenerate this file after editing them.

## Contents

- [Learning Sail: An Internal Guide](#doc-readme)
- [01 · End to End: One Query Through All of Sail](#doc-01-end-to-end-workflow)
- [02 · Entry Points, Configuration, and the Server Framework](#doc-02-entry-points-and-server)
- [03 · Spark Connect: How PySpark Talks to Sail](#doc-03-spark-connect)
- [04 · SQL Parser and Analyzer: From Text to a Plan](#doc-04-sql-parser-and-analyzer)
- [05 · The Spec Plan: Sail's Neutral Description of a Query](#doc-05-spec-plan-and-config)
- [06 · Sessions and the Runtime Environment](#doc-06-session-and-runtime)
- [07 · The Plan Resolver: Turning Intent into a Plan](#doc-07-plan-resolver)
- [08 · Plans, Extension Nodes, and Optimizers](#doc-08-logical-physical-plans-and-optimizers)
- [09 · Local and Cluster Execution](#doc-09-local-and-cluster-execution)
- [10 · Shuffle and Streams: Moving Data Between Tasks](#doc-10-shuffle-and-streams)
- [11 · Catalogs: Finding Tables](#doc-11-catalogs)
- [12 · Data Sources and File Formats](#doc-12-data-sources-and-formats)
- [13 · Lakehouse Tables: Delta Lake and Apache Iceberg](#doc-13-lakehouse-delta-and-iceberg)
- [14 · Object Storage: Reaching S3, GCS, Azure, and More](#doc-14-object-storage)
- [15 · Functions and Python UDFs](#doc-15-functions-and-python-udfs)
- [16 · Flight SQL, Telemetry, and Observability](#doc-16-flight-sql-and-telemetry)
- [17 · Testing and Development](#doc-17-testing-and-development)


---

<a id="doc-readme"></a>

# Learning Sail: An Internal Guide

These notes explain how Sail works inside, one component at a time. Each
document is written to be read from top to bottom, like a colleague explaining
the code to you: it starts with the problem a component solves, explains why it
was built the way it was, walks through how it works using a concrete example,
and ends with a short paragraph pointing to the code.

Sail is a query engine written in Rust that speaks Spark's language. PySpark
programs connect to it through Spark Connect, SQL tools through Arrow Flight
SQL, and Python programs can embed it directly. Underneath, it uses Apache Arrow
for data and Apache DataFusion for query planning and execution. It can run a
query inside one process or spread it across a driver and many workers on
Kubernetes.

These notes describe the learning-docs branch, which is based on deepdive. Where
the disk-shuffle-hardening branch changes behavior (blocking shuffle and stage
retry), the docs say so.

---

## Where to start

Start with [01 · End to End](#doc-01-end-to-end-workflow). It follows one query
through every component, so you'll see how the pieces fit before diving into
any of them. After that, pick a path. If you're new to Sail, read 02, 03, 05,
07, and 09 in order. If you'll work on SQL or Spark compatibility, read 04, 05,
07, 15, and 17. For distributed execution, read 09, 10, 06, and 08. For storage,
formats, and catalogs, read 11 through 14.

| # | Document | What it explains |
|---|----------|------------------|
| 01 | [End to End](#doc-01-end-to-end-workflow) | One query through all of Sail |
| 02 | [Entry Points and the Server Framework](#doc-02-entry-points-and-server) | The sail binary, configuration, actors, gRPC servers |
| 03 | [Spark Connect](#doc-03-spark-connect) | How PySpark talks to Sail |
| 04 | [SQL Parser and Analyzer](#doc-04-sql-parser-and-analyzer) | How SQL text becomes a plan |
| 05 | [The Spec Plan](#doc-05-spec-plan-and-config) | Sail's neutral description of a query |
| 06 | [Sessions and the Runtime](#doc-06-session-and-runtime) | What a session holds, memory, spilling, caches |
| 07 | [The Plan Resolver](#doc-07-plan-resolver) | How names get their meaning |
| 08 | [Plans, Extension Nodes, and Optimizers](#doc-08-logical-physical-plans-and-optimizers) | Sail's own operators and optimizer rules |
| 09 | [Local and Cluster Execution](#doc-09-local-and-cluster-execution) | Drivers, workers, stages, and tasks |
| 10 | [Shuffle and Streams](#doc-10-shuffle-and-streams) | How data moves between tasks |
| 11 | [Catalogs](#doc-11-catalogs) | How table names are found |
| 12 | [Data Sources and File Formats](#doc-12-data-sources-and-formats) | Reading and writing files |
| 13 | [Delta Lake and Iceberg](#doc-13-lakehouse-delta-and-iceberg) | Lakehouse tables |
| 14 | [Object Storage](#doc-14-object-storage) | S3, GCS, Azure, and friends |
| 15 | [Functions and Python UDFs](#doc-15-functions-and-python-udfs) | Built-in functions and Python code |
| 16 | [Flight SQL and Telemetry](#doc-16-flight-sql-and-telemetry) | SQL clients, logs, traces, metrics |
| 17 | [Testing and Development](#doc-17-testing-and-development) | Building, testing, making changes |

---

## The big picture

The diagram below shows the layers a request passes through. Clients at the top
send requests to a front end, which turns them into Sail's neutral plan. The
planner gives that plan meaning and decides how to run it. The execution layer
runs it, locally or on a cluster, reading data through catalogs, table formats,
and storage at the bottom.

```text
 Clients:  PySpark (Spark Connect)        SQL tools (Flight SQL)        pysail (Python library)
               │                                  │                           │
               ▼                                  ▼                           ▼
 ┌───────────────────────────────────────────────────────────────────────────────────┐
 │ Front ends      sail-spark-connect          sail-flight            sail-python     │  03, 16, 02
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

The biggest crates are the Delta Lake implementation and the function library
(about 37,000 lines each), the plan resolver (about 24,000), the Iceberg
implementation (about 21,000), and the execution engine (about 15,500). Most
other crates are a few thousand lines or fewer.

---

## A few words you'll see everywhere

A **session** is everything Sail keeps for one client: settings, catalogs,
temporary views, and the machinery for running its queries. The **spec plan** is
Sail's own description of a query, produced by every front end, in which names
haven't been looked up yet. The **resolver** turns that into a real plan by
finding every table, column, and function. A **job runner** executes a finished
plan, either in-process or through a **driver** that coordinates **workers**. A
job is split into **stages** at the points where data must be exchanged, and
each stage runs as one **task** per partition. Data moving between stages is
called a **shuffle**, and travels as **streams** of Arrow record batches. The
driver, workers, and session manager are **actors**: components that own their
state and handle one message at a time.

---

## How these docs were written

Every behavior described here was checked against the code on this branch, and
default values come from the configuration schema in
crates/sail-common/src/config/application.yaml. When the code changes, the
"Where to look in the code" paragraph at the end of each doc points to the files
worth re-reading. Corrections are welcome: edit the relevant document and say
what changed in the commit message.


---

<a id="doc-01-end-to-end-workflow"></a>

# 01 · End to End: One Query Through All of Sail

The best way to understand Sail is to watch one query travel through it. This
doc follows a single PySpark query from the moment you type it until results
appear on your screen. Each step names the component involved and links to the
doc that explains it in depth. Read this one first; the rest of the guide will
make more sense once you have the whole journey in mind.

Here's the query. An analyst connects to a Sail server running in cluster mode
on Kubernetes and asks for order totals per region:

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.remote("sc://sail.example.com:50051").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

orders = spark.table("glue.sales.orders")          # a Parquet table registered in AWS Glue
result = (orders
    .filter(F.col("amount") > 100)
    .groupBy("region")
    .agg(F.count("*").alias("n"), F.sum("amount").alias("total"))
    .orderBy(F.desc("total")))

result.show()
```

---

## Before anyone connects

Long before this query, someone started the server with `sail spark server`.
At startup Sail read its configuration (defaults compiled into the binary,
overridden by environment variables starting with SAIL_), created two thread
pools (one for query work, one for storage I/O), set up telemetry, and began
listening for Spark Connect requests on port 50051. All of this is covered in
[02](#doc-02-entry-points-and-server).

## Connecting creates a session

When the analyst calls `getOrCreate()`, PySpark invents a session ID and sends
its first request. The server has never seen this ID, so it builds a new
session: a bundle holding everything this client will need. The session gets a
catalog manager that knows about the `glue` catalog, a registry of file and
table formats, a fresh driver for running distributed jobs, a memory pool,
file caches, and a registry that will create storage clients on demand. Each
session gets its own driver, so one user's work never interferes with
another's. Sessions are explained in [06](#doc-06-session-and-runtime), catalogs
in [11](#doc-11-catalogs).

The `spark.conf.set` call is a small configuration request. Sail stores the
time zone in the session, where it will later influence how timestamps are
interpreted ([03](#doc-03-spark-connect)).

## Building the DataFrame does nothing yet

The lines that build `result` don't contact the server at all. PySpark simply
assembles a description of the query in memory: read this table, filter it,
group it, aggregate it, sort it. This is Spark's familiar laziness, and Sail
preserves it exactly. Work only begins when an *action* such as `show()`,
`collect()`, `count()`, or a write is called.

## show() sends the plan

Calling `show()` packages the whole description as a tree of Spark protobuf
messages and sends it in one request. Sail's Spark Connect front end receives
it and immediately translates it into Sail's own neutral form, the *spec plan*.
At this point, nothing has been checked. The spec plan simply records that the
user wants to read something called glue.sales.orders, filter on something
called amount, and so on ([03](#doc-03-spark-connect),
[05](#doc-05-spec-plan-and-config)).

## Resolving gives every name a meaning

Now the resolver goes to work ([07](#doc-07-plan-resolver)). It asks the catalog
manager about glue.sales.orders. The manager recognizes glue as a catalog and
asks the Glue service, through a cache and on the I/O thread pool, for the table
definition. Glue reports a Parquet table stored under s3://lake/sales/orders/,
partitioned by date.

The resolver hands that description to the Parquet table format
([12](#doc-12-data-sources-and-formats)), which needs to know which files exist.
It asks the storage registry for an S3 client. The registry has never seen this
bucket, so it creates one lazily: the AWS credentials are loaded and the
bucket's region is detected the first time the client is actually used
([14](#doc-14-object-storage)). The format lists the files, skips bookkeeping
files like _SUCCESS, and returns a table source.

With the table in hand, the resolver gives each column a private name so that
nothing can be confused later, matches amount in the filter, finds count and
sum in its function tables ([15](#doc-15-functions-and-python-udfs)), builds the
aggregation, and resolves the sort against the new total column. The result is
a DataFusion logical plan plus the names the user should see: region, n, total.

## Planning decides how to run it

DataFusion's optimizer, extended with Sail's rules, now improves the plan
([08](#doc-08-logical-physical-plans-and-optimizers)). The filter moves into the
Parquet scan, so files and row groups that can't contain amount > 100 are
skipped, and only the two needed columns are read. The aggregation is split into
a partial step, which runs next to the data, and a final step, with a
repartition by region in between so all rows for one region meet in one place.
The sorted result is merged into a single stream at the end:

```text
ShowString
  SortPreservingMerge [total DESC]           ← exchange: merge into one stream
    Sort [total DESC]
      Aggregate (final) [region]
        Repartition hash(region, 8)          ← exchange: shuffle by region
          Aggregate (partial) [region]
            Parquet scan (filter amount>100, columns region, amount)
```

## Running it across a cluster

Because the server runs in cluster mode, the plan goes to the session's driver
([09](#doc-09-local-and-cluster-execution)). The driver cuts the plan wherever
data has to move between machines, which here means at the repartition and at
the final merge. That gives three stages: the first scans and partially
aggregates, the second finishes the aggregation and sorts, and the third merges
everything into one sorted result and formats it for display.

These stages are *pipelined*: they run at the same time, with data streaming
from one to the next. That means they need worker capacity at the same moment.
The driver started four worker pods when the session was created, and each
worker has eight task slots. Tasks from different stages can share a slot, so
this job needs about as many slots as its widest stage, eight, and it fits.

Each worker receives its tasks, decodes the plan fragments, and starts running.
The first-stage tasks read their Parquet files straight from S3, compute partial
counts and sums, and split their output by region into separate in-memory
streams. Second-stage tasks each collect the streams for their share of regions,
reading local streams directly and fetching remote ones from other workers over
Arrow Flight ([10](#doc-10-shuffle-and-streams)). They finish the totals and sort.
The single last task merges the sorted pieces and formats the first twenty rows
as a text table.

## Results stream back

The driver hands the final stream back, and the Spark Connect front end sends it
to PySpark: first the schema, then the batch containing the formatted table,
then a completion message. If the query had taken a long time, Sail would have
sent small heartbeat messages to keep the connection alive. If the network had
dropped, PySpark could have reconnected and resumed from the last message it
received ([03](#doc-03-spark-connect)). PySpark prints the table.

Afterwards, the tasks report success, the job is marked done, and its streams
are cleaned up. Telemetry has recorded a span for every operator in every task
([16](#doc-16-flight-sql-and-telemetry)), and the job now shows up in
system.execution.jobs. A minute later, idle workers shut down. Fifteen minutes
after the analyst's last request, the session itself is removed, though its
history remains visible.

## The same query, two variations

If the server runs in **local mode** instead, everything up to planning is
identical. The difference is in execution: there's no driver, no stages, and no
workers. DataFusion runs the plan with all CPU cores inside the server process,
and the repartition becomes an in-memory exchange between threads.

If the analyst writes the query as **SQL** instead, only one step changes. The
SQL text is broken into tokens, parsed into a syntax tree, and analyzed into the
same spec plan the DataFrame produced ([04](#doc-04-sql-parser-and-analyzer)).
From the resolver onward, the two are indistinguishable.

## When something goes wrong

The error message usually tells you where in this journey the query stopped. A
ParseException means the SQL itself was malformed ([04](#doc-04-sql-parser-and-analyzer)).
TABLE_OR_VIEW_NOT_FOUND comes from the catalog lookup ([11](#doc-11-catalogs)).
"Cannot resolve attribute" and "unknown function" come from the resolver
([07](#doc-07-plan-resolver), [15](#doc-15-functions-and-python-udfs)). Access
errors on S3 point at storage credentials ([14](#doc-14-object-storage)). A "task
scheduling timeout" or "lost worker" points at cluster capacity or worker health
([09](#doc-09-local-and-cluster-execution)). A worker killed for running out of
memory usually means the memory pool was left unbounded
([06](#doc-06-session-and-runtime)).


---

<a id="doc-02-entry-points-and-server"></a>

# 02 · Entry Points, Configuration, and the Server Framework

Every Sail process starts the same way: one binary, a configuration file baked
into it, a couple of thread pools, and a handful of long-running components
that talk to each other through messages. This doc explains that foundation:
how the `sail` binary decides what to be, where configuration comes from, why
Sail uses two thread pools, and why its coordinators are built as actors.

---

## One binary, many roles

Sail ships as a single executable called `sail`. The first word after it picks a
role. `sail spark server` starts a Spark Connect server that PySpark clients can
connect to. `sail spark shell` starts an interactive PySpark shell backed by a
private Sail server. `sail spark run -f job.py` runs a PySpark script against a
private server and exits. `sail spark mcp-server` exposes Sail to AI assistants
through the Model Context Protocol. `sail flight server` starts an Arrow Flight
SQL server for SQL tools. And `sail worker` turns the process into a cluster
worker, though you never run that yourself: the driver starts workers for you.

Why one binary instead of several? Because it guarantees that the server and its
workers always run exactly the same code, and because it makes shipping simple:
one container image serves every role. A Kubernetes worker pod runs the same
image as the server, just with the worker argument.

The shell and script runner deserve a closer look, because they show a pattern
Sail uses often. Rather than asking you to start a server first, they start one
themselves, bound to 127.0.0.1 on a random free port so it isn't reachable from
the network. They then start PySpark connected to that address, and shut the
server down as soon as Python exits. From your point of view, `sail spark shell`
behaves like a self-contained PySpark.

The same engine is also available as a Python package, pysail. A Python program
can start a Spark Connect server inside its own process:

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

## The binary that pretends to be Python

There's one unusual trick in the startup code. Sail runs Python user-defined
functions in an embedded Python interpreter (see
[15](#doc-15-functions-and-python-udfs)). Some Python libraries, such as
multiprocessing, start helper processes by re-running "the current Python
executable". Inside Sail, the current executable is the sail binary, not python.

To make this work, Sail sets an internal environment variable,
SAIL_INTERNAL__RUN_PYTHON, before starting its embedded Python. Any child
process inherits it. When the sail binary starts and sees that variable, it skips
all of its own logic and hands control straight to Python's main function. The
library that launched the child never notices that it wasn't talking to a real
Python executable.

## Configuration: defaults in the binary, overrides from the environment

All of Sail's settings are described in one YAML file that's compiled into the
binary. Each entry has a key such as execution.batch_size, a type, a default, a
description, and sometimes a note that it's experimental. When Sail starts, it
loads these defaults and then applies any environment variables that start with
SAIL_. Double underscores in a variable name stand for dots, so
SAIL_EXECUTION__BATCH_SIZE=4096 sets execution.batch_size to 4096. List values
use a compact inline syntax, for example
SAIL_CATALOG__DEFAULT_DATABASE='["analytics"]'.

The loaded values are checked against Rust types that reject unknown fields.
That's a deliberate choice: if you misspell a variable, say
SAIL_EXECUTION__BATCHSIZE, Sail refuses to start instead of silently ignoring
it. Variables starting with SAIL_INTERNAL__ are reserved for Sail's own use and
are ignored by normal configuration.

The settings are grouped by area. The mode setting decides where queries run.
The runtime group controls threads, the memory pool, and spill files. The
cluster group covers drivers, workers, task slots, retries, and timeouts. There
are also groups for DataFusion execution, Parquet, Kubernetes, catalogs, the
optimizer, Spark and Flight sessions, telemetry, and Python data sources. Each of
these is explained in the doc for the component it controls.

## Three ways to run: local, local cluster, Kubernetes

The mode setting is the most important switch. In **local** mode, the default,
every query runs inside the server process using DataFusion's multi-threaded
engine. That's the right choice for a laptop or a single large machine. In
**local-cluster** mode, Sail runs a driver and several workers inside the same
process. They talk to each other over gRPC on localhost exactly as they would
across machines, which makes this mode ideal for testing distributed behavior
without Kubernetes. In **kubernetes-cluster** mode, the driver runs in the
server and starts workers as separate pods. How each mode executes a query is
the subject of [09](#doc-09-local-and-cluster-execution).

A local cluster with a bounded memory pool might be started like this:

```bash
export SAIL_MODE=local-cluster
export SAIL_CLUSTER__WORKER_INITIAL_COUNT=4
export SAIL_CLUSTER__WORKER_TASK_SLOTS=8
export SAIL_RUNTIME__MEMORY_POOL__TYPE=fair
export SAIL_RUNTIME__MEMORY_POOL__FAIR__MAX_SIZE=8589934592   # 8 GiB
sail spark server
```

## Why two thread pools

Sail builds on Tokio, Rust's async runtime, and creates two separate runtimes
in every process. The primary runtime handles query execution and RPCs. The io
runtime handles object storage, catalog services, and other work that may wait
on slow networks.

The reason is isolation. If storage requests and query work shared one pool, a
burst of slow S3 calls could occupy every thread and stall query execution even
though the CPU was idle. Keeping them apart means a slow metastore or bucket only
slows down the work that actually depends on it. There's a second, subtler
reason: network clients are often tied to the runtime that created them, so
creating and using them consistently on the io runtime avoids hard-to-debug
failures.

## What happens when the server starts

Starting `sail spark server` runs through a short sequence. The binary first
checks for the embedded-Python variable, optionally installs the faster mimalloc
memory allocator, and hands off to the command-line runner. The runner installs
a TLS crypto provider (needed for HTTPS storage and for TLS between cluster
nodes), parses the arguments, loads configuration, and creates the two runtimes.
It initializes telemetry, binds a TCP listener to the requested address
(127.0.0.1:50051 by default), and serves the Spark Connect service on it. The
server then runs until it receives Ctrl+C. Sail catches that signal itself,
because the embedded Python would otherwise swallow it.

A worker's startup is similar but shorter. It loads the same configuration (the
driver passes the worker's ID and the driver's address through environment
variables), creates a worker session, initializes telemetry, registers with the
driver, and starts accepting tasks.

## Why the coordinators are actors

The driver, each worker, and the session manager have a hard job: their state
changes constantly and from many directions at once. A driver tracks jobs,
stages, tasks, attempts, workers, and data streams, and events about all of
them arrive concurrently.

Sail handles this with the actor model. An actor owns its state privately and
receives messages through a small mailbox. It processes **one message at a
time**, so its state never needs locks, and the full list of things that can
happen to it is simply its list of message types. An actor must never block
while handling a message. When it needs to do something slow, it starts a
background task, which sends the result back as another message.

Timeouts work the same way. An actor never sleeps; it sends itself a message
with a delay. Here's how the session manager notices an idle session, assuming
the default 15-minute timeout:

```text
t=0s     request arrives      → record "active at 0s", schedule Probe(at=0s) for t=900s
t=300s   request arrives      → record "active at 300s", schedule Probe(at=300s) for t=1200s
t=900s   Probe(at=0s) fires   → last activity (300s) is newer than 0s → ignore
t=1200s  Probe(at=300s) fires → no activity since 300s → the session is idle, delete it
```

The driver uses exactly the same pattern for worker heartbeats, task launch
timeouts, and waiting for data streams.

## One way to build every gRPC server

Spark Connect, Flight SQL, the driver, and every worker all serve gRPC, and they
all do it through one shared builder. That keeps behavior consistent: every
server has a standard health-check service, reflection so tools like grpcurl
can discover its API, a tracing layer that records a span for every request,
TCP keep-alives and HTTP/2 pings (every 60 seconds, with a 10-second timeout) to
detect dead connections, and graceful shutdown. A shared retry helper, with
either fixed delays or exponential backoff, is used for calls between the driver
and workers.

---

## Common problems

If Sail refuses to start with an "unknown field" or "duplicate field" error, an
environment variable is misspelled or conflicts with another; compare it with
the YAML schema and remember that double underscores stand for dots. If clients
on other machines can't connect, the server is probably listening only on
127.0.0.1; start it with `--ip 0.0.0.0`, and put an authenticating gateway in
front of it in production. If Kubernetes worker pods start and exit
immediately, check the driver's externally reachable address
(cluster.driver_external_host and cluster.driver_external_port) and the pod
template.

## Where to look in the code

The binary's entry point and the embedded-Python trick are in
crates/sail-cli/src/main.rs and python.rs, the subcommands in runner.rs, and the
Spark server, shell, script runner, and MCP server under src/spark/. The worker
entry point is src/worker/entrypoint.rs. The Python bindings live in
crates/sail-python, with the Python-facing server class in
python/pysail/spark/__init__.py. Configuration is defined in
crates/sail-common/src/config/application.yaml and loaded in application.rs,
and the two runtimes are created in crates/sail-common/src/runtime.rs. The actor
framework, the gRPC server builder, and the retry helper are in crates/sail-server
(actor.rs, builder.rs, retry.rs).


---

<a id="doc-03-spark-connect"></a>

# 03 · Spark Connect: How PySpark Talks to Sail

Spark Connect, introduced in Spark 3.4, split Spark into a thin client and a
server. A PySpark program using Spark Connect doesn't run a JVM at all. It
builds a description of what it wants, sends that description over gRPC, and
receives results back. Sail implements the server side of this protocol, which
is why existing PySpark code runs on Sail unchanged: you only point the client
at a different address.

```python
spark = SparkSession.builder.remote("sc://sail-host:50051").getOrCreate()
```

This doc follows one PySpark action through the Spark Connect front end and
explains the design decisions along the way: why translation happens
immediately, how results are streamed, how dropped connections are survived, and
how errors are made to look exactly like Spark's.

---

## Compatibility at the wire, not in the code

An obvious way to support PySpark would be to reimplement its API. Sail doesn't
do that. It reimplements the *protocol* that PySpark speaks. Everything the
client library does locally, including building DataFrames, converting results
to pandas, and formatting errors, keeps working, because Sail sends exactly the
messages a real Spark server would send.

The protocol is a handful of RPCs. ExecutePlan runs a query or a command and is
by far the most important. AnalyzePlan answers questions about a plan without
running it, such as its schema or its explain string. Config reads and writes
session settings. Others upload artifacts like Python files, cancel running
operations, reconnect to running operations, acknowledge received results, and
end sessions. Sail also accepts gzip- and zstd-compressed messages.

## Finding the session

Every request carries a session ID and a user ID. The first thing Sail does is
ask the session manager (see [06](#doc-06-session-and-runtime)) for that session,
creating it if this is the first request. A Spark Connect session holds the IDs,
the user's Spark settings, the operations currently running, and any streaming
queries. Sail sets the session time zone to the server's local time zone by
default. A session that receives no requests for 15 minutes
(spark.session_timeout_secs) is removed.

Some Spark settings change how a query is interpreted, not just how it runs.
When a plan is resolved, Sail collects these into a small *plan config*: the
session time zone, whether ANSI mode is on, whether a plain TIMESTAMP means "with
local time zone" or "without time zone", the default table format, the warehouse
directory, whether implicit cross joins are allowed, whether to use 64-bit Arrow
string offsets, and the pandas settings that Python UDFs need. Changing any of
these with `spark.conf.set` takes effect on the next query.

## Translating once, at the edge

Let's follow a typical action:

```python
df = (spark.read.parquet("s3://bucket/orders")
        .filter("amount > 100")
        .groupBy("region").count())
df.show()
```

When `show()` runs, PySpark sends ExecutePlan with a tree of protobuf
*relations*: roughly "show the result of aggregating the result of filtering the
result of reading these files", plus a fresh operation ID.

Sail's first move is to translate this tree into its own neutral spec plan (see
[05](#doc-05-spec-plan-and-config)): a read node, a filter node holding the
unresolved expression amount > 100, an aggregate node grouping by an unresolved
column called region, and a show-string node on top. Spark's protocol has about
sixty relation types and many command types, each with its own converter.
Expressions, literals, and data types have converters too.

Why translate immediately? Because it keeps Spark's protocol from leaking into
the rest of Sail. Nothing past this point ever sees a Spark protobuf. The
resolver, the optimizer, and the execution engine work on the same spec plan
whether the request came from PySpark, from SQL text, or from a Flight SQL
client. Protocol quirks stay in one crate, and features don't have to be
implemented twice.

The spec plan then goes to the resolver and planner (see
[07](#doc-07-plan-resolver)), which return a physical plan. That plan is handed to
the session's job runner (see [09](#doc-09-local-and-cluster-execution)), which
starts executing it and returns a stream of Arrow record batches.

## Streaming results back

For a query, Sail starts an *executor*: a small background task that reads the
result stream and turns it into Spark Connect responses. It sends the schema
first, then one message per Arrow batch, then a completion message. If the
result is empty, it still sends one empty batch so the client learns the column
types. Each message gets a unique response ID.

Two features make this streaming robust. The first is heartbeats. A query might
run for minutes before producing its first row, and proxies and load balancers
often cut connections that stay silent too long. So while it waits, the
executor sends an empty batch every 15 seconds
(spark.execution_heartbeat_interval_secs).

The second is reconnection. PySpark marks its operations as *reattachable*,
which means Sail keeps each response in a buffer until the client confirms it
has received it. If the network drops in the middle of a long result, the client
reconnects and says which response it saw last, and Sail replays everything
after that from its buffer, then continues. Here's what a two-batch result looks
like on the wire, slightly simplified:

```text
Client → Server   ExecutePlan(session=S, operation=O, reattachable)
Server → Client   response r1: schema
Server → Client   response r2: Arrow batch (rows 0–8191)
Client → Server   ReleaseExecute(O, up to r2)          ← Sail may now forget r1 and r2
Server → Client   response r3: Arrow batch (rows 8192–9999)
Server → Client   response r4: complete
Client → Server   ReleaseExecute(O, all)
```

If the connection had broken after r2, the client would send ReattachExecute
naming r2, and Sail would replay r3 and r4. The release messages are what keep
the buffer from growing forever.

## Commands run now; queries run later

Not every request is a query. Most commands, such as writes and view creation,
are run to completion right away, and only a completion message is returned.

SQL strings need special care, because `spark.sql(...)` arrives as a single
"SQL command" whether the text is a query or a command, and Spark treats the two
differently. Sail translates the SQL first and checks which it is. For a query
like `SELECT * FROM t`, it doesn't run anything. It returns the query back to the
client as a relation, which runs later, when you call `show()` or `collect()`.
That's Spark's normal laziness. For a command like `CREATE TABLE`, it runs the
command immediately, collects its output into one small batch, and returns that.
That matches Spark too: `spark.sql("CREATE TABLE …")` has already created the
table by the time the call returns.

## Cancelling and streaming queries

Clients can cancel work with the Interrupt RPC, targeting every operation in the
session, every operation with a given tag (set with `spark.addTag`), or a single
operation ID. Sail removes the matching executors from the session, stops them,
and reports which operation IDs were interrupted.

Structured Streaming works through the same front end. Calling
`writeStream.start()` resolves a streaming plan and attaches its result stream
to a streaming query object kept in the session. Later commands check status,
stop the query, wait for it to end, or list running queries by working on those
objects. Client-side streaming listeners aren't supported yet.

## Errors that look like Spark's

When something fails, PySpark expects a particular exception class. Code like
`except AnalysisException:` depends on it, and so do notebooks that format
errors. So Sail classifies every error and sends the Spark class name PySpark
expects. A syntax error becomes ParseException. An unknown table, column, or
function becomes AnalysisException. A failure during execution becomes
QueryExecutionException or SparkRuntimeException. Overflow in ANSI mode becomes
ArithmeticException, a failing Python UDF becomes PythonException, and an
unsupported feature becomes UnsupportedOperationException.

Like Spark itself, Sail sends all of these with the gRPC status INTERNAL plus
error details naming the class, and PySpark reads those details to raise the
matching Python exception:

```python
>>> spark.sql("SELECT no_such_column FROM range(1)").show()
pyspark.errors.exceptions.connect.AnalysisException: ... cannot resolve attribute ...
```

One practical detail: gRPC limits the total size of response headers to 16 KB,
and long Python tracebacks can easily exceed that. Sail cuts error messages off
at about 3.7 KB and marks them as truncated. The full message is always in the
server log.

---

## Common problems

"Not implemented" errors mean the relation, command, or analysis type hasn't
been built yet; the converters list every unsupported case explicitly, so that's
the place to add support. If a DataFrame operation and the equivalent SQL
return different results, the two translation paths produced different spec
plans; turn on debug logging for this crate, which logs every request and
response, and compare them. If long queries fail behind a proxy, lower the
heartbeat interval below the proxy's idle timeout. If session settings seem to
be forgotten, the session was probably idle for longer than 15 minutes and was
removed.

## Where to look in the code

Everything lives in crates/sail-spark-connect. The server starts in
src/entrypoint.rs, and the RPC implementations are in src/server.rs. The
handlers for executing plans, analyzing plans, configuration, and artifacts are
under src/service/. The executor that streams results, buffers them for
reconnection, and sends heartbeats is in src/executor.rs. Translation from Spark
protobuf to the spec plan is under src/proto/, with plan.rs handling relations
and commands. The Spark session extension is in src/session.rs, the session
factory and timeout in src/session_manager.rs, the mapping from Spark settings to
the plan config in src/config.rs, error classification in src/error.rs, and
streaming queries in src/streaming.rs.


---

<a id="doc-04-sql-parser-and-analyzer"></a>

# 04 · SQL Parser and Analyzer: From Text to a Plan

SQL reaches Sail from several directions: `spark.sql(...)` calls from PySpark,
statements from Flight SQL clients, and the definitions of saved views, which
are stored as SQL text and parsed again each time the view is read. Smaller
fragments arrive too, such as a type string like `"id INT, tags ARRAY<STRING>"`
in `createDataFrame`, an expression in `df.selectExpr("price * qty")`, or a
table name in `spark.table("db.t")`.

All of these go through the same three steps. A **lexer** cuts the text into
tokens, a **parser** arranges the tokens into a syntax tree, and an
**analyzer** turns the tree into Sail's spec plan. This doc explains each step,
why Sail wrote its own parser, and how to add new syntax.

```text
"SELECT region, count(*) FROM sales.orders WHERE amount > 100 GROUP BY region"
   │  lexer    → SELECT · region · , · count · ( · * · ) · FROM · sales · . · orders · WHERE · …
   │  parser   → a syntax tree: Query { select list, from, where, group by }
   │  analyzer → spec plan: Aggregate(Filter(Read("sales.orders"), amount > 100), group by region)
   ▼
spec plan (names still unresolved)
```

---

## Why Sail has its own Spark SQL parser

General-purpose SQL parsers don't match Spark's dialect closely enough. Spark
has backtick-quoted identifiers, `LATERAL VIEW`, `DISTRIBUTE BY` and `SORT BY`,
`INSERT OVERWRITE DIRECTORY`, typed literals such as `INTERVAL '1' DAY` and
`X'0A'`, the null-safe equality operator `<=>`, integer division with `DIV`, and
dozens of statement forms. Owning the parser lets Sail follow Spark's grammar
exactly and report errors the way Spark does.

The parser is built on chumsky, a Rust library for writing parsers out of small
composable pieces. It keeps track of where each token came from and what kind of
thing the parser expected at each point, which lets errors point at the exact
spot ("expected an expression at line 3, column 17") instead of just saying
"syntax error".

## The grammar is written as Rust types

The most unusual design decision is that Sail's SQL grammar isn't written as
grammar rules at all. It's written as ordinary Rust structs and enums, and a
macro generates the parser from them.

The idea is simple. A struct means "these parts, in this order". An enum means
"one of these alternatives". An optional field means "this part may be
missing", and a list field means "this part may repeat", optionally separated by
commas. So the definition of a query (an optional WITH clause, a body, then
modifiers like ORDER BY and LIMIT) is literally a struct with those three
fields. The syntax tree *is* the grammar.

This pays off when Spark adds syntax: supporting a new clause usually means
adding a field or an enum variant, and the parser follows automatically. The
same macro also generates a way to print the tree back as SQL, which the tests
use to check that parsing round-trips correctly, and a description of the syntax
that can be used to document the grammar.

Some parts of the grammar can't be generated this way and are written by hand.
Terminals such as keywords, literals, and operators are hand-written. Recursive
structures, like a query containing a subquery or an expression containing
expressions, are declared first and filled in afterwards. Operator precedence
gets special handling too, as described below.

## Step one: cutting text into tokens

Sail's keyword list is a plain text file of about 360 Spark keywords, one per
line, in alphabetical order. At build time, a script reads the file and
generates a keyword type, a fast lookup table, and one small parser per keyword.
Adding a keyword means adding a line to the file.

The lexer walks through the text and produces tokens, remembering the exact
original text and position of each one. A word might be a keyword, a name, or
the start of a number (numbers are interpreted later, so 1e5 and 10L both start
life as words). Strings remember which quote style they used, because in Spark a
backtick-quoted string is an identifier while a single-quoted one is a literal.
Escape sequences aren't processed yet. Whitespace and comments become tokens
too, so positions stay exact, and the parser skips them. Block comments can be
nested.

## Step two: arranging tokens into a tree

The parser tries the grammar's alternatives against the tokens. For statements,
that means trying each statement form until one fits. Sail recognizes about 45:
queries; USE and SET CATALOG; creating, altering, and dropping databases,
tables, and views; the SHOW and DESCRIBE family; INSERT INTO and INSERT
OVERWRITE (including the REPLACE WHERE and DIRECTORY forms); MERGE, UPDATE,
DELETE, and LOAD DATA; caching commands; SET for configuration and time zone;
EXPLAIN; ANALYZE TABLE; and COMMENT ON.

Inside queries, the parser handles everything Spark allows in a FROM clause:
table names, subqueries, VALUES, nested joins, and table functions, joined with
inner, cross, left, right, and full outer joins, semi and anti joins, natural
joins, and USING. Set operations (UNION, INTERSECT, and EXCEPT or MINUS, each
with ALL or DISTINCT) and the modifiers WINDOW, ORDER BY, SORT BY, CLUSTER BY,
DISTRIBUTE BY, LIMIT, and OFFSET are all part of the query structure.

### How operator precedence works

Expressions need more care than the rest of the grammar, because
`a + b * c > 10 AND NOT d OR e` has to group the way Spark groups it. Sail uses
a Pratt parser, which assigns each operator a binding strength. From tightest to
loosest, matching Spark: field access, subscripts, and `::` casts first; then
unary plus, minus, and bitwise not; then multiplication, division, modulo, and
DIV; then addition, subtraction, and string concatenation with `||`; then
bit shifts; then bitwise and, xor, and or; then all the comparisons; then
postfix tests like IS NULL, BETWEEN, IN, and LIKE; then NOT; then AND; and
finally OR.

So `a + b * c > 10 AND NOT d OR e` parses as
`((a + (b * c)) > 10 AND (NOT d)) OR e`, and `a = 1 OR b = 2 AND c = 3` means
`a = 1 OR (b = 2 AND c = 3)`. When in doubt, add parentheses; the parser keeps
them.

### What a syntax error looks like

A malformed statement fails here, before anything else runs. Because tokens
carry their positions and the parser knows what it expected, the error can point
at the problem:

```python
>>> spark.sql("SELECT * FORM t")
pyspark.errors.exceptions.connect.ParseException: ... (points at the unexpected token "FORM")
```

## Step three: turning the tree into a spec plan

The analyzer walks the syntax tree and builds spec plan nodes (see
[05](#doc-05-spec-plan-and-config)). At the top level it decides whether a
statement is a query or a command. A SELECT becomes a query plan. A CREATE TABLE
becomes a create-table command carrying the column list, format, partitioning,
location, and options. USE db becomes a set-current-database command, and so on.

Within a query, the analyzer maps clauses to relational steps: FROM becomes a
read of a named table, WHERE a filter, the SELECT list a projection, GROUP BY an
aggregation, ORDER BY a sort, LIMIT a limit. WITH clauses become named
subqueries, and LATERAL VIEW explode(...) becomes a generator step. Expressions
become spec expressions: names turn into unresolved column references, function
calls into unresolved function calls, and `*` into an unresolved star. CASE,
CAST, EXTRACT, SUBSTRING, lambdas, subqueries, and EXISTS each have their own
spec form.

Literals are interpreted fully at this stage, because their meaning doesn't
depend on the session. That covers numbers with Spark's suffixes (10L for a
long, 1.5BD for a decimal, 2D for a double), strings with escapes, binary
literals like X'0A', dates, timestamps with and without time zones, and
intervals in both styles: INTERVAL '1-2' YEAR TO MONTH and INTERVAL 3 DAYS 4
HOURS. Spark type names like DECIMAL(10,2), VARCHAR(10), and
ARRAY<STRUCT<...>> are turned into spec types.

### Nothing is looked up yet

The analyzer never consults a catalog, the function registry, or a schema. A
query that mentions a table that doesn't exist analyzes perfectly well:

```python
df = spark.sql("SELECT x FROM table_that_does_not_exist")   # no error yet
df.show()                                                   # now: TABLE_OR_VIEW_NOT_FOUND
```

The first line succeeds because the shape is valid SQL. The table is only looked
up when the plan is resolved (see [07](#doc-07-plan-resolver)). Keeping lookups out
of parsing and analysis makes both steps simple, deterministic, and easy to
test, and it puts every "doesn't exist" error in one place.

### Parameters

Parameterized queries, such as `spark.sql("SELECT * FROM t WHERE id = ?",
args=[42])` or the named form with `:id`, are supported. The placeholders become
placeholder expressions in the spec plan, the Spark Connect layer attaches the
argument values, and the resolver fills them in.

---

## Following one statement end to end

Take `spark.sql("CREATE TABLE t (id INT) USING parquet")`. Spark Connect receives
a SQL command and calls the parser's single-statement entry point. The lexer
produces the tokens CREATE, TABLE, t, (, id, INT, ), USING, and parquet. The
parser tries the statement forms and matches create-table, with the name t, a
column list holding id of type INT, and a USING clause. The analyzer turns this
into a create-table command with one Int32 column, format parquet, and no
location, which means a managed table. Because it's a command, Spark Connect runs
it immediately: the resolver wraps it in a catalog command, the catalog creates
the table, and an empty result goes back to the client.

## Adding new syntax

To support a new clause or statement, add any new keywords to the keyword file
in alphabetical order, then add a field or variant to the syntax tree types in
the parser crate. A new statement is usually a new variant whose fields are its
keywords and parts in order. If the new part contains queries or expressions,
use the macro's dependency attributes so it receives the already-declared
recursive parsers. Then teach the analyzer to turn the new tree node into spec
nodes, adding a spec node and resolver support if nothing existing fits. Finish
with a round-trip parser test and an end-to-end test; the gold data tests
compare results with real Spark (see [17](#doc-17-testing-and-development)).

Three pitfalls are worth knowing. Generated parsers can't handle left recursion,
so such rules must be written by hand, as the expression operators are.
Alternatives are tried in order, so longer and more specific forms (LEFT SEMI)
must come before shorter ones (LEFT). And a new keyword can break existing
queries that use that word as a column name, so check whether Spark treats it as
reserved, and test it as an identifier.

## Where to look in the code

The keyword list is crates/sail-sql-parser/data/keywords.txt, turned into code
by that crate's build.rs. The lexer and tokens are in src/lexer.rs, token.rs, and
string.rs, and the syntax tree, which doubles as the grammar, is under src/ast/
(statement.rs, query.rs, expression.rs, data_type.rs, literal.rs,
identifier.rs). Parser options are in src/options.rs. The derive macros that
generate parsers live in crates/sail-sql-macro. The analyzer is
crates/sail-sql-analyzer: parser.rs holds the entry points such as
parse_one_statement and parse_expression, and statement.rs, query.rs,
expression.rs, data_type.rs, and literal/ turn the tree into spec plans. Callers
include the Spark Connect translator, the Flight SQL service, and view expansion
in the resolver.


---

<a id="doc-05-spec-plan-and-config"></a>

# 05 · The Spec Plan: Sail's Neutral Description of a Query

Every request that reaches Sail, whether it's a PySpark DataFrame, a SQL string,
or a Flight SQL statement, is first turned into the same thing: a **spec plan**.
The spec plan is the common language between Sail's front ends and its planner.
This doc explains what it looks like, why it exists, and a few of its less
obvious features. It also covers the rest of sail-common, the small crate of
foundations shared by everything else.

```text
PySpark DataFrame ──(Spark Connect translator)──┐
spark.sql("…")    ──(SQL parser + analyzer)─────┼──► spec plan ──► resolver ──► DataFusion
Flight SQL query  ──(SQL parser + analyzer)─────┘
```

---

## What a spec plan is

A spec plan is a tree of plain Rust data: enums and structs, with nothing
clever inside. It can be printed for debugging and serialized. What makes it
special is what it *doesn't* contain: it records what the user asked for,
without having checked any of it.

Table names are just names, like ["sales", "orders"]. Columns are *unresolved
attributes*: a name, plus optionally the ID of the DataFrame the reference came
from. Functions are *unresolved functions*: a name, its arguments, and flags such
as DISTINCT or IGNORE NULLS. Even the comparison `>` is, at this stage, just a
call to a function named ">". Turning these names into real tables, columns, and
functions is the resolver's job (see [07](#doc-07-plan-resolver)).

At the top level, a spec plan is either a **query**, which produces rows, or a
**command**, which does something, such as creating a table or writing data,
and may return a small result.

## Why have a separate representation at all?

It would be possible to go straight from Spark protobuf or SQL syntax trees to
DataFusion plans. Sail adds this middle step for several reasons, and the most
important is avoiding duplicate work. Resolution is the hardest part of
planning: catalogs, schemas, functions, Spark's naming rules. With a shared spec
plan, that logic is written once. Supporting a new protocol means writing one
translator, not a second resolver.

The second reason is that translators stay simple. Because the spec plan doesn't
require anything to be looked up, the SQL analyzer and the Spark Connect
translator need no session, no catalogs, and no function registry. They're pure
functions from input to output, which makes them easy to test.

Third, it normalizes. A DataFrame call like `df.filter(col("x") > 5)` and the SQL
`WHERE x > 5` become the same filter node, so the resolver handles one form, not
two.

Fourth, it has room for things DataFusion doesn't. pandas UDFs, `mapInPandas`,
streaming watermarks, `df.na.fill`, `df.stat.crosstab` and other Spark-only
features all have spec nodes, even though DataFusion has no equivalent.

Finally, because the spec plan is plain data, you can print it. When a query
returns a wrong result, printing the spec plan tells you quickly whether the
problem is in translation or later.

## What's inside

There are about 70 kinds of query steps. They cover reading (named tables, files
with a format and options, table functions, inline data, ranges), the usual
relational operations (projection, filter, join, aggregate, sort, limit,
distinct, union and friends, aliases), column operations (adding, renaming, and
dropping columns), reshaping (pivot, unpivot, and generators like explode),
repartitioning, sampling, Python operations (map in pandas, grouped and
co-grouped pandas functions, Python table functions), streaming watermarks, the
DataFrame helpers for missing values and statistics, and SQL structure such as
WITH clauses and bound parameters.

There are about 60 kinds of commands. They manage catalogs, databases, tables,
views, and functions; write data (`df.write`, `df.writeTo`, streaming writes,
INSERT, LOAD DATA); change rows (MERGE, UPDATE, DELETE); manage caching; and
display things (`df.show()`, notebook HTML, EXPLAIN, DESCRIBE).

Expressions include literals, column references, function calls, stars (with
Spark 4's options such as `SELECT * EXCEPT (a)`), aliases, casts, sort orders,
window specifications, lambdas for higher-order functions, struct field updates,
subqueries, grouping sets, inline Python UDF definitions, and parameter
placeholders.

Data types closely follow Arrow's: integers of every width, floats, decimals,
dates, times, timestamps, intervals, binary and string types, lists, structs,
maps, and so on. Literals carry their exact type, down to timestamp precision.

## Why DataFrame IDs matter: the self-join

Each DataFrame in PySpark carries a plan ID, and every column reference made
through a DataFrame remembers it. This sounds like a detail until you join a
table to itself:

```python
people = spark.table("people")
managers = people.alias("m")
pairs = people.join(managers, people["manager_id"] == managers["id"])
pairs.select(people["name"], managers["name"]).show()
```

Both sides of the join have columns called name and id. In the spec plan,
`people["name"]` is "the column called name, from the DataFrame people", and
`managers["name"]` is "the column called name, from the DataFrame managers". The
resolver uses these IDs to pick the right column on each side. A plain SQL
reference to name, with no ID, would be ambiguous here and fail with "ambiguous
attribute", which is exactly what Spark does.

## Types that wait for the session

A few spec types are deliberately left undecided, because their final form
depends on session settings that the translators don't know.

Spark's STRING type is a good example. In Arrow it could be a regular string
array, with 32-bit offsets, or a large string array, with 64-bit offsets that
allow more than 2 GB of text per batch. Which one Sail uses depends on the
session setting spark.sql.execution.arrow.useLargeVarTypes. So the spec plan
records a "configured string" type, and the resolver picks the concrete Arrow
type once it can see the session. Spark's plain TIMESTAMP works the same way: it
means "with local time zone" or "without time zone" depending on
spark.sql.timestampType.

This is a small but clear example of why resolution is a separate step. The
same SQL text can legitimately produce different Arrow types in two different
sessions, so the decision can't be made while parsing.

A few other types exist that Arrow doesn't have: user-defined types that carry
their Java or Python class names, Spark 4's VARIANT for semi-structured data, and
geometry and geography types for spatial data.

## One query, two front ends

The DataFrame code

```python
orders = spark.table("sales.orders")
orders.filter(orders["amount"] > 100).select("region", "amount").limit(5)
```

and the SQL

```sql
SELECT region, amount FROM sales.orders WHERE amount > 100 LIMIT 5
```

produce essentially the same spec plan:

```text
Query: Limit(5)
  Project [ column region, column amount ]
    Filter function ">"(column amount, literal Int32 100)
      Read named table ["sales", "orders"]
```

The only difference is that, in the DataFrame version, the amount reference in
the filter also carries the plan ID of orders. After this point, Sail can't tell
which front end the query came from.

## The rest of sail-common

Besides the spec plan, sail-common holds the foundations everything else relies
on. The configuration module defines Sail's settings in a YAML file compiled
into the binary and loads environment variable overrides (see
[02](#doc-02-entry-points-and-server)). The runtime module creates the two thread
pools, one for query work and one for I/O. The error module defines the basic
error kinds (missing argument, invalid argument, not supported, internal error)
that each crate wraps in its own error type. A small object module makes trait
objects comparable and hashable, which DataFusion needs for Sail's custom plan
nodes and functions. There are also helpers for dates, times, and strings.

## Extending the spec

Add a spec node when a feature can't be expressed with the existing ones. Give
it fields that capture what the user *meant*, using spec expressions and names
rather than DataFusion types. Produce it from every front end that supports the
feature: the Spark Connect translator and, if there's SQL syntax for it, the SQL
analyzer. Then teach the resolver to handle it. Keep field names stable, because
spec plans appear in tests and logs.

## Where to look in the code

The spec plan lives in crates/sail-common/src/spec/: plan.rs defines plans,
query steps, and commands, expression.rs defines expressions and names,
data_type.rs the data types, and literal.rs the literals. Configuration is under
src/config/, the runtimes in src/runtime.rs, and the error types in src/error.rs.
The session settings that affect resolution, the plan config, are defined in
crates/sail-plan/src/config.rs.


---

<a id="doc-06-session-and-runtime"></a>

# 06 · Sessions and the Runtime Environment

A session is everything Sail keeps on behalf of one client: settings, catalogs,
temporary views, the machinery that runs its queries, and the resources those
queries may use. This doc explains what goes into a session, why it's assembled
the way it is, how sessions are created and cleaned up, and how the runtime
environment controls memory, spilling, caches, and storage access.

---

## A session is a DataFusion context with Sail's services attached

Sail builds on Apache DataFusion, whose central object is the SessionContext. It
holds configuration, a runtime environment, optimizer rules, a query planner,
and registered functions. Sail creates one SessionContext per client session and
customizes it heavily.

The key to that customization is a DataFusion feature called *extensions*. A
session's configuration can carry arbitrary typed objects, and any code holding
the session can fetch one by type. Sail attaches its services this way: a
catalog manager that knows about databases and tables, a registry of table
formats (Parquet, CSV, Delta, Iceberg, and so on), a job service that runs
queries, an activity tracker used for idle timeouts, a service that answers
queries on the system tables, and a cache for Delta table state. The Spark
Connect front end adds one more: a Spark session extension holding the client's
IDs and Spark settings.

Why extensions rather than wrapping DataFusion's types in Sail's own? Because
wrapping would mean forking or re-exporting large parts of DataFusion's API, and
every new service would change function signatures throughout the codebase.
With extensions, a function that needs the catalog manager simply asks the
session for it.

## Sail turns DataFusion's defaults off

A session built by Sail deliberately leaves out DataFusion's built-in catalog,
its information schema, its built-in table formats, and its built-in functions.
Sail provides all of these itself, because Spark's behavior differs. For
example, Spark's concat returns NULL when any argument is NULL, while
DataFusion's ignores NULL arguments. Leaving DataFusion's version registered
would make it too easy for the wrong one to be used.

There's one exception that shows how tightly the pieces fit. One of
DataFusion's optimizer rules quietly assumes that a function called first_value
is registered. Sail registers that one function explicitly so the rule keeps
working.

The session also receives Sail's optimizer rules (with lakehouse rules placed
ahead of DataFusion's own; see [08](#doc-08-logical-physical-plans-and-optimizers)),
Sail's query planner, and execution settings copied from configuration: the
batch size (8,192 rows by default), the number of partitions to target (by
default one per CPU core), statistics collection, and about thirty Parquet read
and write settings.

## Server sessions and worker sessions

Sail builds two kinds of session, and the difference explains a whole class of
bugs.

A **server session** serves a client. It owns user-visible state such as
temporary views and settings, can submit jobs, and resolves plans. It has
everything described above.

A **worker session** exists on cluster workers and only runs pieces of plans it
receives from the driver. It needs no catalogs, no user state, and no job
runner. It does need a runtime environment for memory and storage, and it keeps
DataFusion's default functions, because when a worker decodes a plan fragment,
every function the fragment mentions must be found by name.

So if a query works in local mode but fails only on a cluster, a good first
question is whether something was set up in the server session but not in the
worker session.

## Each session gets its own job runner

When a server session is built, it creates a new job runner according to the
mode setting: an in-process runner for local mode, or a cluster runner with its
own driver for local-cluster and Kubernetes modes (see
[09](#doc-09-local-and-cluster-execution)). In cluster mode, this means **every
session gets its own driver and its own workers**. One user's heavy job can't
take over another session's workers, and shutting down a session cleanly shuts
down everything it started.

## The runtime environment: memory, spilling, caches, storage

Each session also has a DataFusion *runtime environment*, which controls the
physical resources its queries use.

**Memory.** Operators such as sorts, joins, and aggregations ask a memory pool
before using large amounts of memory. Sail offers three pools, chosen with
runtime.memory_pool.type. The **unbounded** pool, which is the default, never
says no. The **greedy** pool hands out memory first come, first served, up to a
maximum. The **fair** pool shares a maximum among operators that are able to
spill, so no single operator can starve the others.

The difference matters a lot in practice. Suppose you use the fair pool with an
8 GiB limit, and a query runs two sorts and a hash aggregation at the same time.
While only the aggregation is running, it can use the full 8 GiB. Once all three
are running, each spillable operator's share is about a third. When a sort
exceeds its share, it writes sorted runs to disk and carries on with less memory.
With the default unbounded pool, none of this happens: operators keep allocating
until the process hits the machine's or container's limit and is killed. For
anything beyond development, choose a bounded pool.

**Spilling.** Spill files go to the directories listed in
runtime.temporary_files.paths, or to the operating system's temporary directory
if none are listed. The total is capped by runtime.temporary_files.max_size,
64 GiB by default, and setting that cap to zero disables spilling entirely. In
that case, an operator that runs out of memory fails with a "resources
exhausted" error instead of spilling.

**Storage.** Each runtime environment gets a dynamic object store registry,
which creates S3, GCS, Azure, HTTP, or local file clients on demand the first
time a path uses them (see [14](#doc-14-object-storage)).

**Caches.** Listing a large directory or reading Parquet footers can dominate
query time on big tables, so Sail can cache three things: directory listings,
per-file statistics, and Parquet metadata. Each cache can be turned off, shared
by every session in the process (global), or kept separately for each session.
Global caching makes repeated queries over stable tables much faster, at the
cost that files changed by other systems are only noticed when cached entries
expire. Per-session caching isolates users from each other. No caching means
every query sees the latest files and pays to list them again. By default, the
statistics and metadata caches are global and the listing cache is off. A
dashboard over stable tables benefits from turning the listing cache on with a
short expiry. An ingestion pipeline that writes every minute should leave it off.

Here's a configuration that bounds memory and sends spills to a fast disk:

```bash
export SAIL_RUNTIME__MEMORY_POOL__TYPE=fair
export SAIL_RUNTIME__MEMORY_POOL__FAIR__MAX_SIZE=17179869184      # 16 GiB
export SAIL_RUNTIME__TEMPORARY_FILES__PATHS='["/mnt/nvme/sail-spill"]'
```

## How sessions come and go

Requests for the same session can arrive at the same time, for example from
several threads of one PySpark program. Creating, checking, and deleting
sessions is therefore handled by a session manager **actor** (see
[02](#doc-02-entry-points-and-server)), which processes these events one at a time
so they can't race.

When a request arrives, the manager either returns the existing session or
asks the factory to build a new one. Either way, it records the time of this
activity and schedules a reminder to check the session again after the timeout:
15 minutes for Spark Connect, one hour for Flight SQL. When a reminder arrives,
the manager compares the session's last activity with the time the reminder was
set. If there's been activity since, a newer reminder already exists, and this
one is ignored. If not, the session has been idle for the whole timeout and is
deleted.

Deleting a session stops its job runner. In cluster mode, that shuts down the
session's driver and its workers. The runner hands back a history of the jobs,
stages, tasks, and workers it ran, and the manager keeps that history so the
system tables can still report on finished sessions.

## System tables: Sail describing itself

Sail exposes its own runtime state as read-only tables in a catalog called
system. You can list sessions and their settings (system.session.sessions and
system.session.options), jobs, stages, and tasks (system.execution.jobs, stages,
and tasks), and cluster workers (system.cluster.workers). They answer questions
like "what's running right now?" and "which task attempts failed?" with ordinary
SQL:

```python
spark.sql("SELECT * FROM system.execution.jobs").show()
spark.sql("SELECT stage, status, count(*) FROM system.execution.tasks GROUP BY 1, 2").show()
```

The table definitions are generated at build time from a YAML file. A query on
them is answered by asking the session manager, which asks each session's job
runner, or its saved history for sessions that have ended.

## Following one new client

When a PySpark client connects for the first time, its first request carries a
new session ID. The front end asks the session manager for that session, and the
manager asks the server session factory to build it. The factory creates the
table format registry (including any Python data sources), the catalog manager
from the configured catalog list, the activity tracker, a new job runner for the
configured mode, the system table service, and the Delta cache. It builds the
runtime environment with the memory pool, spill directories, caches, and storage
registry. It assembles the session state without DataFusion's defaults, adds
Sail's rules and planner, and registers first_value. The Spark Connect front end
then adds its own extension and sets the default time zone. The manager stores
the session, records the activity, and schedules the first idle check. Every
later request reuses the same context and pushes the idle deadline forward.

---

## Common problems

A process killed for running out of memory during big joins or sorts almost
always means the memory pool was left unbounded; switch to the fair pool with a
limit below the container's memory. "Resources exhausted" errors mean the pool
is too small or spilling is disabled. If newly written files don't show up, a
global cache with a long expiry is serving old listings or statistics; shorten
the expiry, use session scope, or run REFRESH TABLE. A function that works
locally but fails on workers is usually missing from the worker session. And if
temporary views disappear, the session was idle longer than its timeout.

## Where to look in the code

The two session factories are in crates/sail-session/src/session_factory/
(server.rs and worker.rs). The runtime environment, with the memory pool, spill
settings, and caches, is built in src/runtime.rs, and the cache implementations
live in crates/sail-cache. The session manager actor is under
src/session_manager/. Table format registration is in src/formats.rs, catalog
setup in src/catalog.rs, the optimizer rule lists in src/optimizer.rs, and the
extension planner in src/planner.rs. The extension mechanism, the job runner
interface, and the activity tracker are defined in crates/sail-common-datafusion
(extension.rs and session/). The system table definitions are in
crates/sail-common-datafusion/data/system/databases.yaml, with the system catalog
in crates/sail-catalog-system.


---

<a id="doc-07-plan-resolver"></a>

# 07 · The Plan Resolver: Turning Intent into a Plan

When a query reaches the resolver, Sail knows *what the user asked for* but not
yet *what any of it means*. The query still talks about "a table called
sales.orders", "a column called amount", and "a function called count". None of
these names has been checked. The resolver is the component that gives every
name its meaning, applies Spark's rules for how names behave, and produces a
plan that DataFusion can optimize and run. If Sail behaves like Spark, it's
largely because the resolver makes it so.

This doc follows one query from start to finish:

```sql
SELECT region, count(*) AS n
FROM sales.orders
WHERE amount > 100
GROUP BY region
ORDER BY n DESC
```

By the time it reaches the resolver, the SQL text has already been parsed and
turned into Sail's neutral *spec plan* (see [04](#doc-04-sql-parser-and-analyzer)
and [05](#doc-05-spec-plan-and-config)). The resolver receives that spec plan,
plus a handful of session settings such as the time zone and whether ANSI mode
is on.

---

## Why a separate resolver exists at all

DataFusion, the engine Sail is built on, already knows how to plan and run SQL.
So why not hand it the query directly?

Because Spark and DataFusion disagree in many small but visible ways. Spark
matches column names without caring about upper or lower case. It lets you
select the same column twice. It lets you join a table to itself, and both sides
keep their column names. It lets GROUP BY refer to an alias defined in the
SELECT list, and lets ORDER BY use a column you never selected. It has
hundreds of functions whose behavior differs slightly from DataFusion's
versions: how they treat NULLs, which date format letters they use, what happens
on overflow.

If Sail left these differences to DataFusion, every one of them would show up as
a wrong result or a confusing error. Instead, the resolver handles all of
Spark's rules first, and gives DataFusion a plan that is already unambiguous and
already uses Spark-compatible functions. DataFusion then does what it's best at:
optimizing and executing.

The resolver also owns the rest of the planning pipeline, so that one function
call takes a spec plan all the way to something runnable:

```text
spec plan  (names unresolved)
  │ 1. Resolve        → DataFusion logical plan, columns renamed to private IDs
  │ 2. Analyze        → DataFusion's analyzer rules (for example, type coercion)
  │ 3. Optimize       → Sail and DataFusion logical optimizer rules
  │ 4. Stream rewrite → only if the query reads from a streaming source
  │ 5. Physical plan  → Sail's extension planners, then physical optimizer rules
  │ 6. Rename         → private column IDs swapped back to the user's names
  ▼
physical plan, ready for a job runner
```

Steps 2, 3, and 5 mostly reuse DataFusion's machinery with Sail's own rules
added (see [08](#doc-08-logical-physical-plans-and-optimizers)). The interesting,
Spark-specific work happens in step 1, which is what the rest of this doc is
about.

---

## Why every column gets a private name

Spark lets you do things that would confuse a simpler engine. You can select the
same column twice, join a table to itself, and sort by a column you didn't
select. DataFusion, on the other hand, insists that the columns at each step of a
plan have distinct names.

The resolver bridges this gap by giving every column it produces a private name,
like #7, and keeping a notebook that says "#7 is what the user calls amount".
Two columns both called id become #3 and #9, so they never collide. Only when
the plan is finished are the private names swapped back for the names the user
expects to see.

The notebook records a little more than the user-facing name. For each private
name it also remembers:

- **Which DataFrames it came from.** PySpark tags every DataFrame with an ID,
  and a column reference like `orders["amount"]` carries that tag. In a
  self-join, both sides have an *amount*, but only one of them belongs to the
  `orders` DataFrame. The tag tells the resolver which one you meant.
- **Whether it's hidden.** Sometimes the resolver needs a column that the user
  shouldn't see in the result. More on this below.

When the resolver has finished, it hands over the plan together with the list of
names the user should see, in order. Sail calls this pair a *named plan*. Step 6
of the pipeline uses that list to rename the output columns.

As a safety net, after resolving each step of the query, the resolver checks that
every column in the output is in its notebook. If someone adds a feature and
forgets to register a column, the query fails right away with a clear error
instead of producing a plan with mysterious names.

---

## Following the query: reading the table

The resolver works from the bottom of the query upward, so the first thing it
meets is *FROM sales.orders*.

A table name could refer to several different things, and the resolver checks
them in a fixed order. First, it checks whether the name is really a file path in
disguise. Spark lets you write `` parquet.`s3://bucket/path` `` to query files
directly, so if the first part of a two-part name is a known file format, the
resolver treats the rest as a path. Next, it checks whether a WITH clause in
the same query defined that name. Only if neither applies does it ask the
catalog manager (see [11](#doc-11-catalogs)).

The catalog manager works out which catalog and database *sales.orders* belongs
to, then looks it up. It checks temporary views first, then real tables, then
saved views. The answer comes back in one of three shapes:

- **A table.** The catalog reports its columns, file format, location,
  partitioning, and properties. The resolver passes these to the matching table
  format, Parquet in our case (see [12](#doc-12-data-sources-and-formats)). The
  format lists the files and returns a table source that can be scanned.
- **A saved view.** Views are stored as SQL text. The resolver parses that text
  again and resolves it like any other query. Doing this every time means the
  view always sees the current definitions of the tables it uses.
- **A temporary view.** These are stored as already-resolved plans, so the
  resolver reuses the plan and just hands out fresh private names for its
  columns.

In our example, *sales.orders* is a Parquet table with columns order_id,
region, and amount. The resolver registers them in its notebook:

```text
#0 → order_id      #1 → region      #2 → amount
```

Time travel (VERSION AS OF, TIMESTAMP AS OF) and table sampling
(TABLESAMPLE) are applied at this point too, for the formats that support
them. Time travel isn't allowed on CTEs or views, because they have no version
history.

---

## Following the query: resolving columns in the filter

Next comes *WHERE amount > 100*. The resolver has to decide which column
*amount* means.

It begins by listing the possible readings of the name. A single word like
*amount* can only be a column, but a dotted name like *t.amount* might mean
"column amount of table t" or "field amount inside a struct column called t".
For each reading, the resolver searches the columns that are visible at this
point in the plan, comparing names without regard to case and checking the
DataFrame tag if there is one.

If nothing matches, it keeps looking in a few more places. Inside a GROUP BY
query, it also looks at the outputs of the aggregation. It looks at hidden
columns. And inside a subquery, it looks outward at the enclosing query's
columns, which is how correlated subqueries like `WHERE x > (SELECT avg(x) FROM
t2 WHERE t2.k = t1.k)` find *t1.k*.

If the search finds exactly one match, that's the answer. If it finds none, the
user gets *cannot resolve attribute*. If it finds more than one, for example
because two joined tables both have an *amount*, the user gets *ambiguous
attribute* and needs to qualify the name.

Here, *amount* matches #2. The comparison *> 100* compares a double column with
an integer, so DataFusion's type coercion later turns the literal into 100.0.

---

## Following the query: finding functions

The query uses one function, *count(\*)*. Function names are resolved by looking
in three places, in this order:

1. **Functions registered in the session.** These are mostly Python UDFs that the
   user registered with `spark.udf.register`. Because they're checked first, a
   user function named *upper* would replace the built-in *upper*, just as in
   Spark.
2. **Built-in scalar and generator functions**, such as *upper*, *date_format*,
   or *explode*.
3. **Built-in aggregate functions**, such as *count*, *sum*, or *percentile_cont*.

If a name isn't found anywhere, the user sees *unknown function*.

The built-ins aren't registered in DataFusion's own function registry. Sail keeps
its own table that maps each Spark function name to a small piece of code called
a *builder*. A builder receives the already-resolved arguments plus the session
settings, and returns a DataFusion expression. Many builders are trivial: *sqrt*
maps straight to DataFusion's square root, with a cast to double. Others do real
work. *concat* uses Sail's own implementation, because Spark returns NULL when
any argument is NULL. Date functions translate Spark's Java-style format letters.
Arithmetic functions check whether ANSI mode is on, which decides whether an
overflow becomes NULL or an error. This design is why Sail can match Spark
function by function without changing DataFusion itself.

*count(\*)* gets one small special case: the star is rewritten to count a
constant, because there's no column to count. The resolver also handles the
other aggregate extras here: DISTINCT, FILTER (WHERE …), IGNORE NULLS, and
ordered-set syntax like `percentile_cont(0.5) WITHIN GROUP (ORDER BY x)`.
Functions that take lambdas, such as `transform(arr, x -> x + 1)`, are resolved
here too.

---

## Following the query: grouping and sorting

*GROUP BY region* and *ORDER BY n DESC* look simple, but they show off two Spark
rules that need care.

For the aggregation, the resolver works in three passes. First it resolves the
grouping expressions and the SELECT list, pulling out the aggregate calls
(*count(\*)*) so they're computed once. Second, if there were a HAVING clause,
it would resolve it with access to both, so *HAVING count(\*) > 10* reuses the
same count instead of computing a new one. Third, it builds DataFusion's form of
the query: an aggregate step, an optional filter for HAVING, and a projection
on top. ROLLUP, CUBE, and GROUPING SETS are handled in the same place.

The new outputs get new private names:

```text
#3 → region   (the grouping key, as output by the aggregate)
#4 → n        (the count)
```

*ORDER BY n DESC* refers to an alias, which the resolver finds among the
aggregate's outputs.

Now suppose the query had been `SELECT region FROM … ORDER BY amount` instead.
*amount* isn't in the SELECT list, yet Spark allows sorting by it. The resolver
handles this with a **hidden column**. It keeps *amount* flowing through the
projection, marks it hidden in its notebook, lets the sort use it, and then adds
a final step that drops every hidden column. The same trick carries join keys
through outer joins and tracks positions for exploded arrays. The user never sees
these columns. They exist only so the plan can do what Spark promises.

After resolution, the logical plan for our example looks roughly like this:

```text
Sort: #4 DESC
  Projection: #3, #4
    Aggregate: groupBy=[#1 AS #3], aggr=[count(1) AS #4]
      Filter: #2 > 100.0
        TableScan: sales.orders  projection=[#1, #2]
```

along with the user-facing names *region* and *n*.

---

## Commands go through the same pipeline

Not every request is a query. CREATE TABLE, INSERT, SHOW TABLES,
`df.write`, DELETE, and `df.show()` all reach the resolver too. Sail's choice is
to turn each of them into a logical plan as well, so they flow through the same
pipeline and the same job runner as queries. One path to maintain is simpler than
two.

The trick is a small set of special plan nodes. Catalog operations (create,
drop, alter, list, describe, USE) become a *catalog command* node, which at run
time asks the catalog manager to do the work and returns a small result table.
Writes become a *file write* node that hands the data to the table format's
writer, wrapped in a *barrier* so that any preparation, such as creating the
target table, finishes before the write starts. DELETE, UPDATE, and MERGE
become nodes that the lakehouse formats know how to plan (see
[13](#doc-13-lakehouse-delta-and-iceberg)). `df.show()` becomes a node that formats
rows as Spark's familiar text table. EXPLAIN runs the pipeline in explain mode
and returns the plans as text.

One rule matters a lot here: **resolving a command never changes anything**.
The resolver only describes the work, and the work happens when the plan runs.
That's why `EXPLAIN CREATE TABLE … AS SELECT …` is safe: it shows the plan
without creating a table (see [08](#doc-08-logical-physical-plans-and-optimizers)
for how barriers enforce this).

---

## Putting it together

Back to our query. The resolver found *sales.orders* through the catalog and got
a Parquet source back. It gave the columns private names, matched *amount* in
the filter, found *count* in the aggregate table, built the aggregation, and
resolved the sort against the alias *n*. It then handed the plan to DataFusion's
analyzer and optimizer. They pushed the filter into the Parquet scan, so row
groups whose statistics rule out *amount > 100* are skipped, and split the
aggregation into a partial step and a final step around a repartition by region.
The extension planners produced the physical plan. Finally, the output columns
were renamed from #3 and #4 back to *region* and *n*. The finished physical plan
goes to the job runner (see [09](#doc-09-local-and-cluster-execution)), and
`EXPLAIN EXTENDED` would show you the initial logical plan, the optimized logical
plan, and the physical plan that the pipeline recorded along the way.

When something goes wrong, the error message usually tells you which part of
this story failed. *Cannot resolve attribute* means the column search came up
empty: check the spelling, the qualifier, and whether the column was projected
away earlier. *Ambiguous attribute* means two inputs share a name: qualify it,
or use the DataFrame form `df["col"]`. *Unknown function* means the name isn't a
built-in or a registered UDF in this session. And if a function returns a
different answer than Spark, the place to look is that function's builder, which
is where NULL handling, casts, and ANSI behavior are decided.

---

## Adding to the resolver

To support a new Spark function, first check whether DataFusion already has one
that behaves the same way. If so, adding a line to Sail's function table is
enough. If not, write the implementation in the functions crate (see
[15](#doc-15-functions-and-python-udfs)), then add a builder that calls it. Either
way, add a test that compares the result with Spark's.

To support a new kind of query step, add a resolver function for the new spec
node. Make sure every column it produces is registered in the notebook (the
safety net will remind you if you forget). If DataFusion has no plan node that
fits, create a Sail extension node (see
[08](#doc-08-logical-physical-plans-and-optimizers)).

---

## Where to look in the code

Everything described here lives in the `sail-plan` crate. The pipeline itself
is the function `resolve_and_execute_plan` in `src/lib.rs`. The resolver is
under `src/resolver/`: `state.rs` holds the notebook of private names and hidden
columns, `query/` has one file per kind of query step (`read.rs` for table
lookup, `aggregate.rs`, `join.rs`, and so on), `expression/` handles columns and
functions (`attribute.rs` for the column search, `function.rs` for the
function lookup order), and `command/` handles writes, merges, and catalog
commands. The function tables and their builders are in `src/function/`.
EXPLAIN is in `src/explain.rs`, the `show()` formatting in `src/formatter.rs`,
and the session settings that affect resolution in `src/config.rs`.


---

<a id="doc-08-logical-physical-plans-and-optimizers"></a>

# 08 · Plans, Extension Nodes, and Optimizers

DataFusion comes with a rich set of operators: scans, filters, projections,
joins, aggregations, sorts, windows, unions, and repartitioning. Spark needs a
few more. This doc explains which operators Sail adds and why, how they plug
into DataFusion without forking it, which optimizer rules run, and how Sail's
cost-based join reordering works.

---

## Two kinds of plans

Planning in DataFusion happens in two layers. A **logical plan** says what to
compute: "filter these rows, group them by region, count them". A **physical
plan** says how: "scan these Parquet files in eight parallel partitions,
pre-aggregate each one, repartition by a hash of region, then finish the
aggregation". The resolver (see [07](#doc-07-plan-resolver)) produces the logical
plan. Optimizer rules improve it, a planner turns it into a physical plan, and
more rules improve that.

Sail adds its own operators at both layers. Each Spark-specific feature gets a
logical **extension node** describing what it means, and a matching physical
operator that does the work. DataFusion explicitly supports this: applications
can define their own logical nodes and physical operators and register a
planner that connects them. Because Sail uses these extension points rather than
changing DataFusion, it can keep up with DataFusion releases.

## The operators Sail adds, and why

Most of Sail's extension nodes exist because Spark defines a behavior precisely
and users depend on it.

**Range** implements `spark.range(...)`, generating numbers in parallel across a
chosen number of partitions. **Show string** implements `df.show()`, formatting
rows as Spark's familiar text table (or HTML in notebooks), with Spark's
truncation rules. **Monotonic ID** implements `monotonically_increasing_id()`
with Spark's exact formula: the partition number shifted left by 33 bits, plus
the row's position within the partition. So partition 0 produces 0, 1, 2, and
so on, partition 1 starts at 8,589,934,592, and the values match what Spark
would produce for the same partitioning. **Spark partition ID** returns the
current partition number.

**Sort within partitions** implements `sortWithinPartitions` and SORT BY, which
sort each partition separately without a global sort. **Explicit repartition**
records a repartition the user asked for, such as `df.repartition(8,
"customer_id")` or DISTRIBUTE BY, so that the optimizer doesn't remove it; a late
rule turns it into a real repartition. That matters because users repartition on
purpose, for example to control how many output files a write produces.

**Map partitions** runs Python functions over whole partitions for `mapInPandas`,
`mapInArrow`, and Python table functions (see
[15](#doc-15-functions-and-python-udfs)). **File write**, **file delete**, and
**merge into** describe writes, DELETE, and MERGE; their physical form comes from
the table format (see [12](#doc-12-data-sources-and-formats) and
[13](#doc-13-lakehouse-delta-and-iceberg)). A **catalog command** node wraps DDL
and metadata commands and calls the catalog manager when the plan runs. Finally,
a handful of streaming nodes run plans over unbounded input (more on these
below).

## Planning never changes anything: the barrier

There's one principle behind several of these nodes: **resolving or planning a
query must never have side effects**. Consider CREATE TABLE ... AS SELECT. It's
tempting to create the table while planning and then plan the write. But then
EXPLAIN of that statement would create the table, and a planning error after the
create would leave an empty, orphaned table behind.

Sail solves this with a **barrier** node. A barrier holds some *precondition*
plans and a *main* plan, and guarantees that the preconditions run to
completion before the main plan starts. Every write is wrapped in one: the
precondition might be "create the target table" and the main plan "write the
data". Planning only describes the work. At execution time the barrier runs the
create, waits for it, and then starts the write. A physical optimizer rule
enforces the waiting, even when the plans have different numbers of partitions,
by making sure no partition of the main plan can start before every partition of
the preconditions has finished.

## From logical node to physical operator

When DataFusion meets a logical node it doesn't recognize, it asks the
registered extension planners, in order, whether they can handle it. Sail
registers three. The lakehouse planners come first, because DELETE, UPDATE, and
MERGE on Delta and Iceberg tables need format-specific plans. The system table
planner comes next, for the `system.*` tables. Sail's general planner comes last
and handles everything else described above. The first planner that recognizes
the node produces the operator. If none does, planning fails, so **every new
logical node needs a planner case**.

## Logical optimization

Sail takes DataFusion's standard logical rules, such as pushing filters down,
removing unused columns, folding constants, eliminating common subexpressions,
and decorrelating subqueries, and makes three changes. Lakehouse rules run
**first**, so that DELETE, UPDATE, and MERGE are expanded into ordinary scans and
joins before the standard rules optimize them. A rule for a common lateral-join
pattern runs before DataFusion's own lateral-join rule: queries like
`LATERAL (SELECT t1.a + 1)` are rewritten into a simple cross join plus a
projection. And one DataFusion rule that pushes down leaf projections is
removed, because it doesn't suit how Sail uses projections.

## Physical optimization

After the physical plan is built, about twenty rules adjust it. Most come from
DataFusion: computing count, min, and max from statistics when possible;
choosing join algorithms and which side to build hash tables on; pushing
filters, projections, limits, and sorts down; inserting repartitions so that
joins and aggregations see the data partitioned the way they need; adding or
removing sorts; using top-K for ORDER BY ... LIMIT; making long-running operators
yield to the scheduler; and finally a sanity check that every requirement is
met.

Sail keeps **all** of DataFusion's default physical rules, **in the same
order**, and a test fails if that ever stops being true. That guards against
silently losing an optimization when DataFusion is upgraded. Sail inserts four
rules of its own. Join reordering runs early, if enabled (see below). Near the
end, one rule turns the user's explicit repartition into a real repartition
operator. Another makes sure the build side of a "collect left" hash join has
exactly one partition, a safety net that matters after joins have been
reordered. The last enforces the barrier behavior described above.

## Streaming plans

If a query reads from a streaming source, such as the rate or socket test
sources, an extra rewrite runs after logical optimization. It turns the plan
into a *streaming plan*, in which every operator except the sink carries extra
fields marking each row as an addition or a retraction. This is how a streaming
aggregation can correct a result it already emitted. A collector node at the top
can turn this stream of updates back into final results, as long as the query is
bounded.

## Cost-based join reordering

When a query joins many tables, as star and snowflake schemas do, the order of
the joins can change the runtime by orders of magnitude. Sail includes an
optional join reorder rule, enabled with optimizer.enable_join_reorder, that
chooses the order using estimated sizes.

Here's the intuition with numbers. Suppose a sales fact table has a billion
rows, a stores table filtered to one region has ten rows, and a products table
has 100,000 rows. Joining sales with products first produces a billion-row
intermediate result, and joining that with stores then shrinks it to about five
million. Joining sales with stores first produces only about five million rows,
and the products join keeps it there. The cost model adds up the sizes of the
intermediate results, so the first order costs around a billion and the second
around ten million. The rule picks the second, the same choice an experienced
engineer would make by hand.

To get there, the rule first finds regions of the plan made of inner equi-joins
(along with filters and projections between them) and treats everything else as
an opaque input. It builds a graph in which tables are vertices and join
conditions are edges, and groups columns that are joined by equality, so they
share one estimate of how many distinct values they hold. It estimates the size
of each table and each join from statistics: row counts, distinct counts, and
guesses for filter selectivity. Joins without equality conditions are estimated
pessimistically on purpose, to avoid accidentally building near-cross-products.

It then searches for the best order with dynamic programming over connected
subsets of tables, using the DPhyp algorithm. The cost of joining two subplans is
the cost of each plus the size of their join. If the search would generate more
than 10,000 candidate plans, it falls back to a greedy algorithm, and above ten
tables it prunes more aggressively. For star schemas with one dominant fact
table, it penalizes joining dimensions to each other first when the estimates
are uncertain. Finally it rebuilds the plan in the chosen order, restoring the
original column order and the filters and projections.

## Following one node through planning

Take `spark.range(1_000_000).selectExpr("id % 10 AS k").groupBy("k").count().show()`.
The resolver produces a show-string node over an aggregation over a projection
over a range node. Logical optimization simplifies the projection, and the two
extension nodes pass through untouched. The extension planner turns the range
node into a range operator split across the target number of partitions and the
show-string node into its operator, while DataFusion plans the aggregation as a
partial and a final step. Physical optimization inserts a hash repartition on k
between the two steps, and Sail's rules check the result. When it runs, the
show-string operator returns one batch containing the formatted table.

## Adding a new operator

Define the logical node in sail-logical-plan: its name, inputs, output schema,
expressions, and how to rebuild it with new inputs, with no side effects.
Implement the physical operator in sail-physical-plan, including its
partitioning and ordering properties and an execute method that returns a stream
of batches. Add a planner case connecting the two in sail-session. Teach the
resolver to produce the node (see [07](#doc-07-plan-resolver)). If the operator
must run on cluster workers, add it to the plan codec so it can be serialized
(see [09](#doc-09-local-and-cluster-execution)). And if it has partitioning or
ordering requirements, declare them, so the distribution and sorting rules can
work around it.

## Where to look in the code

Sail's logical extension nodes are in crates/sail-logical-plan/src and their
physical operators in crates/sail-physical-plan/src. The catalog command node is
defined in crates/sail-plan/src/catalog.rs. The planner that maps nodes to
operators is crates/sail-session/src/planner.rs. The logical rule list is
assembled in crates/sail-logical-optimizer/src/lib.rs and
crates/sail-session/src/optimizer.rs, and the physical rule list in
crates/sail-physical-optimizer/src/lib.rs. Join reordering lives under
crates/sail-physical-optimizer/src/join_reorder/ (graph.rs,
cardinality_estimator.rs, enumerator.rs, cost_model.rs, reconstructor.rs), and
the barrier and collect-left rules sit next to it in barrier.rs and
collect_left.rs. The streaming rewrite is in crates/sail-plan/src/streaming/.


---

<a id="doc-09-local-and-cluster-execution"></a>

# 09 · Local and Cluster Execution

Everything before this point produces one thing: a DataFusion physical plan.
This doc is about running it. Sail can run a plan inside a single process or
spread it across a driver and many workers, and the code that asked for the
result can't tell the difference. Here's how both modes work, how a plan is cut
into stages and tasks, how tasks get scheduled onto workers, and what happens
when things fail.

The data exchange between tasks (streams and shuffles) gets its own doc,
[10](#doc-10-shuffle-and-streams).

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

The driver is an actor (see [02](#doc-02-entry-points-and-server)) with four
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
through memory (see [10](#doc-10-shuffle-and-streams)). That gives low latency, but
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
[10](#doc-10-shuffle-and-streams)).

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
logs and memory, and bound the memory pool (see [06](#doc-06-session-and-runtime)).
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


---

<a id="doc-10-shuffle-and-streams"></a>

# 10 · Shuffle and Streams: Moving Data Between Tasks

When the driver cuts a plan into stages (see
[09](#doc-09-local-and-cluster-execution)), data has to flow from tasks in one
stage to tasks in the next. This doc explains the data plane that moves it: what
a stream is, how the shuffle write and read operators work, how readers and
writers find each other even when they start in the wrong order, how slow
consumers are kept from exhausting memory, how workers fetch data from each
other, and where Sail's disk-based shuffle stands.

```text
 Stage 0 task (partition p)                         Stage 1 task (partition q)
 ┌───────────────────────────┐                      ┌────────────────────────────┐
 │ … plan fragment …         │                      │ shuffle read                │
 │ shuffle write             │  channel q of p      │   reads channel q from      │
 │   hash(k) → channels 0..N ├────────────────────► │   every upstream partition  │
 └───────────────────────────┘  (memory or Flight)  │ … plan fragment …           │
                                                    └────────────────────────────┘
```

---

## Everything is a named stream

Every piece of data exchanged between tasks is a **stream** of Arrow record
batches, and every stream has a name made of five parts: the job, the stage, the
partition, the attempt, and the channel. "Channel 3 from attempt 0 of partition
5 of stage 2 of job 17" names exactly one stream.

Two parts of that name deserve a comment. The *channel* is one slice of a task's
output: when a task's rows are split by hash into eight buckets, each bucket is
a channel. The *attempt* means that when a task is retried, its new output gets
new names. A retry can never accidentally mix its data with that of the attempt
that failed.

## Why streams, why Arrow

Sail never converts data to rows between stages. Batches move as Arrow arrays in
memory, or as Arrow's binary IPC format over the network. There's no custom
serialization format to design or maintain, and almost no parsing cost on the
receiving side.

By default, data also never touches disk. In the default **pipelined** mode, a
downstream task reads its input *while* the upstream task is still producing
it. That gives low latency, which suits interactive queries. The trade-off, as
[09](#doc-09-local-and-cluster-execution) explains, is that producers and
consumers must run at the same time, and losing a task means rerunning its whole
connected region.

## Writing: the shuffle writer

At the root of every upstream task, the task runner places a **shuffle writer**.
It knows, for its partition, where each output channel should go. When it runs,
it opens a destination for every channel, then reads batches from the plan
fragment below it. It splits each batch according to the stage's
distribution, either by hashing the key columns into the number of channels, or
round-robin, and sends each slice to its channel.

A destination can report that it's *closed*, which happens when the consumer no
longer needs data, for example because a LIMIT has already been satisfied. The
writer then skips that channel, and if every channel is closed, it stops early
instead of computing rows nobody wants. When the input ends, the writer closes
all its destinations. Its own output is empty, because the real output is in the
streams.

## Reading: the shuffle reader

On the other side, placeholders in the downstream plan are replaced with
**shuffle readers**. Each one knows which streams it needs, and that depends on
how the stage was cut. After a hash repartition, partition q reads channel q
from every upstream partition, which is the classic shuffle. When stages are
linked one-to-one, partition q reads everything from upstream partition q.
Before an order-preserving merge, the single reader reads everything from every
upstream partition. For a broadcast, every downstream partition reads every
upstream partition's complete output.

The reader opens all of its streams at once and merges them, passing along
batches in whatever order they arrive. Rows from different upstream tasks are
interleaved, which is fine because operators like aggregation don't care about
order. Operators that do care, such as an order-preserving merge, sort as they
read.

## Where the bytes live

Each write destination says where its stream should live. A **memory** stream is
kept in the producing process's stream manager. Readers in the same process take
it directly, and readers elsewhere fetch it over the network. A **disk** stream is
written to a file, so it can be read later and more than once, even after the
producer has exited; that's the basis of the blocking shuffle described at the
end. A third kind, **remote** storage such as an object store, is defined but not
implemented yet: asking for it returns "not implemented: remote stream".

A memory stream can be read a fixed number of times, because the job graph
calculates in advance how many consumers each stage's output has. Usually that's
one reader per stream, but for a broadcast it's one per downstream partition.
The stream keeps that many *replicas*. Because Arrow batches are
reference-counted, a replica costs a pointer copy, not a data copy.

## When the reader arrives before the writer

In pipelined mode, a downstream task can easily ask for a stream before the
upstream task has created it, since everything starts at roughly the same time.
The stream manager on each driver and worker handles this with three states.
When a reader asks for a stream that doesn't exist yet, the manager creates a
**pending** entry, hands the reader a channel immediately, and parks it there.
When the producer publishes the stream, the entry becomes **created** and the
parked reader is connected. If the producer fails, the entry becomes **failed**
and readers receive the error.

To avoid waiting forever, the manager also schedules a check when it creates a
pending entry:

```text
t=0ms    consumer asks worker A for stream K   → no K yet: create PENDING(K), park the reader
t=0ms    A schedules a check of K after cluster.task_stream_creation_timeout_secs
t=40ms   producer on A publishes K              → PENDING becomes CREATED; the reader is connected
t=41ms   batches start flowing
```

If the producer never shows up, for example because its task crashed before
starting, the check fires, the waiting reader gets an error, and the consumer
task fails. That failure makes the driver reschedule the region (see
[09](#doc-09-local-and-cluster-execution)).

## Keeping slow consumers from exhausting memory

What if a producer is fast and its consumer is slow? Each memory stream is a
bounded channel holding at most cluster.task_stream_buffer batches, 16 by
default. With the default batch size of 8,192 rows, that's roughly 130,000 rows
in flight per stream. When the buffer is full, the producer's next write simply
waits until the consumer catches up. A slow consumer slows the pipeline down
instead of making memory grow without limit. This is called backpressure, and
it falls out of the design for free.

## Between machines: Arrow Flight

Every worker, and the driver, runs a small Arrow Flight service. When a reading
task needs a stream that lives on another worker, its worker uses a client for
that peer (it learns peer addresses from the driver) and makes a Flight request
whose ticket encodes the stream's five-part name. The other worker's Flight
service asks its stream manager for that stream, waiting in the pending state if
needed, and sends the batches back as Arrow IPC messages. On the receiving side,
the Flight response becomes an ordinary stream of batches, merged with the
reader's other inputs. The driver uses the same mechanism to read the final
stage's output from workers.

## Following one shuffle

Suppose stage 0 has three partitions, P0, P1, and P2, and stage 1 has two, Q0
and Q1, with rows hashed by k into two channels. Each producer splits its partial
results into channel 0 and channel 1, so there are six streams in total. Q0
reads channel 0 from P0, P1, and P2 and merges them, and Q1 reads channel 1 from
all three. If P1 and Q0 happen to run on the same worker, Q0 reads P1's stream
straight from memory, while P0's and P2's streams arrive over Flight from their
workers. If Q1 is slow, its three input streams fill up to 16 batches each and
the producers pause on that channel until it catches up.

When the job ends, or when every consumer of a stage has finished, the driver
tells workers to clean up that job's streams, and their stream managers release
the memory (and delete the files, for disk streams).

## The disk-based "blocking" shuffle

Pipelined shuffle is fast but fragile, and it needs every stage's tasks running
at once. Sail also has a **blocking** mode: upstream tasks write each channel to
disk and exit, and downstream stages start only after the upstream stage has
finished. That frees capacity between stages, and it unlocks two features. One is
resizing downstream stages based on how much data was actually produced
(adaptive coalescing). The other is recovering from a lost worker by recomputing
only the output it held.

On this branch, the disk stream itself is implemented and tested. Files are
written under a temporary name and renamed into place, with a small index file
recording byte and row counts. Adaptive coalescing based on those counts is
implemented and used in tests. But blocking mode can't be turned on from
configuration: the driver always uses pipelined mode.

The disk-shuffle-hardening branch makes blocking shuffle usable. It adds the
cluster.shuffle_mode, cluster.shuffle_dir, and adaptive coalescing settings,
has workers report shuffle statistics to the driver, retries stages when a
worker is lost or shuffle data can't be fetched, gives each process its own
shuffle directory, and buffers disk I/O. Those changes are described in
docs/concepts/architecture-deep-dive.md and
docs/concepts/architecture/adaptive-disk-shuffle-design.md.

---

## Common problems

If a reader fails while waiting on a pending stream, the upstream task never
started or crashed; check its status and its worker's logs, and raise
cluster.task_stream_creation_timeout_secs if workers start slowly. "Not
implemented: remote stream" means something requested object-store stream
storage, which doesn't exist yet. Memory growing on a worker during a big shuffle
reflects pipelined streams holding data until it's read; fewer partitions, a
bounded memory pool, or the disk shuffle on the hardening branch help. And a
retried job should never produce duplicates, because the attempt is part of every
stream's name; if you see duplicates, report them along with the task attempts
from system.execution.tasks.

## Where to look in the code

Everything is in crates/sail-execution. The shuffle write and read operators and
the stage-input placeholder are under src/plan/ (shuffle_write.rs,
shuffle_read.rs, stage_input.rs). Stream names are defined in src/id.rs. Read
and write locations, and the merging of several input streams, are under
src/stream/ (reader.rs, writer.rs, merge.rs). The stream manager with its
pending, created, and failed states is src/stream_manager/core.rs, and the memory
and disk streams are in src/stream_manager/local.rs. Choosing between local and
remote access is under src/stream_accessor/, and the Arrow Flight service under
src/stream_service/. The input and output locations recorded in task definitions
are in src/task/definition.rs. The disk shuffle design is in
docs/concepts/architecture/adaptive-disk-shuffle-design.md.


---

<a id="doc-11-catalogs"></a>

# 11 · Catalogs: Finding Tables

When a query mentions prod.sales.orders, something has to turn that name into a
table with columns, a file format, and a location. That something is the catalog
layer. This doc explains how Sail interprets table names, how several catalogs
live side by side, how temporary views fit in, which metastores Sail can talk
to, and why caching and thread isolation are layered around them.

---

## Two jobs, two kinds of component

A catalog has to answer metadata questions ("which databases exist?", "what are
the columns and location of sales.orders?") and carry out DDL (create, alter,
and drop databases, tables, and views). Sail splits this work between two kinds
of component.

**Catalog providers** store metadata. Each one talks to a particular metastore:
an in-memory store, AWS Glue, a Hive Metastore, an Iceberg REST catalog, Unity
Catalog, or Microsoft OneLake. There's also a built-in, read-only system catalog
that describes Sail itself.

The **catalog manager**, one per session, interprets names. It decides which
catalog and database a name refers to, keeps track of the current catalog and
database, holds temporary views, and runs catalog commands.

The split exists because Spark's rules for interpreting names are subtle (one-,
two-, and three-part names, current catalog and database, the global temporary
database), and if every provider implemented them, they would drift apart. So
the manager implements them once, and providers only ever see a catalog, a
namespace, and an object name. Adding a new metastore means implementing about a
dozen simple operations, such as creating, getting, listing, altering, and
dropping databases, tables, and views, and translating between that metastore's
types and Sail's.

## Configuring catalogs

Catalogs are declared in one setting, catalog.list, written in a compact inline
syntax:

```bash
export SAIL_CATALOG__LIST='[
  {name="sail", type="memory", initial_database=["default"]},
  {name="glue", type="glue", region="us-east-1"},
  {name="ice",  type="iceberg_rest", uri="https://rest.example.com", warehouse="s3://lake/wh"}
]'
export SAIL_CATALOG__DEFAULT_CATALOG=sail
```

Each type takes its own settings. Glue takes a region and optional endpoint.
Hive Metastore takes server URIs plus optional transport, Kerberos, and timeout
settings. Iceberg REST takes a URI, warehouse, prefix, and OAuth or bearer
token. Unity takes a URI, default catalog, and token. OneLake takes a URL and
an optional bearer token (otherwise it asks the Azure CLI for one). Every remote
type also accepts cache settings, described below. Secrets are kept as secret
strings and never printed in logs.

Out of the box, Sail has a single in-memory catalog called sail with a database
called default. If you define exactly one catalog and don't name a default, it
becomes the default. With several catalogs, you must choose one, or Sail refuses
to start. A catalog called system is always added, and that name is reserved.

## How a name is interpreted

The manager keeps a current catalog, a current database, and the name of the
global temporary database (global_temp). With those, it reads names like this.
A name with one part is an object in the current catalog and current database. A
longer name splits into the object (the last part) and a qualifier (everything
before it). If the qualifier's first part is the name of a registered catalog,
that's the catalog and the rest is the namespace; otherwise the whole qualifier
is a namespace in the current catalog. Namespaces can have several levels, which
some catalogs such as Iceberg REST support.

Here's how that plays out with catalogs sail (the default) and glue, and current
database default. orders means sail.default.orders. sales.orders means the
orders table in database sales of the sail catalog, because sales isn't a
catalog. glue.sales.orders means catalog glue, database sales. glue.orders is an
error, "empty namespace", because it names a catalog but no database.
global_temp.v refers to a global temporary view. After `USE glue.sales`, the bare
name orders means glue.sales.orders.

## The order of lookup

Reading a table goes through a fixed sequence. If the name has one part, the
manager first checks the session's **temporary views**, so a temporary view
named orders hides a table named orders. If the qualifier is the global
temporary database, it checks **global temporary views**. Otherwise it asks the
catalog's provider for a **table**, and if there's none, for a saved **view**.
If all of those come up empty, the user gets TABLE_OR_VIEW_NOT_FOUND.

What comes back is a description of the object. For a table, that includes its
columns (with types, nullability, comments, defaults, and generated
expressions), its file format, location, partition columns, bucketing, sort
order, properties, and whether it's external. A saved view comes back as its SQL
text and columns. A temporary view comes back as a logical plan. The resolver
then uses a table's format and location to ask the matching table format for a
reader (see [07](#doc-07-plan-resolver) and [12](#doc-12-data-sources-and-formats)).

## Temporary views and other session objects

`df.createOrReplaceTempView("v")` stores the DataFrame's plan in the session, and
`createGlobalTempView` stores it under global_temp. The manager also holds
objects that external catalogs can't: Python UDFs registered in the session
(which the resolver checks before built-in functions; see
[15](#doc-15-functions-and-python-udfs)), and functions and plans that Spark
Connect clients register and later refer to by ID.

## Wrapping remote catalogs: threads and caches

Every remote catalog gets two layers around it, and both exist for practical
reasons.

The first runs every catalog call on Sail's I/O thread pool (see
[02](#doc-02-entry-points-and-server)). A slow metastore then can't tie up the
threads that execute queries, and the client libraries for Glue, Iceberg REST,
and others get the runtime they expect.

The second is a cache. Planning a query can involve several catalog calls, and
going to a remote metastore each time adds latency. The cache remembers
databases, tables, and views, with configurable size and expiry for each. When
you change something through Sail, with ALTER TABLE or DROP TABLE for example,
the affected entries are invalidated immediately. When another system changes a
table, say Spark or Athena writing to the same Glue catalog, Sail keeps serving
the cached version until it expires. Choose the expiry with that trade-off in
mind:

```bash
export SAIL_CATALOG__LIST='[{name="hms", type="hms", uris=["thrift://metastore:9083"],
  table_cache_type="global", table_cache_ttl_secs=60}]'
```

## Catalog commands

DDL and metadata commands such as CREATE, DROP, SHOW TABLES, DESCRIBE, and USE
become catalog commands, about sixty kinds in all. The resolver wraps them in a
plan node, and when the plan runs, the command uses the manager and the
providers to do its work and returns a small table shaped like Spark's output.
In cluster mode these commands always run on the driver, because only the driver
holds the session's catalog state.

## The providers in brief

The **memory** catalog lives inside the session's catalog manager. Its metadata
lasts only as long as the session, though any data files it wrote remain. That
makes it good for development and tests, and a surprise if you expect tables to
persist. The **system** catalog serves read-only tables generated from Sail's
runtime state (see [06](#doc-06-session-and-runtime)). **AWS Glue** uses the AWS
SDK with the standard credential chain, and understands Hive-style tables and
Iceberg tables. **Hive Metastore** speaks Thrift, optionally with Kerberos, and
translates Hive storage descriptors (input format, output format, SerDe) to and
from Sail's formats. **Iceberg REST** implements the Iceberg REST catalog API,
including nested namespaces and token authentication. **Unity Catalog** is a
REST client configured with a token. **OneLake** talks to Microsoft Fabric's
OneLake Delta table API.

## Following one lookup

Take `spark.table("glue.analytics.events")`. Spark Connect produces a read of the
named table glue.analytics.events. The resolver checks that it isn't a file-path
shortcut or a WITH-clause name, then asks the catalog manager. Because the name
has three parts, it can't be a temporary view. glue is a registered catalog, so
the catalog is glue, the namespace analytics, and the object events. The cache
layer looks for the table, and on a miss passes the request to the thread layer,
which runs the Glue call on the I/O pool. The Glue provider fetches the table
definition and translates its columns, format, location, and partition keys.
The resolver builds a description of the source and asks the Parquet format for
a reader, and planning continues.

---

## Common problems

"Cannot infer default catalog when multiple catalogs are defined" means
catalog.default_catalog needs to be set. A table you just created that can't be
found was probably created in an in-memory catalog in a different session, or is
hidden behind a stale cache. "Cannot define catalog with reserved name: system"
means a configured catalog is called system. A name that resolves to the wrong
catalog happens when a database shares its name with a catalog; qualify the name
fully. And queries that stall on metadata usually point at a slow metastore;
turn on caching and check the network and credentials.

## Where to look in the code

The provider interface is in crates/sail-catalog/src/provider/mod.rs, with
namespaces and options alongside it, and the caching and thread layers in
provider/cache.rs and provider/runtime.rs. The catalog manager and its name
resolution are under src/manager/, temporary views in src/temp_view.rs, catalog
commands in src/command.rs, and Hive storage format mapping in src/hive_format.rs.
The shape of a table description is defined in crates/sail-common-datafusion
under src/catalog/. Catalogs are created for each session in
crates/sail-session/src/catalog.rs, from the settings defined in
crates/sail-common/src/config. Each provider has its own crate:
sail-catalog-memory, -system, -glue, -hms, -iceberg, -unity, and -onelake.


---

<a id="doc-12-data-sources-and-formats"></a>

# 12 · Data Sources and File Formats

Every time Sail reads or writes data, it goes through a **table format**: a named
plugin that knows how to read and write one kind of data. Parquet, CSV, JSON,
Avro, Arrow, plain text, and binary files each have one, as do Delta Lake and
Iceberg tables, a few streaming test sources, and any data source a user writes
in Python. This doc explains the table format interface, how file formats find
files and work out schemas, how options are defined and combined, how writing
works, and how Python data sources plug in. Lakehouse formats get their own doc
([13](#doc-13-lakehouse-delta-and-iceberg)), and so does storage access
([14](#doc-14-object-storage)).

---

## One interface for every format

The resolver doesn't know whether it's reading Parquet or Iceberg, and it
shouldn't have to. When it needs to read a table, it builds a neutral
description: the paths, the schema if one is known, the partition columns,
bucketing and sort information from the catalog, and the options. Then it asks
the session's format registry for the format by name and hands over the
description. The format returns a table source that DataFusion can scan with the
usual optimizations: reading only the needed columns, pushing filters down, and
stopping early for limits.

Writing works the same way. The resolver describes the write: the plan producing
the rows, the mode (append, overwrite, overwrite matching a condition, overwrite
partitions, error if the target exists, or ignore if it exists), the partition
columns, the options, and the output path. The format returns a plan that
consumes the rows and writes them.

Lakehouse formats implement a few extra capabilities: writers for DELETE,
UPDATE, and MERGE, a choice between rewriting files and recording deletions
separately, and changes to table properties and column types. Plain file formats
don't need these.

The payoff of this design is that adding a format means implementing the
interface and registering it. Nothing else in the planner changes.

## File formats are built on DataFusion's listing table

DataFusion already has a capable "listing table" for directories of files. It
splits files into partitions for parallel scanning, uses file statistics,
applies projection and filter pushdown, prunes partition directories, and
writes files in parallel. Sail's file formats are built on it, and add the
behavior Spark users expect: Hadoop-style path patterns, hidden-file handling,
partition inference rules, Spark's option names and defaults, and Spark-style
output file names.

The built-in formats are Parquet (with row-group pruning, page-index filtering,
filter pushdown, and bloom filters), CSV, newline-delimited JSON, Avro, Arrow IPC,
text (one string column per line), and binary (one row per file, with the path,
modification time, length, and content; it can't be written). There are also
three streaming helpers: rate generates rows at a steady pace, socket reads lines
from a TCP connection, and console prints streaming results.

## Finding files

Reading `s3://lake/events/` starts with working out which files that means. Sail
parses the path with its own URL parser, because Spark uses **Hadoop's glob
syntax**, which differs from shell globs in small ways: alternatives are written
`{a,b}`, a negated character class is `[^x]` rather than `[!x]`, and `*` and `?`
work as usual. A pattern such as `logs/{2024,2025}/*/app-[^t]*.json` is split
into a fixed base path and a pattern, and the alternatives are expanded.

The files are then listed through the storage registry, using the listing cache
if it's turned on. When you point at a directory without your own pattern,
files whose names start with a dot or an underscore are skipped, just as in
Spark, so bookkeeping files like _SUCCESS and .crc checksums never get read as
data.

## Working out the schema and partitions

Data lakes usually organize files in directories named after column values:

```text
s3://lake/events/date=2024-01-01/country=US/part-0.parquet
s3://lake/events/date=2024-01-01/country=DE/part-1.parquet
s3://lake/events/date=2024-01-02/country=US/part-2.parquet
s3://lake/events/_SUCCESS
```

If you don't name the partition columns, Sail discovers them from directory names
like these and adds them to the schema. Reading the directory above produces the
columns stored in the files plus date and country. A filter like country = 'DE'
then opens only part-1.parquet, because the other directories can't match.

If you don't supply a schema, the format infers one. Parquet reads it from file
footers, CSV and JSON sample rows, and Avro uses the schema in the file. Some
formats honor options that change this, such as CSV's inferSchema=false, which
makes every column a string.

## Reading efficiently

During execution, the listing table groups files, and for Parquet even row
groups within files, into partitions that are read in parallel. It reads only
the columns the query needs. For Parquet, it uses each row group's statistics and
the page index to skip data that can't match a filter, can apply filters while
decoding, and can check bloom filters. These behaviors are controlled by the
parquet.* settings. It also prunes whole partition directories using their
values. The file statistics and metadata caches (see
[06](#doc-06-session-and-runtime)) keep repeated queries from re-reading footers.
In cluster mode, the scan description travels inside each task's plan, and every
worker reads its assigned files directly from storage.

## Options, defined as data

Spark has dozens of options per format, many with several names (sep and
delimiter mean the same thing for CSV), forms that can be stored as table
properties (option.sep), defaults, and rules about whether they apply to
reading, writing, or both. Hand-written parsing for all of this would be
repetitive and easy to get subtly wrong.

So Sail describes each format's options in a YAML file, about 250 options in
total. CSV alone has about 40, Parquet about 35, and Iceberg about 100. Each
entry gives the option's names, its default, whether it applies to reading,
writing, or both, the type it should become, and whether Sail supports it yet. At
build time, these files become typed Rust structures with parsers. Option
handling is therefore consistent, the YAML doubles as documentation, and an
option marked unsupported can produce a clear error instead of silently doing
nothing.

Options can come from several places for one read, and they're applied in
**layers**, with later layers winning key by key. Here's an example. A CSV table
is created with table properties saying the separator is a semicolon and the
files have a header row, and then a query overrides the separator:

```sql
CREATE TABLE raw_events (id INT, msg STRING) USING csv
  LOCATION 's3://lake/raw/' TBLPROPERTIES ('option.sep' = ';', 'option.header' = 'true');
```

```python
spark.read.option("sep", "|").table("raw_events")
```

The defaults say the separator is a comma and there's no header. The table
properties change that to a semicolon with a header. The query option then
changes the separator to a pipe, and doesn't mention the header. So the read
uses a pipe separator and a header row. Names match without regard to case, and
aliases count as the same option.

## Writing files

When writing, a plain file format first rejects things it can't do. It can't
write a streaming query to a plain file path, apply bucketing, or apply
partition transforms. It then treats the output path as a directory and always
writes several files. It chooses file extensions, including the compression
codec for Parquet (for example .zstd.parquet), and writes in parallel, putting
partition columns in directory names rather than inside the files. Parquet
writing honors about twenty parquet.* settings such as compression, dictionary
encoding, statistics, row group size, page size, bloom filters, and writer
version.

One limitation is worth knowing. The plain file writer currently **only
appends**: it ignores the write mode, a known gap marked in the code. The checks
that happen before writing to a *table*, such as whether it exists, creating it,
and refusing or skipping because of the mode, are done by the resolver, as
preconditions that run before the write (see [07](#doc-07-plan-resolver) and
[08](#doc-08-logical-physical-plans-and-optimizers)). But writing to an existing
directory of plain files adds files rather than replacing them. Lakehouse formats
implement every mode properly, as transactions.

## Python data sources

PySpark 4 lets users write data sources in Python, and Sail supports them in two
ways. A program can register one for its session with
`spark.dataSource.register(MySource)`, which sends the pickled class to Sail.
And at startup, Sail looks for installed Python packages that advertise data
sources through Python's standard entry-point mechanism, and registers those for
everyone.

When such a source is read, Sail calls its schema method, asks its reader to
split the work into partitions, and runs the reader's read method for each
partition in the embedded Python interpreter, converting the rows it yields
into Arrow batches. Simple filters can be passed to the reader. Writing runs the
Python writer for each partition, then a commit step that calls the writer's
commit or abort with the messages each partition returned:

```python
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

## Following one read

Take `spark.read.parquet("s3://lake/events/").filter("date = '2024-01-01'").count()`.
Spark Connect produces a read of format parquet with one path and no schema. The
resolver asks the registry for the Parquet format and passes it the description.
The format parses the path, finds no pattern, and lists the directory through
the S3 client, skipping _SUCCESS. It discovers the date directories and adds a
date partition column, reads one footer (possibly from cache) to get the file
schema, and builds a listing table. The optimizer turns the filter into a
partition filter, and because count needs no columns, row counts can come from
file metadata. At execution, only the files under date=2024-01-01 are opened.

---

## Common problems

"Bucketing for writing listing table format" means bucketBy was used with a
plain file format, which isn't supported. "Cannot write streaming data to listing
table" means a streaming query tried to write to a plain file path. If a write to
an existing path added files instead of replacing them, that's the append-only
behavior described above; write to a table or a lakehouse format, or clear the
path first. An unexpected extra column usually comes from partition inference
finding col=value directories; pass an explicit schema to control it. An option
that seems to do nothing may be misspelled, unsupported, or meant for the other
direction; the format's YAML file says which. And a glob that doesn't match may be
using shell syntax; remember `[^x]` and `{a,b}`.

## Where to look in the code

The table format interface, with the read and write descriptions, option layers,
and write modes, is in crates/sail-common-datafusion/src/datasource.rs, and the
formats are registered for each session in crates/sail-session/src/formats.rs.
The listing-based reading and writing shared by file formats is in
crates/sail-data-source/src/listing/. Each built-in format has a directory under
src/formats/ (parquet, csv, json, avro, arrow, text, binary, rate, socket,
console), and Python data sources are under src/formats/python/. Option
definitions are the YAML files in src/options/data/, turned into code by the
crate's build.rs, and path and glob parsing is in src/url.rs. Write planning is in
crates/sail-physical-plan/src/file_write.rs.


---

<a id="doc-13-lakehouse-delta-and-iceberg"></a>

# 13 · Lakehouse Tables: Delta Lake and Apache Iceberg

A directory of Parquet files is a fine way to store data, until you need to
change it safely. Two writers can step on each other. A reader can see a
half-finished write. There's no way to look at yesterday's version, change the
schema cleanly, or delete a few rows without rewriting everything by hand.
*Lakehouse table formats* solve this by adding a metadata layer that records
exactly which files make up each version of a table. This doc explains how Sail
reads and writes the two most popular ones, Delta Lake and Apache Iceberg, and
how DELETE, UPDATE, and MERGE work on top of them.

---

## The idea both formats share

Both formats separate *data files*, ordinary Parquet, from *metadata* that says
which data files belong to the table right now. Writing a new version means
writing new data files first, then publishing a new piece of metadata that
points at them. Publishing is a single atomic step, so readers see either the
old version or the new one, never a mix. Old versions remain described by old
metadata, which is what makes time travel possible.

Delta Lake keeps its metadata in a `_delta_log` directory: a numbered series of
small JSON files, one per commit, with periodic Parquet *checkpoints* that
summarize everything up to a point. Iceberg keeps a table metadata file that
points to *snapshots*, each snapshot points to a *manifest list*, and each
manifest list points to *manifests*, Avro files that list data files along with
statistics about them.

The formats also differ in how they handle deleting a few rows. Delta can
rewrite the affected files (copy-on-write) or record *deletion vectors*, small
bitmaps marking which rows of a file are gone. Iceberg can rewrite files, or
record *delete files* that list deleted rows by position or by key values.

## Why Sail implements them natively

Sail implements both formats in Rust, on top of DataFusion and Sail's table format
interface (see [12](#doc-12-data-sources-and-formats)), rather than calling out to
another library. The main reason is scale. On a big table, just *reading the
metadata* can be a large job: thousands of log files or manifests describing
millions of data files. Sail expresses that metadata work as ordinary physical
plans (scan the log, replay it, filter the file list, then scan the data), so it
runs in parallel with the same engine as everything else, and on workers in
cluster mode.

The second reason is clean planning. The resolver produces format-neutral nodes
for writes, deletes, and merges. A lakehouse rule then expands them, and each
format decides *how* to carry them out: rewrite files, write deletion vectors,
or write delete files. Logical planning never needs to know which format is
involved.

## Reading a Delta table

Reading starts by choosing a version. Sail lists the `_delta_log` directory and
picks the newest checkpoint plus every JSON commit after it, or stops earlier if
you asked for a particular version or timestamp. From these it loads the
table's protocol and metadata: the schema, the partition columns, the
properties, and which reader features the table uses (column mapping, deletion
vectors, type widening, and so on).

Then it **replays the log** to find the active files. Every log entry is an
action, mostly "add this file" or "remove this file". Replay groups the actions
by file and keeps only the newest one for each. Here's a tiny example:

```text
00000000000000000000.json   add(part-a)  add(part-b)
00000000000000000001.json   add(part-c)
00000000000000000002.json   remove(part-a)  add(part-d)      ← an UPDATE rewrote part-a as part-d
```

Read at the latest version, the newest action for part-a is a remove, so the
active files are part-b, part-c, and part-d. Read as of version 1, replay stops
before commit 2, and the active files are part-a, part-b, and part-c. That's all
time travel is: replaying less of the log. Once a checkpoint exists, at version
10 for example (tables checkpoint every few commits, ten by default), replay
starts from it instead of from the beginning. In Sail, this replay is itself a
plan operator, so it scales to very large logs, and how it groups actions can be
tuned with table options.

Each add action also carries statistics about its file: row counts, minimum and
maximum values, and null counts. Sail uses them, together with partition values,
to **skip files** that can't match the query's filters before reading any data.

Finally, the remaining files are scanned as Parquet. Two adjustments happen
while scanning. An adapter maps physical column names to logical ones (Delta's
column mapping) and fills in missing columns for files written before a schema
change. And for files that have a deletion vector, Sail tracks each row's
position while reading and drops the rows the bitmap marks as deleted.

## Writing to a Delta table

A write begins by checking the incoming data against the table's schema. The
mergeSchema and overwriteSchema options allow the schema to evolve, generated
columns are computed from their expressions, and invariants are enforced. Sail
then writes Parquet files, collecting statistics for each one.

Then comes the commit. Sail gathers the new add actions (plus remove actions for
replaced files, and metadata or protocol changes if any) and tries to write the
next numbered log file using a "create only if it doesn't exist" operation. If
another writer got there first, Sail's conflict checker compares what this
transaction read and wrote with what the winning commit did. If they don't
overlap in a way that matters, Sail retries at the next version number.
Otherwise the write fails with a concurrency error. After committing, Sail writes
a checkpoint when the version reaches the table's checkpoint interval, and cleans
up the log as needed.

Delta writes support appending, overwriting, replaceWhere (overwrite only the
rows matching a condition), and the error-if-exists and ignore-if-exists modes.
Many of Spark's Delta options are defined but marked unsupported, including
partitionOverwriteMode, maxRecordsPerFile, change data feed reads, and the
streaming source options. The options file for Delta says which ones work.

## DELETE, UPDATE, and MERGE on Delta

Row-level operations are expanded early by a lakehouse optimizer rule (it runs
before DataFusion's own rules) and then planned by Delta. Sail carries a few
internal columns through these plans: which file each row came from, its
position within that file, and what kind of change applies to it. That's how the
writer knows exactly which rows to rewrite or mark. The columns are removed
before any data is written.

By default, Sail uses **copy-on-write**. For a DELETE, it uses the metadata to
find files that might contain matching rows, rewrites those files without the
matching rows, and commits removes for the old files and adds for the new ones.
UPDATE rewrites affected files with the new values. MERGE joins the source with
the target, applies the WHEN MATCHED, WHEN NOT MATCHED, and WHEN NOT MATCHED BY
SOURCE clauses, rewrites the files it touched, and checks that no target row
matched more than one source row, as the SQL standard requires.

If the table has deletion vectors enabled, DELETE and MERGE switch to
**merge-on-read**. Instead of rewriting files, Sail scans the affected files to
find the positions of the matching rows, writes small deletion vector files,
and commits "remove the old entry, add the same file with this deletion
vector". The difference can be dramatic. Deleting ten rows from a 1 GB file with
copy-on-write writes a new 1 GB file. With a deletion vector, it writes a bitmap
of a few bytes, and readers skip those ten rows from then on. The cost moves to
readers, which is why tables are compacted from time to time. UPDATE with
deletion vectors isn't implemented yet.

## Reading and writing Iceberg

An Iceberg read starts by loading the table's current metadata file, from the
catalog or the table location, and choosing a snapshot: the current one, a
specific snapshot ID, the snapshot as of a timestamp, or a branch or tag. Sail
reads that snapshot's manifest list and then its manifests. A manifest scan
operator turns manifest entries into rows describing data files, with their
partition values and column statistics.

Iceberg partitions with *transforms*, so a table can be partitioned by the day
of a timestamp or a hash bucket of an ID without storing extra columns. Sail
understands identity, bucket, truncate, year, month, day, and hour transforms,
and uses them, along with column statistics, to skip manifests and files.
Iceberg identifies columns by stable IDs rather than names, so Sail maps files
written with older schemas onto the current one. If other engines have written
delete files, Sail applies them: position deletes remove specific rows of
specific files, equality deletes remove rows matching given key values, and
sequence numbers decide which deletes apply to which files.

Writing supports appending and overwriting. Sail writes data files with the
partition transforms applied, writes new manifests and a manifest list, and
commits a new snapshot by writing a new metadata file (and updating the version
hint) next to the table. Setting and unsetting table properties is supported.
Not supported yet: bucketing, overwriting only matching rows or partitions, and
DELETE, UPDATE, and MERGE on Iceberg tables. About a hundred Iceberg options are
defined, each marked supported or not.

## Following one DELETE

Take `DELETE FROM sales.orders WHERE status = 'cancelled'` on a Delta table. The
SQL analyzer produces a delete command, and the resolver looks up the table (a
Delta table stored at s3://lake/orders) and produces a file-delete node. The
lakehouse rule expands it into a format-neutral row-level delete, and Delta
checks the table's properties to choose a strategy. Its metadata pipeline (scan
the log, replay it, read statistics, filter) finds the files whose statistics
for status could include 'cancelled'. With copy-on-write, those files are
rewritten without the matching rows. With deletion vectors, the matching
positions are collected into bitmaps and written as deletion vector files. Either
way, the changes are committed as the next log version, with the conflict check
protecting against concurrent writers, and the client gets back metrics such as
how many rows were deleted.

---

## Common problems

"Merge-on-Read strategy for UPDATE is not yet implemented for Delta Lake" means
an UPDATE ran on a table with deletion vectors enabled. "Predicate or partition
overwrite for Iceberg" means replaceWhere or a partition overwrite was attempted
on Iceberg. A concurrency error on commit means two writers changed overlapping
files or metadata; retry, or make the writers take turns. A slow first query on
a huge Delta table usually means a long log without recent checkpoints. And
schema mismatches on append can be solved with mergeSchema on Delta, or by
evolving the schema explicitly.

## Where to look in the code

Delta lives in crates/sail-delta-lake. Its entry points for reading, writing,
row-level operations, and table changes are in src/table_format.rs. Log listing,
segments, replay, and cleanup are under src/delta_log/. Snapshots, checkpoints,
and transactions with the conflict checker are under src/kernel/. File skipping
and scan setup are in src/datasource/. The physical operators (log replay, scan
with deletion vectors, writer, commit, deletion vector writer) are under
src/physical_plan/, with the planners for write, delete, update, merge, and
commit in src/physical_plan/planner/. Protocol types are in src/spec/ and deletion
vectors in src/deletion_vector/. Iceberg lives in crates/sail-iceberg: its entry
points are in src/table_format.rs, the format specification (metadata,
manifests, partition transforms, types, delete index) in src/spec/, reading in
src/datasource/, operators in src/physical_plan/, and append and overwrite
operations in src/operations/. The rule that expands row-level operations is in
crates/sail-plan-lakehouse, and the option definitions for both formats are in
crates/sail-data-source/src/options/data/ (delta.yaml and iceberg.yaml).


---

<a id="doc-14-object-storage"></a>

# 14 · Object Storage: Reaching S3, GCS, Azure, and More

All of Sail's file I/O goes through one abstraction: the object store interface
from the Apache Arrow project. Listing directories, reading byte ranges of
Parquet files, writing output files, committing Delta logs, and reading Iceberg
metadata all use it. This doc explains how Sail turns a URL into a working
storage client, which kinds of storage are supported, where credentials and
regions come from, and why every client is wrapped in a few small layers.

---

## The problem: Spark users expect any path to just work

A Spark user writes `spark.read.parquet("s3://any-bucket/path")` and expects it
to work, with no setup per bucket. DataFusion's default approach is the
opposite: every storage location must be registered before it can be used. That
doesn't fit a Spark-compatible engine, where a single query may read from one
bucket and write to another, or even cross clouds.

Sail's answer is a **dynamic registry**. When DataFusion needs storage for a URL,
it asks the registry, which looks at the URL's scheme and authority (for
example s3 plus a bucket name). If it has already built a client for that pair,
it returns it. If not, it builds one, remembers it, and returns it. The lookup
and insertion happen as one atomic step, so two tasks asking for the same bucket
at the same moment always share one client.

## What's supported

Local files (plain paths or file:// URLs) are always available. Amazon S3 works
with s3://, s3a://, and HTTPS URLs on amazonaws.com, and so do S3-compatible
services such as MinIO and Cloudflare R2 once you set an endpoint. Google Cloud
Storage uses gs://. Azure Blob Storage and Data Lake Storage use az://,
abfs://, abfss://, adl://, and HTTPS URLs on Azure's domains. Other http:// and
https:// URLs are treated as generic web servers, which in practice is
read-only. Hugging Face datasets are reachable as
hf://datasets/user/dataset@revision/path; only the datasets repository type is
supported. HDFS works when Sail is built with the hdfs feature. An in-memory
store exists for tests.

## Three layers around every client

Each client the registry creates is wrapped in three layers, and each solves a
practical problem.

The innermost layer makes cloud clients **lazy**. Building an S3 client can
mean loading AWS configuration, refreshing credentials, or even making a network
call to find out which region a bucket lives in. If that happened as soon as a
plan mentioned a path, planning would be slow and paths that are never actually
read would still cost something. With the lazy layer, the real client is built
the first time it's used for I/O.

The middle layer **logs** every storage operation (reads, writes, listings) at
debug level. When you need to know exactly what a query read, turning on debug
logging for this crate tells you.

The outer layer runs the client's creation and all of its I/O on Sail's
dedicated **I/O thread pool** (see [02](#doc-02-entry-points-and-server)). Network
clients are often tied to the runtime that created them, and keeping storage
I/O separate means slow storage can't starve query execution of threads.

## Amazon S3 in detail

S3 is the most common case, so its setup is the most complete. Sail starts
from settings in environment variables such as AWS_ENDPOINT, AWS_ALLOW_HTTP,
and the access key variables. It then loads the AWS SDK's default configuration
once per process. That's the same credential chain the AWS command-line tool
uses. It tries environment variables, then the shared credentials and config
files (selected with AWS_PROFILE), then single sign-on and web identity (which
is how EKS service accounts work), then the container or instance role. If that
chain finds credentials, Sail uses them through a caching provider that refreshes
them automatically, so temporary credentials keep working through long jobs.

Next, Sail works out the bucket from the URL. It understands the plain
s3://bucket/key form as well as both styles of HTTPS URL: virtual-hosted, where
the bucket is part of the host name, and the older path style, where the bucket
is the first part of the path.

Finally, it needs a region. If the URL contains one, or AWS_REGION is set, or
the AWS profile names one, that's used. Otherwise Sail asks S3 where the bucket
lives, which costs one extra request the first time and is then remembered with
the client. Setting AWS_REGION skips that request.

For an S3-compatible service, point Sail at it with environment variables:

```bash
export AWS_ACCESS_KEY_ID=minio AWS_SECRET_ACCESS_KEY=minio123
export AWS_ENDPOINT=http://minio:9000 AWS_ALLOW_HTTP=true AWS_REGION=us-east-1
sail spark server
```

## Google Cloud Storage, Azure, HTTP, and Hugging Face

The other cloud clients are configured from the environment variables that the
object store library understands, for example GOOGLE_APPLICATION_CREDENTIALS
for GCS, and AZURE_STORAGE_ACCOUNT_NAME with an account key or SAS token for
Azure. Generic HTTP stores read their client options from matching environment
variables. For Hugging Face, Sail parses the user, dataset, optional revision,
and path from the URL and talks to the Hugging Face Hub API, so public datasets
can be queried directly:

```python
spark.read.parquet("hf://datasets/user/dataset/data/*.parquet").show()
```

## Who uses the registry

Every table format uses it to list, read, and write files (see
[12](#doc-12-data-sources-and-formats)), and so do the Delta and Iceberg
implementations for their logs and metadata (see
[13](#doc-13-lakehouse-delta-and-iceberg)). Every session has its own registry,
and so does every worker session, which is why cluster workers read data
directly from storage rather than through the driver.

## Following a first read

The first time a query touches s3://analytics/events/, the listing step asks the
registry for storage for s3 plus analytics. There's nothing cached, so the
registry builds the three-layer stack around a lazy S3 client and remembers it.
No network calls have happened yet. The first listing request triggers the lazy
layer, which, on the I/O thread pool, loads the AWS configuration, sets up the
credential provider, parses the URL, and resolves the region if needed. The
listing returns the files, and Parquet footers are then read with byte-range
requests, each one logged at debug level. Later queries in the same session reuse
the same client and credentials.

---

## Common problems

"S3 bucket name must be specified in url" means the URL is malformed; use
s3://bucket/key. Access denied usually means the credential chain found nothing
suitable, so check AWS_PROFILE, the environment variables, and the instance or
pod role. A slow first access to a bucket is usually region detection or a
credential refresh, which setting AWS_REGION avoids. "Unsupported object store
URL" means the scheme isn't supported, or HDFS support wasn't compiled in.
"Unsupported repository type" on an hf:// URL means it isn't under
hf://datasets/. And to see what a query actually read, turn on debug logging for
the object store crate.

## Where to look in the code

Everything lives in crates/sail-object-store. The dynamic registry and the
choice of client for each scheme are in src/registry.rs. S3 credentials, URL
parsing, and region detection are in src/s3.rs, and the Hugging Face client is
in src/hugging_face.rs. The lazy, logging, and thread-pool layers are under
src/layers/. The registry is created for each session in
crates/sail-session/src/runtime.rs.


---

<a id="doc-15-functions-and-python-udfs"></a>

# 15 · Functions and Python UDFs

A large part of Spark compatibility comes down to functions. Spark has hundreds
of built-in functions, and users expect every one of them to behave exactly as
it does in Spark, down to how it treats NULLs and which date format letters it
accepts. PySpark users also expect their own Python functions to just work. This
doc explains how Sail provides about 500 Spark-compatible functions, how a
function name in a query becomes executable code, and how Python UDFs run inside
Sail.

---

## DataFusion first, Spark semantics where they differ

DataFusion already ships a large function library, and many Spark functions map
straight onto it: square root, absolute value, lower case. Others differ in ways
that would produce wrong answers if Sail used DataFusion's version. Spark's
concat returns NULL if any argument is NULL. Spark's date_format uses Java-style
patterns like yyyy-MM-dd rather than strftime's %Y-%m-%d. element_at counts
array positions from 1. Dividing by zero returns NULL unless ANSI mode is on, in
which case it raises an ArithmeticException, and casting the string 'abc' to an
integer behaves the same way.

For functions like these, the sail-function crate provides Spark-specific
implementations: Spark's concat, Spark's date parts and intervals, Spark's number
formatting and parsing, Spark-compatible hashing, and so on, organized by area
(strings, dates and times, arrays, maps, math, JSON, CSV, XML, URLs, variants,
geospatial data). It also adds aggregates that DataFusion lacks, such as mode,
percentile, kurtosis, skewness, product, max_by and min_by, numeric histograms,
try_avg, and bitmap aggregates, plus Spark's version of the ntile window
function. Each is an ordinary DataFusion function: it works on whole Arrow
arrays at a time, runs on cluster workers just like a built-in, and takes part in
DataFusion's optimizations.

## From a name to code: builders

Sail doesn't put its Spark functions into DataFusion's function registry.
Instead, it keeps its own tables that map each Spark function name to a small
piece of code called a **builder**. There are separate tables for scalar
functions (about 360, the largest groups being string, math, and date
functions), aggregates (about 64), window functions, generators such as explode
and inline, and table-valued functions such as range.

A builder receives the function's arguments, already resolved and typed, along
with the session's settings, and returns a DataFusion expression. That's more
flexible than a plain registry entry, and the flexibility is the point. Many
builders are one line: square root maps directly to DataFusion's version with a
cast to double. Some point at a Sail implementation: concat uses Spark's concat.
Some contain real logic: they inspect argument types, insert casts, rewrite one
call into several expressions, or read the session's time zone or ANSI setting
to decide how to behave.

When the resolver meets a function call (see [07](#doc-07-plan-resolver)), it
looks in three places, in order. First come functions registered in the session,
which are mostly Python UDFs. Then the built-in scalar and generator table. Then
the aggregate table. Window functions in an OVER clause use the window table. A
name found nowhere produces an "unknown function" error. Because session
functions are checked first, a user function named upper replaces the built-in
upper, just as in Spark.

A few calls get special handling. count(\*) is rewritten to count a constant,
because there's no column to count. Aggregates also support DISTINCT, FILTER
(WHERE …), IGNORE NULLS, and ordered-set syntax like
`percentile_cont(0.5) WITHIN GROUP (ORDER BY x)`. Higher-order functions such as
`transform(arr, x -> x + 1)` take lambdas, and named arguments are supported.

## Python UDFs run inside Sail

Spark runs Python UDFs in separate Python worker processes and ships data to
them over sockets. Sail takes a different approach: it **embeds CPython** in its
own process using PyO3, and hands Arrow data to Python directly. There's no
inter-process serialization and no pool of Python processes to manage.

Here's what happens to a UDF. When a PySpark program defines and uses one, it
sends Sail the function pickled with cloudpickle, along with its return type,
its *eval type* (which tells Sail what style of UDF it is), and whether it's
deterministic. Sail stores it as a session function. During resolution, Sail
creates the matching expression or operator, recording the input and output
Arrow types and the relevant session settings (time zone and pandas conversion
options). The first time the function runs, Sail acquires Python's interpreter
lock, unpickles it, and keeps the Python object for reuse. For each batch of
data, Sail converts the input Arrow arrays into what the function expects, calls
it, and converts the results back to Arrow using the declared return type. A
bundled Python helper module does the conversions, using PySpark's own
serializers where possible. If the function raises an exception, the traceback
is captured and reaches the client as a PythonException (see
[03](#doc-03-spark-connect)).

## Styles of Python UDF

PySpark offers several styles of Python function, and Sail supports them all.
The difference between them is mostly about how much data each call to Python
sees.

A plain `@udf` is called once per row. Sail converts each value into a Python
object, calls the function, and converts the result back. `@udf(useArrow=True)`
and Spark 4's Arrow UDFs receive whole Arrow batches. `@pandas_udf` receives
each column as a pandas Series, one batch at a time, and can also come in an
iterator form. A grouped-aggregate pandas UDF collects each group's rows and
returns one value. `applyInPandas` after a groupBy gives the function each
group as a pandas DataFrame and expects rows back, and the cogrouped form hands
two groups to the function together. `mapInPandas` and `mapInArrow` stream each
whole partition through the function, using a dedicated operator (see
[08](#doc-08-logical-physical-plans-and-optimizers)). Python table functions
(`@udtf`) can produce many rows from each input row. Any of these can be
registered with `spark.udf.register` so SQL can call it.

```python
from pyspark.sql.functions import udf, pandas_udf
import pandas as pd

@udf("int")
def plus_one(x): return x + 1                       # called once per row

@pandas_udf("double")
def zscore(v: pd.Series) -> pd.Series:              # called once per batch
    return (v - v.mean()) / v.std()

spark.udf.register("plus_one", plus_one)
spark.sql("SELECT plus_one(id) FROM range(3)").show()
```

## Making UDFs fast

The style you choose matters more than anything else. Take a column of ten
million rows processed in batches of 8,192. A plain `@udf` makes about ten million
Python calls, each converting one value in and one value out. A pandas or Arrow
UDF makes about 1,220 calls, one per batch, and does its conversions on whole
arrays. That's roughly eight thousand times fewer trips into Python, and the
work inside those calls usually happens in pandas, NumPy, or Arrow, which are
written in native code.

There's a second reason vectorized UDFs scale better. All Python code in one
process shares Python's interpreter lock, so when several partitions run
pure-Python code in parallel, they take turns. Native libraries like pandas and
NumPy release the lock for much of their work, so vectorized UDFs spend far less
time waiting.

In cluster mode, each worker runs its own embedded Python, so every worker image
needs the same Python packages your UDFs import.

## Following one UDF

Take `df.select(zscore("v"))`. PySpark pickles zscore and sends a projection
whose expression is an inline UDF of the scalar pandas style returning a double.
Spark Connect translates it into a spec inline-UDF expression. The resolver
creates a Python UDF expression of the right kind, with v's type and the session
settings. Physical planning places it in a projection, and in cluster mode the
pickled bytes travel inside the task's plan. For each batch, Sail takes the
interpreter lock, fetches the cached function, turns column v into a pandas
Series, calls zscore, converts the result into an Arrow array of doubles, and
moves on.

## Adding a built-in function

First check whether DataFusion already has the function with Spark's behavior.
If it does, a new line in the right builder table is enough. If not, write the
implementation in sail-function under the right area, handling NULLs and ANSI
mode, then add a builder that calls it. If you've created a new kind of function
object that must run on workers, make sure the plan codec can serialize it (see
[09](#doc-09-local-and-cluster-execution)). Finally add tests that compare with
Spark (see [17](#doc-17-testing-and-development)), and update the compatibility
data under python/pysail/data/compatibility/functions/, which records each
function's status as supported, partially supported, planned, or not supported.

## Where to look in the code

The builder tables live in crates/sail-plan/src/function/: common.rs defines the
builder styles, scalar/ holds the scalar tables by category, and aggregate.rs,
window.rs, generator.rs, and table/ hold the rest. The order in which the
resolver looks up functions is in crates/sail-plan/src/resolver/expression/function.rs.
Spark-compatible implementations are in crates/sail-function/src/, under scalar/,
aggregate/, and window/. The Python UDF runtime is crates/sail-python-udf: the UDF
styles are under src/udf/, conversion between Arrow and Python in conversion.rs
and the bundled python/spark.py, pickled payload handling in src/cereal/, and UDF
settings in config.rs. The pysail bindings are in crates/sail-python.


---

<a id="doc-16-flight-sql-and-telemetry"></a>

# 16 · Flight SQL, Telemetry, and Observability

This doc covers two things that sit at the edges of Sail. The first is Arrow
Flight SQL, a second front door for tools that speak SQL rather than PySpark.
The second is observability: the logs, traces, and metrics that let you see
what a query actually did.

---

## Arrow Flight SQL: a front door for SQL tools

Spark Connect is ideal for PySpark, but many tools only speak SQL: BI tools,
database IDEs, and programs using ADBC or JDBC drivers. Arrow Flight SQL is a
protocol built for them. It runs SQL over gRPC and returns results as Arrow
record batches, so there's no row-by-row conversion on either side. Sail runs a
Flight SQL server with `sail flight server --port 32010`.

What makes this front end small is reuse. It uses the same SQL parser and
analyzer as `spark.sql` (see [04](#doc-04-sql-parser-and-analyzer)), and the same
resolver, sessions, job runners, catalogs, and formats as Spark Connect.
Supporting a new protocol meant writing a translator and a thin service, not a
second engine.

Flight SQL runs a query in two steps. In the first, the client sends the SQL
text and asks for "flight info". Sail parses the SQL into a spec plan, resolves
and plans it, starts executing it on the session's job runner, and stores the
result stream under a new random handle. It replies with the result's schema
and a ticket containing the handle. In the second step, the client presents the
ticket, and Sail looks up the stream, removes it from storage (so each result
can be read exactly once), and streams the Arrow batches back.

Commands such as CREATE TABLE or INSERT are run to completion in the first step,
with their small result kept in memory until the client fetches it. Queries
start in the first step but stream lazily in the second.

Three limitations matter in practice. First, all Flight SQL clients currently
share **one session**, named flight-default, so temporary views and settings
created by one client are visible to the others. That session is removed after
an hour of inactivity. Second, the handshake is accepted but no authentication
happens, so put the server behind an authenticating gateway in production.
Third, the metadata calls that some tools use to browse tables, and prepared
statements, are minimal or missing; plain SQL like SHOW TABLES and DESCRIBE
works.

```python
import adbc_driver_flightsql.dbapi as flight_sql
conn = flight_sql.connect("grpc://localhost:32010")
cur = conn.cursor()
cur.execute("SELECT region, count(*) FROM parquet.`/data/orders` GROUP BY region")
print(cur.fetch_arrow_table())
```

---

## Observability: seeing what a query did

When a query is slow or wrong, you want to know where time went, how many rows
each step produced, whether anything spilled to disk, and which task failed.
Sail answers these questions in four ways: logs, traces, metrics, and the system
tables.

**Logs** come from Rust's standard logging macros used throughout the code. They
go to standard error, filtered by the RUST_LOG environment variable (info by
default), so `RUST_LOG=sail_execution=debug,info` turns on detailed execution
logs. Each log line is also attached to the current trace span as an event, and
can be exported over OpenTelemetry.

**Traces** record spans for gRPC requests, actor messages, planning steps, job
execution, and every physical operator. **Metrics** turn DataFusion's operator
statistics into OpenTelemetry metrics: output rows, batches, and bytes; compute
time; memory used; spill counts and sizes; expression time; filter input and
output; and a family of join metrics. Flight SQL adds statement counts,
durations, and row counts. Traces and metrics are exported over OTLP to any
compatible collector when you turn them on.

The **system tables** (see [06](#doc-06-session-and-runtime)) show live and
historical jobs, stages, tasks, workers, and sessions through ordinary SQL, and
`EXPLAIN ANALYZE` shows a plan annotated with runtime numbers.

## Why it's built this way

Three design decisions shape Sail's telemetry.

**Instrument once, at the end of planning.** Rather than adding timing code to
every operator, Sail wraps the finished physical plan just before execution.
Every operator is placed inside a tracing wrapper that opens a span and turns
the operator's statistics into metrics. Both the local job runner and the
cluster task runner do this, so instrumentation is complete and consistent
without touching any operator's code.

**Use the same labels everywhere.** Every span and metric carries the job ID,
stage, partition, attempt, and operator ID and name, plus the expression ID and
name for expression timing. That makes it easy to ask "how long did stage 2's
aggregation take across all partitions?" or "which attempt spilled?". The list
of metrics and labels is defined in YAML files, and code is generated from them.

**Off by default, and cheap when off.** Nothing is exported unless you turn it
on, so local development needs no setup.

Trace context also crosses process boundaries. The gRPC client and server layers
pass the standard W3C trace context, and Kubernetes worker pods receive it when
they start, so spans from the driver and from workers join a single trace. Each
process reports its role, server or worker, so you can filter by it.

## Turning it on

Telemetry is controlled by the telemetry.* settings. export_traces,
export_metrics, and export_logs are each false by default. otlp_endpoint
defaults to http://localhost:4317, and otlp_protocol defaults to grpc. Further
settings control export intervals, batch sizes, and queue sizes. To send traces
to a local Jaeger:

```bash
docker run -d -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one
export SAIL_TELEMETRY__EXPORT_TRACES=true
export SAIL_TELEMETRY__OTLP_ENDPOINT=http://localhost:4317
RUST_LOG=info sail spark server
```

Run a query and open Jaeger at http://localhost:16686. A simplified trace for a
local-mode query looks like this:

```text
ExecutePlan (gRPC server span)                                   210 ms
├─ handle_execute_plan
│  ├─ resolve_and_execute_plan                                    18 ms
│  └─ JobRunner::execute                                           2 ms
└─ result stream                                                 188 ms
   └─ operators
      ├─ Projection             rows=10   compute=0.1 ms
      ├─ Aggregate (final)      rows=10   compute=3 ms
      ├─ Repartition            rows=80   compute=1 ms
      ├─ Aggregate (partial)    rows=80   compute=40 ms
      └─ Parquet scan           rows=2.1M bytes=48 MB compute=120 ms
```

Read it from the top down to see where time went. Here, most of it went to the
scan, so better pruning, through filters on partition columns or on sorted
columns, would help most. In cluster mode, the same trace also contains the
driver's message handling and spans from each worker's tasks.

---

## Where to look in the code

The Flight SQL server starts in crates/sail-flight/src/lib.rs (launched from
crates/sail-cli/src/flight/server.rs). The two-step statement handling is in
src/service.rs, stored result streams and their handles in src/state.rs, and
Flight metrics in src/metrics.rs. Telemetry lives in crates/sail-telemetry:
setup and shutdown in src/telemetry.rs, the wrapper that instruments physical
plans in src/execution/physical_plan.rs, the gRPC tracing layers under
src/layers/, the loggers that write to standard error and span events under
src/loggers/, and the metric and label definitions in src/metrics/data/
(registry.yaml and attributes.yaml). The telemetry.* settings are defined in
crates/sail-common/src/config/application.yaml.


---

<a id="doc-17-testing-and-development"></a>

# 17 · Testing and Development

This doc is a practical guide to working on Sail: how the repository is laid
out, how to build it, which kinds of tests exist and what each is good for, how
the "gold data" tests work, and a sensible workflow for making a change.

---

## Finding your way around

Rust code lives in crates/, about 35 crates in total; the README's crate list
says which doc covers each. The Python package, pysail, lives in
python/pysail/, along with its tests, examples, and the compatibility data that
tracks which Spark functions are supported. The public documentation site is in
docs/. Scripts for running Spark's own test suite and for collecting gold data
are in scripts/. Container images, Kubernetes manifests, and a compose file for
local services such as a Hive Metastore or MinIO live in docker/, k8s/,
deployments/, and compose.yml. Two files tie it together: Cargo.toml defines the
Rust workspace, and pyproject.toml defines the Python build and the hatch
environments used for development.

## Building

pysail is a Rust extension module built with PyO3 and maturin, plus some Python
code, so the sail binary and the Python package share all of the engine code.
You build the Rust crates with `cargo build`, and a release CLI binary with
`cargo build -r -p sail-cli --bins`. To use your changes from Python, run
`hatch run maturin develop`, which builds pysail and installs it into the
development environment. `hatch run maturin build` builds a wheel instead, and
`hatch run install-pysail` installs a built wheel into the test environment.
Before sending a change, format and lint with `cargo +nightly fmt`,
`cargo clippy --all-targets --all-features`, and `hatch fmt`.

Builds of the full workspace are slow, so test one crate at a time with
`cargo test -p <crate>` while iterating. The development docs under
docs/development/ have a recipe for reducing build times with linker and
profile settings, as well as setup instructions, debugging tips, and notes on
cross-compiling.

## The kinds of tests, and when to use each

**Rust unit tests** live next to the code they test and are the right tool for
algorithms and small components: partition coalescing, parsers, the plan codec,
actor behavior.

**Gold data tests** lock in Sail's exact output for large sets of inputs: SQL
syntax, expressions, functions, data types, and plans. They're described in
detail below.

**Python integration tests** under python/pysail/tests/ start a real Sail server,
usually inside the test process through pysail, and run PySpark code against it.
Their folders mirror Sail's components, with tests for aggregation, catalogs,
data sources, Delta, Iceberg, execution, functions, Glue, Hive Metastore,
streaming, and more. Some of them use pytest-bdd **feature files**, readable
scenario tables describing behaviors, and some use **snapshot tests** that
store expected plans or outputs in files, refreshed with `--snapshot-update`.

Beyond its own tests, Sail is regularly checked against **Spark's own PySpark
test suite**. Scripts build a patched PySpark (3.5 or 4.1), start a Sail server,
run the suite against it, and produce a report of what passed and failed. This
is how compatibility is tracked over time. The **Ibis** project's backend test
suite is run the same way. A few integration tests need external services, such
as a Kerberos-secured Hive Metastore, and are marked as ignored so they only run
when asked for explicitly.

## Gold data: reviewing behavior like code

A gold data file records the *actual* output Sail produces for many inputs, in
JSON, and is committed and reviewed like code. An entry looks like this:

```json
{ "input":  { "query": "SELECT map(1.0, '2', 3.0, '4');",
              "result": ["{1.0:\"2\",3.0:\"4\"}"],
              "schema": { "type": "struct", "fields": [ ... ] } },
  "output": { "success": "ok" } }
```

The input side, the query along with the result and schema Spark produced, was
collected once from Spark's own test suite by a bootstrap script, which is rerun
only when Spark is upgraded. The output side records what Sail does with that
input: success, or the error message it produced. When you change behavior, you
regenerate the outputs and commit them:

```bash
env SAIL_UPDATE_GOLD_DATA=1 cargo test     # rewrite outputs from the current code
cargo test                                  # confirm they're stable
```

The value of this approach shows up in code review. Suppose you fix concat so it
returns NULL when any argument is NULL. The diff shows exactly that change in
the one entry that exercises it. If ten other functions' outputs changed too,
that's obvious in the diff and worth investigating before merging. Gold data
lives under tests/gold_data/ in the crates that use it, such as sail-sql-parser
for syntax and sail-spark-connect for data types, expressions, functions by
category, plans, and table schemas.

## A workflow for fixing a function

Suppose a Spark function returns the wrong answer. Start by reproducing it:
build pysail with `hatch run maturin develop` and write a small failing test in
python/pysail/tests/spark/function/, or a quick script against a development
server. Find the function's builder in sail-plan's function tables and, if it
has its own implementation, the code in sail-function (see
[15](#doc-15-functions-and-python-udfs)). Fix it, and add a Rust unit test if the
logic stands on its own.

Then regenerate gold data with `env SAIL_UPDATE_GOLD_DATA=1 cargo test` and read
the diff carefully, making sure only the outputs you intended changed. Run the
Python tests for the area and refresh snapshots if needed. If the function's
support status changed, update the compatibility data. Finish by formatting and
linting.

## A workflow for a cluster-only bug

If something fails only in cluster mode, reproduce it with a local cluster by
setting SAIL_MODE=local-cluster (see [09](#doc-09-local-and-cluster-execution)).
Turn on detailed logs with `RUST_LOG=sail_execution=debug,info`: the task runner
logs each task's final plan, and the driver logs its state changes. Query
system.execution.stages and system.execution.tasks to see which stage and
attempt failed. If an operator can't be sent to workers, look at the plan codec
in sail-execution's codec.rs.

## A few habits that help

Test one crate at a time rather than the whole workspace. Prefer gold data and
feature files over ad-hoc assertions when the thing you're testing is Spark
behavior, because reviewers can see the exact change and the expected outputs
are easy to regenerate. Use RUST_LOG with a crate name and debug to see what
Sail is doing: every Spark Connect request and response, every task plan, and
every storage call is logged at that level. And when something is running on a
cluster, the system tables are the fastest way to see what's going on.

## Where to look in the code

The development docs are in docs/development/ (setup/, build/, recipes/, and
spark-tests/), and contributor guidance is in CONTRIBUTING.md and
docs/development/architecture-contributor-guide.md. The gold data helper,
including the SAIL_UPDATE_GOLD_DATA switch, is in crates/sail-common/src/tests.rs,
with supporting code in crates/sail-gold-test. Gold data files are under
tests/gold_data/ in each crate that uses them, and the bootstrap script is in
scripts/spark-gold-data/. The Spark test harness is in scripts/spark-tests/, the
Python tests in python/pysail/tests/, and the hatch environments in
pyproject.toml.

