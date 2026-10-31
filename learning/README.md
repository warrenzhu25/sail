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

If you prefer one long page, [ALL-IN-ONE.md](ALL-IN-ONE.md) merges every document
into a single file.

---

## Where to start

Start with [01 · End to End](01-end-to-end-workflow.md). It follows one query
through every component, so you'll see how the pieces fit before diving into
any of them. After that, pick a path. If you're new to Sail, read 02, 03, 05,
07, and 09 in order. If you'll work on SQL or Spark compatibility, read 04, 05,
07, 15, and 17. For distributed execution, read 09, 10, 06, and 08. For storage,
formats, and catalogs, read 11 through 14.

| # | Document | What it explains |
|---|----------|------------------|
| 01 | [End to End](01-end-to-end-workflow.md) | One query through all of Sail |
| 02 | [Entry Points and the Server Framework](02-entry-points-and-server.md) | The sail binary, configuration, actors, gRPC servers |
| 03 | [Spark Connect](03-spark-connect.md) | How PySpark talks to Sail |
| 04 | [SQL Parser and Analyzer](04-sql-parser-and-analyzer.md) | How SQL text becomes a plan |
| 05 | [The Spec Plan](05-spec-plan-and-config.md) | Sail's neutral description of a query |
| 06 | [Sessions and the Runtime](06-session-and-runtime.md) | What a session holds, memory, spilling, caches |
| 07 | [The Plan Resolver](07-plan-resolver.md) | How names get their meaning |
| 08 | [Plans, Extension Nodes, and Optimizers](08-logical-physical-plans-and-optimizers.md) | Sail's own operators and optimizer rules |
| 09 | [Local and Cluster Execution](09-local-and-cluster-execution.md) | Drivers, workers, stages, and tasks |
| 10 | [Shuffle and Streams](10-shuffle-and-streams.md) | How data moves between tasks |
| 11 | [Catalogs](11-catalogs.md) | How table names are found |
| 12 | [Data Sources and File Formats](12-data-sources-and-formats.md) | Reading and writing files |
| 13 | [Delta Lake and Iceberg](13-lakehouse-delta-and-iceberg.md) | Lakehouse tables |
| 14 | [Object Storage](14-object-storage.md) | S3, GCS, Azure, and friends |
| 15 | [Functions and Python UDFs](15-functions-and-python-udfs.md) | Built-in functions and Python code |
| 16 | [Flight SQL and Telemetry](16-flight-sql-and-telemetry.md) | SQL clients, logs, traces, metrics |
| 17 | [Testing and Development](17-testing-and-development.md) | Building, testing, making changes |

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
