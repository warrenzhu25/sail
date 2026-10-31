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

**How each document is laid out.** Each starts with **In One Minute**, five bullets you can read on their own. Then come *What* (what the component does), *Why* (design reasons), *How* (step by step), worked **examples** with concrete inputs and outputs, an **end-to-end** walkthrough, **common problems**, and **Where to Look** (concept → file).

Prefer one long page? Read [ALL-IN-ONE.md](ALL-IN-ONE.md), which merges every document below into a single file.

---

## Reading Order

| # | Document | Read it to understand |
|---|----------|------------------------|
| 01 | [End-to-End Workflow](01-end-to-end-workflow.md) | **Start here.** One query through every component. |
| 02 | [Entry Points, Configuration, and the Server Framework](02-entry-points-and-server.md) | The `sail` binary and its roles, configuration loading, actors, gRPC servers |
| 03 | [Spark Connect](03-spark-connect.md) | How PySpark talks to Sail: RPCs, sessions, streaming responses, errors |
| 04 | [SQL Parser and Analyzer](04-sql-parser-and-analyzer.md) | SQL text → tokens → AST → spec plan |
| 05 | [The Spec Plan Model](05-spec-plan-and-config.md) | Sail's protocol-neutral intermediate representation |
| 06 | [Sessions and the Runtime Environment](06-session-and-runtime.md) | Session contents and lifecycle, memory pools, spilling, caches, system tables |
| 07 | [The Plan Resolver](07-plan-resolver.md) | Spec plan → DataFusion plan: names, columns, functions, tables, commands |
| 08 | [Logical and Physical Plans, Extension Nodes, and Optimizers](08-logical-physical-plans-and-optimizers.md) | Sail's custom operators, optimizer rules, join reordering |
| 09 | [Local and Cluster Execution](09-local-and-cluster-execution.md) | Job runners, driver, stages, tasks, slots, workers, failures |
| 10 | [Shuffle and Streams](10-shuffle-and-streams.md) | How data moves between tasks: memory, disk, Arrow Flight |
| 11 | [Catalogs](11-catalogs.md) | Name resolution, temporary views, Glue, HMS, Iceberg REST, Unity, OneLake |
| 12 | [Data Sources and File Formats](12-data-sources-and-formats.md) | Parquet, CSV, JSON, …: the table format interface, options, listing, Python data sources |
| 13 | [Lakehouse Formats: Delta Lake and Iceberg](13-lakehouse-delta-and-iceberg.md) | Snapshots, logs, manifests, commits, deletion vectors, `MERGE` |
| 14 | [Object Storage](14-object-storage.md) | S3, GCS, Azure, HDFS, HTTP, Hugging Face |
| 15 | [Functions and Python UDFs](15-functions-and-python-udfs.md) | Built-in Spark functions and embedded Python execution |
| 16 | [Flight SQL, Telemetry, and Observability](16-flight-sql-and-telemetry.md) | The Flight SQL front end, logs, traces, metrics |
| 17 | [Testing and Development Workflow](17-testing-and-development.md) | Building, gold data, Python tests, the Spark test suite |

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
