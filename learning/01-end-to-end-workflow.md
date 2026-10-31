# 01 · End-to-End: One Query Through All of Sail

> **You'll learn:** the complete journey of a single PySpark query, from the
> client to Parquet files on S3 and back, touching every component once. Read
> this first, then dive into the component docs it links to.


## In One Minute

- A query goes through seven stages: **connect → build the DataFrame (client
  only) → send → resolve → plan → execute → stream back**.
- Nothing runs on the server until an **action** (`show`, `collect`, `count`,
  a write) is called.
- The same planning pipeline serves local and cluster mode. Only the *execute*
  stage differs.
- Each step below links to the doc that explains it in depth.

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
| `sail spark server` loads its configuration (YAML defaults + `SAIL_*` environment variables), creates the **primary** and **io** runtimes, sets up telemetry, and serves the Spark Connect gRPC service through the shared server builder (health, reflection, tracing). | [02](02-entry-points-and-server.md) |

## Stage 1: Connecting and Setting Configuration

| # | What happens | Where |
|---|--------------|-------|
| 1 | `getOrCreate()` generates a session ID. The first RPC reaches the server. | [03](03-spark-connect.md) |
| 2 | The **session manager actor** finds no session with that ID, so it asks the **server session factory** to build one: table format registry, **catalog manager** (with `glue` and the reserved `system` catalog), activity tracker, a **cluster job runner** (with its own **driver** actor), system tables, and the runtime environment (memory pool, spill directories, file caches, **dynamic object store registry**). The Spark front end adds its Spark session extension. | [06](06-session-and-runtime.md), [11](11-catalogs.md) |
| 3 | `spark.conf.set(...)` is a `Config` RPC. The value is stored in the session's Spark configuration and later becomes the **plan config**'s `session_timezone`. | [03](03-spark-connect.md) |

## Stage 2: Building the DataFrame (no execution yet)

| # | What happens | Where |
|---|--------------|-------|
| 4 | `spark.table`, `filter`, `groupBy`, `agg`, and `orderBy` only build a relation tree **in the client**. Nothing is sent until an action runs. | |

## Stage 3: `show()` Sends the Plan

| # | What happens | Where |
|---|--------------|-------|
| 5 | `show()` sends `ExecutePlan` with a root relation: ShowString(Sort(Aggregate(Filter(Read("glue.sales.orders"))))), a new operation ID, and the *reattachable* option. | [03](03-spark-connect.md) |
| 6 | The relation is translated into a **spec plan**: a show-string command over a sort, an aggregate (grouping `region`, aggregates `count(*) AS n` and `sum(amount) AS total`), a filter with the unresolved function `>` applied to `amount` and the literal `100`, and a read of the named table `["glue","sales","orders"]`. Names are still unresolved. | [05](05-spec-plan-and-config.md) |

## Stage 4: Resolving

| # | What happens | Where |
|---|--------------|-------|
| 7 | The **resolver** starts with an empty state. For the read, the catalog manager resolves `glue.sales.orders` to catalog `glue`, namespace `sales`, table `orders`. The cache wrapper misses, the runtime wrapper runs the call on the io runtime, and the **Glue provider** returns a table status: format `parquet`, location `s3://lake/sales/orders/`, columns, partitioned by `dt`. | [07](07-plan-resolver.md), [11](11-catalogs.md) |
| 8 | The resolver asks the **Parquet table format** for a source. It parses the location, lists files through the **S3 store** (created lazily on the io runtime, with AWS credentials and the bucket region), skips hidden files, and builds a listing table with partition column `dt`. | [12](12-data-sources-and-formats.md), [14](14-object-storage.md) |
| 9 | Columns get internal IDs (`#0` order_id, `#1` region, `#2` amount, `#3` dt). `amount > 100` resolves to `#2 > 100`, with the literal coerced to `amount`'s type. `count(*)` and `sum` are found in the **aggregate table**. `desc(total)` resolves against the aggregate's output. The result is a **named plan**: the DataFusion logical plan plus user names `[region, n, total]`, wrapped in a show-string node. | [07](07-plan-resolver.md), [15](15-functions-and-python-udfs.md) |

## Stage 5: Optimizing and Physical Planning

| # | What happens | Where |
|---|--------------|-------|
| 10 | **Logical optimization:** the filter is pushed into the scan and only `region` and `amount` are read. | [08](08-logical-physical-plans-and-optimizers.md) |
| 11 | **Physical planning:** the extension planner maps show-string to its operator. DataFusion plans the scan (Parquet with pruning), a **partial aggregate**, a **hash repartition on `region`**, a **final aggregate**, a **sort**, and a **sort-preserving merge** to a single output. | [08](08-logical-physical-plans-and-optimizers.md) |
| 12 | **Physical optimization:** enforce distribution and sorting, pushdowns, and Sail's checks. Output columns are renamed from IDs to `region`, `n`, `total`. | [08](08-logical-physical-plans-and-optimizers.md), [07](07-plan-resolver.md) |

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
| 13 | The **cluster job runner** sends *execute job* to the session's **driver**. | [09](09-local-and-cluster-execution.md) |
| 14 | The **job graph builder** cuts the plan at the repartition and the merge: **Stage 0** (scan → partial aggregate, output hashed by `region` into 8 channels), **Stage 1** (shuffle read → final aggregate → sort, 8 partitions), **Stage 2** (merge read → show string, 1 partition). | [09](09-local-and-cluster-execution.md) |
| 15 | When the session's driver was created (step 2), it already asked the **Kubernetes worker manager** for `worker_initial_count` (4) pods running `sail worker`. All links are pipelined, so the three stages form **one region**. They share slots by partition, so the region needs about 8 slots (the widest stage). | [09](09-local-and-cluster-execution.md) |
| 16 | The workers start, create **worker sessions**, register with the driver, and begin heartbeats. As soon as enough slots are free, the region is placed, and each task's **definition** (plan serialized by the codec, input locators, output locators) is sent to its worker. More workers are requested only if the region doesn't fit. | [09](09-local-and-cluster-execution.md), [06](06-session-and-runtime.md) |
| 17 | On each worker, the **task runner** decodes the plan, adds **shuffle write** at the root of each Stage 0 task and **shuffle read** where Stage 1 and 2 read their inputs, wraps operators with tracing, executes, and reports *running*. | [09](09-local-and-cluster-execution.md), [16](16-flight-sql-and-telemetry.md) |
| 18 | **Stage 0 tasks** read their Parquet files directly from S3 (row groups pruned by `amount` statistics), compute partial counts and sums, hash rows by `region`, and push each slice into an **in-memory stream** keyed (job, 0, p, 0, channel). | [12](12-data-sources-and-formats.md), [10](10-shuffle-and-streams.md) |
| 19 | **Stage 1 task q** opens channel *q* of every Stage 0 task: local streams directly, remote ones through **Arrow Flight `DoGet`** to peer workers. It merges them, computes final aggregates for its regions, and sorts. | [10](10-shuffle-and-streams.md) |
| 20 | **Stage 2** reads all Stage 1 outputs, merge-sorts by `total`, and formats the top 20 rows as a text table. | [10](10-shuffle-and-streams.md), [08](08-logical-physical-plans-and-optimizers.md) |
| 21 | The driver opens the final stage's output stream and returns it as the job's result stream. | [09](09-local-and-cluster-execution.md) |

## Stage 7: Streaming the Response

| # | What happens | Where |
|---|--------------|-------|
| 22 | The Spark Connect **executor** sends a schema message, the Arrow batch containing the formatted table, and "complete", buffering each by response ID (with empty heartbeat batches every 15 seconds if needed). PySpark prints the table. | [03](03-spark-connect.md) |
| 23 | Tasks report *succeeded*. The region and job succeed. Streams are cleaned up. Telemetry records spans and metrics for every operator, labeled by job, stage, partition, and attempt. `system.execution.jobs` now lists the job. | [09](09-local-and-cluster-execution.md), [16](16-flight-sql-and-telemetry.md) |
| 24 | After 60 seconds without tasks, idle workers are stopped. After 15 minutes without requests, the session is removed, its driver shuts down, and its history is kept. | [09](09-local-and-cluster-execution.md), [06](06-session-and-runtime.md) |

## The Whole Journey on One Line Per Component

| Component | Input | Output |
|-----------|-------|--------|
| PySpark client | Your Python code | A protobuf relation tree |
| Spark Connect (03) | Protobuf relation | Spec plan |
| Session manager (06) | Session ID | Session (catalogs, formats, job runner, runtime) |
| Resolver (07) | Spec plan + plan config | DataFusion logical plan + user names |
| Catalog (11) + table format (12) + object store (14) | Table name | Table source over a list of files |
| Optimizers (08) | Logical plan | Optimized physical plan |
| Job graph builder (09) | Physical plan | Stages and tasks |
| Driver + workers (09) | Tasks | Running operators on workers |
| Shuffle (10) | Partial results | Results regrouped by key |
| Spark Connect executor (03) | Arrow batches | Responses sent to the client |

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
an equivalent spec plan ([04](04-sql-parser-and-analyzer.md)). Everything after
that is the same.

---

## What Could Go Wrong, and Where to Look

| Failure | Component |
|---------|-----------|
| `ParseException` | [04 · SQL parser](04-sql-parser-and-analyzer.md) |
| `TABLE_OR_VIEW_NOT_FOUND` | [11 · Catalogs](11-catalogs.md) |
| `cannot resolve attribute` / `unknown function` | [07 · Resolver](07-plan-resolver.md), [15 · Functions](15-functions-and-python-udfs.md) |
| Access denied on S3 | [14 · Object storage](14-object-storage.md) |
| "task scheduling timeout" or a lost worker | [09 · Execution](09-local-and-cluster-execution.md) |
| Reader stuck on a pending stream | [10 · Shuffle and streams](10-shuffle-and-streams.md) |
| Worker out of memory | [06 · Sessions and runtime](06-session-and-runtime.md) (memory pool) |
| Long query drops behind a proxy | [03 · Spark Connect](03-spark-connect.md) (heartbeats and reattach) |
