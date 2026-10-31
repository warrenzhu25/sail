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
[02](02-entry-points-and-server.md).

## Connecting creates a session

When the analyst calls `getOrCreate()`, PySpark invents a session ID and sends
its first request. The server has never seen this ID, so it builds a new
session: a bundle holding everything this client will need. The session gets a
catalog manager that knows about the `glue` catalog, a registry of file and
table formats, a fresh driver for running distributed jobs, a memory pool,
file caches, and a registry that will create storage clients on demand. Each
session gets its own driver, so one user's work never interferes with
another's. Sessions are explained in [06](06-session-and-runtime.md), catalogs
in [11](11-catalogs.md).

The `spark.conf.set` call is a small configuration request. Sail stores the
time zone in the session, where it will later influence how timestamps are
interpreted ([03](03-spark-connect.md)).

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
called amount, and so on ([03](03-spark-connect.md),
[05](05-spec-plan-and-config.md)).

## Resolving gives every name a meaning

Now the resolver goes to work ([07](07-plan-resolver.md)). It asks the catalog
manager about glue.sales.orders. The manager recognizes glue as a catalog and
asks the Glue service, through a cache and on the I/O thread pool, for the table
definition. Glue reports a Parquet table stored under s3://lake/sales/orders/,
partitioned by date.

The resolver hands that description to the Parquet table format
([12](12-data-sources-and-formats.md)), which needs to know which files exist.
It asks the storage registry for an S3 client. The registry has never seen this
bucket, so it creates one lazily: the AWS credentials are loaded and the
bucket's region is detected the first time the client is actually used
([14](14-object-storage.md)). The format lists the files, skips bookkeeping
files like _SUCCESS, and returns a table source.

With the table in hand, the resolver gives each column a private name so that
nothing can be confused later, matches amount in the filter, finds count and
sum in its function tables ([15](15-functions-and-python-udfs.md)), builds the
aggregation, and resolves the sort against the new total column. The result is
a DataFusion logical plan plus the names the user should see: region, n, total.

## Planning decides how to run it

DataFusion's optimizer, extended with Sail's rules, now improves the plan
([08](08-logical-physical-plans-and-optimizers.md)). The filter moves into the
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
([09](09-local-and-cluster-execution.md)). The driver cuts the plan wherever
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
Arrow Flight ([10](10-shuffle-and-streams.md)). They finish the totals and sort.
The single last task merges the sorted pieces and formats the first twenty rows
as a text table.

## Results stream back

The driver hands the final stream back, and the Spark Connect front end sends it
to PySpark: first the schema, then the batch containing the formatted table,
then a completion message. If the query had taken a long time, Sail would have
sent small heartbeat messages to keep the connection alive. If the network had
dropped, PySpark could have reconnected and resumed from the last message it
received ([03](03-spark-connect.md)). PySpark prints the table.

Afterwards, the tasks report success, the job is marked done, and its streams
are cleaned up. Telemetry has recorded a span for every operator in every task
([16](16-flight-sql-and-telemetry.md)), and the job now shows up in
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
same spec plan the DataFrame produced ([04](04-sql-parser-and-analyzer.md)).
From the resolver onward, the two are indistinguishable.

## When something goes wrong

The error message usually tells you where in this journey the query stopped. A
ParseException means the SQL itself was malformed ([04](04-sql-parser-and-analyzer.md)).
TABLE_OR_VIEW_NOT_FOUND comes from the catalog lookup ([11](11-catalogs.md)).
"Cannot resolve attribute" and "unknown function" come from the resolver
([07](07-plan-resolver.md), [15](15-functions-and-python-udfs.md)). Access
errors on S3 point at storage credentials ([14](14-object-storage.md)). A "task
scheduling timeout" or "lost worker" points at cluster capacity or worker health
([09](09-local-and-cluster-execution.md)). A worker killed for running out of
memory usually means the memory pool was left unbounded
([06](06-session-and-runtime.md)).
