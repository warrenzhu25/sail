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
analyzer as `spark.sql` (see [04](04-sql-parser-and-analyzer.md)), and the same
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

The **system tables** (see [06](06-session-and-runtime.md)) show live and
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
