# 16 · Arrow Flight SQL, Telemetry, and Observability

> **You'll learn:** how SQL clients connect to Sail through Arrow Flight SQL, and
> how Sail produces logs, traces, and metrics (with OpenTelemetry) so you can see
> what a query did.
>
> **Crates:** `sail-flight`, `sail-telemetry`, plus the system tables from
> [06](06-session-and-runtime.md)


## In One Minute

- **Flight SQL** lets SQL tools query Sail over gRPC and get Arrow results. It
  reuses the same parser, planner, and engine as Spark Connect.
- Flight SQL runs a query in two steps: *GetFlightInfo* (plan and start) returns
  a ticket, then *DoGet* streams the results (readable once).
- All Flight clients currently share **one session**, and there's no built-in
  authentication.
- **Telemetry** is OpenTelemetry-based: logs (stderr and span events), traces
  (every operator), and metrics (rows, bytes, time, memory, spills, joins). It's
  all **off by default**.
- Spans and metrics carry the same labels (job, stage, partition, attempt,
  operator), so you can slice by query or stage.

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
| 1 | `GetFlightInfo` (statement query) | Parse the SQL ([04](04-sql-parser-and-analyzer.md)) into a spec plan. Get the session. Resolve and plan ([07](07-plan-resolver.md)). **Start execution** on the job runner. Store the result stream under a new **query handle**. Return a `FlightInfo` with the schema and a ticket containing the handle. |
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

### A.5 What a trace looks like

A simplified trace for one Spark Connect query in local mode:

```text
ExecutePlan (gRPC server span)                                   210 ms
├─ handle_execute_plan
│  ├─ resolve_and_execute_plan                                    18 ms
│  └─ JobRunner::execute                                           2 ms
└─ ExecutorTaskContext::next (stream)                            188 ms
   └─ job 7
      ├─ ProjectionExec        rows=10   compute=0.1 ms
      ├─ AggregateExec(final)  rows=10   compute=3 ms
      ├─ RepartitionExec       rows=80   compute=1 ms
      ├─ AggregateExec(partial)rows=80   compute=40 ms
      └─ DataSourceExec parquet rows=2.1M bytes=48 MB compute=120 ms
```

Read it top-down to see where time went. Here most of it is in the scan, so
pruning (filters on partition columns or sorted columns) would help most.

---

## Part B: Telemetry and Observability

### B.1 What you get

| Signal | Source | Where it goes |
|--------|--------|---------------|
| **Logs** | Rust `log` macros throughout Sail | stderr (filtered by `RUST_LOG`, default `info`). Also attached to the current trace span as events, and optionally exported over OTLP. |
| **Traces** | `fastrace` spans: gRPC requests, actor messages, planning steps, job execution, **every physical operator** | OTLP collector (Jaeger, Tempo, …) when `telemetry.export_traces=true` |
| **Metrics** | Operator metrics from DataFusion, plus Flight statement metrics | OTLP when `telemetry.export_metrics=true` |
| **System tables** | Live job, stage, task, worker, and session state | `SELECT * FROM system.execution.jobs`, … (see [06](06-session-and-runtime.md)) |
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
