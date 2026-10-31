# 03 · Spark Connect: The Front Door

> **You'll learn:** how a PySpark client talks to Sail, how Spark protobuf
> messages become Sail plans, how results stream back, and how sessions,
> configuration, errors, reconnection, and cancellation work.
>
> **Crate:** `sail-spark-connect`


## In One Minute

- PySpark builds a **plan** in the client and sends it over gRPC. Sail is the
  server that runs it, so existing PySpark code works unchanged.
- Each request is translated **once** into Sail's spec plan, and nothing deeper
  in Sail sees Spark protobuf.
- Results stream back as Arrow batches, with **heartbeats** (every 15 seconds)
  and **reattach** support for dropped connections.
- `spark.sql("SELECT …")` is lazy, but `spark.sql("CREATE …")` runs immediately,
  just like in Spark.
- Errors come back as the Spark exception class PySpark expects
  (`AnalysisException`, `ParseException`, …).

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
   (see [05 · The Spec Plan Model](05-spec-plan-and-config.md)),
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
(see [06 · Sessions and Runtime](06-session-and-runtime.md)) returns the
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
   [07 · The Plan Resolver](07-plan-resolver.md)).
5. **Execute.** The physical plan goes to the session's job runner, which returns
   a stream of Arrow batches (see
   [09 · Local and Cluster Execution](09-local-and-cluster-execution.md)).
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

### 4.5 What travels over the wire for one `collect()`

Simplified message sequence for a query that returns two batches:

```text
Client → Server   ExecutePlan(session=S, operation=O, plan=Root(...), reattachable=true)
Server → Client   ExecutePlanResponse(response_id=r1, schema=<id: long, name: string>)
Server → Client   ExecutePlanResponse(response_id=r2, arrow_batch=<rows 0..8191>)
Client → Server   ReleaseExecute(operation=O, until=r2)        # server can drop r1, r2
Server → Client   ExecutePlanResponse(response_id=r3, arrow_batch=<rows 8192..9999>)
Server → Client   ExecutePlanResponse(response_id=r4, result_complete)
Client → Server   ReleaseExecute(operation=O, all)
```

If the connection breaks after `r2`, the client sends
`ReattachExecute(operation=O, last_response_id=r2)` and Sail replays `r3`, `r4`
from its buffer, then keeps streaming.

### 4.6 What an error looks like to the user

```python
>>> spark.sql("SELECT no_such_column FROM range(1)").show()
pyspark.errors.exceptions.connect.AnalysisException: ... cannot resolve attribute ...
```

Behind the scenes: the resolver fails, the error is classified as an analysis
error, and Sail sends gRPC status `INTERNAL` with error details naming
`org.apache.spark.sql.AnalysisException`. PySpark reads the class name and
raises the matching Python exception, which is why `except AnalysisException:`
blocks in existing code keep working.

### 4.7 Lazy vs. eager, side by side

| Call | When Sail does the work | What the client gets back immediately |
|------|-------------------------|----------------------------------------|
| `df = spark.table("t").filter(...)` | Never (client-side only) | Nothing is sent |
| `df.count()` | Now | The count |
| `df2 = spark.sql("SELECT * FROM t")` | Only parsing and translation now. Execution later. | A relation describing the query |
| `spark.sql("CREATE TABLE t2 (x INT)")` | Now, the table is created | An empty result |
| `df.write.parquet(path)` | Now | Completion |

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
(see [05 · The Spec Plan Model](05-spec-plan-and-config.md)), convert the
protobuf fields into it, then teach the resolver to handle it (see
[07 · The Plan Resolver](07-plan-resolver.md)).

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
