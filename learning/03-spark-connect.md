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
ask the session manager (see [06](06-session-and-runtime.md)) for that session,
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
[05](05-spec-plan-and-config.md)): a read node, a filter node holding the
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
[07](07-plan-resolver.md)), which return a physical plan. That plan is handed to
the session's job runner (see [09](09-local-and-cluster-execution.md)), which
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
