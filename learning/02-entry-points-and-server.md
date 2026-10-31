# 02 · Entry Points, Configuration, and the Server Framework

> **You'll learn:** how Sail starts, the roles one binary can play, how
> configuration is loaded, and the actor and gRPC building blocks every service
> uses.
>
> **Crates:** `sail-cli`, `sail-python`, `sail-server`, `sail-common`
> (configuration and runtime)


## In One Minute

- There's **one binary**, `sail`. Its subcommand picks the role: Spark Connect
  server, shell, script runner, MCP server, Flight SQL server, or cluster
  worker.
- **Configuration** = YAML defaults compiled into the binary, overridden by
  `SAIL_*` environment variables (`__` becomes `.`).
- Every process has two thread pools: **primary** (queries, RPC) and **io**
  (storage, catalogs).
- Long-lived coordinators (driver, workers, session manager) are **actors**:
  one message at a time, no shared locks.
- All gRPC services are built with one shared **server builder** (health
  checks, reflection, tracing, keep-alives).

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
   [16 · Flight SQL, Telemetry, and Observability](16-flight-sql-and-telemetry.md)).
   A guard object shuts telemetry down cleanly on exit.
7. **Listen.** A TCP listener binds to the address, and the Spark Connect
   service is served on it (see [03 · Spark Connect](03-spark-connect.md)).
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
[09 · Local and Cluster Execution](09-local-and-cluster-execution.md)):

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
[06 · Sessions and Runtime](06-session-and-runtime.md)), sets up telemetry
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

### 4.5 How configuration is resolved: a worked example

Suppose the YAML default for `execution.batch_size` is `8192`, and you start
Sail like this:

```bash
export SAIL_EXECUTION__BATCH_SIZE=4096
export SAIL_CATALOG__DEFAULT_DATABASE='["analytics"]'
export SAIL_MODE=local-cluster
sail spark server
```

| Key | YAML default | Environment | Final value |
|-----|--------------|-------------|-------------|
| `execution.batch_size` | `8192` | `4096` | **4096** |
| `catalog.default_database` | `["default"]` | `["analytics"]` | **["analytics"]** (arrays use inline TOML-style syntax) |
| `mode` | `local` | `local-cluster` | **local-cluster** |
| `cluster.worker_task_slots` | `8` | (unset) | **8** |

A misspelled variable like `SAIL_EXECUTION__BATCHSIZE=4096` doesn't fail
silently. Unknown fields are rejected and the server refuses to start, so typos
surface right away.

### 4.6 How an actor handles a timeout: a worked example

Actors never sleep or block. To act later, they **send themselves a message
with a delay**. Here's how the session manager detects an idle session (see
[06](06-session-and-runtime.md)):

```text
t=0s     request arrives      → actor records "active at 0s", schedules Probe(at=0s) for t=900s
t=300s   request arrives      → actor records "active at 300s", schedules Probe(at=300s) for t=1200s
t=900s   Probe(at=0s)  fires  → last active (300s) is later than 0s → ignore
t=1200s  Probe(at=300s) fires → last active (300s) is not later → session is idle → delete it
```

The same pattern is used for worker heartbeats, task launch timeouts, and stream
creation timeouts in the driver (see [09](09-local-and-cluster-execution.md)).

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
