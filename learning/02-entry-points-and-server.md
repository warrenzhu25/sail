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
[15](15-functions-and-python-udfs.md)). Some Python libraries, such as
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
the subject of [09](09-local-and-cluster-execution.md).

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
