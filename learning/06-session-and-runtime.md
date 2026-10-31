# 06 · Sessions and the Runtime Environment

A session is everything Sail keeps on behalf of one client: settings, catalogs,
temporary views, the machinery that runs its queries, and the resources those
queries may use. This doc explains what goes into a session, why it's assembled
the way it is, how sessions are created and cleaned up, and how the runtime
environment controls memory, spilling, caches, and storage access.

---

## A session is a DataFusion context with Sail's services attached

Sail builds on Apache DataFusion, whose central object is the SessionContext. It
holds configuration, a runtime environment, optimizer rules, a query planner,
and registered functions. Sail creates one SessionContext per client session and
customizes it heavily.

The key to that customization is a DataFusion feature called *extensions*. A
session's configuration can carry arbitrary typed objects, and any code holding
the session can fetch one by type. Sail attaches its services this way: a
catalog manager that knows about databases and tables, a registry of table
formats (Parquet, CSV, Delta, Iceberg, and so on), a job service that runs
queries, an activity tracker used for idle timeouts, a service that answers
queries on the system tables, and a cache for Delta table state. The Spark
Connect front end adds one more: a Spark session extension holding the client's
IDs and Spark settings.

Why extensions rather than wrapping DataFusion's types in Sail's own? Because
wrapping would mean forking or re-exporting large parts of DataFusion's API, and
every new service would change function signatures throughout the codebase.
With extensions, a function that needs the catalog manager simply asks the
session for it.

## Sail turns DataFusion's defaults off

A session built by Sail deliberately leaves out DataFusion's built-in catalog,
its information schema, its built-in table formats, and its built-in functions.
Sail provides all of these itself, because Spark's behavior differs. For
example, Spark's concat returns NULL when any argument is NULL, while
DataFusion's ignores NULL arguments. Leaving DataFusion's version registered
would make it too easy for the wrong one to be used.

There's one exception that shows how tightly the pieces fit. One of
DataFusion's optimizer rules quietly assumes that a function called first_value
is registered. Sail registers that one function explicitly so the rule keeps
working.

The session also receives Sail's optimizer rules (with lakehouse rules placed
ahead of DataFusion's own; see [08](08-logical-physical-plans-and-optimizers.md)),
Sail's query planner, and execution settings copied from configuration: the
batch size (8,192 rows by default), the number of partitions to target (by
default one per CPU core), statistics collection, and about thirty Parquet read
and write settings.

## Server sessions and worker sessions

Sail builds two kinds of session, and the difference explains a whole class of
bugs.

A **server session** serves a client. It owns user-visible state such as
temporary views and settings, can submit jobs, and resolves plans. It has
everything described above.

A **worker session** exists on cluster workers and only runs pieces of plans it
receives from the driver. It needs no catalogs, no user state, and no job
runner. It does need a runtime environment for memory and storage, and it keeps
DataFusion's default functions, because when a worker decodes a plan fragment,
every function the fragment mentions must be found by name.

So if a query works in local mode but fails only on a cluster, a good first
question is whether something was set up in the server session but not in the
worker session.

## Each session gets its own job runner

When a server session is built, it creates a new job runner according to the
mode setting: an in-process runner for local mode, or a cluster runner with its
own driver for local-cluster and Kubernetes modes (see
[09](09-local-and-cluster-execution.md)). In cluster mode, this means **every
session gets its own driver and its own workers**. One user's heavy job can't
take over another session's workers, and shutting down a session cleanly shuts
down everything it started.

## The runtime environment: memory, spilling, caches, storage

Each session also has a DataFusion *runtime environment*, which controls the
physical resources its queries use.

**Memory.** Operators such as sorts, joins, and aggregations ask a memory pool
before using large amounts of memory. Sail offers three pools, chosen with
runtime.memory_pool.type. The **unbounded** pool, which is the default, never
says no. The **greedy** pool hands out memory first come, first served, up to a
maximum. The **fair** pool shares a maximum among operators that are able to
spill, so no single operator can starve the others.

The difference matters a lot in practice. Suppose you use the fair pool with an
8 GiB limit, and a query runs two sorts and a hash aggregation at the same time.
While only the aggregation is running, it can use the full 8 GiB. Once all three
are running, each spillable operator's share is about a third. When a sort
exceeds its share, it writes sorted runs to disk and carries on with less memory.
With the default unbounded pool, none of this happens: operators keep allocating
until the process hits the machine's or container's limit and is killed. For
anything beyond development, choose a bounded pool.

**Spilling.** Spill files go to the directories listed in
runtime.temporary_files.paths, or to the operating system's temporary directory
if none are listed. The total is capped by runtime.temporary_files.max_size,
64 GiB by default, and setting that cap to zero disables spilling entirely. In
that case, an operator that runs out of memory fails with a "resources
exhausted" error instead of spilling.

**Storage.** Each runtime environment gets a dynamic object store registry,
which creates S3, GCS, Azure, HTTP, or local file clients on demand the first
time a path uses them (see [14](14-object-storage.md)).

**Caches.** Listing a large directory or reading Parquet footers can dominate
query time on big tables, so Sail can cache three things: directory listings,
per-file statistics, and Parquet metadata. Each cache can be turned off, shared
by every session in the process (global), or kept separately for each session.
Global caching makes repeated queries over stable tables much faster, at the
cost that files changed by other systems are only noticed when cached entries
expire. Per-session caching isolates users from each other. No caching means
every query sees the latest files and pays to list them again. By default, the
statistics and metadata caches are global and the listing cache is off. A
dashboard over stable tables benefits from turning the listing cache on with a
short expiry. An ingestion pipeline that writes every minute should leave it off.

Here's a configuration that bounds memory and sends spills to a fast disk:

```bash
export SAIL_RUNTIME__MEMORY_POOL__TYPE=fair
export SAIL_RUNTIME__MEMORY_POOL__FAIR__MAX_SIZE=17179869184      # 16 GiB
export SAIL_RUNTIME__TEMPORARY_FILES__PATHS='["/mnt/nvme/sail-spill"]'
```

## How sessions come and go

Requests for the same session can arrive at the same time, for example from
several threads of one PySpark program. Creating, checking, and deleting
sessions is therefore handled by a session manager **actor** (see
[02](02-entry-points-and-server.md)), which processes these events one at a time
so they can't race.

When a request arrives, the manager either returns the existing session or
asks the factory to build a new one. Either way, it records the time of this
activity and schedules a reminder to check the session again after the timeout:
15 minutes for Spark Connect, one hour for Flight SQL. When a reminder arrives,
the manager compares the session's last activity with the time the reminder was
set. If there's been activity since, a newer reminder already exists, and this
one is ignored. If not, the session has been idle for the whole timeout and is
deleted.

Deleting a session stops its job runner. In cluster mode, that shuts down the
session's driver and its workers. The runner hands back a history of the jobs,
stages, tasks, and workers it ran, and the manager keeps that history so the
system tables can still report on finished sessions.

## System tables: Sail describing itself

Sail exposes its own runtime state as read-only tables in a catalog called
system. You can list sessions and their settings (system.session.sessions and
system.session.options), jobs, stages, and tasks (system.execution.jobs, stages,
and tasks), and cluster workers (system.cluster.workers). They answer questions
like "what's running right now?" and "which task attempts failed?" with ordinary
SQL:

```python
spark.sql("SELECT * FROM system.execution.jobs").show()
spark.sql("SELECT stage, status, count(*) FROM system.execution.tasks GROUP BY 1, 2").show()
```

The table definitions are generated at build time from a YAML file. A query on
them is answered by asking the session manager, which asks each session's job
runner, or its saved history for sessions that have ended.

## Following one new client

When a PySpark client connects for the first time, its first request carries a
new session ID. The front end asks the session manager for that session, and the
manager asks the server session factory to build it. The factory creates the
table format registry (including any Python data sources), the catalog manager
from the configured catalog list, the activity tracker, a new job runner for the
configured mode, the system table service, and the Delta cache. It builds the
runtime environment with the memory pool, spill directories, caches, and storage
registry. It assembles the session state without DataFusion's defaults, adds
Sail's rules and planner, and registers first_value. The Spark Connect front end
then adds its own extension and sets the default time zone. The manager stores
the session, records the activity, and schedules the first idle check. Every
later request reuses the same context and pushes the idle deadline forward.

---

## Common problems

A process killed for running out of memory during big joins or sorts almost
always means the memory pool was left unbounded; switch to the fair pool with a
limit below the container's memory. "Resources exhausted" errors mean the pool
is too small or spilling is disabled. If newly written files don't show up, a
global cache with a long expiry is serving old listings or statistics; shorten
the expiry, use session scope, or run REFRESH TABLE. A function that works
locally but fails on workers is usually missing from the worker session. And if
temporary views disappear, the session was idle longer than its timeout.

## Where to look in the code

The two session factories are in crates/sail-session/src/session_factory/
(server.rs and worker.rs). The runtime environment, with the memory pool, spill
settings, and caches, is built in src/runtime.rs, and the cache implementations
live in crates/sail-cache. The session manager actor is under
src/session_manager/. Table format registration is in src/formats.rs, catalog
setup in src/catalog.rs, the optimizer rule lists in src/optimizer.rs, and the
extension planner in src/planner.rs. The extension mechanism, the job runner
interface, and the activity tracker are defined in crates/sail-common-datafusion
(extension.rs and session/). The system table definitions are in
crates/sail-common-datafusion/data/system/databases.yaml, with the system catalog
in crates/sail-catalog-system.
