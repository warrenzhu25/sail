# 06 · Sessions and the Runtime Environment

> **You'll learn:** what a Sail session contains, how it's assembled, how
> sessions are created, tracked, timed out, and deleted, and how memory,
> spilling, caches, and storage access are configured for each session.
>
> **Crates:** `sail-session`, `sail-cache`, and the session parts of
> `sail-common-datafusion`


## In One Minute

- A **session** is a DataFusion `SessionContext` with Sail's services attached
  as typed **extensions**: catalogs, table formats, the job runner, system
  tables, and an activity tracker.
- Server sessions serve clients. Worker sessions only run tasks.
- Each session has its **own job runner**. In cluster mode that means its own
  driver and workers.
- The **runtime environment** sets the memory pool (unbounded by default!),
  spill directories, file caches, and the object store registry.
- Idle sessions are deleted after a timeout (15 minutes for Spark Connect).
  Their history stays visible in `system.*` tables.

---

## 1. What This Component Does

Sail builds on Apache DataFusion, whose central object is the
**`SessionContext`**: configuration, a runtime environment (memory, disk,
storage), optimizer rules, a query planner, and registered functions. Sail
creates one per client session and customizes it heavily.

This component:

1. **assembles** a session context with everything Sail needs (catalogs, table
   formats, the job runner, the planner, optimizer rules, and Spark-compatible
   settings),
2. **manages** session lifecycles (create on first use, reuse, time out, delete,
   and remember history),
3. **builds the runtime environment**: the memory pool, temporary-file
   (spill) directories, file metadata caches, and the object store registry.

---

## 2. Why It's Built This Way

**Extensions instead of wrappers.** DataFusion's session configuration can hold
arbitrary typed **extensions**. Sail attaches its own services there, and any
code holding the session can get one by type:
`ctx.extension::<CatalogManager>()`. That means Sail doesn't have to wrap or
fork DataFusion's types, and new services can be added without changing
function signatures across the codebase.

**Sail turns off DataFusion's defaults.** Server sessions are created
*without* DataFusion's default catalog, information schema, built-in table
formats, or built-in functions. Sail manages all of these itself, for Spark
compatibility. For example, Spark's `concat` with a NULL argument returns NULL,
unlike DataFusion's version. One DataFusion function, `first_value`, is
registered explicitly because one of DataFusion's optimizer rules quietly
depends on it.

**Two factories for two jobs.** Client-facing sessions and worker sessions need
different things. A worker only has to *run* plan fragments, so it doesn't need
catalogs, user state, or a job runner, but it does need every function and
format available to decode the plan it receives.

**One job runner per session.** Each server session creates its own job runner.
In cluster mode, that means **each session gets its own driver**, with its own
workers. Sessions are isolated: one user's job can't take over another
session's workers.

**An actor for the session lifecycle.** Requests for the same session can
arrive concurrently. Creation, idle checks, and deletion are messages handled
one at a time by a session manager actor, so they can't race.

---

## 3. How It Works

### 3.1 What a server session contains

| Extension | Purpose | Read more |
|-----------|---------|-----------|
| Table format registry | Parquet, CSV, JSON, Avro, Arrow, text, binary, socket, rate, console, Delta, Iceberg, and Python data sources | [12](12-data-sources-and-formats.md), [13](13-lakehouse-delta-and-iceberg.md) |
| Catalog manager | Catalogs, databases, tables, views, temporary objects, name resolution | [11](11-catalogs.md) |
| Activity tracker | Time of last activity, used for idle timeout | below |
| Job service | Wraps the job runner (local or cluster) | [09](09-local-and-cluster-execution.md) |
| System table service | Lets `system.*` tables see sessions, jobs, stages, tasks, and workers | below |
| Delta table cache | Caches loaded Delta table state within the session | [13](13-lakehouse-delta-and-iceberg.md) |
| Spark session | Added by the Spark Connect front end: IDs, Spark settings, executors, streaming queries | [03](03-spark-connect.md) |

The session state also includes:

- **analyzer and optimizer rules**: Sail's logical rules. Lakehouse rules (such
  as expanding row-level operations) run **first**, ahead of DataFusion's
  built-in rules,
- **physical optimizer rules**: Sail's list, with optional cost-based join
  reordering (`optimizer.enable_join_reorder`, off by default) (see
  [08](08-logical-physical-plans-and-optimizers.md)),
- **the query planner**: Sail's extension planner, which knows how to plan Sail's
  custom nodes,
- **DataFusion execution settings** copied from Sail configuration: batch size
  (8,192 rows by default), target partitions (`execution.default_parallelism`;
  0 means one per CPU core), statistics collection, and about 30 Parquet read and
  write settings (page index, pruning, filter pushdown, compression, row group
  size, bloom filters, and so on).

### 3.2 The runtime environment

The runtime environment controls the physical resources a session uses:

| Resource | Setting | Behavior |
|----------|---------|----------|
| **Memory pool** | `runtime.memory_pool.type` | `unbounded` (default): no limit. `greedy`: first come, first served up to `max_size`. `fair`: `max_size` shared fairly among operators that can spill. Only bounded pools make operators **spill** instead of growing without limit. |
| **Spill files** | `runtime.temporary_files.paths`, `.max_size` | A max size of 0 turns spilling off. No paths means the OS temporary directory. Otherwise spills go to the listed directories. The default limit is 64 GiB. |
| **Object stores** | (see [14](14-object-storage.md)) | A dynamic registry that creates S3, GCS, Azure, HTTP, HDFS, or local clients on demand. |
| **File listing cache** | `execution.file_listing_cache.*` | Caches directory listings. Off by default. |
| **File statistics cache** | `parquet.file_statistics_cache.*` | Caches per-file statistics. Global by default. |
| **File metadata cache** | `parquet.file_metadata_cache.*` | Caches Parquet footers. Global by default. |

Each cache has a **scope**: `none`, `global` (one cache shared by every
session in the process), or `session` (a new cache per session). Caches are
built on `moka` with a time-to-live and a size or entry limit.

| Scope | Good for | Watch out for |
|-------|----------|---------------|
| Global | Many sessions reading the same tables | Changes made outside Sail are seen only after the TTL expires. |
| Session | Isolation between users | Each session pays to warm its own cache. |
| None | Data that changes constantly | Every query lists files and reads footers again. |

### 3.3 Worker sessions

A worker session is minimal: a runtime environment (the same memory, spill, and
storage setup), a Delta table cache, and DataFusion's **default features**
(built-in functions and formats). The defaults are needed because when a worker
decodes a plan fragment, every function the plan refers to must be resolvable
by name.

### 3.4 Session lifecycle

```text
                GetOrCreateSession
   (none) ────────────────────────────► Running ──DeleteSession──► Deleting ──► Deleted (history kept)
                                          │
                                          └── creation or stop failed ──► Failed
```

1. **Get or create.** The front end sends *get or create (session ID, user ID)*.
   If the session is running, the existing context is returned. Otherwise the
   factory builds a new one.
2. **Track activity and schedule a probe.** Every *get or create* call records
   "active now" in the session's activity tracker and schedules a *probe idle
   session* message, carrying that timestamp, for after the timeout
   (`spark.session_timeout_secs`, 15 minutes by default, or
   `flight.session_timeout_secs`, 1 hour, for Flight SQL).
3. **Check the probe.** When a probe arrives, the actor compares the session's
   last activity with the probe's timestamp. If nothing has happened since,
   the session is idle and is deleted. If there was activity, that later
   request already scheduled its own probe, so this one is ignored.
4. **Delete.** The job runner is stopped. In cluster mode this shuts down the
   session's driver and its workers. The job runner returns a **history**
   (jobs, stages, tasks, workers), which is kept in the *Deleted* state so
   system tables can still report on finished sessions.

### 3.5 System tables

Sail exposes its own runtime state as read-only SQL tables in a catalog named
`system`:

| Table | Shows |
|-------|-------|
| `system.session.sessions` | Sessions and their status |
| `system.session.options` | Session settings |
| `system.execution.jobs` | Jobs per session: ID, status, start and stop times |
| `system.execution.stages` | Stages: partitions, inputs, mode, distribution, placement, status |
| `system.execution.tasks` | Tasks: stage, partition, attempt, status, times |
| `system.cluster.workers` | Workers: ID, host, port, status, times |

The table definitions are generated at build time from a YAML file. Queries on
them are answered by asking the session manager actor, which asks each session's
job runner (or its saved history).

---

## 4. Examples

### 4.1 Bound memory and send spills to a fast disk

```bash
export SAIL_RUNTIME__MEMORY_POOL__TYPE=fair
export SAIL_RUNTIME__MEMORY_POOL__FAIR__MAX_SIZE=17179869184      # 16 GiB
export SAIL_RUNTIME__TEMPORARY_FILES__PATHS='["/mnt/nvme/sail-spill"]'
sail spark server
```

A large sort or hash aggregation now writes sorted runs to `/mnt/nvme/sail-spill`
once it reaches its share of the 16 GiB, instead of growing until the operating
system kills the process.

### 4.2 Cache directory listings for a read-heavy dashboard

```bash
export SAIL_EXECUTION__FILE_LISTING_CACHE__TYPE=global
export SAIL_EXECUTION__FILE_LISTING_CACHE__TTL=300   # seconds
```

### 4.3 See what's running

```python
spark.sql("SELECT * FROM system.execution.jobs").show()
spark.sql("SELECT stage, status, count(*) FROM system.execution.tasks GROUP BY 1, 2").show()
```

### 4.4 Memory pools with numbers

Suppose `runtime.memory_pool.type=fair` with `max_size=8 GiB`, and a query
runs two sorts and one hash aggregation at the same time, all able to spill:

| Moment | What the pool allows |
|--------|----------------------|
| Only the hash aggregation is running | It can use up to the full 8 GiB |
| All three are running | Each spillable operator gets about 8 / 3 ≈ 2.7 GiB |
| A sort needs more than its share | That sort **spills** sorted runs to `temporary_files.paths` and continues with less memory |
| Spilling is disabled (`temporary_files.max_size=0`) | The sort fails with a "resources exhausted" error instead |

With the default **unbounded** pool, none of this happens. Operators keep
allocating until the process hits the machine or container limit and is killed.
For anything beyond development, set a bounded pool.

### 4.5 Which cache scope should I use?

| Workload | Recommended setting | Reason |
|----------|---------------------|--------|
| BI dashboard over stable tables, many sessions | `parquet.file_metadata_cache.type=global`, listing cache `global` with a TTL of a few minutes | Footers and listings are reused across users |
| Ingestion pipeline writing every minute | Listing cache `none` | New files must be visible immediately |
| Multi-tenant server | `session` scope | No cross-tenant sharing of cached metadata |

---

## 5. End-to-End: A New PySpark Client Connects

| # | What happens |
|---|--------------|
| 1 | The first request arrives with a new session ID. |
| 2 | The front end asks the session manager actor to *get or create* it. |
| 3 | The actor calls the server session factory, which builds the extensions (table formats including Python data sources, catalogs from `catalog.list`, activity tracker, a new job runner for `mode`, system tables, Delta cache). |
| 4 | It builds the runtime environment: memory pool, disk manager, caches, and object store registry. |
| 5 | It builds the session state without DataFusion's defaults, adding Sail's analyzer, optimizer, and physical rules and the extension planner. It registers `first_value`. |
| 6 | The Spark Connect hook adds the Spark session extension and sets the default time zone. |
| 7 | The actor stores the session as *Running*, records the activity time, and schedules an idle probe. |
| 8 | Later requests reuse the context. Each one updates the activity time and schedules its own probe. |
| 9 | When a probe finds no activity since it was scheduled (15 minutes by default), the session is deleted, its job runner is stopped, and its history is kept. |

---

## 6. Common Problems

| Symptom | Cause | Fix |
|---------|-------|-----|
| Process killed for running out of memory on big joins or sorts | Default `unbounded` memory pool | Use `fair` with a `max_size` below the container limit. |
| "Resources exhausted" errors | Pool too small, or the operator can't spill | Raise `max_size`, or make sure spilling isn't disabled (`temporary_files.max_size` > 0). |
| New files in a table aren't picked up | A global listing or statistics cache with a long TTL | Lower the TTL, use `session` scope, or run `REFRESH TABLE`. |
| A function works locally but fails on workers | The function isn't available in the worker session | Check that it's registered somewhere the worker can decode it. |
| Temporary views disappear | Session idle timeout | Raise `spark.session_timeout_secs`. |

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Server session factory | `crates/sail-session/src/session_factory/server.rs` |
| Worker session factory | `crates/sail-session/src/session_factory/worker.rs` |
| Runtime environment (memory, spill, caches) | `crates/sail-session/src/runtime.rs` |
| Caches | `crates/sail-cache/src/` |
| Session manager actor | `crates/sail-session/src/session_manager/` |
| Table format registration | `crates/sail-session/src/formats.rs` |
| Catalog setup | `crates/sail-session/src/catalog.rs` |
| Optimizer rule lists | `crates/sail-session/src/optimizer.rs` |
| Extension planner | `crates/sail-session/src/planner.rs` |
| Extension trait, job runner trait, activity tracker | `crates/sail-common-datafusion/src/extension.rs`, `session/` |
| System table definitions | `crates/sail-common-datafusion/data/system/databases.yaml`, `crates/sail-catalog-system/` |
