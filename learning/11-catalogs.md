# 11 · Catalogs: Finding Tables

> **You'll learn:** how Sail turns a name like `prod.sales.orders` into a table
> with a schema, format, and location; how multiple catalogs coexist; how
> temporary views work; which metastores Sail supports and how to configure
> them; and how caching and runtime isolation are layered on.
>
> **Crates:** `sail-catalog` (interface, manager, commands, caching and runtime
> wrappers), `sail-catalog-memory`, `sail-catalog-system`, `sail-catalog-glue`,
> `sail-catalog-hms`, `sail-catalog-iceberg`, `sail-catalog-unity`,
> `sail-catalog-onelake`


## In One Minute

- **Catalog providers** store metadata: in-memory, Glue, Hive Metastore, Iceberg
  REST, Unity, OneLake, and the read-only `system` catalog.
- The per-session **catalog manager** interprets names (one-, two-, and
  three-part, with the current catalog and database) and holds temporary views.
- Lookup order for reads: **temporary view → global temporary view → table →
  view**.
- Remote providers are wrapped with a **cache** (TTL and size limits,
  invalidated on DDL) and a **runtime** layer (calls run on the io thread pool).
- Catalogs are configured in `catalog.list`. The in-memory catalog's metadata
  lasts **only as long as the session**.

---

## 1. What This Component Does

A **catalog** answers metadata questions: *Which databases exist? What tables
are in `sales`? What are the columns, format, location, and partitioning of
`sales.orders`?* It also carries out DDL: create, alter, and drop databases,
tables, and views.

Sail separates two jobs:

| Job | Who | What |
|-----|-----|------|
| **Interpreting names** | The **catalog manager** (one per session) | Decides which catalog and database a name refers to, handles temporary views, keeps the current catalog and database, and runs catalog commands |
| **Storing metadata** | **Catalog providers** (one per configured catalog) | Talk to a metastore: in-memory, AWS Glue, Hive Metastore, Iceberg REST, Unity Catalog, or Microsoft OneLake |

---

## 2. Why It's Built This Way

**One small interface for every metastore.** Every provider implements the same
dozen operations: create, get, list, and drop databases; create, get, list,
alter, and drop tables; and create, get, list, and drop views. Adding a metastore
means implementing those and translating between its types and Sail's.

**Name resolution in one place.** Spark's rules for one-, two-, and three-part
names, the current catalog and database, and the global temporary database are
subtle. If each provider implemented them, the providers would drift apart. The
manager implements them once, and providers only see "catalog, namespace,
object".

**Multi-level namespaces.** Some catalogs (Iceberg REST, Unity) have nested
namespaces. Sail represents a database as a **namespace**, a non-empty list of
parts, rather than a single string.

**Layers for cross-cutting behavior.** Two things apply to every remote
catalog, so they're added as **wrappers** around providers:

- a **runtime wrapper**, which runs every provider call on Sail's **io**
  runtime. A slow metastore can't block query execution threads, and client
  libraries that expect a particular Tokio runtime get one.
- a **caching wrapper**, which caches database, table, and view lookups (with
  size and TTL limits) and invalidates entries on DDL.

A typical remote catalog looks like *cache → runtime → provider*.

---

## 3. How It Works

### 3.1 Configuration

Catalogs are declared in `catalog.list`, a list of inline tables (TOML-style)
in an environment variable:

```bash
export SAIL_CATALOG__LIST='[
  {name="sail", type="memory", initial_database=["default"]},
  {name="glue", type="glue", region="us-east-1"},
  {name="ice",  type="iceberg-rest", uri="https://rest.example.com", warehouse="s3://lake/wh"}
]'
export SAIL_CATALOG__DEFAULT_CATALOG=sail
export SAIL_CATALOG__DEFAULT_DATABASE='["default"]'
```

| Type | Required settings | Optional settings |
|------|-------------------|-------------------|
| `memory` | `name`, `initial_database` | `initial_database_comment` |
| `glue` | `name` | `region`, `endpoint_url` |
| `hive_metastore` (`hms`) | `name`, `uris` | `thrift_transport`, `auth` (for example Kerberos), `kerberos_service_principal`, `min_sasl_qop`, `connect_timeout_secs` |
| `iceberg_rest` | `name`, `uri` | `warehouse`, `prefix`, `oauth_access_token`, `bearer_access_token` |
| `unity` | `name` | `uri`, `default_catalog`, `token` |
| `onelake` | `name`, `url` | `bearer_token` (otherwise uses the Azure CLI) |

Every remote type also accepts **cache settings**: a cache type, size, and TTL
for databases, tables, and views. Secrets are kept as secret strings and never
printed.

**Defaults:** the default configuration has one in-memory catalog named `sail`
with a database named `default`. If `catalog.default_catalog` is empty and
exactly one catalog is defined, that catalog becomes the default. With several
catalogs you must choose one. A catalog named **`system`** is always added
(see [06](06-session-and-runtime.md)), and that name is reserved.

### 3.2 Name resolution

The manager keeps a **current catalog**, a **current database**, and the name
of the **global temporary database** (`global_temp`).

**Database references:**

| Reference | Resolves to |
|-----------|-------------|
| `[db]` | current catalog, database `db` |
| `[c, db]`, if `c` is a registered catalog | catalog `c`, database `db` |
| `[c, ns1, ns2]`, if `c` is a registered catalog | catalog `c`, namespace `ns1.ns2` |
| `[ns1, ns2]`, if `ns1` isn't a catalog | current catalog, namespace `ns1.ns2` |

**Table and view references:** the last part is the object name. The rest is
resolved as a database reference, or means the current database if there's only
one part.

**Lookup order** for "get table or view", which is what reads use:

1. A **one-part** name: check **temporary views** first.
2. If the qualifier is the global temporary database (`global_temp.v`), check
   **global temporary views**.
3. Ask the provider for a **table**.
4. Ask the provider for a **view**.
5. If nothing is found: `[TABLE_OR_VIEW_NOT_FOUND] Table or view not found: …`.

### 3.3 What a table lookup returns

A lookup returns a **table status** describing the object:

| Kind | Contents |
|------|----------|
| **Table** | Columns (name, type, nullable, comment, default, generated expression, partition, bucket, and cluster flags), constraints, format (for example `parquet`, `delta`, `iceberg`), location, partition columns, sort order, bucketing, properties, and whether it's external |
| **View** | The SQL definition text, columns, and properties |
| **Temporary view** / **global temporary view** | A logical plan |

The resolver uses the format and location to ask the matching table format for
a reader (see [07](07-plan-resolver.md) and
[12](12-data-sources-and-formats.md)).

### 3.4 Temporary views and tracked objects

- `df.createOrReplaceTempView("v")` stores the DataFrame's **logical plan** in
  the session's temporary view manager. Global temporary views live under
  `global_temp`.
- Spark Connect clients can register functions and plans that later requests
  refer to by ID. The manager's **tracker** gives them internal IDs.
- Registered **Python UDFs** are kept as session functions, which the resolver
  checks before built-in functions (see [15](15-functions-and-python-udfs.md)).

### 3.5 Catalog commands

DDL and metadata commands are resolved into a **catalog command** (about 60
kinds) wrapped in a logical node (see [07](07-plan-resolver.md)). When the plan
runs, the operator calls the command's `execute`, which uses the manager and
providers and returns a small Arrow table formatted like Spark's output for
`SHOW TABLES`, `DESCRIBE`, and so on. Commands run on the **driver** in cluster
mode, because only the driver has the session's catalog state.

### 3.6 Providers in brief

| Provider | Notes |
|----------|-------|
| **Memory** | Kept inside the session's catalog manager. Metadata is **per session** and lost when the session ends (the data files themselves stay). Good for development and tests. |
| **System** | Read-only `system.*` tables generated from Sail's runtime state |
| **AWS Glue** | Uses the AWS SDK. Maps Glue databases and tables to Sail status (Hive-style formats, Iceberg tables). Credentials come from the standard AWS chain. |
| **Hive Metastore** | A Thrift client, with optional SASL/Kerberos (GSSAPI). Maps Hive storage descriptors (input/output formats, SerDe) to and from Sail formats. |
| **Iceberg REST** | A client for the Iceberg REST catalog API (separate `apis` and `models` modules), with nested namespaces and OAuth or bearer tokens |
| **Unity Catalog** | A REST client with token or credential configuration. `default_catalog` selects the Unity catalog. |
| **OneLake** | Microsoft Fabric's OneLake Delta table API. Uses a bearer token or gets one from the Azure CLI. |

---

## 4. Examples

### 4.1 Working with multiple catalogs

```python
spark.sql("SHOW CATALOGS").show()                    # sail, glue, ice, system
spark.sql("USE glue.analytics")
spark.table("events").limit(5).show()                # glue.analytics.events
spark.table("ice.marketing.campaigns").show()        # three-part name
spark.sql("CREATE TABLE sail.default.tmp USING parquet AS SELECT 1 AS x")
```

### 4.2 Temporary views

```python
df.createOrReplaceTempView("recent")
spark.sql("SELECT count(*) FROM recent").show()      # temporary view wins for one-part names
df.createGlobalTempView("shared")
spark.sql("SELECT * FROM global_temp.shared").show()
```

### 4.3 Caching a slow metastore

```bash
export SAIL_CATALOG__LIST='[{name="hms", type="hms", uris=["thrift://metastore:9083"],
  table_cache_type="global", table_cache_ttl_secs=60}]'
```

### 4.4 Name resolution, worked out

Setup: catalogs `sail` (default, memory) and `glue`. The current database is
`default`.

| You write | Catalog | Namespace | Object | Notes |
|-----------|---------|-----------|--------|-------|
| `orders` | `sail` | `default` | `orders` | Temporary view `orders` wins if one exists |
| `sales.orders` | `sail` | `sales` | `orders` | `sales` isn't a catalog, so it's a database |
| `glue.sales.orders` | `glue` | `sales` | `orders` | First part matches a catalog |
| `glue.orders` | `glue` | (empty) | `orders` | **Error: "empty namespace"**, because a catalog was named without a database |
| `global_temp.v` | (none) | | `v` | Global temporary view |
| `a.b.c.t` with catalog `a` | `a` | `b.c` | `t` | Multi-level namespace |

After `USE glue.sales`, the bare name `orders` resolves to `glue.sales.orders`.

### 4.5 How the cache stays correct

| Action | Cache effect |
|--------|--------------|
| `spark.table("glue.sales.orders")` (miss) | Calls Glue, stores the table status |
| Same call again within the TTL | Served from cache, no Glue call |
| `ALTER TABLE glue.sales.orders …` or `DROP TABLE …` through Sail | Entry invalidated |
| Table changed by **another** system (for example Spark or Athena) | Sail sees the old metadata until the TTL expires |

---

## 5. End-to-End: `spark.table("glue.analytics.events")`

| # | Component | What happens |
|---|-----------|--------------|
| 1 | Spark Connect | Creates a read of the named table `["glue", "analytics", "events"]`. |
| 2 | Resolver | Isn't a `format.path` shortcut or a CTE, so it asks the catalog manager for the table or view. |
| 3 | Catalog manager | Three parts, so it isn't a temporary view. `glue` is a registered catalog, so catalog = `glue`, namespace = `analytics`, object = `events`. |
| 4 | Cache wrapper | Checks the table cache. On a miss, it calls the wrapped provider. |
| 5 | Runtime wrapper | Runs the call on the io runtime. |
| 6 | Glue provider | Calls Glue `GetTable` and converts columns, format (for example Parquet), location, and partition keys into a table status. |
| 7 | Resolver | Builds source info and asks the Parquet table format for a source (see [12](12-data-sources-and-formats.md)). |
| 8 | Planner and execution | Scan, prune, and read (see [07](07-plan-resolver.md), [09](09-local-and-cluster-execution.md)). |

---

## 6. Common Problems

| Symptom | Cause | Fix |
|---------|-------|-----|
| "cannot infer default catalog when multiple catalogs are defined" | Several catalogs without `catalog.default_catalog` | Set `SAIL_CATALOG__DEFAULT_CATALOG`. |
| `TABLE_OR_VIEW_NOT_FOUND` for a table you just created | Different session (memory catalogs are per session), or a stale cache | Use a persistent catalog, or lower cache TTLs. |
| "cannot define catalog with reserved name: system" | A catalog named `system` in `catalog.list` | Rename it. |
| Name resolves to the wrong catalog | The first name part matches a catalog name | Fully qualify the name, or rename the catalog or database. |
| Queries stall on metadata calls | A slow metastore | Turn on catalog caching. Check network and authentication. |

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Provider interface | `crates/sail-catalog/src/provider/mod.rs` |
| Namespaces, options | `crates/sail-catalog/src/provider/namespace.rs`, `options.rs` |
| Caching and runtime wrappers | `crates/sail-catalog/src/provider/cache.rs`, `runtime.rs` |
| Catalog manager (name resolution) | `crates/sail-catalog/src/manager/` (`mod.rs`, `table.rs`, `view.rs`, `database.rs`, `function.rs`, `tracker.rs`) |
| Temporary views | `crates/sail-catalog/src/temp_view.rs` |
| Catalog commands | `crates/sail-catalog/src/command.rs` |
| Hive storage formats | `crates/sail-catalog/src/hive_format.rs` |
| Table status types | `crates/sail-common-datafusion/src/catalog/` |
| Wiring catalogs into sessions | `crates/sail-session/src/catalog.rs` |
| Catalog configuration | `crates/sail-common/src/config/application.rs` (catalog types), `application.yaml` (`catalog.*`) |
| Providers | `crates/sail-catalog-{memory,system,glue,hms,iceberg,unity,onelake}/` |
