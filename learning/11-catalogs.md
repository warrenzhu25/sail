# 11 · Catalogs: Finding Tables

When a query mentions prod.sales.orders, something has to turn that name into a
table with columns, a file format, and a location. That something is the catalog
layer. This doc explains how Sail interprets table names, how several catalogs
live side by side, how temporary views fit in, which metastores Sail can talk
to, and why caching and thread isolation are layered around them.

---

## Two jobs, two kinds of component

A catalog has to answer metadata questions ("which databases exist?", "what are
the columns and location of sales.orders?") and carry out DDL (create, alter,
and drop databases, tables, and views). Sail splits this work between two kinds
of component.

**Catalog providers** store metadata. Each one talks to a particular metastore:
an in-memory store, AWS Glue, a Hive Metastore, an Iceberg REST catalog, Unity
Catalog, or Microsoft OneLake. There's also a built-in, read-only system catalog
that describes Sail itself.

The **catalog manager**, one per session, interprets names. It decides which
catalog and database a name refers to, keeps track of the current catalog and
database, holds temporary views, and runs catalog commands.

The split exists because Spark's rules for interpreting names are subtle (one-,
two-, and three-part names, current catalog and database, the global temporary
database), and if every provider implemented them, they would drift apart. So
the manager implements them once, and providers only ever see a catalog, a
namespace, and an object name. Adding a new metastore means implementing about a
dozen simple operations, such as creating, getting, listing, altering, and
dropping databases, tables, and views, and translating between that metastore's
types and Sail's.

## Configuring catalogs

Catalogs are declared in one setting, catalog.list, written in a compact inline
syntax:

```bash
export SAIL_CATALOG__LIST='[
  {name="sail", type="memory", initial_database=["default"]},
  {name="glue", type="glue", region="us-east-1"},
  {name="ice",  type="iceberg_rest", uri="https://rest.example.com", warehouse="s3://lake/wh"}
]'
export SAIL_CATALOG__DEFAULT_CATALOG=sail
```

Each type takes its own settings. Glue takes a region and optional endpoint.
Hive Metastore takes server URIs plus optional transport, Kerberos, and timeout
settings. Iceberg REST takes a URI, warehouse, prefix, and OAuth or bearer
token. Unity takes a URI, default catalog, and token. OneLake takes a URL and
an optional bearer token (otherwise it asks the Azure CLI for one). Every remote
type also accepts cache settings, described below. Secrets are kept as secret
strings and never printed in logs.

Out of the box, Sail has a single in-memory catalog called sail with a database
called default. If you define exactly one catalog and don't name a default, it
becomes the default. With several catalogs, you must choose one, or Sail refuses
to start. A catalog called system is always added, and that name is reserved.

## How a name is interpreted

The manager keeps a current catalog, a current database, and the name of the
global temporary database (global_temp). With those, it reads names like this.
A name with one part is an object in the current catalog and current database. A
longer name splits into the object (the last part) and a qualifier (everything
before it). If the qualifier's first part is the name of a registered catalog,
that's the catalog and the rest is the namespace; otherwise the whole qualifier
is a namespace in the current catalog. Namespaces can have several levels, which
some catalogs such as Iceberg REST support.

Here's how that plays out with catalogs sail (the default) and glue, and current
database default. orders means sail.default.orders. sales.orders means the
orders table in database sales of the sail catalog, because sales isn't a
catalog. glue.sales.orders means catalog glue, database sales. glue.orders is an
error, "empty namespace", because it names a catalog but no database.
global_temp.v refers to a global temporary view. After `USE glue.sales`, the bare
name orders means glue.sales.orders.

## The order of lookup

Reading a table goes through a fixed sequence. If the name has one part, the
manager first checks the session's **temporary views**, so a temporary view
named orders hides a table named orders. If the qualifier is the global
temporary database, it checks **global temporary views**. Otherwise it asks the
catalog's provider for a **table**, and if there's none, for a saved **view**.
If all of those come up empty, the user gets TABLE_OR_VIEW_NOT_FOUND.

What comes back is a description of the object. For a table, that includes its
columns (with types, nullability, comments, defaults, and generated
expressions), its file format, location, partition columns, bucketing, sort
order, properties, and whether it's external. A saved view comes back as its SQL
text and columns. A temporary view comes back as a logical plan. The resolver
then uses a table's format and location to ask the matching table format for a
reader (see [07](07-plan-resolver.md) and [12](12-data-sources-and-formats.md)).

## Temporary views and other session objects

`df.createOrReplaceTempView("v")` stores the DataFrame's plan in the session, and
`createGlobalTempView` stores it under global_temp. The manager also holds
objects that external catalogs can't: Python UDFs registered in the session
(which the resolver checks before built-in functions; see
[15](15-functions-and-python-udfs.md)), and functions and plans that Spark
Connect clients register and later refer to by ID.

## Wrapping remote catalogs: threads and caches

Every remote catalog gets two layers around it, and both exist for practical
reasons.

The first runs every catalog call on Sail's I/O thread pool (see
[02](02-entry-points-and-server.md)). A slow metastore then can't tie up the
threads that execute queries, and the client libraries for Glue, Iceberg REST,
and others get the runtime they expect.

The second is a cache. Planning a query can involve several catalog calls, and
going to a remote metastore each time adds latency. The cache remembers
databases, tables, and views, with configurable size and expiry for each. When
you change something through Sail, with ALTER TABLE or DROP TABLE for example,
the affected entries are invalidated immediately. When another system changes a
table, say Spark or Athena writing to the same Glue catalog, Sail keeps serving
the cached version until it expires. Choose the expiry with that trade-off in
mind:

```bash
export SAIL_CATALOG__LIST='[{name="hms", type="hms", uris=["thrift://metastore:9083"],
  table_cache_type="global", table_cache_ttl_secs=60}]'
```

## Catalog commands

DDL and metadata commands such as CREATE, DROP, SHOW TABLES, DESCRIBE, and USE
become catalog commands, about sixty kinds in all. The resolver wraps them in a
plan node, and when the plan runs, the command uses the manager and the
providers to do its work and returns a small table shaped like Spark's output.
In cluster mode these commands always run on the driver, because only the driver
holds the session's catalog state.

## The providers in brief

The **memory** catalog lives inside the session's catalog manager. Its metadata
lasts only as long as the session, though any data files it wrote remain. That
makes it good for development and tests, and a surprise if you expect tables to
persist. The **system** catalog serves read-only tables generated from Sail's
runtime state (see [06](06-session-and-runtime.md)). **AWS Glue** uses the AWS
SDK with the standard credential chain, and understands Hive-style tables and
Iceberg tables. **Hive Metastore** speaks Thrift, optionally with Kerberos, and
translates Hive storage descriptors (input format, output format, SerDe) to and
from Sail's formats. **Iceberg REST** implements the Iceberg REST catalog API,
including nested namespaces and token authentication. **Unity Catalog** is a
REST client configured with a token. **OneLake** talks to Microsoft Fabric's
OneLake Delta table API.

## Following one lookup

Take `spark.table("glue.analytics.events")`. Spark Connect produces a read of the
named table glue.analytics.events. The resolver checks that it isn't a file-path
shortcut or a WITH-clause name, then asks the catalog manager. Because the name
has three parts, it can't be a temporary view. glue is a registered catalog, so
the catalog is glue, the namespace analytics, and the object events. The cache
layer looks for the table, and on a miss passes the request to the thread layer,
which runs the Glue call on the I/O pool. The Glue provider fetches the table
definition and translates its columns, format, location, and partition keys.
The resolver builds a description of the source and asks the Parquet format for
a reader, and planning continues.

---

## Common problems

"Cannot infer default catalog when multiple catalogs are defined" means
catalog.default_catalog needs to be set. A table you just created that can't be
found was probably created in an in-memory catalog in a different session, or is
hidden behind a stale cache. "Cannot define catalog with reserved name: system"
means a configured catalog is called system. A name that resolves to the wrong
catalog happens when a database shares its name with a catalog; qualify the name
fully. And queries that stall on metadata usually point at a slow metastore;
turn on caching and check the network and credentials.

## Where to look in the code

The provider interface is in crates/sail-catalog/src/provider/mod.rs, with
namespaces and options alongside it, and the caching and thread layers in
provider/cache.rs and provider/runtime.rs. The catalog manager and its name
resolution are under src/manager/, temporary views in src/temp_view.rs, catalog
commands in src/command.rs, and Hive storage format mapping in src/hive_format.rs.
The shape of a table description is defined in crates/sail-common-datafusion
under src/catalog/. Catalogs are created for each session in
crates/sail-session/src/catalog.rs, from the settings defined in
crates/sail-common/src/config. Each provider has its own crate:
sail-catalog-memory, -system, -glue, -hms, -iceberg, -unity, and -onelake.
