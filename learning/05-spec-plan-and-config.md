# 05 · The Spec Plan Model (and Shared Foundations)

> **You'll learn:** what Sail's intermediate representation, the *spec plan*,
> looks like, why it exists, how its pieces fit together, and what else lives in
> the shared `sail-common` crate.
>
> **Crate:** `sail-common` (`spec`, `config`, `error`, `runtime`, `object`,
> `datetime`, `string`)

---

## 1. What This Component Does

The spec plan is the **common language** between Sail's front ends and its
planner. Every request ends up as a spec plan:

```
PySpark DataFrame ──(Spark Connect translator)──┐
spark.sql("…")    ──(SQL parser + analyzer)─────┼──► spec::Plan ──► Plan resolver ──► DataFusion
Flight SQL query  ──(SQL parser + analyzer)─────┘
```

A spec plan is a tree of plain Rust data (enums and structs) that can be
serialized with serde. It records **what the user asked for**, with names still
unresolved:

- Table names are just names (`["sales", "orders"]`), not table handles.
- Columns are *unresolved attributes*: a name and, optionally, the ID of the
  DataFrame it came from.
- Functions are *unresolved functions*: a name, arguments, and flags such as
  `DISTINCT` or `IGNORE NULLS`.
- Types are Sail's own data types, which closely follow Arrow types plus a few
  Spark-only ones.

---

## 2. Why It Exists

| Reason | What it means in practice |
|--------|---------------------------|
| **One resolver for every protocol** | SQL and DataFrame code both become spec plans, so the complicated resolution logic is written once. Adding a protocol means writing one translator. |
| **Resolve later** | Translators don't need a session, catalogs, or functions. They can be pure and simple to test. |
| **Normalization** | `df.filter(col("x") > 5)` and `WHERE x > 5` become the same filter node. The resolver handles one form, not two. |
| **Spark-only features** | Pandas UDFs, `mapInPandas`, streaming watermarks, `na.fill`, `stat.crosstab`, and similar features have spec nodes even though DataFusion has no equivalent. |
| **Inspectable** | Spec plans implement `Debug` and serde. When a result is wrong, print the spec plan to see whether the bug is in translation or later. |

---

## 3. What's in a Spec Plan

### 3.1 Top level

A **plan** is either:

- a **query plan**, which produces rows, or
- a **command plan**, which has a side effect and may produce a small result.

Both carry an optional **plan ID**. Spark Connect assigns an ID to each
DataFrame. When you write `df1["id"]`, the column reference carries `df1`'s
plan ID, so the resolver can tell `df1.id` from `df2.id` in a self-join, even
though both are named `id`.

### 3.2 Query nodes

About 70 query node types, grouped by purpose:

| Group | Nodes |
|-------|-------|
| Reading | Read (named table, data source with format/paths/options, table function, dynamic table), LocalRelation (inline data), Values, Range, Empty, CachedLocalRelation |
| Relational | Project, Filter, Join, LateralJoin, Aggregate, Sort, Limit (with optional offset), Tail, Deduplicate, SetOperation (union, intersect, except), SubqueryAlias, TableAlias |
| Columns | WithColumns, WithColumnsRenamed, Drop, ToDf, ToSchema |
| Reshaping | Pivot, Unpivot, LateralView (generators like `explode`) |
| Partitioning | Repartition, RepartitionByExpression, Hint |
| Sampling | Sample, TableSample |
| Python | MapPartitions (`mapInPandas`/`mapInArrow`), GroupMap (`applyInPandas`), CoGroupMap, ApplyInPandasWithState, user-defined table functions |
| Streaming | WithWatermark |
| DataFrame helpers | FillNa, DropNa, Replace, StatSummary, StatDescribe, StatCrosstab, StatCov, StatCorr, StatApproxQuantile, StatFreqItems, StatSampleBy, CollectMetrics, Parse (`from_json`/`from_csv` on a DataFrame) |
| SQL structure | WithCtes (`WITH`), WithParameters (bound `?`/`:name` parameters), WithRelations |

### 3.3 Command nodes

About 60 command types:

| Group | Commands |
|-------|----------|
| Catalogs and databases | CurrentCatalog, SetCurrentCatalog, CreateCatalog, CurrentDatabase, SetCurrentDatabase, CreateDatabase, DropDatabase, ListCatalogs, ListDatabases, GetDatabase, DatabaseExists, DescribeCatalog, DescribeDatabase |
| Tables | CreateTable, CreateTableAsSelect, AlterTable, DropTable, RefreshTable, RefreshByPath, RecoverPartitions, ListTables, ShowTables, ShowTableExtended, GetTable, TableExists, ListColumns, DescribeTable, AnalyzeTable(s) |
| Views | CreateView, AlterView, DropView, CreateTemporaryView, DropTemporaryView, ListViews |
| Functions | RegisterFunction, RegisterTableFunction, RefreshFunction, DropFunction, ListFunctions, GetFunction, FunctionExists, DescribeFunction |
| Writes | Write (v1 `df.write`), WriteTo (v2 `df.writeTo`), WriteStream, InsertInto, InsertOverwriteDirectory, LoadData |
| Row-level | MergeInto, Update, Delete |
| Caching | CacheTable, UncacheTable, ClearCache, IsCached |
| Display | ShowString (`df.show()`), HtmlString (notebooks), Explain, DescribeQuery |
| Other | SetVariable, CommentOn… |

### 3.4 Expressions

| Expression | Meaning | Example source |
|------------|---------|----------------|
| Literal | A typed constant | `5`, `'abc'`, `DATE '2024-01-01'` |
| UnresolvedAttribute | A column reference by name (plus an optional plan ID, and whether it's a metadata column) | `amount`, `t.amount`, `df["amount"]` |
| UnresolvedFunction | A function call by name, with positional and named arguments, `DISTINCT`, `IGNORE NULLS`, `FILTER (WHERE …)`, and `ORDER BY` inside aggregates | `count(DISTINCT x)`, `percentile_cont(0.5) WITHIN GROUP (ORDER BY x)` |
| UnresolvedStar | `*` or `t.*`, optionally with exclusions | `SELECT * EXCEPT (a)` |
| UnresolvedRegex | Columns selected by regular expression | `` df.colRegex("`a.*`") `` |
| UnresolvedExtractValue | Field, index, or key access | `s.field`, `arr[0]`, `m['k']` |
| Alias, Cast, SortOrder, Window | What the names say | `x AS y`, `CAST(x AS INT)`, `ORDER BY x DESC NULLS LAST`, `OVER (…)` |
| LambdaFunction, UnresolvedNamedLambdaVariable | Higher-order functions | `transform(arr, x -> x + 1)` |
| UpdateFields | Change struct fields | `withField`, `dropFields` |
| Subqueries | ScalarSubquery, InSubquery, Exists | `WHERE x IN (SELECT …)` |
| Grouping | Rollup, Cube, GroupingSets | `GROUP BY ROLLUP(a, b)` |
| CommonInlineUserDefinedFunction | An inline Python UDF definition | `@udf` functions |
| Placeholder, NamedArgument, CallFunction | Parameters, `f(arg => value)`, `call_function("name", …)` | |

### 3.5 Data types

Spec data types follow Arrow closely: integers (signed and unsigned, 8–64
bits), floats (16, 32, 64), decimals (128 and 256), dates, times, timestamps,
durations, intervals, binary and string types (regular, large, and view
variants), lists, structs, maps, unions, and dictionaries. A few types aren't
in Arrow:

| Type | Why |
|------|-----|
| **ConfiguredUtf8 / ConfiguredBinary** | Spark's `STRING` and `BINARY`. Whether they become 32-bit or 64-bit-offset Arrow types depends on `spark.sql.execution.arrow.useLargeVarTypes`, which isn't known until resolution. |
| **Timestamp with "configured" time zone** | Spark's plain `TIMESTAMP` means LTZ or NTZ depending on `spark.sql.timestampType`, also decided at resolution. |
| **UserDefined** | Spark UDTs, which keep their JVM/Python class names and an underlying SQL type. |
| **Variant** | Spark 4's semi-structured `VARIANT`. |
| **Geometry / Geography** | Spatial types with a spatial reference ID (Spark 4.1). |

Deciding these "configured" types late is a good example of why resolution is
a separate step: the same SQL text can produce different Arrow types in
different sessions.

### 3.6 Literals

Literals carry their exact type: every integer width, floats, decimals with
precision and scale, timestamps at each precision (second to nanosecond) with an
optional time zone, dates, times, intervals (year-month, day-time,
month-day-nanosecond), binary, strings, and nested arrays, maps, and structs.

---

## 4. Other Shared Foundations in `sail-common`

| Module | What it provides |
|--------|------------------|
| `config` | The application configuration: a YAML schema compiled into the binary, plus `SAIL_*` environment variable overrides (see [02](02-entry-points-and-server.md)). Also the internal CLI configuration. |
| `runtime` | The primary and io Tokio runtimes and the handle passed around to reach them. |
| `error` | Common error kinds: missing argument, invalid argument, not supported, internal error. Each crate wraps these in its own error type. |
| `object` | Helpers that let trait objects be compared, hashed, and ordered. DataFusion needs this for Sail's custom plan nodes and UDFs. |
| `datetime` | Time zone and timestamp helpers shared by functions and planning. |
| `string` | String helpers, such as case-insensitive comparison. |

---

## 5. Example: The Same Query From Two Front Ends

**DataFrame:**

```python
orders = spark.table("sales.orders")
orders.filter(orders["amount"] > 100).select("region", "amount").limit(5)
```

**SQL:**

```sql
SELECT region, amount FROM sales.orders WHERE amount > 100 LIMIT 5
```

Both produce essentially this spec plan (shown as an indented tree):

```text
Query: Limit(5)
  Project [ UnresolvedAttribute(region), UnresolvedAttribute(amount) ]
    Filter UnresolvedFunction(">", [UnresolvedAttribute(amount), Literal(Int32 100)])
      Read NamedTable(name = ["sales", "orders"])
```

The only difference: in the DataFrame version, the `amount` reference in the
filter carries the plan ID of `orders`. Note that `>` is just a function named
`>` at this stage. Operators become real comparison expressions during
resolution.

---

## 6. End-to-End: Life of a Spec Plan

| # | Stage | What happens to the spec plan |
|---|-------|-------------------------------|
| 1 | Front end | Built from protobuf or SQL. Pure data, no session needed. |
| 2 | Debugging (optional) | Can be printed (`{:?}`) or serialized. |
| 3 | Resolver | Walked top-down and bottom-up. Names are looked up, types are fixed using the session's plan config, and the result is a DataFusion logical plan (see [07](07-plan-resolver.md)). |
| 4 | After resolution | No longer used. Everything downstream works on DataFusion plans. |

---

## 7. Extending the Spec

Add a new spec node when a feature can't be expressed with existing nodes:

1. Add the variant to the query node, command node, or expression enum, with
   fields that capture **intent**, not implementation. Use spec expressions and
   object names, never DataFusion types.
2. Produce it from every front end that supports the feature: Spark Connect
   (`proto/plan.rs`) and, if there's SQL syntax for it, the SQL analyzer.
3. Resolve it in `sail-plan` (see [07](07-plan-resolver.md)).
4. Keep the field order and serde names stable. The spec is used in tests and
   logs.

---

## 8. Where to Look

| Topic | Location |
|-------|----------|
| Plans, query nodes, command nodes | `crates/sail-common/src/spec/plan.rs` |
| Expressions, object names, identifiers | `crates/sail-common/src/spec/expression.rs` |
| Data types | `crates/sail-common/src/spec/data_type.rs` |
| Literals | `crates/sail-common/src/spec/literal.rs` |
| Configuration | `crates/sail-common/src/config/` |
| Runtimes | `crates/sail-common/src/runtime.rs` |
| Errors | `crates/sail-common/src/error.rs` |
| Settings that affect resolution (plan config) | `crates/sail-plan/src/config.rs` |
