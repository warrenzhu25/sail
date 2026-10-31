# 07 · The Plan Resolver: From Intent to a DataFusion Plan

> **You'll learn:** how Sail turns a spec plan into an optimized DataFusion
> physical plan; how names, columns, functions, tables, views, and commands are
> resolved; and why Sail gives every column an internal ID.
>
> **Crate:** `sail-plan` (resolver, function mapping, catalog command node,
> explain, result formatting, streaming rewrite)

---

## 1. What This Component Does

The resolver is the **brain of Sail's Spark compatibility**. It takes the
protocol-neutral spec plan (see [05](05-spec-plan-and-config.md)) and the
session's plan config (time zone, ANSI mode, timestamp type, …) and produces
DataFusion plans that behave like Spark.

It owns the whole planning pipeline:

```text
spec::Plan
  │ 1. Resolve       → DataFusion LogicalPlan (with internal column IDs) + user-facing names
  │ 2. Analyze       → DataFusion analyzer rules (type coercion, …)
  │ 3. Optimize      → Sail and DataFusion logical optimizer rules
  │ 4. Stream rewrite→ adjustments for streaming plans (if the plan is streaming)
  │ 5. Physical plan → the extension planner turns logical nodes into operators, then physical optimizer rules run
  │ 6. Rename        → internal column IDs replaced with the user-facing names
  ▼
Arc<dyn ExecutionPlan>  (ready for a job runner)
```

The entry point is `resolve_and_execute_plan(session, plan_config, spec_plan)`.
It returns the physical plan plus text renderings of the initial logical,
final logical, and final physical plans, which `EXPLAIN` uses.

---

## 2. Why It's Built This Way

**Spark semantics differ from DataFusion's in many small ways.** Name matching
is case-insensitive. Duplicate column names are allowed (`SELECT a, a`). Join
outputs keep both sides' columns even when they share a name. `GROUP BY` can
refer to aliases. `HAVING` and `ORDER BY` can refer to columns that aren't in the
`SELECT` list. Hundreds of functions have Spark-specific behavior. The resolver
handles all of this **before** DataFusion sees the plan, so DataFusion only has
to execute well-formed plans.

**Every column gets an internal ID.** When the resolver produces a column, it
names it with an opaque ID like `#7` and records `#7 → "amount"` in its state.
This solves several problems:

- **Duplicates and ambiguity.** `SELECT a, a` or a self-join produces two columns
  named `a`. In DataFusion they'd clash. As `#3` and `#9` they don't.
- **DataFrame references.** A Spark Connect column reference can carry the plan
  ID of the DataFrame it came from (`df1["id"]`). Each ID records which plan IDs
  it belongs to, so `df1.id` and `df2.id` resolve to different columns.
- **Hidden columns.** Some operations need extra columns internally, such as the
  join keys of both sides of an outer join, or columns used only by `ORDER BY`.
  These are marked *hidden* and removed from the final output.

At the end, a **named plan** carries the logical plan and the list of
user-facing names. After physical planning, the output columns are renamed
from IDs back to those names.

**Functions are a lookup table of builders.** Sail doesn't register its Spark
functions in DataFusion's registry. It keeps a table that maps each function name
to a small builder that receives already-resolved arguments and the session
context, and returns a DataFusion expression. A builder can map directly to a
DataFusion function (`sqrt`), wrap it with casts, call a Sail-specific UDF
(`concat` uses Sail's Spark-compatible concat), or do custom logic
(`upper`, `element_at`). Behavior that depends on the session, such as ANSI mode
or time zone, is handled here.

---

## 3. How It Works

### 3.1 Resolver state

One state object is threaded through the whole resolution of a plan:

| Field | Purpose |
|-------|---------|
| Field registry | Maps each internal ID (`#0`, `#1`, …) to a user name, the plan IDs that refer to it, and whether it's hidden |
| Outer query schema | The schema visible to a correlated subquery from its enclosing query |
| Aggregate state | Whether we're resolving `GROUP BY` or `HAVING`, and the projections and grouping expressions available, so `HAVING count(*) > 1` can reuse aggregate results |
| CTEs | `WITH` definitions visible to the current query |
| Subquery references | Relations attached through Spark Connect's "with relations" mechanism |
| Parameter values | Bound `?` and `:name` values, so `IDENTIFIER(:tbl)` can be evaluated |

### 3.2 Resolving queries

The query resolver is a big dispatch over spec query nodes. Each node type has a
focused module: read, project, filter, join, aggregate, sort, limit, set
operations, pivot, sample, values, lateral views, lateral joins, column operations
(`withColumns`, rename, drop), NA functions, statistics, UDF and UDTF nodes, time
travel, and so on.

After resolving each node, the resolver **checks that every output column is
registered** in its state. If a new code path forgets to register a column, it
fails right away with a clear error instead of producing a plan with
unexplained names.

### 3.3 Resolving a named table

When a query reads `sales.orders`, the resolver tries these in order:

1. **`format.path` shortcut.** If the name has two parts and the first is a
   registered format, as in `` parquet.`s3://bucket/path` ``, it's a direct file
   read.
2. **CTE.** If a `WITH` clause defined that name, its plan is used.
3. **Catalog lookup.** The catalog manager (see [11](11-catalogs.md)) returns
   what the name refers to:
   - **Table**: its columns, format, location, partitioning, bucketing, sort
     order, constraints, and properties. The resolver builds a *source info*
     (paths, schema, partitioning, and layered options: table properties, then
     query options, then time-travel options) and asks the **table format** for a
     table source (see [12](12-data-sources-and-formats.md)).
   - **View**: the stored SQL text is **parsed again** and resolved, so the view
     sees current table definitions.
   - **Temporary or global temporary view**: the stored logical plan is reused,
     with fresh column IDs.
4. **Table sample** (`TABLESAMPLE`) and **time travel** (`VERSION AS OF`,
   `TIMESTAMP AS OF`) are applied where supported. Time travel isn't allowed on
   CTEs or views.

### 3.4 Resolving columns

To resolve a column reference like `t.amount` or `df["amount"]`:

1. Generate candidate interpretations. `a.b.c` could be table `a`, column `b`,
   field `c`, or column `a` with nested fields `b.c`, and so on.
2. Look for matching fields in the current input schema, comparing names
   case-insensitively and checking the plan ID if there is one.
3. In an aggregate context, also look in the aggregate's outputs.
4. Look at **hidden** fields (for example, sorting by a column that was
   projected away).
5. If still not found and we're inside a subquery, look in the **outer**
   query's schema. That makes it a correlated reference.
6. Zero matches gives *cannot resolve attribute*. More than one gives
   *ambiguous attribute*.

### 3.5 Resolving functions

For each unresolved function:

1. Resolve the arguments first.
2. Check the session's registered **user functions** (Python UDFs, see
   [15](15-functions-and-python-udfs.md)).
3. Look the name up in the **built-in tables**: scalar, generator (`explode`,
   `posexplode`, `inline`, `stack`, …), aggregate, window, and table-valued
   functions (`range`, …).
4. Call the builder with the arguments and a function context (session time
   zone, ANSI mode, …). It returns a DataFusion expression.
5. Unknown names produce *unknown function*, which the client sees as an
   `AnalysisException`.

Special cases include `count(*)` (rewritten to count a constant), `DISTINCT`
and `FILTER (WHERE …)` on aggregates, `IGNORE NULLS`, ordered-set aggregates
(`percentile_cont … WITHIN GROUP`), named arguments, and lambda functions
(`transform`, `filter`, `aggregate`).

### 3.6 Aggregation

Resolving an aggregate query happens in three steps:

1. Resolve the **grouping expressions** (which may refer to `SELECT` aliases)
   and the **projections**, pulling out the aggregate calls.
2. Resolve `HAVING` with access to both, so `HAVING count(*) > 10` reuses the
   same aggregate.
3. Rewrite into DataFusion's form: an aggregate node with grouping and
   aggregate expressions, an optional filter for `HAVING`, and a projection on
   top. `ROLLUP`, `CUBE`, and `GROUPING SETS` become grouping-set expressions,
   with `grouping()` and `grouping_id()` support.

### 3.7 Resolving commands

Commands become logical plans too, so they go through the same pipeline and job
runner as queries:

| Command kind | Logical plan produced | Executed by |
|--------------|-----------------------|-------------|
| Catalog commands (create, drop, alter, list, describe, `USE`, …) | A **catalog command node** wrapping a catalog command | A catalog command operator that calls the catalog manager and returns a small result table |
| Writes (`df.write`, `writeTo`, `INSERT`, CTAS, `INSERT OVERWRITE DIRECTORY`) | A **file write node** with sink info (mode, partitioning, bucketing, options) | The table format's writer (see [12](12-data-sources-and-formats.md)) |
| `DELETE`, `UPDATE`, `MERGE INTO` | **File delete** and **merge into** nodes | Lakehouse planners (see [13](13-lakehouse-delta-and-iceberg.md)) |
| `df.show()`, notebook HTML | A **show string** node | Formats rows as a text table, Spark style |
| `EXPLAIN` | Runs the pipeline in explain mode (simple, extended, codegen, cost, formatted, verbose, analyze) | Returns the plan text |
| Register UDF/UDTF, `SET` variable | Updates the session | Returns an empty result |
| Streaming write | A streaming plan, rewritten for streaming execution | A streaming query in the session |

Other Sail logical nodes the resolver produces: range, monotonically increasing
ID, Spark partition ID, explicit repartition, sort within partitions, map
partitions (Python), and barrier. See
[08](08-logical-physical-plans-and-optimizers.md).

---

## 4. Example: Resolving a Query Step by Step

```sql
SELECT region, count(*) AS n
FROM sales.orders
WHERE amount > 100
GROUP BY region
```

**Spec plan** (from [04](04-sql-parser-and-analyzer.md)):
Aggregate(group=[region], proj=[region, count(*) AS n], input=Filter(amount > 100, Read("sales.orders"))).

**Resolution:**

| Step | Action | State after |
|------|--------|-------------|
| Read | The catalog says `sales.orders` is a Parquet table at `s3://lake/orders` with columns `order_id` (bigint), `region` (string), and `amount` (double). The Parquet format returns a table source. Columns are registered. | `#0→order_id`, `#1→region`, `#2→amount` |
| Filter | `amount` resolves to `#2`. `>` with the integer literal `100` becomes `#2 > 100.0` after type coercion. | unchanged |
| Aggregate | `region` resolves to `#1`. `count(*)` becomes `count(1)`. The alias `n` registers a new field. | `#3→region` (group output), `#4→n` |
| Named plan | Logical plan plus user names `["region", "n"]` | |

**DataFusion logical plan** (simplified):

```text
Projection: #3, #4
  Aggregate: groupBy=[[#1 AS #3]], aggr=[[count(Int64(1)) AS #4]]
    Filter: #2 > Float64(100)
      TableScan: sales.orders projection=[#1, #2]
```

**After optimization and physical planning:** the filter is pushed into the
Parquet scan (row-group pruning and page-index filtering), the aggregate is
split into partial and final stages around a hash repartition on `region`, and
the output columns are renamed to `region` and `n`.

---

## 5. End-to-End: Where the Resolver Sits

| # | Caller | Resolver's role |
|---|--------|-----------------|
| 1 | Spark Connect or Flight SQL hands over a spec plan and plan config | |
| 2 | | Builds a resolver bound to the session |
| 3 | | Resolves the named plan (table lookups through the catalog manager and table formats, functions through the builder tables) |
| 4 | | Runs DataFusion's analyzer and optimizer through the session state (Sail rules included) |
| 5 | | Applies the streaming rewrite if the plan reads from a streaming source |
| 6 | | Asks the session's query planner for a physical plan (Sail extension planners, then physical optimizer rules) |
| 7 | | Renames output columns to user-facing names |
| 8 | Job runner receives the physical plan (see [09](09-local-and-cluster-execution.md)) | |

---

## 6. Extending and Debugging

**Adding a Spark function:**

1. Implement it in `sail-function` if DataFusion has no equivalent (see
   [15](15-functions-and-python-udfs.md)).
2. Add `("name", builder)` to the matching list in `sail-plan/src/function/`
   (scalar by category, aggregate, window, generator, table).
3. Pick the builder style: `unary` or `binary` for direct mappings, `udf` for a
   scalar UDF, or `custom` when you need the function context or argument
   rewriting.
4. Add a test and update the compatibility data (see
   [17](17-testing-and-development.md)).

**Adding a new query node:** add a resolver function in `resolver/query/`, make
sure every new output column is registered, and if no DataFusion node fits,
create a Sail logical extension node (see
[08](08-logical-physical-plans-and-optimizers.md)).

| Symptom | Where to look |
|---------|---------------|
| `cannot resolve attribute` | Column resolution. Check case, qualifiers, plan IDs, and whether the column was projected away before it's used. |
| `ambiguous attribute` | A join of two inputs that share a name. Qualify it, or use `df1["col"]`. |
| `unknown function: x` | Missing from the built-in tables, or the UDF isn't registered in this session |
| Wrong column names in results | The named plan's user-facing names, or the final rename |
| Different result from Spark for a function | That function's builder. Check casts, NULL handling, and ANSI behavior. |
| "attribute … is missing from the schema" from a new feature | A new column wasn't registered in the resolver state |

`EXPLAIN EXTENDED` shows the initial logical, final logical, and physical plans
that this pipeline records.

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Pipeline entry point (`resolve_and_execute_plan`) | `crates/sail-plan/src/lib.rs` |
| Resolver and named plan | `crates/sail-plan/src/resolver/mod.rs`, `plan.rs` |
| Resolver state (column IDs, hidden columns, CTEs) | `crates/sail-plan/src/resolver/state.rs` |
| Query nodes | `crates/sail-plan/src/resolver/query/` (`read.rs`, `aggregate.rs`, `join.rs`, `project.rs`, …) |
| Expressions | `crates/sail-plan/src/resolver/expression/` (`attribute.rs`, `function.rs`, `window.rs`, `subquery.rs`, `wildcard.rs`, `udf.rs`, …) |
| Commands | `crates/sail-plan/src/resolver/command/` (`write.rs`, `merge.rs`, `catalog/`, …) |
| Function builder tables | `crates/sail-plan/src/function/` (`scalar/`, `aggregate.rs`, `window.rs`, `generator.rs`, `table/`, `common.rs`) |
| Catalog command node | `crates/sail-plan/src/catalog.rs` |
| `EXPLAIN` | `crates/sail-plan/src/explain.rs` |
| `show()` formatting | `crates/sail-plan/src/formatter.rs` |
| Settings that affect resolution | `crates/sail-plan/src/config.rs` |
| Streaming rewrite | `crates/sail-plan/src/streaming/` |
