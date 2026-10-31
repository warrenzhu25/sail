# 08 · Logical and Physical Plans, Extension Nodes, and Optimizers

> **You'll learn:** which operators Sail adds to DataFusion and why; how custom
> logical nodes become physical operators; which optimizer rules run and in
> what order; and how cost-based join reordering works.
>
> **Crates:** `sail-logical-plan`, `sail-physical-plan`,
> `sail-logical-optimizer`, `sail-physical-optimizer`, plus the extension
> planner in `sail-session/src/planner.rs`


## In One Minute

- DataFusion provides most operators. Sail adds about 15 **extension nodes** for
  Spark-only behavior: `range`, `show`, Python `mapInPandas`,
  `monotonically_increasing_id`, writes, deletes, merge, catalog commands,
  barriers, and streaming.
- Each logical extension node has a physical operator, connected by the
  **extension planner**.
- Side effects only happen during **execution**, never while planning. The
  **barrier** node makes "create the table, then write" safe.
- Sail keeps **all** of DataFusion's physical optimizer rules, in order, and
  adds a few of its own.
- Optional **cost-based join reordering** picks a good join order for many-table
  queries.

---

## 1. What This Component Does

DataFusion has a rich set of built-in operators: scans, filters, projections,
joins, aggregates, sorts, windows, unions, and repartitioning. Spark needs a few
more. Sail fills the gap in two layers:

| Layer | What Sail adds |
|-------|----------------|
| **Logical** (what to compute) | About 15 **extension nodes** that represent Spark-only operations |
| **Physical** (how to compute) | A matching **execution operator** for each, plus a few physical-only helpers |
| **Planning** | An **extension planner** that maps each logical node to its operator |
| **Optimization** | A curated list of DataFusion's logical and physical rules plus Sail's own rules, including **cost-based join reordering** |

---

## 2. Why It's Built This Way

**Use DataFusion's extension points, don't fork it.** DataFusion lets
applications define their own logical nodes (`UserDefinedLogicalNode`) and
physical operators (`ExecutionPlan`), and plug in a planner for them. Sail's
Spark-specific behavior lives entirely in these extensions, so Sail can keep up
with DataFusion releases.

**The resolver has no side effects.** Resolving a plan must not change anything.
For example, `CREATE TABLE AS SELECT` can't create the table while planning,
because `EXPLAIN` of that statement must not create the table either. Side
effects are expressed as nodes and happen only during **execution**. The
**barrier** node exists for this: "run these preconditions (such as a catalog
change) first, then the main plan."

**Keep every DataFusion rule, add a few.** Sail's physical rule list is checked
by a test: it must contain **all** of DataFusion's default physical rules, **in
the same order**, with Sail's rules inserted around them. This guards against
accidentally dropping an optimization when upgrading DataFusion.

---

## 3. How It Works

### 3.1 Sail's extension nodes

| Logical node | Physical operator | Spark feature | What it does |
|--------------|-------------------|---------------|--------------|
| Range | RangeExec | `spark.range(…)`, `range()` table function | Generates numbers in parallel, split across a set number of partitions |
| ShowString | ShowStringExec | `df.show()`, notebook HTML | Formats rows as a text or HTML table, with Spark's truncation rules |
| MapPartitions | MapPartitionsExec | `mapInPandas`, `mapInArrow`, Python UDTFs | Streams each partition through a Python function (see [15](15-functions-and-python-udfs.md)) |
| MonotonicId | MonotonicIdExec | `monotonically_increasing_id()` | Computes `(partition_id << 33) + row_number_within_partition`, exactly like Spark |
| SparkPartitionId | SparkPartitionIdExec | `spark_partition_id()` | Returns the current partition number |
| SortWithinPartitions | a sort that keeps partitioning | `sortWithinPartitions`, `SORT BY` | Sorts each partition separately |
| ExplicitRepartition | ExplicitRepartitionExec → rewritten to a repartition | `repartition(n)`, `repartition(n, cols)`, `DISTRIBUTE BY` | Records a user-requested repartition so the optimizer keeps it |
| SchemaPivot | SchemaPivotExec | (internal helper) | Collects its input and outputs a single string column built from the given column names. The current resolver doesn't produce this node. |
| FileWrite | the table format's writer | `df.write`, `INSERT`, CTAS | Writes data through the format's sink (see [12](12-data-sources-and-formats.md)) |
| FileDelete | the format's row-level writer | `DELETE FROM` | Deletes rows in lakehouse tables (see [13](13-lakehouse-delta-and-iceberg.md)) |
| MergeInto | the lakehouse merge plan, with a cardinality check | `MERGE INTO` | Runs merge actions and enforces that each target row matches at most one source row |
| CatalogCommand (in `sail-plan`) | CatalogCommandExec | DDL, `SHOW`, `DESCRIBE`, `USE`, … | Calls the catalog manager at execution time and returns a result table |
| Barrier | BarrierExec | Every write | Wraps each file write. Runs precondition plans (such as the catalog command that creates the target table) to completion before the write starts. |
| Stream source wrapper, adapter, filter, limit, collector | Stream* operators | Structured Streaming | Run a plan over unbounded input and produce updates (see 3.5) |

### 3.2 The extension planner

When DataFusion finds a logical node it doesn't know, it asks the registered
extension planners **in order**:

1. **Lakehouse planners**: Delta and Iceberg row-level operations, such as
   expanding `DELETE`, `UPDATE`, and `MERGE` into scan, rewrite, and commit plans.
2. **System table planner**: the `system.*` tables.
3. **Sail's general planner**: everything in the table above.

The first planner that recognizes the node produces the operator. If none does,
planning fails, so **every new logical node needs a planner case**.

### 3.3 Logical optimizer rules

Sail takes DataFusion's default analyzer rules (type coercion and so on) and
default optimizer rules, with these changes:

| Change | Why |
|--------|-----|
| Lakehouse rules run **first** (for example, *expand row-level operation*) | `DELETE`, `UPDATE`, and `MERGE` must become ordinary scans and joins before the standard rules optimize them |
| **Decorrelate lateral projection** is added before DataFusion's lateral-join decorrelation | Handles the common case `LATERAL (SELECT t1.a + 1)` by turning it into a cross join plus a projection |
| *Push down leaf projections* is removed | Doesn't fit how Sail uses projections |

Everything else (filter pushdown, projection pruning, constant folding, common
subexpression elimination, subquery decorrelation, limit pushdown, …) is standard
DataFusion.

### 3.4 Physical optimizer rules

The rules run in this order. Sail's additions are in **bold**.

| # | Rule | Purpose |
|---|------|---------|
| 1 | Output requirements (add) | Remember required ordering and distribution at the root |
| 2 | Aggregate statistics | Answer `count(*)`, `min`, `max` from statistics when possible |
| 3 | **Join reorder** (if `optimizer.enable_join_reorder`) | Cost-based join order, see 3.6 |
| 4 | Join selection | Choose hash, sort-merge, or nested-loop join, build side, and partitioned vs. collect-left |
| 5 | Limited distinct aggregation | `SELECT DISTINCT … LIMIT n` stops early |
| 6 | Filter pushdown | Push filters into scans and through joins |
| 7 | Enforce distribution | Insert repartitions so joins and aggregates get the partitioning they need |
| 8 | Combine partial/final aggregate | Merge two aggregation steps when there's no repartition between them |
| 9 | Enforce sorting | Insert or remove sorts |
| 10 | Optimize aggregate order | Use existing order for aggregates |
| 11 | Projection pushdown | Drop unused columns early |
| 12 | Output requirements (remove) | |
| 13 | Top-K aggregation | `ORDER BY agg LIMIT k` |
| 14 | Limit push past windows, limit pushdown | Push limits down |
| 15 | Projection pushdown (again), push down sort | |
| 16 | Ensure cooperative | Make long-running operators yield to the async scheduler |
| 17 | Filter pushdown (post-optimization) | Dynamic filters, for example from hash join build sides |
| 18 | **Rewrite explicit repartition** | Turn the user's `repartition()` node into a real repartition operator |
| 19 | **Rewrite collect-left hash join** | Safety net: make sure the build side of a collect-left hash join has exactly one partition, even after reordering |
| 20 | **Enforce barrier partitioning** | Make sure all barrier preconditions finish before the main plan starts |
| 21 | Sanity check | Check that distribution and ordering requirements are met |

### 3.5 Streaming plans

If a plan reads from a streaming source (rate, socket, Kafka-style formats, …),
a rewrite step after logical optimization turns it into a **streaming plan**.
Every operator except the sink carries a *flow event* schema: the data columns
plus extra fields that mark whether a row is being added or retracted. A
**collector** node at the top can turn the stream of retractable updates back
into final results, as long as the query is bounded.

### 3.6 Cost-based join reordering

When a query joins many tables (star and snowflake schemas, TPC-DS style), the
order of the joins can change runtime by orders of magnitude. Sail's join
reorder rule (off by default) works on physical plans:

1. **Find reorderable regions.** Walk the plan bottom-up, collecting maximal
   groups of inner equi-joins (plus filters and projections), and treat
   everything else as a leaf relation.
2. **Build a join graph.** Relations are vertices, and join conditions are
   edges. Columns joined by equality are placed in **equivalence sets**, which
   share a single estimate of distinct values.
3. **Estimate cardinalities** from scan statistics: row counts, distinct counts,
   and filter selectivity (with fixed guesses for non-equality filters and a
   deliberately pessimistic guess for theta joins, to avoid near-cross-products).
4. **Enumerate join orders** with dynamic programming over connected subsets
   (the DPhyp approach). The cost of joining A and B is cost(A) + cost(B) +
   cardinality(A ⋈ B). The search falls back to a **greedy** algorithm when more
   than 10,000 candidate plans would be generated, and prunes more aggressively
   above 10 relations.
5. **Protect the fact table.** In star schemas with one dominant "fact" table
   (at least 5 relations, with the fact table holding most of the rows), joins
   that avoid the fact table and have low-confidence estimates get a penalty,
   so dimensions aren't joined to each other first.
6. **Rebuild the plan** in the chosen order, restoring the original column order
   and any filters and projections.

---

## 4. Examples

### 4.1 `monotonically_increasing_id()`

```python
spark.range(0, 10, numPartitions=2).withColumn("id2", F.monotonically_increasing_id()).show()
```

Partition 0 produces IDs 0–4. Partition 1 produces `1<<33` (8589934592) through
8589934596. Those are the same values Spark produces.

### 4.2 `repartition` stays where you put it

```python
df.repartition(8, "customer_id").write.parquet("out/")
```

The explicit repartition node survives logical optimization, and the late
physical rule turns it into a hash repartition into 8 partitions. That gives 8
output file groups, as in Spark.

### 4.3 CTAS uses a barrier

```sql
CREATE TABLE top_customers AS
SELECT customer_id, sum(amount) AS total FROM orders GROUP BY 1 ORDER BY total DESC LIMIT 100
```

The plan is roughly *Barrier(precondition = create-table catalog command,
main = FileWrite(query))*. The table is created during execution, never during
planning, and the write starts only after the create succeeds.

### 4.4 Turning on join reordering

```bash
export SAIL_OPTIMIZER__ENABLE_JOIN_REORDER=true
```

Then compare `EXPLAIN` output for a five-table star join with the setting off
and on.

### 4.5 Join reordering with numbers

A star query joins a large fact table with two dimensions:

| Relation | Rows after filters |
|----------|--------------------|
| `sales` (fact) | 1,000,000,000 |
| `stores` (filtered to one region) | 10 |
| `products` | 100,000 |

Assume each join shrinks or keeps rows as follows: `sales ⋈ stores` keeps the
sales for 10 stores, about **5,000,000** rows. Joining that with `products` (a
lookup) keeps 5,000,000.

| Order | Intermediate result | Cost = sum of intermediate sizes |
|-------|---------------------|----------------------------------|
| (`sales` ⋈ `products`) ⋈ `stores` | 1,000,000,000 rows first | ≈ 1,005,000,000 |
| (`sales` ⋈ `stores`) ⋈ `products` | 5,000,000 rows first | ≈ 10,000,000 |

The cost model (cost of children + size of the join result) picks the second
order, joining the selective dimension first. That's the decision a human
would make, and the optimizer makes it automatically when
`optimizer.enable_join_reorder=true`.

### 4.6 Why a barrier instead of "just run the catalog command first"

If the resolver created the table while planning, then:

- `EXPLAIN CREATE TABLE … AS SELECT …` would create the table.
- A planning error *after* the create would leave an empty, orphaned table.

With a barrier, planning has no side effects. At execution time the barrier
runs the create command to completion, then starts the write, and both appear
in one plan you can inspect with `EXPLAIN`.

---

## 5. End-to-End: A Logical Node's Journey

Take `spark.range(1_000_000).selectExpr("id % 10 AS k").groupBy("k").count().show()`:

| # | Stage | Plan shape |
|---|-------|------------|
| 1 | Resolver | ShowString( Aggregate( Projection( **RangeNode**(0..1e6) ) ) ) |
| 2 | Logical optimizer | Projection simplified. ShowString and Range stay as extension nodes. |
| 3 | Extension planner | RangeNode → RangeExec (split across target partitions). ShowString → ShowStringExec. Aggregate → partial and final AggregateExec. |
| 4 | Physical optimizer | Enforce distribution inserts a hash repartition on `k` between the partial and final aggregates. Sail's rules check the plan. |
| 5 | Job runner | Executes. ShowStringExec returns one batch with the formatted table. |

---

## 6. Adding a New Operator

1. **Logical node** in `sail-logical-plan`: implement DataFusion's user-defined
   logical node interface (name, inputs, schema, expressions, and how to rebuild
   with new inputs). Keep it free of side effects.
2. **Physical operator** in `sail-physical-plan`: implement `ExecutionPlan`
   (properties such as partitioning and ordering, `execute` returning a stream).
3. **Planner case** in `sail-session/src/planner.rs`: map node → operator.
4. **Resolver** in `sail-plan`: produce the node from the right spec plan
   (see [07](07-plan-resolver.md)).
5. **Cluster support**: if the operator must run on workers, add it to the plan
   codec so it can be serialized and sent to workers (see
   [09](09-local-and-cluster-execution.md)).
6. **Optimizer interaction**: if the operator has partitioning or ordering
   requirements, declare them so the distribution and sorting rules work around
   it.

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Logical extension nodes | `crates/sail-logical-plan/src/` |
| Physical operators | `crates/sail-physical-plan/src/` |
| Catalog command node and operator | `crates/sail-plan/src/catalog.rs`, `crates/sail-physical-plan/src/catalog_command.rs` |
| Extension planner (node → operator) | `crates/sail-session/src/planner.rs` |
| Logical rule list | `crates/sail-logical-optimizer/src/lib.rs`, `crates/sail-session/src/optimizer.rs` |
| Physical rule list | `crates/sail-physical-optimizer/src/lib.rs` |
| Join reordering | `crates/sail-physical-optimizer/src/join_reorder/` (`graph.rs`, `cardinality_estimator.rs`, `enumerator.rs`, `cost_model.rs`, `reconstructor.rs`) |
| Barrier and collect-left rules | `crates/sail-physical-optimizer/src/barrier.rs`, `collect_left.rs` |
| Streaming rewrite | `crates/sail-plan/src/streaming/rewriter.rs`, `crates/sail-logical-plan/src/streaming/` |
