# 08 · Plans, Extension Nodes, and Optimizers

DataFusion comes with a rich set of operators: scans, filters, projections,
joins, aggregations, sorts, windows, unions, and repartitioning. Spark needs a
few more. This doc explains which operators Sail adds and why, how they plug
into DataFusion without forking it, which optimizer rules run, and how Sail's
cost-based join reordering works.

---

## Two kinds of plans

Planning in DataFusion happens in two layers. A **logical plan** says what to
compute: "filter these rows, group them by region, count them". A **physical
plan** says how: "scan these Parquet files in eight parallel partitions,
pre-aggregate each one, repartition by a hash of region, then finish the
aggregation". The resolver (see [07](07-plan-resolver.md)) produces the logical
plan. Optimizer rules improve it, a planner turns it into a physical plan, and
more rules improve that.

Sail adds its own operators at both layers. Each Spark-specific feature gets a
logical **extension node** describing what it means, and a matching physical
operator that does the work. DataFusion explicitly supports this: applications
can define their own logical nodes and physical operators and register a
planner that connects them. Because Sail uses these extension points rather than
changing DataFusion, it can keep up with DataFusion releases.

## The operators Sail adds, and why

Most of Sail's extension nodes exist because Spark defines a behavior precisely
and users depend on it.

**Range** implements `spark.range(...)`, generating numbers in parallel across a
chosen number of partitions. **Show string** implements `df.show()`, formatting
rows as Spark's familiar text table (or HTML in notebooks), with Spark's
truncation rules. **Monotonic ID** implements `monotonically_increasing_id()`
with Spark's exact formula: the partition number shifted left by 33 bits, plus
the row's position within the partition. So partition 0 produces 0, 1, 2, and
so on, partition 1 starts at 8,589,934,592, and the values match what Spark
would produce for the same partitioning. **Spark partition ID** returns the
current partition number.

**Sort within partitions** implements `sortWithinPartitions` and SORT BY, which
sort each partition separately without a global sort. **Explicit repartition**
records a repartition the user asked for, such as `df.repartition(8,
"customer_id")` or DISTRIBUTE BY, so that the optimizer doesn't remove it; a late
rule turns it into a real repartition. That matters because users repartition on
purpose, for example to control how many output files a write produces.

**Map partitions** runs Python functions over whole partitions for `mapInPandas`,
`mapInArrow`, and Python table functions (see
[15](15-functions-and-python-udfs.md)). **File write**, **file delete**, and
**merge into** describe writes, DELETE, and MERGE; their physical form comes from
the table format (see [12](12-data-sources-and-formats.md) and
[13](13-lakehouse-delta-and-iceberg.md)). A **catalog command** node wraps DDL
and metadata commands and calls the catalog manager when the plan runs. Finally,
a handful of streaming nodes run plans over unbounded input (more on these
below).

## Planning never changes anything: the barrier

There's one principle behind several of these nodes: **resolving or planning a
query must never have side effects**. Consider CREATE TABLE ... AS SELECT. It's
tempting to create the table while planning and then plan the write. But then
EXPLAIN of that statement would create the table, and a planning error after the
create would leave an empty, orphaned table behind.

Sail solves this with a **barrier** node. A barrier holds some *precondition*
plans and a *main* plan, and guarantees that the preconditions run to
completion before the main plan starts. Every write is wrapped in one: the
precondition might be "create the target table" and the main plan "write the
data". Planning only describes the work. At execution time the barrier runs the
create, waits for it, and then starts the write. A physical optimizer rule
enforces the waiting, even when the plans have different numbers of partitions,
by making sure no partition of the main plan can start before every partition of
the preconditions has finished.

## From logical node to physical operator

When DataFusion meets a logical node it doesn't recognize, it asks the
registered extension planners, in order, whether they can handle it. Sail
registers three. The lakehouse planners come first, because DELETE, UPDATE, and
MERGE on Delta and Iceberg tables need format-specific plans. The system table
planner comes next, for the `system.*` tables. Sail's general planner comes last
and handles everything else described above. The first planner that recognizes
the node produces the operator. If none does, planning fails, so **every new
logical node needs a planner case**.

## Logical optimization

Sail takes DataFusion's standard logical rules, such as pushing filters down,
removing unused columns, folding constants, eliminating common subexpressions,
and decorrelating subqueries, and makes three changes. Lakehouse rules run
**first**, so that DELETE, UPDATE, and MERGE are expanded into ordinary scans and
joins before the standard rules optimize them. A rule for a common lateral-join
pattern runs before DataFusion's own lateral-join rule: queries like
`LATERAL (SELECT t1.a + 1)` are rewritten into a simple cross join plus a
projection. And one DataFusion rule that pushes down leaf projections is
removed, because it doesn't suit how Sail uses projections.

## Physical optimization

After the physical plan is built, about twenty rules adjust it. Most come from
DataFusion: computing count, min, and max from statistics when possible;
choosing join algorithms and which side to build hash tables on; pushing
filters, projections, limits, and sorts down; inserting repartitions so that
joins and aggregations see the data partitioned the way they need; adding or
removing sorts; using top-K for ORDER BY ... LIMIT; making long-running operators
yield to the scheduler; and finally a sanity check that every requirement is
met.

Sail keeps **all** of DataFusion's default physical rules, **in the same
order**, and a test fails if that ever stops being true. That guards against
silently losing an optimization when DataFusion is upgraded. Sail inserts four
rules of its own. Join reordering runs early, if enabled (see below). Near the
end, one rule turns the user's explicit repartition into a real repartition
operator. Another makes sure the build side of a "collect left" hash join has
exactly one partition, a safety net that matters after joins have been
reordered. The last enforces the barrier behavior described above.

## Streaming plans

If a query reads from a streaming source, such as the rate or socket test
sources, an extra rewrite runs after logical optimization. It turns the plan
into a *streaming plan*, in which every operator except the sink carries extra
fields marking each row as an addition or a retraction. This is how a streaming
aggregation can correct a result it already emitted. A collector node at the top
can turn this stream of updates back into final results, as long as the query is
bounded.

## Cost-based join reordering

When a query joins many tables, as star and snowflake schemas do, the order of
the joins can change the runtime by orders of magnitude. Sail includes an
optional join reorder rule, enabled with optimizer.enable_join_reorder, that
chooses the order using estimated sizes.

Here's the intuition with numbers. Suppose a sales fact table has a billion
rows, a stores table filtered to one region has ten rows, and a products table
has 100,000 rows. Joining sales with products first produces a billion-row
intermediate result, and joining that with stores then shrinks it to about five
million. Joining sales with stores first produces only about five million rows,
and the products join keeps it there. The cost model adds up the sizes of the
intermediate results, so the first order costs around a billion and the second
around ten million. The rule picks the second, the same choice an experienced
engineer would make by hand.

To get there, the rule first finds regions of the plan made of inner equi-joins
(along with filters and projections between them) and treats everything else as
an opaque input. It builds a graph in which tables are vertices and join
conditions are edges, and groups columns that are joined by equality, so they
share one estimate of how many distinct values they hold. It estimates the size
of each table and each join from statistics: row counts, distinct counts, and
guesses for filter selectivity. Joins without equality conditions are estimated
pessimistically on purpose, to avoid accidentally building near-cross-products.

It then searches for the best order with dynamic programming over connected
subsets of tables, using the DPhyp algorithm. The cost of joining two subplans is
the cost of each plus the size of their join. If the search would generate more
than 10,000 candidate plans, it falls back to a greedy algorithm, and above ten
tables it prunes more aggressively. For star schemas with one dominant fact
table, it penalizes joining dimensions to each other first when the estimates
are uncertain. Finally it rebuilds the plan in the chosen order, restoring the
original column order and the filters and projections.

## Following one node through planning

Take `spark.range(1_000_000).selectExpr("id % 10 AS k").groupBy("k").count().show()`.
The resolver produces a show-string node over an aggregation over a projection
over a range node. Logical optimization simplifies the projection, and the two
extension nodes pass through untouched. The extension planner turns the range
node into a range operator split across the target number of partitions and the
show-string node into its operator, while DataFusion plans the aggregation as a
partial and a final step. Physical optimization inserts a hash repartition on k
between the two steps, and Sail's rules check the result. When it runs, the
show-string operator returns one batch containing the formatted table.

## Adding a new operator

Define the logical node in sail-logical-plan: its name, inputs, output schema,
expressions, and how to rebuild it with new inputs, with no side effects.
Implement the physical operator in sail-physical-plan, including its
partitioning and ordering properties and an execute method that returns a stream
of batches. Add a planner case connecting the two in sail-session. Teach the
resolver to produce the node (see [07](07-plan-resolver.md)). If the operator
must run on cluster workers, add it to the plan codec so it can be serialized
(see [09](09-local-and-cluster-execution.md)). And if it has partitioning or
ordering requirements, declare them, so the distribution and sorting rules can
work around it.

## Where to look in the code

Sail's logical extension nodes are in crates/sail-logical-plan/src and their
physical operators in crates/sail-physical-plan/src. The catalog command node is
defined in crates/sail-plan/src/catalog.rs. The planner that maps nodes to
operators is crates/sail-session/src/planner.rs. The logical rule list is
assembled in crates/sail-logical-optimizer/src/lib.rs and
crates/sail-session/src/optimizer.rs, and the physical rule list in
crates/sail-physical-optimizer/src/lib.rs. Join reordering lives under
crates/sail-physical-optimizer/src/join_reorder/ (graph.rs,
cardinality_estimator.rs, enumerator.rs, cost_model.rs, reconstructor.rs), and
the barrier and collect-left rules sit next to it in barrier.rs and
collect_left.rs. The streaming rewrite is in crates/sail-plan/src/streaming/.
