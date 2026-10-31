# 07 · The Plan Resolver: Turning Intent into a Plan

When a query reaches the resolver, Sail knows *what the user asked for* but not
yet *what any of it means*. The query still talks about "a table called
sales.orders", "a column called amount", and "a function called count". None of
these names has been checked. The resolver is the component that gives every
name its meaning, applies Spark's rules for how names behave, and produces a
plan that DataFusion can optimize and run. If Sail behaves like Spark, it's
largely because the resolver makes it so.

This doc follows one query from start to finish:

```sql
SELECT region, count(*) AS n
FROM sales.orders
WHERE amount > 100
GROUP BY region
ORDER BY n DESC
```

By the time it reaches the resolver, the SQL text has already been parsed and
turned into Sail's neutral *spec plan* (see [04](04-sql-parser-and-analyzer.md)
and [05](05-spec-plan-and-config.md)). The resolver receives that spec plan,
plus a handful of session settings such as the time zone and whether ANSI mode
is on.

---

## Why a separate resolver exists at all

DataFusion, the engine Sail is built on, already knows how to plan and run SQL.
So why not hand it the query directly?

Because Spark and DataFusion disagree in many small but visible ways. Spark
matches column names without caring about upper or lower case. It lets you
select the same column twice. It lets you join a table to itself, and both sides
keep their column names. It lets GROUP BY refer to an alias defined in the
SELECT list, and lets ORDER BY use a column you never selected. It has
hundreds of functions whose behavior differs slightly from DataFusion's
versions: how they treat NULLs, which date format letters they use, what happens
on overflow.

If Sail left these differences to DataFusion, every one of them would show up as
a wrong result or a confusing error. Instead, the resolver handles all of
Spark's rules first, and gives DataFusion a plan that is already unambiguous and
already uses Spark-compatible functions. DataFusion then does what it's best at:
optimizing and executing.

The resolver also owns the rest of the planning pipeline, so that one function
call takes a spec plan all the way to something runnable:

```text
spec plan  (names unresolved)
  │ 1. Resolve        → DataFusion logical plan, columns renamed to private IDs
  │ 2. Analyze        → DataFusion's analyzer rules (for example, type coercion)
  │ 3. Optimize       → Sail and DataFusion logical optimizer rules
  │ 4. Stream rewrite → only if the query reads from a streaming source
  │ 5. Physical plan  → Sail's extension planners, then physical optimizer rules
  │ 6. Rename         → private column IDs swapped back to the user's names
  ▼
physical plan, ready for a job runner
```

Steps 2, 3, and 5 mostly reuse DataFusion's machinery with Sail's own rules
added (see [08](08-logical-physical-plans-and-optimizers.md)). The interesting,
Spark-specific work happens in step 1, which is what the rest of this doc is
about.

---

## Why every column gets a private name

Spark lets you do things that would confuse a simpler engine. You can select the
same column twice, join a table to itself, and sort by a column you didn't
select. DataFusion, on the other hand, insists that the columns at each step of a
plan have distinct names.

The resolver bridges this gap by giving every column it produces a private name,
like #7, and keeping a notebook that says "#7 is what the user calls amount".
Two columns both called id become #3 and #9, so they never collide. Only when
the plan is finished are the private names swapped back for the names the user
expects to see.

The notebook records a little more than the user-facing name. For each private
name it also remembers:

- **Which DataFrames it came from.** PySpark tags every DataFrame with an ID,
  and a column reference like `orders["amount"]` carries that tag. In a
  self-join, both sides have an *amount*, but only one of them belongs to the
  `orders` DataFrame. The tag tells the resolver which one you meant.
- **Whether it's hidden.** Sometimes the resolver needs a column that the user
  shouldn't see in the result. More on this below.

When the resolver has finished, it hands over the plan together with the list of
names the user should see, in order. Sail calls this pair a *named plan*. Step 6
of the pipeline uses that list to rename the output columns.

As a safety net, after resolving each step of the query, the resolver checks that
every column in the output is in its notebook. If someone adds a feature and
forgets to register a column, the query fails right away with a clear error
instead of producing a plan with mysterious names.

---

## Following the query: reading the table

The resolver works from the bottom of the query upward, so the first thing it
meets is *FROM sales.orders*.

A table name could refer to several different things, and the resolver checks
them in a fixed order. First, it checks whether the name is really a file path in
disguise. Spark lets you write `` parquet.`s3://bucket/path` `` to query files
directly, so if the first part of a two-part name is a known file format, the
resolver treats the rest as a path. Next, it checks whether a WITH clause in
the same query defined that name. Only if neither applies does it ask the
catalog manager (see [11](11-catalogs.md)).

The catalog manager works out which catalog and database *sales.orders* belongs
to, then looks it up. It checks temporary views first, then real tables, then
saved views. The answer comes back in one of three shapes:

- **A table.** The catalog reports its columns, file format, location,
  partitioning, and properties. The resolver passes these to the matching table
  format, Parquet in our case (see [12](12-data-sources-and-formats.md)). The
  format lists the files and returns a table source that can be scanned.
- **A saved view.** Views are stored as SQL text. The resolver parses that text
  again and resolves it like any other query. Doing this every time means the
  view always sees the current definitions of the tables it uses.
- **A temporary view.** These are stored as already-resolved plans, so the
  resolver reuses the plan and just hands out fresh private names for its
  columns.

In our example, *sales.orders* is a Parquet table with columns order_id,
region, and amount. The resolver registers them in its notebook:

```text
#0 → order_id      #1 → region      #2 → amount
```

Time travel (VERSION AS OF, TIMESTAMP AS OF) and table sampling
(TABLESAMPLE) are applied at this point too, for the formats that support
them. Time travel isn't allowed on CTEs or views, because they have no version
history.

---

## Following the query: resolving columns in the filter

Next comes *WHERE amount > 100*. The resolver has to decide which column
*amount* means.

It begins by listing the possible readings of the name. A single word like
*amount* can only be a column, but a dotted name like *t.amount* might mean
"column amount of table t" or "field amount inside a struct column called t".
For each reading, the resolver searches the columns that are visible at this
point in the plan, comparing names without regard to case and checking the
DataFrame tag if there is one.

If nothing matches, it keeps looking in a few more places. Inside a GROUP BY
query, it also looks at the outputs of the aggregation. It looks at hidden
columns. And inside a subquery, it looks outward at the enclosing query's
columns, which is how correlated subqueries like `WHERE x > (SELECT avg(x) FROM
t2 WHERE t2.k = t1.k)` find *t1.k*.

If the search finds exactly one match, that's the answer. If it finds none, the
user gets *cannot resolve attribute*. If it finds more than one, for example
because two joined tables both have an *amount*, the user gets *ambiguous
attribute* and needs to qualify the name.

Here, *amount* matches #2. The comparison *> 100* compares a double column with
an integer, so DataFusion's type coercion later turns the literal into 100.0.

---

## Following the query: finding functions

The query uses one function, *count(\*)*. Function names are resolved by looking
in three places, in this order:

1. **Functions registered in the session.** These are mostly Python UDFs that the
   user registered with `spark.udf.register`. Because they're checked first, a
   user function named *upper* would replace the built-in *upper*, just as in
   Spark.
2. **Built-in scalar and generator functions**, such as *upper*, *date_format*,
   or *explode*.
3. **Built-in aggregate functions**, such as *count*, *sum*, or *percentile_cont*.

If a name isn't found anywhere, the user sees *unknown function*.

The built-ins aren't registered in DataFusion's own function registry. Sail keeps
its own table that maps each Spark function name to a small piece of code called
a *builder*. A builder receives the already-resolved arguments plus the session
settings, and returns a DataFusion expression. Many builders are trivial: *sqrt*
maps straight to DataFusion's square root, with a cast to double. Others do real
work. *concat* uses Sail's own implementation, because Spark returns NULL when
any argument is NULL. Date functions translate Spark's Java-style format letters.
Arithmetic functions check whether ANSI mode is on, which decides whether an
overflow becomes NULL or an error. This design is why Sail can match Spark
function by function without changing DataFusion itself.

*count(\*)* gets one small special case: the star is rewritten to count a
constant, because there's no column to count. The resolver also handles the
other aggregate extras here: DISTINCT, FILTER (WHERE …), IGNORE NULLS, and
ordered-set syntax like `percentile_cont(0.5) WITHIN GROUP (ORDER BY x)`.
Functions that take lambdas, such as `transform(arr, x -> x + 1)`, are resolved
here too.

---

## Following the query: grouping and sorting

*GROUP BY region* and *ORDER BY n DESC* look simple, but they show off two Spark
rules that need care.

For the aggregation, the resolver works in three passes. First it resolves the
grouping expressions and the SELECT list, pulling out the aggregate calls
(*count(\*)*) so they're computed once. Second, if there were a HAVING clause,
it would resolve it with access to both, so *HAVING count(\*) > 10* reuses the
same count instead of computing a new one. Third, it builds DataFusion's form of
the query: an aggregate step, an optional filter for HAVING, and a projection
on top. ROLLUP, CUBE, and GROUPING SETS are handled in the same place.

The new outputs get new private names:

```text
#3 → region   (the grouping key, as output by the aggregate)
#4 → n        (the count)
```

*ORDER BY n DESC* refers to an alias, which the resolver finds among the
aggregate's outputs.

Now suppose the query had been `SELECT region FROM … ORDER BY amount` instead.
*amount* isn't in the SELECT list, yet Spark allows sorting by it. The resolver
handles this with a **hidden column**. It keeps *amount* flowing through the
projection, marks it hidden in its notebook, lets the sort use it, and then adds
a final step that drops every hidden column. The same trick carries join keys
through outer joins and tracks positions for exploded arrays. The user never sees
these columns. They exist only so the plan can do what Spark promises.

After resolution, the logical plan for our example looks roughly like this:

```text
Sort: #4 DESC
  Projection: #3, #4
    Aggregate: groupBy=[#1 AS #3], aggr=[count(1) AS #4]
      Filter: #2 > 100.0
        TableScan: sales.orders  projection=[#1, #2]
```

along with the user-facing names *region* and *n*.

---

## Commands go through the same pipeline

Not every request is a query. CREATE TABLE, INSERT, SHOW TABLES,
`df.write`, DELETE, and `df.show()` all reach the resolver too. Sail's choice is
to turn each of them into a logical plan as well, so they flow through the same
pipeline and the same job runner as queries. One path to maintain is simpler than
two.

The trick is a small set of special plan nodes. Catalog operations (create,
drop, alter, list, describe, USE) become a *catalog command* node, which at run
time asks the catalog manager to do the work and returns a small result table.
Writes become a *file write* node that hands the data to the table format's
writer, wrapped in a *barrier* so that any preparation, such as creating the
target table, finishes before the write starts. DELETE, UPDATE, and MERGE
become nodes that the lakehouse formats know how to plan (see
[13](13-lakehouse-delta-and-iceberg.md)). `df.show()` becomes a node that formats
rows as Spark's familiar text table. EXPLAIN runs the pipeline in explain mode
and returns the plans as text.

One rule matters a lot here: **resolving a command never changes anything**.
The resolver only describes the work, and the work happens when the plan runs.
That's why `EXPLAIN CREATE TABLE … AS SELECT …` is safe: it shows the plan
without creating a table (see [08](08-logical-physical-plans-and-optimizers.md)
for how barriers enforce this).

---

## Putting it together

Back to our query. The resolver found *sales.orders* through the catalog and got
a Parquet source back. It gave the columns private names, matched *amount* in
the filter, found *count* in the aggregate table, built the aggregation, and
resolved the sort against the alias *n*. It then handed the plan to DataFusion's
analyzer and optimizer. They pushed the filter into the Parquet scan, so row
groups whose statistics rule out *amount > 100* are skipped, and split the
aggregation into a partial step and a final step around a repartition by region.
The extension planners produced the physical plan. Finally, the output columns
were renamed from #3 and #4 back to *region* and *n*. The finished physical plan
goes to the job runner (see [09](09-local-and-cluster-execution.md)), and
`EXPLAIN EXTENDED` would show you the initial logical plan, the optimized logical
plan, and the physical plan that the pipeline recorded along the way.

When something goes wrong, the error message usually tells you which part of
this story failed. *Cannot resolve attribute* means the column search came up
empty: check the spelling, the qualifier, and whether the column was projected
away earlier. *Ambiguous attribute* means two inputs share a name: qualify it,
or use the DataFrame form `df["col"]`. *Unknown function* means the name isn't a
built-in or a registered UDF in this session. And if a function returns a
different answer than Spark, the place to look is that function's builder, which
is where NULL handling, casts, and ANSI behavior are decided.

---

## Adding to the resolver

To support a new Spark function, first check whether DataFusion already has one
that behaves the same way. If so, adding a line to Sail's function table is
enough. If not, write the implementation in the functions crate (see
[15](15-functions-and-python-udfs.md)), then add a builder that calls it. Either
way, add a test that compares the result with Spark's.

To support a new kind of query step, add a resolver function for the new spec
node. Make sure every column it produces is registered in the notebook (the
safety net will remind you if you forget). If DataFusion has no plan node that
fits, create a Sail extension node (see
[08](08-logical-physical-plans-and-optimizers.md)).

---

## Where to look in the code

Everything described here lives in the `sail-plan` crate. The pipeline itself
is the function `resolve_and_execute_plan` in `src/lib.rs`. The resolver is
under `src/resolver/`: `state.rs` holds the notebook of private names and hidden
columns, `query/` has one file per kind of query step (`read.rs` for table
lookup, `aggregate.rs`, `join.rs`, and so on), `expression/` handles columns and
functions (`attribute.rs` for the column search, `function.rs` for the
function lookup order), and `command/` handles writes, merges, and catalog
commands. The function tables and their builders are in `src/function/`.
EXPLAIN is in `src/explain.rs`, the `show()` formatting in `src/formatter.rs`,
and the session settings that affect resolution in `src/config.rs`.
