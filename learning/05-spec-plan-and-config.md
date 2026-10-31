# 05 · The Spec Plan: Sail's Neutral Description of a Query

Every request that reaches Sail, whether it's a PySpark DataFrame, a SQL string,
or a Flight SQL statement, is first turned into the same thing: a **spec plan**.
The spec plan is the common language between Sail's front ends and its planner.
This doc explains what it looks like, why it exists, and a few of its less
obvious features. It also covers the rest of sail-common, the small crate of
foundations shared by everything else.

```text
PySpark DataFrame ──(Spark Connect translator)──┐
spark.sql("…")    ──(SQL parser + analyzer)─────┼──► spec plan ──► resolver ──► DataFusion
Flight SQL query  ──(SQL parser + analyzer)─────┘
```

---

## What a spec plan is

A spec plan is a tree of plain Rust data: enums and structs, with nothing
clever inside. It can be printed for debugging and serialized. What makes it
special is what it *doesn't* contain: it records what the user asked for,
without having checked any of it.

Table names are just names, like ["sales", "orders"]. Columns are *unresolved
attributes*: a name, plus optionally the ID of the DataFrame the reference came
from. Functions are *unresolved functions*: a name, its arguments, and flags such
as DISTINCT or IGNORE NULLS. Even the comparison `>` is, at this stage, just a
call to a function named ">". Turning these names into real tables, columns, and
functions is the resolver's job (see [07](07-plan-resolver.md)).

At the top level, a spec plan is either a **query**, which produces rows, or a
**command**, which does something, such as creating a table or writing data,
and may return a small result.

## Why have a separate representation at all?

It would be possible to go straight from Spark protobuf or SQL syntax trees to
DataFusion plans. Sail adds this middle step for several reasons, and the most
important is avoiding duplicate work. Resolution is the hardest part of
planning: catalogs, schemas, functions, Spark's naming rules. With a shared spec
plan, that logic is written once. Supporting a new protocol means writing one
translator, not a second resolver.

The second reason is that translators stay simple. Because the spec plan doesn't
require anything to be looked up, the SQL analyzer and the Spark Connect
translator need no session, no catalogs, and no function registry. They're pure
functions from input to output, which makes them easy to test.

Third, it normalizes. A DataFrame call like `df.filter(col("x") > 5)` and the SQL
`WHERE x > 5` become the same filter node, so the resolver handles one form, not
two.

Fourth, it has room for things DataFusion doesn't. pandas UDFs, `mapInPandas`,
streaming watermarks, `df.na.fill`, `df.stat.crosstab` and other Spark-only
features all have spec nodes, even though DataFusion has no equivalent.

Finally, because the spec plan is plain data, you can print it. When a query
returns a wrong result, printing the spec plan tells you quickly whether the
problem is in translation or later.

## What's inside

There are about 70 kinds of query steps. They cover reading (named tables, files
with a format and options, table functions, inline data, ranges), the usual
relational operations (projection, filter, join, aggregate, sort, limit,
distinct, union and friends, aliases), column operations (adding, renaming, and
dropping columns), reshaping (pivot, unpivot, and generators like explode),
repartitioning, sampling, Python operations (map in pandas, grouped and
co-grouped pandas functions, Python table functions), streaming watermarks, the
DataFrame helpers for missing values and statistics, and SQL structure such as
WITH clauses and bound parameters.

There are about 60 kinds of commands. They manage catalogs, databases, tables,
views, and functions; write data (`df.write`, `df.writeTo`, streaming writes,
INSERT, LOAD DATA); change rows (MERGE, UPDATE, DELETE); manage caching; and
display things (`df.show()`, notebook HTML, EXPLAIN, DESCRIBE).

Expressions include literals, column references, function calls, stars (with
Spark 4's options such as `SELECT * EXCEPT (a)`), aliases, casts, sort orders,
window specifications, lambdas for higher-order functions, struct field updates,
subqueries, grouping sets, inline Python UDF definitions, and parameter
placeholders.

Data types closely follow Arrow's: integers of every width, floats, decimals,
dates, times, timestamps, intervals, binary and string types, lists, structs,
maps, and so on. Literals carry their exact type, down to timestamp precision.

## Why DataFrame IDs matter: the self-join

Each DataFrame in PySpark carries a plan ID, and every column reference made
through a DataFrame remembers it. This sounds like a detail until you join a
table to itself:

```python
people = spark.table("people")
managers = people.alias("m")
pairs = people.join(managers, people["manager_id"] == managers["id"])
pairs.select(people["name"], managers["name"]).show()
```

Both sides of the join have columns called name and id. In the spec plan,
`people["name"]` is "the column called name, from the DataFrame people", and
`managers["name"]` is "the column called name, from the DataFrame managers". The
resolver uses these IDs to pick the right column on each side. A plain SQL
reference to name, with no ID, would be ambiguous here and fail with "ambiguous
attribute", which is exactly what Spark does.

## Types that wait for the session

A few spec types are deliberately left undecided, because their final form
depends on session settings that the translators don't know.

Spark's STRING type is a good example. In Arrow it could be a regular string
array, with 32-bit offsets, or a large string array, with 64-bit offsets that
allow more than 2 GB of text per batch. Which one Sail uses depends on the
session setting spark.sql.execution.arrow.useLargeVarTypes. So the spec plan
records a "configured string" type, and the resolver picks the concrete Arrow
type once it can see the session. Spark's plain TIMESTAMP works the same way: it
means "with local time zone" or "without time zone" depending on
spark.sql.timestampType.

This is a small but clear example of why resolution is a separate step. The
same SQL text can legitimately produce different Arrow types in two different
sessions, so the decision can't be made while parsing.

A few other types exist that Arrow doesn't have: user-defined types that carry
their Java or Python class names, Spark 4's VARIANT for semi-structured data, and
geometry and geography types for spatial data.

## One query, two front ends

The DataFrame code

```python
orders = spark.table("sales.orders")
orders.filter(orders["amount"] > 100).select("region", "amount").limit(5)
```

and the SQL

```sql
SELECT region, amount FROM sales.orders WHERE amount > 100 LIMIT 5
```

produce essentially the same spec plan:

```text
Query: Limit(5)
  Project [ column region, column amount ]
    Filter function ">"(column amount, literal Int32 100)
      Read named table ["sales", "orders"]
```

The only difference is that, in the DataFrame version, the amount reference in
the filter also carries the plan ID of orders. After this point, Sail can't tell
which front end the query came from.

## The rest of sail-common

Besides the spec plan, sail-common holds the foundations everything else relies
on. The configuration module defines Sail's settings in a YAML file compiled
into the binary and loads environment variable overrides (see
[02](02-entry-points-and-server.md)). The runtime module creates the two thread
pools, one for query work and one for I/O. The error module defines the basic
error kinds (missing argument, invalid argument, not supported, internal error)
that each crate wraps in its own error type. A small object module makes trait
objects comparable and hashable, which DataFusion needs for Sail's custom plan
nodes and functions. There are also helpers for dates, times, and strings.

## Extending the spec

Add a spec node when a feature can't be expressed with the existing ones. Give
it fields that capture what the user *meant*, using spec expressions and names
rather than DataFusion types. Produce it from every front end that supports the
feature: the Spark Connect translator and, if there's SQL syntax for it, the SQL
analyzer. Then teach the resolver to handle it. Keep field names stable, because
spec plans appear in tests and logs.

## Where to look in the code

The spec plan lives in crates/sail-common/src/spec/: plan.rs defines plans,
query steps, and commands, expression.rs defines expressions and names,
data_type.rs the data types, and literal.rs the literals. Configuration is under
src/config/, the runtimes in src/runtime.rs, and the error types in src/error.rs.
The session settings that affect resolution, the plan config, are defined in
crates/sail-plan/src/config.rs.
