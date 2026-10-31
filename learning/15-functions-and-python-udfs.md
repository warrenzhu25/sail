# 15 · Functions and Python UDFs

A large part of Spark compatibility comes down to functions. Spark has hundreds
of built-in functions, and users expect every one of them to behave exactly as
it does in Spark, down to how it treats NULLs and which date format letters it
accepts. PySpark users also expect their own Python functions to just work. This
doc explains how Sail provides about 500 Spark-compatible functions, how a
function name in a query becomes executable code, and how Python UDFs run inside
Sail.

---

## DataFusion first, Spark semantics where they differ

DataFusion already ships a large function library, and many Spark functions map
straight onto it: square root, absolute value, lower case. Others differ in ways
that would produce wrong answers if Sail used DataFusion's version. Spark's
concat returns NULL if any argument is NULL. Spark's date_format uses Java-style
patterns like yyyy-MM-dd rather than strftime's %Y-%m-%d. element_at counts
array positions from 1. Dividing by zero returns NULL unless ANSI mode is on, in
which case it raises an ArithmeticException, and casting the string 'abc' to an
integer behaves the same way.

For functions like these, the sail-function crate provides Spark-specific
implementations: Spark's concat, Spark's date parts and intervals, Spark's number
formatting and parsing, Spark-compatible hashing, and so on, organized by area
(strings, dates and times, arrays, maps, math, JSON, CSV, XML, URLs, variants,
geospatial data). It also adds aggregates that DataFusion lacks, such as mode,
percentile, kurtosis, skewness, product, max_by and min_by, numeric histograms,
try_avg, and bitmap aggregates, plus Spark's version of the ntile window
function. Each is an ordinary DataFusion function: it works on whole Arrow
arrays at a time, runs on cluster workers just like a built-in, and takes part in
DataFusion's optimizations.

## From a name to code: builders

Sail doesn't put its Spark functions into DataFusion's function registry.
Instead, it keeps its own tables that map each Spark function name to a small
piece of code called a **builder**. There are separate tables for scalar
functions (about 360, the largest groups being string, math, and date
functions), aggregates (about 64), window functions, generators such as explode
and inline, and table-valued functions such as range.

A builder receives the function's arguments, already resolved and typed, along
with the session's settings, and returns a DataFusion expression. That's more
flexible than a plain registry entry, and the flexibility is the point. Many
builders are one line: square root maps directly to DataFusion's version with a
cast to double. Some point at a Sail implementation: concat uses Spark's concat.
Some contain real logic: they inspect argument types, insert casts, rewrite one
call into several expressions, or read the session's time zone or ANSI setting
to decide how to behave.

When the resolver meets a function call (see [07](07-plan-resolver.md)), it
looks in three places, in order. First come functions registered in the session,
which are mostly Python UDFs. Then the built-in scalar and generator table. Then
the aggregate table. Window functions in an OVER clause use the window table. A
name found nowhere produces an "unknown function" error. Because session
functions are checked first, a user function named upper replaces the built-in
upper, just as in Spark.

A few calls get special handling. count(\*) is rewritten to count a constant,
because there's no column to count. Aggregates also support DISTINCT, FILTER
(WHERE …), IGNORE NULLS, and ordered-set syntax like
`percentile_cont(0.5) WITHIN GROUP (ORDER BY x)`. Higher-order functions such as
`transform(arr, x -> x + 1)` take lambdas, and named arguments are supported.

## Python UDFs run inside Sail

Spark runs Python UDFs in separate Python worker processes and ships data to
them over sockets. Sail takes a different approach: it **embeds CPython** in its
own process using PyO3, and hands Arrow data to Python directly. There's no
inter-process serialization and no pool of Python processes to manage.

Here's what happens to a UDF. When a PySpark program defines and uses one, it
sends Sail the function pickled with cloudpickle, along with its return type,
its *eval type* (which tells Sail what style of UDF it is), and whether it's
deterministic. Sail stores it as a session function. During resolution, Sail
creates the matching expression or operator, recording the input and output
Arrow types and the relevant session settings (time zone and pandas conversion
options). The first time the function runs, Sail acquires Python's interpreter
lock, unpickles it, and keeps the Python object for reuse. For each batch of
data, Sail converts the input Arrow arrays into what the function expects, calls
it, and converts the results back to Arrow using the declared return type. A
bundled Python helper module does the conversions, using PySpark's own
serializers where possible. If the function raises an exception, the traceback
is captured and reaches the client as a PythonException (see
[03](03-spark-connect.md)).

## Styles of Python UDF

PySpark offers several styles of Python function, and Sail supports them all.
The difference between them is mostly about how much data each call to Python
sees.

A plain `@udf` is called once per row. Sail converts each value into a Python
object, calls the function, and converts the result back. `@udf(useArrow=True)`
and Spark 4's Arrow UDFs receive whole Arrow batches. `@pandas_udf` receives
each column as a pandas Series, one batch at a time, and can also come in an
iterator form. A grouped-aggregate pandas UDF collects each group's rows and
returns one value. `applyInPandas` after a groupBy gives the function each
group as a pandas DataFrame and expects rows back, and the cogrouped form hands
two groups to the function together. `mapInPandas` and `mapInArrow` stream each
whole partition through the function, using a dedicated operator (see
[08](08-logical-physical-plans-and-optimizers.md)). Python table functions
(`@udtf`) can produce many rows from each input row. Any of these can be
registered with `spark.udf.register` so SQL can call it.

```python
from pyspark.sql.functions import udf, pandas_udf
import pandas as pd

@udf("int")
def plus_one(x): return x + 1                       # called once per row

@pandas_udf("double")
def zscore(v: pd.Series) -> pd.Series:              # called once per batch
    return (v - v.mean()) / v.std()

spark.udf.register("plus_one", plus_one)
spark.sql("SELECT plus_one(id) FROM range(3)").show()
```

## Making UDFs fast

The style you choose matters more than anything else. Take a column of ten
million rows processed in batches of 8,192. A plain `@udf` makes about ten million
Python calls, each converting one value in and one value out. A pandas or Arrow
UDF makes about 1,220 calls, one per batch, and does its conversions on whole
arrays. That's roughly eight thousand times fewer trips into Python, and the
work inside those calls usually happens in pandas, NumPy, or Arrow, which are
written in native code.

There's a second reason vectorized UDFs scale better. All Python code in one
process shares Python's interpreter lock, so when several partitions run
pure-Python code in parallel, they take turns. Native libraries like pandas and
NumPy release the lock for much of their work, so vectorized UDFs spend far less
time waiting.

In cluster mode, each worker runs its own embedded Python, so every worker image
needs the same Python packages your UDFs import.

## Following one UDF

Take `df.select(zscore("v"))`. PySpark pickles zscore and sends a projection
whose expression is an inline UDF of the scalar pandas style returning a double.
Spark Connect translates it into a spec inline-UDF expression. The resolver
creates a Python UDF expression of the right kind, with v's type and the session
settings. Physical planning places it in a projection, and in cluster mode the
pickled bytes travel inside the task's plan. For each batch, Sail takes the
interpreter lock, fetches the cached function, turns column v into a pandas
Series, calls zscore, converts the result into an Arrow array of doubles, and
moves on.

## Adding a built-in function

First check whether DataFusion already has the function with Spark's behavior.
If it does, a new line in the right builder table is enough. If not, write the
implementation in sail-function under the right area, handling NULLs and ANSI
mode, then add a builder that calls it. If you've created a new kind of function
object that must run on workers, make sure the plan codec can serialize it (see
[09](09-local-and-cluster-execution.md)). Finally add tests that compare with
Spark (see [17](17-testing-and-development.md)), and update the compatibility
data under python/pysail/data/compatibility/functions/, which records each
function's status as supported, partially supported, planned, or not supported.

## Where to look in the code

The builder tables live in crates/sail-plan/src/function/: common.rs defines the
builder styles, scalar/ holds the scalar tables by category, and aggregate.rs,
window.rs, generator.rs, and table/ hold the rest. The order in which the
resolver looks up functions is in crates/sail-plan/src/resolver/expression/function.rs.
Spark-compatible implementations are in crates/sail-function/src/, under scalar/,
aggregate/, and window/. The Python UDF runtime is crates/sail-python-udf: the UDF
styles are under src/udf/, conversion between Arrow and Python in conversion.rs
and the bundled python/spark.py, pickled payload handling in src/cereal/, and UDF
settings in config.rs. The pysail bindings are in crates/sail-python.
