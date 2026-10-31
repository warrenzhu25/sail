# 15 · Functions and Python UDFs

> **You'll learn:** how Sail provides about 500 Spark-compatible built-in
> functions, how a function name in a query becomes executable code, and how
> Python UDFs, pandas UDFs, UDAFs, UDTFs, `mapInPandas`, and `applyInPandas`
> run inside Sail's embedded Python interpreter.
>
> **Crates:** `sail-function` (Spark-compatible function implementations),
> `sail-plan/src/function` (the name → builder tables), `sail-python-udf`
> (Python UDF execution), `sail-python` (the `pysail` bindings)

---

## 1. What This Component Does

| Part | Responsibility |
|------|----------------|
| **Function tables** (in `sail-plan`) | Map each Spark function name to a *builder* that produces a DataFusion expression from resolved arguments |
| **Function implementations** (`sail-function`) | Spark-compatible scalar, aggregate, and window functions that DataFusion doesn't provide, or provides with different semantics |
| **Python UDF runtime** (`sail-python-udf`) | Runs user Python code over Arrow data inside the Sail process, through PyO3 |

Approximate counts in the function tables:

| Kind | Entries |
|------|---------|
| Scalar | ≈ 360: string (74), math (64), datetime (54), misc (31), array (25), conversion (14), lambda (12), conditional (11), predicate (11), collection, map, xml, variant, json, hash, bitwise, url, geo, csv, struct |
| Aggregate | ≈ 64 |
| Window | ≈ 70 entries (ranking and offset functions, plus aggregates usable as window functions) |
| Generators | 6 (`explode`, `explode_outer`, `posexplode`, `posexplode_outer`, `inline`, `inline_outer`, …) |
| Table-valued | `range`, … |

The repository also tracks Spark compatibility per function in
`python/pysail/data/compatibility/functions/` (status: supported, partially
supported, planned, or not supported).

---

## 2. Why It's Built This Way

**DataFusion first, Spark semantics where needed.** Many Spark functions map
directly onto DataFusion functions (`sqrt`, `abs`, `lower`). Others differ in
small but important ways:

- `concat` returns NULL if any argument is NULL in Spark.
- Date formats use Java patterns (`yyyy-MM-dd`), not strftime.
- ANSI mode makes overflows and invalid casts raise errors.
- Division by zero returns NULL (non-ANSI).
- Array indexes start at 1 in `element_at`.

For these, `sail-function` provides Spark-specific implementations
(`SparkConcat`, `spark_date_part`, `spark_to_number`, …), and the table points
at those instead.

**Builders, not a flat registry.** A builder gets the **resolved arguments**
and a **function context** (session time zone, ANSI mode, plan config). It can
inspect argument types, insert casts, rewrite into several expressions, or pick
different implementations depending on the session. DataFusion's plain function
registry can't express that.

**Python runs in-process.** Spark runs Python UDFs in separate worker processes
and moves data through sockets. Sail **embeds** CPython (PyO3) and passes Arrow
data to it directly. That removes inter-process serialization and process
management. The function is unpickled lazily, once, and reused.

---

## 3. How It Works

### 3.1 Resolving a function call

When the resolver meets an unresolved function `f(args…)` (see
[07](07-plan-resolver.md)):

1. Resolve the arguments.
2. If the session has a **user function** named `f` (a registered Python UDF),
   use it.
3. Else, if `f` is in the **scalar or generator table**, call its builder.
4. Else, if `f` is in the **aggregate table**, build an aggregate expression,
   applying `DISTINCT`, `FILTER`, `ORDER BY`, and `IGNORE NULLS`.
5. Window functions in `OVER (…)` use the **window table**.
6. Otherwise: *unknown function*.

**Builder styles** (examples from the code):

| Style | Example | Meaning |
|-------|---------|---------|
| `unary` / `binary` | `("sqrt", F::unary(double(expr_fn::sqrt)))` | Fixed arity, with a cast wrapper |
| `udf` | `("concat", F::udf(SparkConcat::new()))` | Use a Sail scalar UDF |
| `custom` | `("upper", F::custom(upper))`, `("try_element_at", F::custom(\|i\| element_at(i, true)))` | Arbitrary logic with access to the function context |
| `nullary` | `current_date()`-style functions | No arguments |

### 3.2 Built-in implementations

`sail-function` is organized by area:

- `scalar/`: array, collection, csv, datetime (Spark date parts, intervals,
  `make_timestamp`, `unix_timestamp`, `convert_tz`, Java-format → chrono format,
  …), geo, hash (Spark-compatible `hash`, `xxhash64`, …), json, map, math, misc,
  predicate, string (`format_number`, `levenshtein`, `soundex`, `mask`,
  `regexp_extract_all`, `sentences`, `split`, `to_number`, …), url, variant, xml,
  struct helpers, `explode`
- `aggregate/`: `mode`, `percentile`, `percentile_disc`, `kurtosis`,
  `skewness`, `product`, `max_by` / `min_by`, `histogram_numeric`, `try_avg`,
  bitmap aggregates, `schema_of_variant_agg`
- `window/`: Spark's `ntile`

Each is a DataFusion UDF, UDAF, or UDWF. That means it's vectorized over Arrow
arrays, works in both local and cluster mode (the plan codec serializes it by
name), and takes part in DataFusion's optimizations.

### 3.3 Python UDF kinds

| PySpark API | Sail kind | How it runs |
|-------------|-----------|-------------|
| `@udf` (row-at-a-time) | Batch scalar UDF | Arrow arrays are converted to Python values, the function is called per row, and results are converted back |
| `@udf(useArrow=True)` | Arrow-batched scalar UDF | Python receives Arrow-backed batches |
| `@pandas_udf` (scalar), iterator-of-series | Scalar pandas, scalar pandas iterator | Columns are passed as `pandas.Series` per batch |
| Spark 4 Arrow scalar UDFs | Scalar Arrow, scalar Arrow iterator | Columns are passed as `pyarrow` arrays |
| `@pandas_udf` grouped aggregate | UDAF | Each group's rows are collected, and the function returns one value |
| `df.groupBy(…).applyInPandas(f, schema)` | Group map | Batches are collected per group into a list of structs, then `f(pandas.DataFrame)` returns rows |
| `df.groupBy(…).cogroup(…).applyInPandas` | Cogroup map | Two groups go to the function together |
| `df.mapInPandas(f, schema)`, `mapInArrow` | Map-partitions iterator | The **map partitions** operator streams each partition's batches through `f` (see [08](08-logical-physical-plans-and-optimizers.md)) |
| `@udtf` | Python UDTF | A table function that can produce many rows per input row |
| `spark.udf.register("name", f)` | Any of the above | Registered in the session, so it's callable from SQL |

### 3.4 Execution details

1. **Registration.** The client sends the function's pickled bytes
   (cloudpickle), return type, eval type, and whether it's deterministic. Sail
   stores it as a session function (see [11](11-catalogs.md)).
2. **Planning.** The resolver creates the right expression or operator for the
   kind, with the input and output Arrow types and the UDF configuration
   (session time zone, pandas conversion settings from
   `spark.sql.execution.pandas.*`).
3. **First call.** Sail takes Python's interpreter lock, unpickles the function,
   and caches the Python object.
4. **Each batch.** Arrow arrays are handed to Python. A bundled Python helper
   module converts between Arrow and PySpark or pandas values, using PySpark's
   own serializers where possible. The function runs, and results are converted
   back to Arrow with the declared return type.
5. **Errors.** Python exceptions are captured with their traceback and appear to
   the client as `PythonException` (see [03](03-spark-connect.md)).

**Performance:**

- Plain `@udf` is the slowest: one Python call per row. Prefer `pandas_udf` or
  Arrow UDFs, which do one call per batch.
- All Python code in a process shares the **interpreter lock**. Pure-Python work
  from parallel partitions takes turns. Vectorized pandas or NumPy code releases
  the lock for much of its work.
- In cluster mode, each worker runs its own embedded Python, so the same Python
  packages must be installed on every worker image.

---

## 4. Examples

```python
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, pandas_udf
import pandas as pd

# Built-ins
df.select(F.date_format("ts", "yyyy-MM-dd"), F.element_at("arr", 1), F.concat("a", "b"))

# Row-at-a-time UDF
@udf("int")
def plus_one(x): return x + 1

# Vectorized pandas UDF
@pandas_udf("double")
def zscore(v: pd.Series) -> pd.Series: return (v - v.mean()) / v.std()

# Grouped map
def normalize(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf["v"] = pdf["v"] - pdf["v"].mean(); return pdf
df.groupBy("k").applyInPandas(normalize, schema="k long, v double")

# mapInPandas
def keep_positive(it):
    for pdf in it: yield pdf[pdf.v > 0]
df.mapInPandas(keep_positive, schema=df.schema)

# Register for SQL
spark.udf.register("plus_one", plus_one)
spark.sql("SELECT plus_one(id) FROM range(3)").show()
```

---

## 5. End-to-End: `df.select(zscore("v"))`

| # | Component | What happens |
|---|-----------|--------------|
| 1 | PySpark | Pickles `zscore` and sends a projection whose expression is an inline UDF (eval type: scalar pandas, return type `double`). |
| 2 | Spark Connect | Converts it to a spec inline UDF expression. |
| 3 | Resolver | Builds a Python scalar UDF expression (kind: scalar pandas) with the input type of `v` and the UDF settings. |
| 4 | Physical planning | A projection operator evaluates the UDF expression. In cluster mode the pickled bytes go into the task plan. |
| 5 | Execution | For each batch: take the interpreter lock, get the cached function, convert column `v` to a `pandas.Series`, call `zscore`, convert the result to an Arrow `Float64` array, and continue. |
| 6 | Result | Arrow batches flow on to the client. |

---

## 6. Adding a Built-in Function

1. Check whether DataFusion already has it with the right semantics. If so,
   just add a table entry.
2. If not, implement a UDF, UDAF, or UDWF in `sail-function` under the right
   area: its signature, return type logic, and an `invoke` that works on Arrow
   arrays and handles NULLs and ANSI mode.
3. Add `("spark_name", builder)` to the matching list in
   `sail-plan/src/function/`.
4. If it's a new UDF type that must run on workers, make sure the plan codec can
   serialize it (see [09](09-local-and-cluster-execution.md)).
5. Add tests (gold data or Spark comparison, see
   [17](17-testing-and-development.md)) and update the compatibility JSON.

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Function tables and builders | `crates/sail-plan/src/function/` (`common.rs`, `scalar/`, `aggregate.rs`, `window.rs`, `generator.rs`, `table/`) |
| Function lookup during resolution | `crates/sail-plan/src/resolver/expression/function.rs` |
| Spark-compatible implementations | `crates/sail-function/src/` (`scalar/`, `aggregate/`, `window/`) |
| Python UDF kinds | `crates/sail-python-udf/src/udf/` |
| Arrow ↔ Python conversion | `crates/sail-python-udf/src/conversion.rs`, `python/spark.py` |
| Pickled payload formats | `crates/sail-python-udf/src/cereal/` |
| UDF settings | `crates/sail-python-udf/src/config.rs` |
| Python bindings (`pysail`) | `crates/sail-python/` |
| Compatibility data | `python/pysail/data/compatibility/functions/` |
