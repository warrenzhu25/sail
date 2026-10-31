# 12 · Data Sources and File Formats

> **You'll learn:** how Sail reads and writes files (Parquet, CSV, JSON, Avro,
> Arrow, text, binary) and streaming test sources; the table format interface
> that all formats implement; how options are defined, layered, and checked;
> how paths, globs, and partition directories are discovered; and how Python
> data sources plug in.
>
> **Crates:** `sail-data-source` (built-in formats, listing, options, URLs,
> Python data sources), the table format interface in
> `sail-common-datafusion/src/datasource.rs`
>
> Lakehouse formats (Delta Lake, Iceberg) are in
> [13](13-lakehouse-delta-and-iceberg.md), and storage access (S3, GCS, …) is in
> [14](14-object-storage.md).


## In One Minute

- Every read and write goes through a **table format** plugin, looked up by name
  (`parquet`, `csv`, `delta`, …) in the session's registry.
- File formats are built on DataFusion's **listing table**: parallel scans,
  pruning, projection and filter pushdown, partition directories.
- Sail adds Spark behavior: Hadoop-style globs, hidden-file filtering
  (`_SUCCESS`, `.crc`), partition inference, and Spark option names.
- About 250 **options** are defined in YAML files and turned into typed structs
  at build time. Options are applied in **layers** (table properties, then query
  options).
- Plain file writers currently **append only**. Lakehouse formats support
  overwrite, replace-where, and row-level changes.

---

## 1. What This Component Does

Whenever Sail reads data, through `spark.read…`, `spark.table(…)` on a file
table, or `SELECT * FROM parquet.`path``, or writes it, through `df.write…`,
`INSERT`, or CTAS, it goes through a **table format**. A table format is a
named plugin that knows how to:

| Capability | Called for |
|------------|------------|
| **Create a source** | Reads: returns a DataFusion table source the planner can scan (with projection, filter, and limit pushdown) |
| **Infer a schema** | Planning when no schema was given |
| **Create a writer** | Writes: returns a physical plan that consumes the input and writes files |
| Create a row-level writer (optional) | `DELETE`, `UPDATE`, `MERGE` (lakehouse formats only) |
| Alter table properties or column types (optional) | `ALTER TABLE` on formats that support it |
| Merge strategy (optional) | Whether row-level changes are copy-on-write (*eager*) or merge-on-read |

All formats live in a **registry** keyed by lowercase name and attached to each
session (see [06](06-session-and-runtime.md)):

| Name | Kind | Read | Write |
|------|------|------|-------|
| `parquet` | File | Yes, with row-group and page pruning, filter pushdown, bloom filters | Yes |
| `csv` | File | Yes, with header handling and schema inference | Yes |
| `json` | File | Yes (newline-delimited) | Yes |
| `avro` | File | Yes | Yes |
| `arrow` | File (Arrow IPC) | Yes | Yes |
| `text` | File (one string column per line) | Yes | Yes |
| `binary` | File (path, modification time, length, content) | Yes | No |
| `delta`, `iceberg` | Lakehouse | See [13](13-lakehouse-delta-and-iceberg.md) | |
| `rate`, `socket` | Streaming test sources | Yes | No |
| `console` | Streaming sink | No | Yes |
| *Python data sources* | User-defined, in Python | Yes | Yes |

---

## 2. Why It's Built This Way

**One interface for every format.** The resolver doesn't know whether it's
reading Parquet or Iceberg. It builds a neutral description (paths, schema,
partitioning, options) and asks the registry. Adding a format means
implementing the interface and registering it. Nothing else in the planner
changes.

**Reuse DataFusion's file machinery.** File formats are built on DataFusion's
**listing table**, which already handles splitting files into partitions,
parallel scans, statistics, projection and filter pushdown, partition-column
pruning, and parallel writes. Sail adds Spark's behavior on top: option names
and defaults, Hadoop-style globs, partition inference rules, hidden-file
handling, and output file naming.

**Spark-compatible options, defined as data.** Spark has dozens of options per
format, with aliases (`sep`/`delimiter`), table-property forms
(`option.delimiter`), defaults, and scopes (read, write, or both). Sail defines
each format's options in a **YAML file**, about 250 options in total, and a
build script generates typed Rust structs and parsers from them. Option
handling stays consistent and self-documenting, and an option marked
unsupported can produce a clear error instead of being silently ignored.

**Options keep track of where they came from.** An option can come from catalog
table properties, the operation (`.option(…)`, `OPTIONS (…)`), the table
location, or time travel. These are passed as ordered **option layers** (later
layers override earlier ones), so a format can tell a table default apart from a
per-query override.

---

## 3. How It Works

### 3.1 Describing a read and a write

**Read description (source info):**

| Field | Meaning |
|-------|---------|
| Paths | One or more paths or URLs, which may contain globs |
| Schema | Optional user or catalog schema, including partition columns |
| Constraints | Primary key and similar constraints, if any |
| Partition by | Partition column names (Hive-style `col=value` directories) |
| Bucket by, sort order | Bucketing and sort metadata from the catalog |
| Options | Option layers |

**Write description (sink info):**

| Field | Meaning |
|-------|---------|
| Input | The physical plan producing the rows |
| Mode | Error if exists, ignore if exists, append, overwrite, overwrite where *condition*, overwrite partitions |
| Partition by | Partition columns (with optional transforms, for lakehouse formats) |
| Bucket by, sort order | |
| Options | Option layers. The output path is the `path` option. |
| Logical schema | The input schema with field metadata, for example Delta generated-column expressions |

### 3.2 Paths, globs, and listing

1. **Parse the URL.** Sail's own URL parser understands **Hadoop glob syntax**,
   which differs from typical shell globs: `{a,b}` alternation, `[^x]` negated
   classes, `*`, `?`. Each glob is split into a fixed base path and a pattern,
   and alternations are expanded.
2. **List files** through the object store registry (see
   [14](14-object-storage.md)), optionally using the file listing cache. Hidden
   files and directories (starting with `_` or `.`, such as `_SUCCESS` or
   `.crc`) are excluded by default, as in Spark.
3. **Infer partitions.** If no partition columns were given, `col=value`
   directory names are discovered from the paths and become columns. Their types
   are inferred.
4. **Infer the schema** if none was given, using the format's inference: Parquet
   footers, sampling CSV and JSON rows, Avro schemas, and so on. Some formats
   handle options such as CSV `inferSchema=false` (all columns become strings).
5. **Build a listing table** with the files, schema, partition columns, target
   partitions (`execution.default_parallelism`), and constraints, then return it
   as a table source.

### 3.3 Reading

During planning and execution, the listing table:

- groups files (and Parquet row groups) into **partitions** to read in parallel,
- applies **projection pushdown** (read only the needed columns),
- applies **filter pushdown**: for Parquet, row-group statistics, page indexes,
  bloom filters, and late materialization (`parquet.pushdown_filters`,
  `parquet.pruning`, `parquet.enable_page_index`, …), plus partition pruning
  from directory values,
- uses the **file statistics and metadata caches** (see
  [06](06-session-and-runtime.md)) to avoid rereading Parquet footers.

In cluster mode the scan is serialized into the task plan (see
[09](09-local-and-cluster-execution.md)), and each worker reads its assigned
files directly from storage.

### 3.4 Writing

For plain file formats, the writer:

1. checks the input isn't a streaming plan (streams can't be written to plain
   file tables),
2. rejects features plain files don't support: **bucketing** and **partition
   transforms**,
3. treats the path as a **directory** and always writes multiple files,
4. picks the file extension, including the compression codec for Parquet (for
   example `.zstd.parquet`),
5. builds DataFusion's file sink with Hive-style **partition directories**
   (partition columns aren't stored inside the files) and runs it in parallel.

At the writer level, plain file formats only **append**: the sink mode is
currently ignored by the listing writer, which is marked as a TODO in the code.
Table-level checks (does the table exist, create it if needed, error or ignore
modes) are done by the resolver as **preconditions** in the barrier node (see
[07](07-plan-resolver.md), [08](08-logical-physical-plans-and-optimizers.md)).
Lakehouse formats implement the full set of modes transactionally.

Parquet writing honors about 20 `parquet.*` settings: compression, dictionary,
statistics, row group size, page size, bloom filters, writer version, and so
on.

### 3.5 Options

Each format's options are listed in `src/options/data/<format>.yaml`:

```yaml
- key: delimiter
  description: Sets a single character separator for each field and value.
  default: { value: ",", parser: parse_char }
  supported: true
  rust_type: char
  scopes: [read, write]
  origins:
    - type: option          # .option("sep", ";") or .option("delimiter", ";")
      keys: [delimiter, sep]
    - type: table_property  # TBLPROPERTIES ('option.sep' = ';')
      keys: [option.delimiter, option.sep]
```

At build time this becomes a typed struct per format and scope (for example,
CSV read options) with parsers. At run time, option layers are applied in order
and each key is matched case-insensitively against its aliases.

| Format | Approximate number of options |
|--------|-------------------------------|
| CSV | 42 |
| Parquet | 34 |
| JSON | 27 |
| Delta | 37 |
| Iceberg | 103 |
| Text, socket, rate, binary | A few each |

### 3.6 Streaming test sources and sinks

- **`rate`**: generates `(timestamp, value)` rows at a configured rate.
- **`socket`**: reads text lines from a TCP socket.
- **`console`**: prints micro-batch results.

These support Structured Streaming examples and tests (see the streaming notes
in [08](08-logical-physical-plans-and-optimizers.md)).

### 3.7 Python data sources

PySpark 4's **Python Data Source API** lets users write readers and writers in
Python. Sail supports it in two ways:

- **Session registration**: `spark.dataSource.register(MySource)` sends the
  pickled class to Sail, which creates a Python table format for that session.
- **Entry-point discovery**: at startup, Sail looks for installed Python
  packages that advertise data sources through `importlib.metadata` entry
  points and registers them globally.

When read, Sail calls the source's `schema()`, plans **partitions** with the
reader's `partitions()`, and runs `read(partition)` for each one in its embedded
Python interpreter, converting the results to Arrow batches. Simple filters can
be pushed into the reader. Writes run the Python writer for each partition, then
a **commit** step that calls the writer's commit or abort with the per-partition
messages. Settings `python.data_source_*` control the write channel capacity
and slow-operation warnings.

---

## 4. Examples

```python
# Parquet with partition discovery: /data/events/date=2024-01-01/part-0.parquet …
df = spark.read.parquet("s3://lake/events/")            # gains a `date` column
df.filter("date = '2024-01-01'").count()                # prunes other date directories

# CSV with options
spark.read.option("header", True).option("sep", ";").csv("/data/*.csv")

# Hadoop-style glob with alternation
spark.read.json("gs://bucket/logs/{2024,2025}/*/app-[^t]*.json")

# Write partitioned Parquet
df.write.partitionBy("country").parquet("/out/by_country/")

# Query a path directly in SQL
spark.sql("SELECT count(*) FROM parquet.`s3://lake/events/`").show()

# Python data source
from pyspark.sql.datasource import DataSource, DataSourceReader
class Counter(DataSource):
    @classmethod
    def name(cls): return "counter"
    def schema(self): return "n INT"
    def reader(self, schema): return CounterReader()
class CounterReader(DataSourceReader):
    def read(self, partition):
        for i in range(3): yield (i,)
spark.dataSource.register(Counter)
spark.read.format("counter").load().show()
```

### 4.1 Option layering, worked out

A CSV table is created with a table property, then read with a query option:

```sql
CREATE TABLE raw_events (id INT, msg STRING) USING csv
  LOCATION 's3://lake/raw/' TBLPROPERTIES ('option.sep' = ';', 'option.header' = 'true');
```

```python
spark.read.option("sep", "|").table("raw_events")
```

| Layer (applied in order) | `sep` | `header` |
|--------------------------|-------|----------|
| Default (from `csv.yaml`) | `,` | `false` |
| Table properties (`option.sep`, `option.header`) | `;` | `true` |
| Query options (`.option("sep", "|")`) | **`\|`** | (not set) |
| **Effective value** | **`\|`** | **`true`** |

Later layers win, key by key. Aliases (`sep` and `delimiter`) match the same
option, and matching is case-insensitive.

### 4.2 What partition inference produces

```text
s3://lake/events/date=2024-01-01/country=US/part-0.parquet
s3://lake/events/date=2024-01-01/country=DE/part-1.parquet
s3://lake/events/date=2024-01-02/country=US/part-2.parquet
s3://lake/events/_SUCCESS                      ← hidden, skipped
```

`spark.read.parquet("s3://lake/events/")` yields the file columns plus
**`date`** and **`country`**, taken from the directory names. A filter such as
`country = 'DE'` opens only `part-1.parquet`.

---

## 5. End-to-End: `spark.read.parquet("s3://lake/events/").filter("date='2024-01-01'").count()`

| # | Component | What happens |
|---|-----------|--------------|
| 1 | Spark Connect | A data source read: format `parquet`, paths `["s3://lake/events/"]`, no schema. |
| 2 | Resolver | Asks the registry for `parquet` and passes source info with an empty schema and the option layers. |
| 3 | URL resolution | Parses the path, finds no globs, and lists files under `s3://lake/events/` through the S3 client from the object store registry (see [14](14-object-storage.md)). Skips `_SUCCESS`. |
| 4 | Partition inference | Finds `date=…` directories and adds a `date` partition column. |
| 5 | Schema inference | Reads one Parquet footer, which may be cached. The schema is the file schema plus `date`. |
| 6 | Listing table | Created with files, schema, partitions, and target partitions. |
| 7 | Optimizer | The filter `date = '2024-01-01'` becomes a **partition filter**. `count(*)` needs no columns, so it may be answered from statistics. |
| 8 | Execution | Only files under `date=2024-01-01/` are opened. Row counts come from Parquet metadata where possible. |

---

## 6. Common Problems

| Symptom | Cause |
|---------|-------|
| "bucketing for writing listing table format" not implemented | `bucketBy` with a plain file format isn't supported |
| "cannot write streaming data to listing table" | A streaming query writing to a plain file path. Use a supported sink. |
| A write to an existing path adds files instead of replacing them | The listing writer appends. Use a table, a lakehouse format, or clear the path first. |
| Unexpected extra column from directories | Partition inference found `col=value` directories. Pass an explicit schema to control it. |
| Option has no effect | Misspelled, not supported, or only valid for the other scope (read vs. write). Check `options/data/<format>.yaml`. |
| Glob doesn't match | Hadoop glob syntax: use `[^x]`, not `[!x]`, and `{a,b}` for alternatives |

---

## 7. Where to Look

| Topic | Location |
|-------|----------|
| Table format interface, source and sink info, option layers, sink modes | `crates/sail-common-datafusion/src/datasource.rs` |
| Format registration | `crates/sail-session/src/formats.rs` |
| Listing-based table format (read and write) | `crates/sail-data-source/src/listing/` |
| Built-in formats | `crates/sail-data-source/src/formats/` (`parquet/`, `csv/`, `json/`, `avro/`, `arrow/`, `text/`, `binary/`, `rate/`, `socket/`, `console/`) |
| Option definitions (YAML) and parsers | `crates/sail-data-source/src/options/` (`data/*.yaml`, `parsers.rs`), `build.rs` |
| URL and glob parsing | `crates/sail-data-source/src/url.rs` |
| Python data sources | `crates/sail-data-source/src/formats/python/` |
| File write planning | `crates/sail-physical-plan/src/file_write.rs` |
