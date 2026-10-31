# 12 · Data Sources and File Formats

Every time Sail reads or writes data, it goes through a **table format**: a named
plugin that knows how to read and write one kind of data. Parquet, CSV, JSON,
Avro, Arrow, plain text, and binary files each have one, as do Delta Lake and
Iceberg tables, a few streaming test sources, and any data source a user writes
in Python. This doc explains the table format interface, how file formats find
files and work out schemas, how options are defined and combined, how writing
works, and how Python data sources plug in. Lakehouse formats get their own doc
([13](13-lakehouse-delta-and-iceberg.md)), and so does storage access
([14](14-object-storage.md)).

---

## One interface for every format

The resolver doesn't know whether it's reading Parquet or Iceberg, and it
shouldn't have to. When it needs to read a table, it builds a neutral
description: the paths, the schema if one is known, the partition columns,
bucketing and sort information from the catalog, and the options. Then it asks
the session's format registry for the format by name and hands over the
description. The format returns a table source that DataFusion can scan with the
usual optimizations: reading only the needed columns, pushing filters down, and
stopping early for limits.

Writing works the same way. The resolver describes the write: the plan producing
the rows, the mode (append, overwrite, overwrite matching a condition, overwrite
partitions, error if the target exists, or ignore if it exists), the partition
columns, the options, and the output path. The format returns a plan that
consumes the rows and writes them.

Lakehouse formats implement a few extra capabilities: writers for DELETE,
UPDATE, and MERGE, a choice between rewriting files and recording deletions
separately, and changes to table properties and column types. Plain file formats
don't need these.

The payoff of this design is that adding a format means implementing the
interface and registering it. Nothing else in the planner changes.

## File formats are built on DataFusion's listing table

DataFusion already has a capable "listing table" for directories of files. It
splits files into partitions for parallel scanning, uses file statistics,
applies projection and filter pushdown, prunes partition directories, and
writes files in parallel. Sail's file formats are built on it, and add the
behavior Spark users expect: Hadoop-style path patterns, hidden-file handling,
partition inference rules, Spark's option names and defaults, and Spark-style
output file names.

The built-in formats are Parquet (with row-group pruning, page-index filtering,
filter pushdown, and bloom filters), CSV, newline-delimited JSON, Avro, Arrow IPC,
text (one string column per line), and binary (one row per file, with the path,
modification time, length, and content; it can't be written). There are also
three streaming helpers: rate generates rows at a steady pace, socket reads lines
from a TCP connection, and console prints streaming results.

## Finding files

Reading `s3://lake/events/` starts with working out which files that means. Sail
parses the path with its own URL parser, because Spark uses **Hadoop's glob
syntax**, which differs from shell globs in small ways: alternatives are written
`{a,b}`, a negated character class is `[^x]` rather than `[!x]`, and `*` and `?`
work as usual. A pattern such as `logs/{2024,2025}/*/app-[^t]*.json` is split
into a fixed base path and a pattern, and the alternatives are expanded.

The files are then listed through the storage registry, using the listing cache
if it's turned on. When you point at a directory without your own pattern,
files whose names start with a dot or an underscore are skipped, just as in
Spark, so bookkeeping files like _SUCCESS and .crc checksums never get read as
data.

## Working out the schema and partitions

Data lakes usually organize files in directories named after column values:

```text
s3://lake/events/date=2024-01-01/country=US/part-0.parquet
s3://lake/events/date=2024-01-01/country=DE/part-1.parquet
s3://lake/events/date=2024-01-02/country=US/part-2.parquet
s3://lake/events/_SUCCESS
```

If you don't name the partition columns, Sail discovers them from directory names
like these and adds them to the schema. Reading the directory above produces the
columns stored in the files plus date and country. A filter like country = 'DE'
then opens only part-1.parquet, because the other directories can't match.

If you don't supply a schema, the format infers one. Parquet reads it from file
footers, CSV and JSON sample rows, and Avro uses the schema in the file. Some
formats honor options that change this, such as CSV's inferSchema=false, which
makes every column a string.

## Reading efficiently

During execution, the listing table groups files, and for Parquet even row
groups within files, into partitions that are read in parallel. It reads only
the columns the query needs. For Parquet, it uses each row group's statistics and
the page index to skip data that can't match a filter, can apply filters while
decoding, and can check bloom filters. These behaviors are controlled by the
parquet.* settings. It also prunes whole partition directories using their
values. The file statistics and metadata caches (see
[06](06-session-and-runtime.md)) keep repeated queries from re-reading footers.
In cluster mode, the scan description travels inside each task's plan, and every
worker reads its assigned files directly from storage.

## Options, defined as data

Spark has dozens of options per format, many with several names (sep and
delimiter mean the same thing for CSV), forms that can be stored as table
properties (option.sep), defaults, and rules about whether they apply to
reading, writing, or both. Hand-written parsing for all of this would be
repetitive and easy to get subtly wrong.

So Sail describes each format's options in a YAML file, about 250 options in
total. CSV alone has about 40, Parquet about 35, and Iceberg about 100. Each
entry gives the option's names, its default, whether it applies to reading,
writing, or both, the type it should become, and whether Sail supports it yet. At
build time, these files become typed Rust structures with parsers. Option
handling is therefore consistent, the YAML doubles as documentation, and an
option marked unsupported can produce a clear error instead of silently doing
nothing.

Options can come from several places for one read, and they're applied in
**layers**, with later layers winning key by key. Here's an example. A CSV table
is created with table properties saying the separator is a semicolon and the
files have a header row, and then a query overrides the separator:

```sql
CREATE TABLE raw_events (id INT, msg STRING) USING csv
  LOCATION 's3://lake/raw/' TBLPROPERTIES ('option.sep' = ';', 'option.header' = 'true');
```

```python
spark.read.option("sep", "|").table("raw_events")
```

The defaults say the separator is a comma and there's no header. The table
properties change that to a semicolon with a header. The query option then
changes the separator to a pipe, and doesn't mention the header. So the read
uses a pipe separator and a header row. Names match without regard to case, and
aliases count as the same option.

## Writing files

When writing, a plain file format first rejects things it can't do. It can't
write a streaming query to a plain file path, apply bucketing, or apply
partition transforms. It then treats the output path as a directory and always
writes several files. It chooses file extensions, including the compression
codec for Parquet (for example .zstd.parquet), and writes in parallel, putting
partition columns in directory names rather than inside the files. Parquet
writing honors about twenty parquet.* settings such as compression, dictionary
encoding, statistics, row group size, page size, bloom filters, and writer
version.

One limitation is worth knowing. The plain file writer currently **only
appends**: it ignores the write mode, a known gap marked in the code. The checks
that happen before writing to a *table*, such as whether it exists, creating it,
and refusing or skipping because of the mode, are done by the resolver, as
preconditions that run before the write (see [07](07-plan-resolver.md) and
[08](08-logical-physical-plans-and-optimizers.md)). But writing to an existing
directory of plain files adds files rather than replacing them. Lakehouse formats
implement every mode properly, as transactions.

## Python data sources

PySpark 4 lets users write data sources in Python, and Sail supports them in two
ways. A program can register one for its session with
`spark.dataSource.register(MySource)`, which sends the pickled class to Sail.
And at startup, Sail looks for installed Python packages that advertise data
sources through Python's standard entry-point mechanism, and registers those for
everyone.

When such a source is read, Sail calls its schema method, asks its reader to
split the work into partitions, and runs the reader's read method for each
partition in the embedded Python interpreter, converting the rows it yields
into Arrow batches. Simple filters can be passed to the reader. Writing runs the
Python writer for each partition, then a commit step that calls the writer's
commit or abort with the messages each partition returned:

```python
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

## Following one read

Take `spark.read.parquet("s3://lake/events/").filter("date = '2024-01-01'").count()`.
Spark Connect produces a read of format parquet with one path and no schema. The
resolver asks the registry for the Parquet format and passes it the description.
The format parses the path, finds no pattern, and lists the directory through
the S3 client, skipping _SUCCESS. It discovers the date directories and adds a
date partition column, reads one footer (possibly from cache) to get the file
schema, and builds a listing table. The optimizer turns the filter into a
partition filter, and because count needs no columns, row counts can come from
file metadata. At execution, only the files under date=2024-01-01 are opened.

---

## Common problems

"Bucketing for writing listing table format" means bucketBy was used with a
plain file format, which isn't supported. "Cannot write streaming data to listing
table" means a streaming query tried to write to a plain file path. If a write to
an existing path added files instead of replacing them, that's the append-only
behavior described above; write to a table or a lakehouse format, or clear the
path first. An unexpected extra column usually comes from partition inference
finding col=value directories; pass an explicit schema to control it. An option
that seems to do nothing may be misspelled, unsupported, or meant for the other
direction; the format's YAML file says which. And a glob that doesn't match may be
using shell syntax; remember `[^x]` and `{a,b}`.

## Where to look in the code

The table format interface, with the read and write descriptions, option layers,
and write modes, is in crates/sail-common-datafusion/src/datasource.rs, and the
formats are registered for each session in crates/sail-session/src/formats.rs.
The listing-based reading and writing shared by file formats is in
crates/sail-data-source/src/listing/. Each built-in format has a directory under
src/formats/ (parquet, csv, json, avro, arrow, text, binary, rate, socket,
console), and Python data sources are under src/formats/python/. Option
definitions are the YAML files in src/options/data/, turned into code by the
crate's build.rs, and path and glob parsing is in src/url.rs. Write planning is in
crates/sail-physical-plan/src/file_write.rs.
