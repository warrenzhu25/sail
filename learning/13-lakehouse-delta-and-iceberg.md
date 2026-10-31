# 13 · Lakehouse Tables: Delta Lake and Apache Iceberg

A directory of Parquet files is a fine way to store data, until you need to
change it safely. Two writers can step on each other. A reader can see a
half-finished write. There's no way to look at yesterday's version, change the
schema cleanly, or delete a few rows without rewriting everything by hand.
*Lakehouse table formats* solve this by adding a metadata layer that records
exactly which files make up each version of a table. This doc explains how Sail
reads and writes the two most popular ones, Delta Lake and Apache Iceberg, and
how DELETE, UPDATE, and MERGE work on top of them.

---

## The idea both formats share

Both formats separate *data files*, ordinary Parquet, from *metadata* that says
which data files belong to the table right now. Writing a new version means
writing new data files first, then publishing a new piece of metadata that
points at them. Publishing is a single atomic step, so readers see either the
old version or the new one, never a mix. Old versions remain described by old
metadata, which is what makes time travel possible.

Delta Lake keeps its metadata in a `_delta_log` directory: a numbered series of
small JSON files, one per commit, with periodic Parquet *checkpoints* that
summarize everything up to a point. Iceberg keeps a table metadata file that
points to *snapshots*, each snapshot points to a *manifest list*, and each
manifest list points to *manifests*, Avro files that list data files along with
statistics about them.

The formats also differ in how they handle deleting a few rows. Delta can
rewrite the affected files (copy-on-write) or record *deletion vectors*, small
bitmaps marking which rows of a file are gone. Iceberg can rewrite files, or
record *delete files* that list deleted rows by position or by key values.

## Why Sail implements them natively

Sail implements both formats in Rust, on top of DataFusion and Sail's table format
interface (see [12](12-data-sources-and-formats.md)), rather than calling out to
another library. The main reason is scale. On a big table, just *reading the
metadata* can be a large job: thousands of log files or manifests describing
millions of data files. Sail expresses that metadata work as ordinary physical
plans (scan the log, replay it, filter the file list, then scan the data), so it
runs in parallel with the same engine as everything else, and on workers in
cluster mode.

The second reason is clean planning. The resolver produces format-neutral nodes
for writes, deletes, and merges. A lakehouse rule then expands them, and each
format decides *how* to carry them out: rewrite files, write deletion vectors,
or write delete files. Logical planning never needs to know which format is
involved.

## Reading a Delta table

Reading starts by choosing a version. Sail lists the `_delta_log` directory and
picks the newest checkpoint plus every JSON commit after it, or stops earlier if
you asked for a particular version or timestamp. From these it loads the
table's protocol and metadata: the schema, the partition columns, the
properties, and which reader features the table uses (column mapping, deletion
vectors, type widening, and so on).

Then it **replays the log** to find the active files. Every log entry is an
action, mostly "add this file" or "remove this file". Replay groups the actions
by file and keeps only the newest one for each. Here's a tiny example:

```text
00000000000000000000.json   add(part-a)  add(part-b)
00000000000000000001.json   add(part-c)
00000000000000000002.json   remove(part-a)  add(part-d)      ← an UPDATE rewrote part-a as part-d
```

Read at the latest version, the newest action for part-a is a remove, so the
active files are part-b, part-c, and part-d. Read as of version 1, replay stops
before commit 2, and the active files are part-a, part-b, and part-c. That's all
time travel is: replaying less of the log. Once a checkpoint exists, at version
10 for example (tables checkpoint every few commits, ten by default), replay
starts from it instead of from the beginning. In Sail, this replay is itself a
plan operator, so it scales to very large logs, and how it groups actions can be
tuned with table options.

Each add action also carries statistics about its file: row counts, minimum and
maximum values, and null counts. Sail uses them, together with partition values,
to **skip files** that can't match the query's filters before reading any data.

Finally, the remaining files are scanned as Parquet. Two adjustments happen
while scanning. An adapter maps physical column names to logical ones (Delta's
column mapping) and fills in missing columns for files written before a schema
change. And for files that have a deletion vector, Sail tracks each row's
position while reading and drops the rows the bitmap marks as deleted.

## Writing to a Delta table

A write begins by checking the incoming data against the table's schema. The
mergeSchema and overwriteSchema options allow the schema to evolve, generated
columns are computed from their expressions, and invariants are enforced. Sail
then writes Parquet files, collecting statistics for each one.

Then comes the commit. Sail gathers the new add actions (plus remove actions for
replaced files, and metadata or protocol changes if any) and tries to write the
next numbered log file using a "create only if it doesn't exist" operation. If
another writer got there first, Sail's conflict checker compares what this
transaction read and wrote with what the winning commit did. If they don't
overlap in a way that matters, Sail retries at the next version number.
Otherwise the write fails with a concurrency error. After committing, Sail writes
a checkpoint when the version reaches the table's checkpoint interval, and cleans
up the log as needed.

Delta writes support appending, overwriting, replaceWhere (overwrite only the
rows matching a condition), and the error-if-exists and ignore-if-exists modes.
Many of Spark's Delta options are defined but marked unsupported, including
partitionOverwriteMode, maxRecordsPerFile, change data feed reads, and the
streaming source options. The options file for Delta says which ones work.

## DELETE, UPDATE, and MERGE on Delta

Row-level operations are expanded early by a lakehouse optimizer rule (it runs
before DataFusion's own rules) and then planned by Delta. Sail carries a few
internal columns through these plans: which file each row came from, its
position within that file, and what kind of change applies to it. That's how the
writer knows exactly which rows to rewrite or mark. The columns are removed
before any data is written.

By default, Sail uses **copy-on-write**. For a DELETE, it uses the metadata to
find files that might contain matching rows, rewrites those files without the
matching rows, and commits removes for the old files and adds for the new ones.
UPDATE rewrites affected files with the new values. MERGE joins the source with
the target, applies the WHEN MATCHED, WHEN NOT MATCHED, and WHEN NOT MATCHED BY
SOURCE clauses, rewrites the files it touched, and checks that no target row
matched more than one source row, as the SQL standard requires.

If the table has deletion vectors enabled, DELETE and MERGE switch to
**merge-on-read**. Instead of rewriting files, Sail scans the affected files to
find the positions of the matching rows, writes small deletion vector files,
and commits "remove the old entry, add the same file with this deletion
vector". The difference can be dramatic. Deleting ten rows from a 1 GB file with
copy-on-write writes a new 1 GB file. With a deletion vector, it writes a bitmap
of a few bytes, and readers skip those ten rows from then on. The cost moves to
readers, which is why tables are compacted from time to time. UPDATE with
deletion vectors isn't implemented yet.

## Reading and writing Iceberg

An Iceberg read starts by loading the table's current metadata file, from the
catalog or the table location, and choosing a snapshot: the current one, a
specific snapshot ID, the snapshot as of a timestamp, or a branch or tag. Sail
reads that snapshot's manifest list and then its manifests. A manifest scan
operator turns manifest entries into rows describing data files, with their
partition values and column statistics.

Iceberg partitions with *transforms*, so a table can be partitioned by the day
of a timestamp or a hash bucket of an ID without storing extra columns. Sail
understands identity, bucket, truncate, year, month, day, and hour transforms,
and uses them, along with column statistics, to skip manifests and files.
Iceberg identifies columns by stable IDs rather than names, so Sail maps files
written with older schemas onto the current one. If other engines have written
delete files, Sail applies them: position deletes remove specific rows of
specific files, equality deletes remove rows matching given key values, and
sequence numbers decide which deletes apply to which files.

Writing supports appending and overwriting. Sail writes data files with the
partition transforms applied, writes new manifests and a manifest list, and
commits a new snapshot by writing a new metadata file (and updating the version
hint) next to the table. Setting and unsetting table properties is supported.
Not supported yet: bucketing, overwriting only matching rows or partitions, and
DELETE, UPDATE, and MERGE on Iceberg tables. About a hundred Iceberg options are
defined, each marked supported or not.

## Following one DELETE

Take `DELETE FROM sales.orders WHERE status = 'cancelled'` on a Delta table. The
SQL analyzer produces a delete command, and the resolver looks up the table (a
Delta table stored at s3://lake/orders) and produces a file-delete node. The
lakehouse rule expands it into a format-neutral row-level delete, and Delta
checks the table's properties to choose a strategy. Its metadata pipeline (scan
the log, replay it, read statistics, filter) finds the files whose statistics
for status could include 'cancelled'. With copy-on-write, those files are
rewritten without the matching rows. With deletion vectors, the matching
positions are collected into bitmaps and written as deletion vector files. Either
way, the changes are committed as the next log version, with the conflict check
protecting against concurrent writers, and the client gets back metrics such as
how many rows were deleted.

---

## Common problems

"Merge-on-Read strategy for UPDATE is not yet implemented for Delta Lake" means
an UPDATE ran on a table with deletion vectors enabled. "Predicate or partition
overwrite for Iceberg" means replaceWhere or a partition overwrite was attempted
on Iceberg. A concurrency error on commit means two writers changed overlapping
files or metadata; retry, or make the writers take turns. A slow first query on
a huge Delta table usually means a long log without recent checkpoints. And
schema mismatches on append can be solved with mergeSchema on Delta, or by
evolving the schema explicitly.

## Where to look in the code

Delta lives in crates/sail-delta-lake. Its entry points for reading, writing,
row-level operations, and table changes are in src/table_format.rs. Log listing,
segments, replay, and cleanup are under src/delta_log/. Snapshots, checkpoints,
and transactions with the conflict checker are under src/kernel/. File skipping
and scan setup are in src/datasource/. The physical operators (log replay, scan
with deletion vectors, writer, commit, deletion vector writer) are under
src/physical_plan/, with the planners for write, delete, update, merge, and
commit in src/physical_plan/planner/. Protocol types are in src/spec/ and deletion
vectors in src/deletion_vector/. Iceberg lives in crates/sail-iceberg: its entry
points are in src/table_format.rs, the format specification (metadata,
manifests, partition transforms, types, delete index) in src/spec/, reading in
src/datasource/, operators in src/physical_plan/, and append and overwrite
operations in src/operations/. The rule that expands row-level operations is in
crates/sail-plan-lakehouse, and the option definitions for both formats are in
crates/sail-data-source/src/options/data/ (delta.yaml and iceberg.yaml).
