# 13 · Lakehouse Formats: Delta Lake and Apache Iceberg

> **You'll learn:** how Sail reads and writes Delta Lake and Iceberg tables
> natively in Rust; how snapshots, transaction logs, and manifests turn into
> file lists; how pruning, deletion vectors, and delete files work; how commits
> and conflict checks keep writes safe; and how `DELETE`, `UPDATE`, and `MERGE`
> are planned.
>
> **Crates:** `sail-delta-lake`, `sail-iceberg`, `sail-plan-lakehouse`

---

## 1. What This Component Does

Plain Parquet directories can't do atomic writes, time travel, schema
evolution, or row-level updates. **Lakehouse table formats** add a metadata
layer that records *exactly which files make up each version of a table*:

| | Delta Lake | Apache Iceberg |
|-|-----------|----------------|
| Metadata | `_delta_log/` with JSON commit files (`00000000000000000042.json`) and Parquet **checkpoints** | `metadata/` with table metadata JSON, **manifest lists**, and **manifests** (Avro) |
| A version is | a log version number | a **snapshot** ID (with optional branches and tags) |
| Row-level deletes | Copy-on-write rewrites, or **deletion vectors** (merge-on-read) | Copy-on-write, or **position and equality delete files** |

Sail implements both formats **natively in Rust**, on top of DataFusion and
Sail's table format interface (see [12](12-data-sources-and-formats.md)). Both
are registered as table formats named `delta` and `iceberg`.

---

## 2. Why It's Built This Way

**Native implementations, designed for distributed execution.** Reading a big
table's metadata can itself be a large job: thousands of log files or
manifests, millions of file entries. Sail expresses metadata work as **physical
plans** (scan the log, replay it, prune files, then scan data), so metadata
processing benefits from the same parallel engine and can run on workers in
cluster mode.

**Logical planning stays format-neutral.** The resolver produces generic nodes
for writes, deletes, and merges. A lakehouse optimizer rule (*expand row-level
operation*) and the lakehouse planners turn them into format-specific physical
plans. Each format decides *how* to delete or merge (rewrite files, or write
deletion vectors or delete files), behind the same interface.

**Correctness first.** Commits are atomic: write data files first, then publish
a new version in a single operation. Concurrent writers are detected, and Delta
runs a **conflict checker** that decides whether a losing commit can be retried
on top of the winning one.

---

## 3. Delta Lake in Sail

### 3.1 Reading: from log to files

1. **Find the log segment.** List `_delta_log/` and choose the latest
   checkpoint (classic single-file, multi-part, or V2 with sidecars) plus the
   JSON commits after it, up to the requested version (`versionAsOf`) or
   timestamp (`timestampAsOf`).
2. **Load protocol and metadata.** The schema, partition columns, table
   properties, and required reader features (column mapping, deletion vectors,
   timestamp without time zone, type widening, …).
3. **Replay the log as a plan.**
   - A *log scan* reads checkpoint and commit files as Arrow rows of actions.
   - **Log replay** groups actions by file key (path plus deletion-vector ID) and
     keeps the **newest** action per key. An `add` that's later `remove`d drops
     out. The result is the active file set for the snapshot. For very large
     logs, the replay strategy is configurable (`deltaLogReplayStrategy`,
     `deltaLogReplayHashThreshold`).
   - Per-file **statistics** (row counts, min, max, null counts) are decoded from
     each `add` action's stats JSON.
4. **Prune files.** Filters are checked against partition values and file
   statistics. Files that can't match are skipped before any data is read.
5. **Scan data.** The remaining files are read as Parquet, with:
   - an **expression adapter** for column mapping (physical vs. logical names),
     type widening, and schema evolution (missing columns become NULL),
   - **deletion vectors**: for files that have one, a row index is tracked while
     scanning and rows whose positions are set in the deletion vector's roaring
     bitmap are filtered out.

### 3.2 Writing

1. **Plan the write.** Validate the input against the table schema. Options such
   as `mergeSchema` and `overwriteSchema` allow evolution. Generated columns are
   computed from their expressions, and invariants are enforced.
2. **Write data files.** Parquet files are written (optionally partitioned),
   collecting statistics for each file. `targetFileSize` and `writeBatchSize`
   control the files (file compression comes from the `parquet.*` settings).
3. **Commit.** A commit operator gathers the `add` (and for overwrites, `remove`)
   actions, plus `metaData` and `protocol` changes if needed, and attempts to
   write the next log version **atomically** (put-if-absent).
4. **On conflict**, if another writer created that version first, the conflict
   checker compares what this transaction read and wrote with the winning
   commit. If they don't overlap in a way that matters, the commit is retried at
   the next version. Otherwise it fails with a concurrency error.
5. **Checkpoint and clean up.** Checkpoints are written periodically to keep
   log replay fast. Log cleanup handles expired files and V2 checkpoint
   compatibility.

**Write modes:** append, overwrite, `replaceWhere` (overwrite rows matching a
condition), error if exists, and ignore if exists. Many Spark Delta options are
defined but marked unsupported, for example `partitionOverwriteMode`,
`maxRecordsPerFile`, change data feed reads, and streaming-source options. Each
option's `supported` flag in `delta.yaml` is authoritative.

### 3.3 Row-level operations

`DELETE`, `UPDATE`, and `MERGE` are first expanded by the lakehouse optimizer
rule (it runs before DataFusion's own rules), then planned by Delta:

| Operation | Copy-on-write (default) | Merge-on-read (when the table has deletion vectors enabled) |
|-----------|-------------------------|-------------------------------------------------------------|
| `DELETE` | Find files containing matching rows using metadata. Rewrite those files without the matching rows. Commit `remove` for the old files and `add` for the new ones. | Find affected files. Scan them to find matching **row positions**. Write deletion vector files (bitmaps). Commit `remove(old)` and `add(same file, new DV)`. No data is rewritten. |
| `UPDATE` | Rewrite affected files with updated rows | Not implemented yet |
| `MERGE` | Join source and target, apply `WHEN MATCHED` / `NOT MATCHED` / `NOT MATCHED BY SOURCE` clauses, rewrite touched files, and enforce that each target row matches at most one source row | Target-side deletes use deletion vectors. Inserted and updated rows are written as new files. |

To find exactly which rows to change, Sail carries internal **metadata
columns** through these plans (the source file path, the row index within the
file, and the operation type). They're stripped before data is written.

---

## 4. Iceberg in Sail

### 4.1 Reading: from snapshot to files

1. **Load table metadata.** Find the current metadata JSON (from the catalog or
   the table location). Pick the snapshot: current, `VERSION AS OF` (snapshot
   ID), `TIMESTAMP AS OF`, or a branch or tag name.
2. **Read the manifest list** for the snapshot, then the **manifests**. A
   manifest scan operator turns manifest entries into rows of data-file metadata:
   path, partition tuple, record count, column bounds, and so on.
3. **Prune** manifests and files using partition values (with Iceberg partition
   **transforms**: identity, bucket, truncate, year, month, day, hour) and
   column statistics.
4. **Scan data files** (Parquet), with **name mapping** and **schema
   evolution** (field IDs map files written with older schemas to the current
   schema).
5. **Apply deletes** written by other engines: **position deletes** (file path
   plus row position) and **equality deletes** (rows matching key values), using
   sequence numbers to decide which deletes apply to which files.

### 4.2 Writing

- **Append** and **overwrite**: data files are written with partition transforms
  applied, new manifests and a manifest list are created, and a new snapshot is
  committed by writing a new metadata JSON file (and `metadata/version-hint.text`)
  next to the table, keeping external catalogs in sync where configured (see
  [11](11-catalogs.md)).
- **Not supported yet:** bucketing, predicate overwrite and partition overwrite,
  and row-level `DELETE`, `UPDATE`, `MERGE` writers for Iceberg.
- **Table properties** can be set and unset (`ALTER TABLE … SET TBLPROPERTIES`).
- About 100 Iceberg read and write options are defined (see
  `sail-data-source/src/options/data/iceberg.yaml`). Check each one's
  `supported` flag.

---

## 5. Examples

```python
# Delta: create, append, time travel
df.write.format("delta").save("s3://lake/orders_delta")
more.write.format("delta").mode("append").save("s3://lake/orders_delta")
spark.read.format("delta").option("versionAsOf", 0).load("s3://lake/orders_delta").count()

# Delta: schema evolution
new_df.write.format("delta").mode("append").option("mergeSchema", True).save("s3://lake/orders_delta")

# Delta: replaceWhere
fixes.write.format("delta").mode("overwrite") \
     .option("replaceWhere", "date = '2024-01-01'").save("s3://lake/orders_delta")
```

```sql
-- Row-level operations on a Delta table registered in a catalog
DELETE FROM sales.orders WHERE status = 'cancelled';
UPDATE sales.orders SET amount = amount * 1.1 WHERE region = 'EU';
MERGE INTO sales.orders t
USING updates s ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Iceberg time travel
SELECT * FROM ice.db.events VERSION AS OF 123456789;
SELECT * FROM ice.db.events TIMESTAMP AS OF '2024-06-01 00:00:00';
```

---

## 6. End-to-End: `DELETE FROM sales.orders WHERE status = 'cancelled'` (Delta)

| # | Component | What happens |
|---|-----------|--------------|
| 1 | SQL analyzer | Produces a delete command with table `sales.orders` and the condition. |
| 2 | Resolver | Looks up the table (format `delta`, location `s3://lake/orders`) and produces a **file delete** node. |
| 3 | Lakehouse optimizer rule | Expands it into a format-neutral **row-level write** node (Delete). |
| 4 | Delta table format | Checks table properties to choose the strategy: copy-on-write, or merge-on-read if deletion vectors are enabled. |
| 5 | Metadata pipeline | Log scan → replay → statistics → prune: finds candidate files whose `status` statistics could include `'cancelled'`. |
| 6a | Copy-on-write | Scans candidate files, writes new files containing only rows where `NOT (status = 'cancelled')`, and emits `remove` and `add` actions. |
| 6b | Merge-on-read | Scans candidate files with row indexes, collects matching positions into bitmaps, writes deletion vector files, and emits `remove(old)` and `add(old path, new DV)`. |
| 7 | Commit | Writes log version *N+1* atomically. On a race, runs the conflict checker and retries or fails. |
| 8 | Result | Returns operation metrics (rows deleted, files added and removed). |

---

## 7. Common Problems

| Symptom | Cause |
|---------|-------|
| "Merge-on-Read strategy for UPDATE is not yet implemented for Delta Lake" | `UPDATE` on a table with deletion vectors enabled |
| "predicate or partition overwrite for Iceberg" not implemented | `replaceWhere` or dynamic overwrite on Iceberg |
| Concurrent write fails with a conflict | Two writers changed overlapping files or metadata. Retry, or serialize the writers. |
| Slow first query on a huge Delta table | Log replay over many commits. Make sure checkpoints are written. Tune the replay strategy options. |
| Schema mismatch on append | Use `mergeSchema` (Delta), or evolve the schema explicitly. |

---

## 8. Where to Look

| Topic | Location |
|-------|----------|
| Delta table format (entry points for read, write, row-level, alter) | `crates/sail-delta-lake/src/table_format.rs` |
| Delta log listing, segments, replay, cleanup | `crates/sail-delta-lake/src/delta_log/` |
| Delta snapshots, checkpoints, transactions, conflict checker | `crates/sail-delta-lake/src/kernel/` |
| Delta read pruning and scan | `crates/sail-delta-lake/src/datasource/` |
| Delta physical operators (log replay, scan by adds, writer, commit, DV writer, remove actions) | `crates/sail-delta-lake/src/physical_plan/` |
| Delta operation planners (write, delete, update, merge, commit) | `crates/sail-delta-lake/src/physical_plan/planner/` |
| Delta protocol types (actions, schema, statistics, properties) | `crates/sail-delta-lake/src/spec/` |
| Deletion vectors | `crates/sail-delta-lake/src/deletion_vector/` |
| Iceberg table format | `crates/sail-iceberg/src/table_format.rs` |
| Iceberg spec (metadata, manifests, partition transforms, types, delete index) | `crates/sail-iceberg/src/spec/` |
| Iceberg read (provider, pruning) | `crates/sail-iceberg/src/datasource/` |
| Iceberg operators (manifest scan, scan by data files, delete apply, writer, commit) | `crates/sail-iceberg/src/physical_plan/` |
| Iceberg write operations (append, overwrite, snapshot) | `crates/sail-iceberg/src/operations/` |
| Row-level expansion rule and lakehouse planners | `crates/sail-plan-lakehouse/src/` |
| Delta and Iceberg options | `crates/sail-data-source/src/options/data/{delta,iceberg}.yaml` |
