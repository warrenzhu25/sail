# 17 · Testing and Development

This doc is a practical guide to working on Sail: how the repository is laid
out, how to build it, which kinds of tests exist and what each is good for, how
the "gold data" tests work, and a sensible workflow for making a change.

---

## Finding your way around

Rust code lives in crates/, about 35 crates in total; the README's crate list
says which doc covers each. The Python package, pysail, lives in
python/pysail/, along with its tests, examples, and the compatibility data that
tracks which Spark functions are supported. The public documentation site is in
docs/. Scripts for running Spark's own test suite and for collecting gold data
are in scripts/. Container images, Kubernetes manifests, and a compose file for
local services such as a Hive Metastore or MinIO live in docker/, k8s/,
deployments/, and compose.yml. Two files tie it together: Cargo.toml defines the
Rust workspace, and pyproject.toml defines the Python build and the hatch
environments used for development.

## Building

pysail is a Rust extension module built with PyO3 and maturin, plus some Python
code, so the sail binary and the Python package share all of the engine code.
You build the Rust crates with `cargo build`, and a release CLI binary with
`cargo build -r -p sail-cli --bins`. To use your changes from Python, run
`hatch run maturin develop`, which builds pysail and installs it into the
development environment. `hatch run maturin build` builds a wheel instead, and
`hatch run install-pysail` installs a built wheel into the test environment.
Before sending a change, format and lint with `cargo +nightly fmt`,
`cargo clippy --all-targets --all-features`, and `hatch fmt`.

Builds of the full workspace are slow, so test one crate at a time with
`cargo test -p <crate>` while iterating. The development docs under
docs/development/ have a recipe for reducing build times with linker and
profile settings, as well as setup instructions, debugging tips, and notes on
cross-compiling.

## The kinds of tests, and when to use each

**Rust unit tests** live next to the code they test and are the right tool for
algorithms and small components: partition coalescing, parsers, the plan codec,
actor behavior.

**Gold data tests** lock in Sail's exact output for large sets of inputs: SQL
syntax, expressions, functions, data types, and plans. They're described in
detail below.

**Python integration tests** under python/pysail/tests/ start a real Sail server,
usually inside the test process through pysail, and run PySpark code against it.
Their folders mirror Sail's components, with tests for aggregation, catalogs,
data sources, Delta, Iceberg, execution, functions, Glue, Hive Metastore,
streaming, and more. Some of them use pytest-bdd **feature files**, readable
scenario tables describing behaviors, and some use **snapshot tests** that
store expected plans or outputs in files, refreshed with `--snapshot-update`.

Beyond its own tests, Sail is regularly checked against **Spark's own PySpark
test suite**. Scripts build a patched PySpark (3.5 or 4.1), start a Sail server,
run the suite against it, and produce a report of what passed and failed. This
is how compatibility is tracked over time. The **Ibis** project's backend test
suite is run the same way. A few integration tests need external services, such
as a Kerberos-secured Hive Metastore, and are marked as ignored so they only run
when asked for explicitly.

## Gold data: reviewing behavior like code

A gold data file records the *actual* output Sail produces for many inputs, in
JSON, and is committed and reviewed like code. An entry looks like this:

```json
{ "input":  { "query": "SELECT map(1.0, '2', 3.0, '4');",
              "result": ["{1.0:\"2\",3.0:\"4\"}"],
              "schema": { "type": "struct", "fields": [ ... ] } },
  "output": { "success": "ok" } }
```

The input side, the query along with the result and schema Spark produced, was
collected once from Spark's own test suite by a bootstrap script, which is rerun
only when Spark is upgraded. The output side records what Sail does with that
input: success, or the error message it produced. When you change behavior, you
regenerate the outputs and commit them:

```bash
env SAIL_UPDATE_GOLD_DATA=1 cargo test     # rewrite outputs from the current code
cargo test                                  # confirm they're stable
```

The value of this approach shows up in code review. Suppose you fix concat so it
returns NULL when any argument is NULL. The diff shows exactly that change in
the one entry that exercises it. If ten other functions' outputs changed too,
that's obvious in the diff and worth investigating before merging. Gold data
lives under tests/gold_data/ in the crates that use it, such as sail-sql-parser
for syntax and sail-spark-connect for data types, expressions, functions by
category, plans, and table schemas.

## A workflow for fixing a function

Suppose a Spark function returns the wrong answer. Start by reproducing it:
build pysail with `hatch run maturin develop` and write a small failing test in
python/pysail/tests/spark/function/, or a quick script against a development
server. Find the function's builder in sail-plan's function tables and, if it
has its own implementation, the code in sail-function (see
[15](15-functions-and-python-udfs.md)). Fix it, and add a Rust unit test if the
logic stands on its own.

Then regenerate gold data with `env SAIL_UPDATE_GOLD_DATA=1 cargo test` and read
the diff carefully, making sure only the outputs you intended changed. Run the
Python tests for the area and refresh snapshots if needed. If the function's
support status changed, update the compatibility data. Finish by formatting and
linting.

## A workflow for a cluster-only bug

If something fails only in cluster mode, reproduce it with a local cluster by
setting SAIL_MODE=local-cluster (see [09](09-local-and-cluster-execution.md)).
Turn on detailed logs with `RUST_LOG=sail_execution=debug,info`: the task runner
logs each task's final plan, and the driver logs its state changes. Query
system.execution.stages and system.execution.tasks to see which stage and
attempt failed. If an operator can't be sent to workers, look at the plan codec
in sail-execution's codec.rs.

## A few habits that help

Test one crate at a time rather than the whole workspace. Prefer gold data and
feature files over ad-hoc assertions when the thing you're testing is Spark
behavior, because reviewers can see the exact change and the expected outputs
are easy to regenerate. Use RUST_LOG with a crate name and debug to see what
Sail is doing: every Spark Connect request and response, every task plan, and
every storage call is logged at that level. And when something is running on a
cluster, the system tables are the fastest way to see what's going on.

## Where to look in the code

The development docs are in docs/development/ (setup/, build/, recipes/, and
spark-tests/), and contributor guidance is in CONTRIBUTING.md and
docs/development/architecture-contributor-guide.md. The gold data helper,
including the SAIL_UPDATE_GOLD_DATA switch, is in crates/sail-common/src/tests.rs,
with supporting code in crates/sail-gold-test. Gold data files are under
tests/gold_data/ in each crate that uses them, and the bootstrap script is in
scripts/spark-gold-data/. The Spark test harness is in scripts/spark-tests/, the
Python tests in python/pysail/tests/, and the hatch environments in
pyproject.toml.
