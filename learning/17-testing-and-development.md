# 17 · Testing and Development Workflow

> **You'll learn:** how the repository is laid out, how to build Sail (Rust and
> Python), the kinds of tests and when to use each, how gold data and snapshot
> tests work, how the PySpark test suite is run against Sail, and a practical
> workflow for making a change.

---

## 1. Repository Map

| Path | Contents |
|------|----------|
| `crates/` | All Rust crates (about 35). See the crate map in [README](README.md). |
| `python/pysail/` | The `pysail` Python package: server wrapper, CLI shims, examples, compatibility data, and **Python tests** |
| `docs/` | The public documentation site (VitePress) |
| `scripts/` | Spark test harness, gold data bootstrap, build helpers |
| `docker/`, `k8s/`, `deployments/`, `compose.yml` | Container images, Kubernetes manifests, local service stacks (for example for HMS or MinIO) |
| `pyproject.toml` | Python build (maturin) and `hatch` environments for development and testing |
| `Cargo.toml` | Rust workspace |

---

## 2. Building

| Goal | Command |
|------|---------|
| Build Rust crates | `cargo build` |
| Build the CLI binary (release) | `cargo build -r -p sail-cli --bins` |
| Build and install `pysail` into the dev environment | `hatch run maturin develop` |
| Build a wheel | `hatch run maturin build` (add `--release`) |
| Install a built wheel into the test environment | `hatch run install-pysail` |
| Format and lint | `cargo +nightly fmt`, `cargo clippy --all-targets --all-features`, `hatch fmt` |

The `pysail` package is a Rust extension (built with PyO3 and maturin) plus
Python modules. The `sail` binary and `pysail` share all of the engine code.

See `docs/development/` (setup, build, recipes) for prerequisites, faster builds,
debugging, and cross-compilation.

---

## 3. Kinds of Tests

| Kind | Where | Run with | Good for |
|------|-------|----------|----------|
| **Rust unit tests** | `#[cfg(test)]` modules in each crate | `cargo test -p <crate>` | Algorithms, small components (coalescing, parsers, codecs, actors) |
| **Gold data tests** | `crates/*/tests/gold_data/*.json` | `cargo test` | Locking in exact outputs for SQL syntax, expressions, functions, data types, and plans |
| **Python integration tests** | `python/pysail/tests/` | `hatch run pytest` | End-to-end PySpark behavior against a real Sail server |
| **BDD feature tests** | `*.feature` files with pytest-bdd | `hatch run pytest` | Readable scenario tables (for example function behaviors, Iceberg and Delta features) |
| **Snapshot tests** | `__snapshots__/` (syrupy) | `hatch run pytest`, with `--snapshot-update` to refresh | Plan and output snapshots for features |
| **Spark's own test suite** | Patched PySpark test suite run against Sail | `scripts/spark-tests/*.sh` | Broad compatibility tracking |
| **Ibis tests** | The Ibis backend test suite against Sail | `hatch run test-ibis:…` | Third-party DataFrame library compatibility |
| **Ignored integration tests** | For example HMS with Kerberos | `cargo test … -- --ignored` | Tests that need external services (containers) |

### 3.1 Gold data tests

**Gold data** records the **actual** output of many inputs (SQL statements,
expressions, function calls) in JSON files that are reviewed like code:

```json
{ "input":  { "query": "SELECT map(1.0, '2', 3.0, '4');",
              "result": ["{1.0:\"2\",3.0:\"4\"}"],
              "schema": { "type": "struct", "fields": [ … ] } },
  "output": { "success": "ok" } }
```

- **Inputs** are collected once from the Spark project's own tests by a bootstrap
  script (`scripts/spark-gold-data/bootstrap.sh`, with a Spark patch). This is
  rare, mainly when Spark is upgraded.
- **Outputs** are what Sail produces: success, a result, or an error message.
- When you change behavior, regenerate and **commit** the gold data so reviewers
  see the difference:

```bash
env SAIL_UPDATE_GOLD_DATA=1 cargo test     # rewrite outputs
cargo test                                  # verify they're stable
```

Gold data lives in, for example, `crates/sail-sql-parser/tests/gold_data/`
(syntax) and `crates/sail-spark-connect/tests/gold_data/` (data types,
expressions, functions by category, plans, table schemas).

### 3.2 Python tests

Python tests start a Sail Spark Connect server, usually in-process through
`pysail`, and run PySpark code against it. Folders mirror the components:
`aggregate`, `catalog`, `catalog_integration`, `datasource`, `delta`,
`iceberg`, `execution`, `function`, `glue`, `hms`, `literal`, `optimizer`,
`streaming`, and more. Some tests run against a real Spark as well, to compare
results.

```bash
hatch run maturin develop          # build pysail into the env
hatch run pytest                   # all Python tests
hatch run pytest python/pysail/tests/spark/delta -k merge
env SPARK_REMOTE="sc://localhost:50051" hatch run pytest   # against an already-running server
```

### 3.3 Running Spark's test suite

`scripts/spark-tests/` builds a patched PySpark (3.5 or 4.1), starts a Sail
server (`run-server.sh`), runs PySpark's tests against it (`run-tests.sh`), and
summarizes the results (`generate-test-report.sh`, plus `jq` scripts to list
passed and failed tests and count errors). CI uses this to track compatibility
over time. See `docs/development/spark-tests/`.

---

## 4. A Practical Workflow for a Change

**Example: fix a Spark function's behavior.**

1. **Reproduce.** Write a failing PySpark snippet against a dev server:
   `hatch run maturin develop`, then a quick script or a new test in
   `python/pysail/tests/spark/function/`.
2. **Locate.** Find the function in `sail-plan/src/function/…` (the builder) and,
   if needed, `sail-function/src/…` (the implementation). See
   [15](15-functions-and-python-udfs.md).
3. **Fix**, and add a Rust unit test if the logic is self-contained.
4. **Update gold data:** `env SAIL_UPDATE_GOLD_DATA=1 cargo test`. Review the JSON
   diff and make sure only the intended outputs changed.
5. **Run Python tests** for the area, and update snapshots if needed
   (`--snapshot-update`).
6. **Update compatibility data** in `python/pysail/data/compatibility/` if
   support status changed.
7. **Format and lint:** `cargo +nightly fmt`, `cargo clippy …`, `hatch fmt`.

**Example: debug a cluster-only failure.**

1. Reproduce with `SAIL_MODE=local-cluster` (see
   [09](09-local-and-cluster-execution.md)).
2. Turn on logs: `RUST_LOG=sail_execution=debug,info`. The task runner logs each
   task's rewritten physical plan, and the driver logs state transitions.
3. Look at `system.execution.stages` and `system.execution.tasks`.
4. If an operator fails to serialize, look at the plan codec
   (`sail-execution/src/codec.rs`).

---

## 5. Tips

| Tip | Why |
|-----|-----|
| Use `cargo test -p <crate>` | Rebuilding and testing the whole workspace is slow |
| Read `docs/development/recipes/reducing-build-time.md` | Linker and profile settings that speed up builds a lot |
| Prefer gold data or feature files over ad-hoc asserts for Spark behavior | Reviewers see the exact behavior change, and outputs are easy to regenerate |
| Use `RUST_LOG=<crate>=debug` | Every Spark Connect request and response, task plan, and object store call is logged at debug level |
| Use the system tables | Fastest way to see what the cluster is doing |

---

## 6. Where to Look

| Topic | Location |
|-------|----------|
| Development docs | `docs/development/` (`setup/`, `build/`, `recipes/`, `spark-tests/`) |
| Contributor guide | `CONTRIBUTING.md`, `docs/development/architecture-contributor-guide.md` |
| Gold data helpers | `crates/sail-common/src/tests.rs` (`SAIL_UPDATE_GOLD_DATA`), `crates/sail-gold-test/` |
| Gold data files | `crates/*/tests/gold_data/` |
| Gold data bootstrap | `scripts/spark-gold-data/` |
| Spark test harness | `scripts/spark-tests/` |
| Python tests | `python/pysail/tests/` |
| Hatch environments | `pyproject.toml` (`[tool.hatch.envs.*]`) |
