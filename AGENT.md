# AI Agent Guidelines for LakeSail (Sail)

This document provides guidelines, conventions, and architectural context for AI agents working in this repository.

---

## 1. Project Overview

**Sail** is an Apache Spark-compatible query engine written in Rust, designed to run workloads seamlessly via Spark Connect with high performance, resource efficiency, and deep Lakehouse integration (Delta Lake, Iceberg, Parquet).

### Key Crates & Subsystems

* **`sail-common`**: Shared utilities, telemetry, error types, and system configuration.
* **`sail-plan`**: Logical-to-physical query plan translation, catalog resolution, and optimization using Apache Arrow DataFusion.
* **`sail-execution`**: Distributed execution subsystem:
  * **`JobScheduler`**: Schedules stages and task regions, tracks task attempts, and coordinates stage transitions.
  * **`TaskAssigner` / `WorkerPool`**: Manages worker nodes, task slots, and task placement.
  * **`TaskRunner`**: Executes DataFusion physical plans on workers.
  * **`StreamManager`**: Manages intermediate data exchange (in-memory channels and on-disk files).
  * **`TaskStreamFlightServer` / `Client`**: Arrow Flight-based streaming for inter-node shuffle.
  * **`adaptive`**: Adaptive Query Execution (AQE) rules for shuffle partition coalescing and skew mitigation.
* **`sail-server`**: Actor runtime, server builder, and gRPC communication abstractions.
* **`sail-session`**: Multi-tenant session state and execution engine runners (`LocalJobRunner`, `ClusterJobRunner`).
* **`sail-spark-connect`**: Spark Connect gRPC protocol server implementation.

---

## 2. Commit & Development Conventions

### 2.1 Atomic Commits

* **Every commit must be atomic and self-contained**: It should represent a single logical change that compiles cleanly and passes relevant tests.
* Do not combine unrelated refactors, documentation changes, and feature implementations into a single commit.
* Follow the Conventional Commits specification:
  * `feat(<scope>): description`
  * `fix(<scope>): description`
  * `test(<scope>): description`
  * `docs(<scope>): description`
  * `refactor(<scope>): description`

### 2.2 Git Workflow

* Branch from and rebase on the target branch (e.g. `main` or feature branch).
* Verify code formatting and linting before committing:
  ```bash
  cargo fmt --check
  cargo clippy --all-targets
  ```
* Push commits to the appropriate remote (`fork` for user forks, `origin` for upstream).

---

## 3. Build & Test Commands

### Rust Workspaces

* **Check specific crate**:
  ```bash
  cargo check -p sail-execution
  ```
* **Run unit tests**:
  ```bash
  cargo test -p sail-execution
  cargo test -p sail-execution --lib driver::job_scheduler::adaptive
  ```
* **Run full workspace tests**:
  ```bash
  cargo test --workspace
  ```

### Python & Spark Parity Tests

* Sail runs PySpark and Ibis parity tests using Hatch environments:
  ```bash
  hatch run test-spark.spark-3.5.7:pytest
  hatch run test-spark.spark-4.1.1:pytest
  ```

---

## 4. Architecture & Design Principles

1. **Shuffle Storage**:
   * **Pipelined (`OutputMode::Pipelined`)**: Uses in-memory Tokio `mpsc` channels for low-latency streaming between co-scheduled stages.
   * **Blocking / Disk (`OutputMode::Blocking`)**: Persists Arrow IPC RecordBatches into `.data` files and writes metadata into `.index` files under `shuffle_dir`. This decouples stage lifetimes and allows stages to scale to large datasets without OOM.
2. **Adaptive Query Execution (AQE)**:
   * Upstream shuffle stages record partition bytes and record counts in `.index` files.
   * Upon upstream stage completion, `JobScheduler` inspects runtime statistics via `StageShuffleStats::from_disk`.
   * Contiguous small channels are coalesced into partition ranges targeting `target_partition_size` (default 64MB).
   * Downstream stages are dynamically rewritten with coalesced partition counts and read ranges.
3. **Stage & Job Lifecycle**:
   * When all consumers of a stage succeed, the driver dispatches `CleanUpJob` RPCs to clean intermediate shuffle files.
   * On job termination, all temporary stream artifacts are deleted.
