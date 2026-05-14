# Sail: Exhaustive Component Deep Dive (All 36 Crates)

This document provides an exhaustive architectural breakdown of **all 36 crates** in the Sail monorepo. To help contributors navigate the codebase, the crates are organized into 7 logical subsystems, tracing the flow from base infrastructure up to distributed query execution.

---

```mermaid
graph TD
    subgraph 1. Infrastructure & Utilities
        SCM[sail-common]
        SCD[sail-common-datafusion]
        TEL[sail-telemetry]
        CACHE[sail-cache]
    end

    subgraph 2. Server, Ingress & CLI
        CLI[sail-cli]
        SRV[sail-server]
        CONN[sail-spark-connect]
        FLIGHT[sail-flight]
    end

    subgraph 3. Session & Catalogs
        SESS[sail-session]
        CAT[sail-catalog]
        subgraph Providers
            MEM[sail-catalog-memory]
            SYS[sail-catalog-system]
            GLUE[sail-catalog-glue]
            HMS[sail-catalog-hms]
            ICE[sail-catalog-iceberg]
            ONE[sail-catalog-onelake]
            UNITY[sail-catalog-unity]
        end
    end

    subgraph 4. Storage & Formats
        OBJ[sail-object-store]
        DS[sail-data-source]
        DL[sail-delta-lake]
        ICETAB[sail-iceberg]
    end

    subgraph 5. SQL Parsing & Analysis
        MACRO[sail-sql-macro]
        PARSER[sail-sql-parser]
        ANALYZER[sail-sql-analyzer]
    end

    subgraph 6. Planning & Optimization
        PLAN[sail-plan]
        LAKEPLAN[sail-plan-lakehouse]
        LPLAN[sail-logical-plan]
        PPLAN[sail-physical-plan]
        LOPT[sail-logical-optimizer]
        POPT[sail-physical-optimizer]
    end

    subgraph 7. Execution & Functions
        EXEC[sail-execution]
        FUNC[sail-function]
        PY[sail-python]
        UDF[sail-python-udf]
    end

    CONN --> SESS
    SESS --> CAT
    CONN --> PLAN
    PARSER --> ANALYZER
    PLAN --> LPLAN
    LPLAN --> LOPT
    LOPT --> PPLAN
    PPLAN --> POPT
    POPT --> EXEC
```

---

## 1. Common Infrastructure & Utilities

### [sail-common](file:///usr/local/google/home/warrenzhu/sail/crates/sail-common)
* **Responsibility:** Core foundational data structures, application configuration parsing, error definitions, and common utility functions used globally across the monorepo.
* **Key Modules:** `config` (`application.rs`), `datetime.rs`, `spec`, `runtime.rs`.
* **Dependencies:** `tokio`, `serde`, `thiserror`.

### [sail-common-datafusion](file:///usr/local/google/home/warrenzhu/sail/crates/sail-common-datafusion)
* **Responsibility:** Core extensions and bridge utilities for integrating Apache DataFusion types with Sail's internal specifications and configuration wrappers.

### [sail-telemetry](file:///usr/local/google/home/warrenzhu/sail/crates/sail-telemetry)
* **Responsibility:** OpenTelemetry metrics, distributed tracing, and structured logging setup. Assigns internal Job IDs to track query plans across cluster boundaries.

### [sail-cache](file:///usr/local/google/home/warrenzhu/sail/crates/sail-cache)
* **Responsibility:** In-memory and distributed caching abstractions for caching execution metadata, catalog schemas, and intermediate shuffle files.

### [sail-build-scripts](file:///usr/local/google/home/warrenzhu/sail/crates/sail-build-scripts) & [sail-gold-test](file:///usr/local/google/home/warrenzhu/sail/crates/sail-gold-test)
* **Responsibility:** Internal development utilities. `sail-build-scripts` handles protobuf compilation and build-time code generation. `sail-gold-test` provides a golden master testing framework for validating query plan outputs against known benchmarks.

---

## 2. Server, Ingress & CLI

### [sail-spark-connect](file:///usr/local/google/home/warrenzhu/sail/crates/sail-spark-connect)
* **Responsibility:** Implements the official Spark Connect gRPC protocol. Serves as the ingress point for PySpark client sessions.
* **Key Modules:** `server.rs`, `service/`, `entrypoint.rs`, `executor.rs`, `streaming.rs`.
* **Internal Mechanics:** Intercepts `ExecutePlanRequest` messages, routes Commands vs Relations, and streams `ExecutePlanResponse` Arrow IPC batches.

### [sail-server](file:///usr/local/google/home/warrenzhu/sail/crates/sail-server)
* **Responsibility:** Top-level server orchestration, binding Tokio TCP listeners, managing graceful shutdowns, and configuring retry policies.
* **Key Modules:** `actor.rs`, `builder.rs`, `retry.rs`.

### [sail-flight](file:///usr/local/google/home/warrenzhu/sail/crates/sail-flight)
* **Responsibility:** Manages the **Data Plane** in cluster mode. Uses Apache Arrow Flight gRPC to stream columnar shuffle data directly between distributed workers without disk IO.
* **Key Modules:** `service.rs`, `session.rs`, `metrics.rs`.

### [sail-cli](file:///usr/local/google/home/warrenzhu/sail/crates/sail-cli)
* **Responsibility:** Command-line interface binary (`sail spark server --port 50051`) for bootstrapping local or cluster server instances.

---

## 3. Session & Catalog Management

### [sail-session](file:///usr/local/google/home/warrenzhu/sail/crates/sail-session)
* **Responsibility:** Manages active client sessions, maintaining execution state, catalog configurations, and attaching local/cluster job runners to DataFusion `SessionContext`s.
* **Key Modules:** `planner.rs`, `catalog.rs`, `session_factory/`, `session_manager/`.

### [sail-catalog](file:///usr/local/google/home/warrenzhu/sail/crates/sail-catalog)
* **Responsibility:** Replaces DataFusion's default memory catalog with a modular, multi-provider catalog framework supporting temporary views, persistent tables, and DDL commands.
* **Key Modules:** `command.rs`, `temp_view.rs`, `manager/`, `provider/`.

### Catalog Providers
* **[sail-catalog-memory](file:///usr/local/google/home/warrenzhu/sail/crates/sail-catalog-memory):** In-memory volatile catalog for temporary tables and testing.
* **[sail-catalog-system](file:///usr/local/google/home/warrenzhu/sail/crates/sail-catalog-system):** Exposes system metadata tables, performance metrics, and cluster worker states.
* **[sail-catalog-glue](file:///usr/local/google/home/warrenzhu/sail/crates/sail-catalog-glue):** AWS Glue Data Catalog integration.
* **[sail-catalog-hms](file:///usr/local/google/home/warrenzhu/sail/crates/sail-catalog-hms):** Apache Hive Metastore integration.
* **[sail-catalog-iceberg](file:///usr/local/google/home/warrenzhu/sail/crates/sail-catalog-iceberg):** Apache Iceberg REST Catalog integration.
* **[sail-catalog-onelake](file:///usr/local/google/home/warrenzhu/sail/crates/sail-catalog-onelake):** Microsoft OneLake metadata integration.
* **[sail-catalog-unity](file:///usr/local/google/home/warrenzhu/sail/crates/sail-catalog-unity):** Databricks Unity Catalog integration.

---

## 4. Storage, Lakehouse Formats & Data Sources

### [sail-object-store](file:///usr/local/google/home/warrenzhu/sail/crates/sail-object-store)
* **Responsibility:** Wraps `object_store` to provide high-performance asynchronous IO across AWS S3, GCS, Azure Blob, Cloudflare R2, HDFS, and local file systems.

### [sail-data-source](file:///usr/local/google/home/warrenzhu/sail/crates/sail-data-source)
* **Responsibility:** Pluggable data source abstraction for reading/writing Parquet, CSV, JSON, and Arrow IPC files with predicate pushdown and partition pruning.

### [sail-delta-lake](file:///usr/local/google/home/warrenzhu/sail/crates/sail-delta-lake) & [sail-iceberg](file:///usr/local/google/home/warrenzhu/sail/crates/sail-iceberg)
* **Responsibility:** Native Lakehouse format integrations. `sail-delta-lake` manages Delta transaction logs and checkpoints. `sail-iceberg` handles Iceberg snapshots, manifests, and schema evolution.

---

## 5. SQL Parsing & Semantic Analysis

### [sail-sql-macro](file:///usr/local/google/home/warrenzhu/sail/crates/sail-sql-macro) & [sail-sql-parser](file:///usr/local/google/home/warrenzhu/sail/crates/sail-sql-parser)
* **Responsibility:** Implements a fully compliant Spark SQL dialect parser using procedural macros and parser combinators.
* **Key Modules:** `lexer.rs`, `parser.rs`, `combinator.rs`, `token.rs`.

### [sail-sql-analyzer](file:///usr/local/google/home/warrenzhu/sail/crates/sail-sql-analyzer)
* **Responsibility:** Performs semantic validation on parsed SQL ASTs, resolving data types, variable scoping, and validating complex expressions before logical plan generation.
* **Key Modules:** `statement.rs`, `expression.rs`, `query.rs`, `data_type.rs`.

---

## 6. Plan Representation & Optimization

### [sail-plan](file:///usr/local/google/home/warrenzhu/sail/crates/sail-plan) & [sail-plan-lakehouse](file:///usr/local/google/home/warrenzhu/sail/crates/sail-plan-lakehouse)
* **Responsibility:** The compiler core. `PlanResolver` recursively translates Spark Connect ASTs and SQL specs into DataFusion `LogicalPlan` nodes. `sail-plan-lakehouse` handles specialized planning for Lakehouse merge, update, and delete operations.
* **Key Modules:** `resolver/`, `formatter.rs`, `explain.rs`.

### [sail-logical-plan](file:///usr/local/google/home/warrenzhu/sail/crates/sail-logical-plan) & [sail-physical-plan](file:///usr/local/google/home/warrenzhu/sail/crates/sail-physical-plan)
* **Responsibility:** Defines custom DataFusion logical and physical extension nodes necessary for Spark parity.
* **Key Nodes:** `repartition.rs`, `map_partitions.rs`, `merge.rs`, `schema_pivot.rs`, `show_string.rs`, `sort.rs`.

### [sail-logical-optimizer](file:///usr/local/google/home/warrenzhu/sail/crates/sail-logical-optimizer) & [sail-physical-optimizer](file:///usr/local/google/home/warrenzhu/sail/crates/sail-physical-optimizer)
* **Responsibility:** Custom optimization rules injected into DataFusion's engine.
* **Optimizations:** Join reordering, view type coercion at output boundaries, streaming micro-batch rewrites, and merge cardinality checks.

---

## 7. Execution Engine & Functions

### [sail-execution](file:///usr/local/google/home/warrenzhu/sail/crates/sail-execution)
* **Responsibility:** Orchestrates physical plan execution across both local threads and distributed clusters.
* **Key Modules:**
  * `job_runner.rs`: Multithreaded local execution driver.
  * `driver/`, `worker/`, `worker_manager/`: Cluster mode actor model implementation.
  * `stream/`, `stream_manager/`: Micro-batch and continuous streaming execution tracking.

### [sail-function](file:///usr/local/google/home/warrenzhu/sail/crates/sail-function)
* **Responsibility:** Native Rust implementations of Spark's extensive standard library functions (scalar, aggregate, windowing, date/time, array/map manipulation).

### [sail-python](file:///usr/local/google/home/warrenzhu/sail/crates/sail-python) & [sail-python-udf](file:///usr/local/google/home/warrenzhu/sail/crates/sail-python-udf)
* **Responsibility:** Bridges Python UDFs, UDAFs, UDTFs, and PySpark client bindings into Sail.
* **Key Modules:** `udf/`, `stream.rs`, `conversion.rs`.
* **Internal Mechanics:** Utilizes **zero-copy Arrow array pointers** to share memory directly between Rust's DataFusion engine and embedded Python runtimes, completely eliminating serialization overhead.
