# Sail Architecture Deep Dive

A comprehensive guide to understanding Sail's architecture, designed for engineers evaluating, contributing to, or learning from the codebase.

## Executive Summary

**Sail** is a drop-in Apache Spark replacement written in 100% Rust, designed to unify batch processing, stream processing, and compute-intensive AI workloads on a distributed, multimodal compute engine.

### Key Value Propositions

| Metric | Spark | Sail |
|--------|-------|------|
| Performance | Baseline | **~4× faster** (up to 8×) |
| Infrastructure Cost | Baseline | **94% cheaper** |
| Memory Footprint | 54 GB peak | **22 GB peak** |
| Shuffle Spill | >110 GB | **0 GB** |
| Startup Time | Minutes (JVM) | **Seconds** |

### Core Differentiators

- **Spark Connect Protocol Compatible**: Existing PySpark code works unchanged
- **100% Rust-Native**: No JVM overhead, no garbage collection pauses
- **Arrow-Native Processing**: Columnar format with SIMD-optimized execution
- **Lightweight Workers**: Start in seconds, scale elastically

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CLIENT LAYER                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│  PySpark Client  │  Spark Connect  │  Arrow Flight SQL  │  SQL CLI         │
└────────┬─────────┴────────┬────────┴─────────┬──────────┴────────┬─────────┘
         │                  │                  │                   │
         │              gRPC (Spark Connect Protocol)              │
         │                  │                  │                   │
┌────────▼──────────────────▼──────────────────▼───────────────────▼─────────┐
│                              SERVER LAYER                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│  sail-spark-connect (Spark Connect Server)                                  │
│  sail-flight (Arrow Flight SQL Server)                                      │
│  sail-server (gRPC Infrastructure, Health, Telemetry)                       │
└────────┬────────────────────────────────────────────────────────────────────┘
         │
┌────────▼────────────────────────────────────────────────────────────────────┐
│                            SESSION LAYER                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│  sail-session (Session Manager, SessionContext, Job Service)                 │
└────────┬────────────────────────────────────────────────────────────────────┘
         │
┌────────▼────────────────────────────────────────────────────────────────────┐
│                           PLANNING LAYER                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│  SQL String ──► sail-sql-parser ──► AST                                      │
│       or                              │                                      │
│  Spark Relation ─────────────────────►│                                      │
│                                       ▼                                      │
│                            sail-sql-analyzer ──► spec::Plan                  │
│                                                      │                       │
│                            sail-plan (resolver) ─────► DataFusion LogicalPlan│
│                                                              │               │
│                            sail-logical-optimizer ───────────► Optimized LP  │
│                                                                     │        │
│                            sail-physical-optimizer ─────────► ExecutionPlan  │
└────────┬────────────────────────────────────────────────────────────────────┘
         │
┌────────▼────────────────────────────────────────────────────────────────────┐
│                           EXECUTION LAYER                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│  LOCAL MODE:                                                                 │
│    LocalJobRunner → DataFusion execute_stream()                              │
│                                                                              │
│  CLUSTER MODE:                                                               │
│    ClusterJobRunner → Driver Actor → Worker Pool → Task Execution            │
│    ┌─────────┐      ┌─────────────┐      ┌──────────────┐                   │
│    │ Driver  │◄────►│   Worker 1  │◄────►│   Worker N   │                   │
│    │ (Actor) │      │   (Actor)   │      │   (Actor)    │                   │
│    └────┬────┘      └──────┬──────┘      └──────┬───────┘                   │
│         │                  │                    │                            │
│         └──────────────────┴────────────────────┘                            │
│                     Arrow Flight (Data Plane)                                │
└────────┬────────────────────────────────────────────────────────────────────┘
         │
┌────────▼────────────────────────────────────────────────────────────────────┐
│                            DATA LAYER                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│  CATALOGS:           │  FORMATS:           │  STORAGE:                       │
│  • sail-catalog      │  • sail-delta-lake  │  • sail-object-store            │
│  • sail-catalog-glue │  • sail-iceberg     │    - AWS S3                     │
│  • sail-catalog-unity│  • Parquet/CSV/JSON │    - Azure Blob                 │
│  • sail-catalog-hms  │  • Avro/Arrow       │    - Google Cloud Storage       │
│  • sail-catalog-     │                     │    - HDFS                       │
│    iceberg           │                     │    - Local filesystem           │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Crate Organization (36 Crates)

### Core Utilities (5 crates)
| Crate | Purpose |
|-------|---------|
| `sail-common` | Base types, error handling, config (no DataFusion deps) |
| `sail-common-datafusion` | DataFusion-specific utilities, table format traits |
| `sail-build-scripts` | Build-time utilities |
| `sail-sql-macro` | SQL parser procedural macros |
| `sail-telemetry` | OpenTelemetry observability integration |

### SQL & Query Planning (7 crates)
| Crate | Purpose |
|-------|---------|
| `sail-sql-parser` | Custom Rust SQL parser (Chumsky-based) |
| `sail-sql-analyzer` | AST → spec::Plan semantic analysis |
| `sail-logical-plan` | Custom logical plan operators |
| `sail-logical-optimizer` | Logical optimization rules |
| `sail-physical-plan` | Custom physical plan operators |
| `sail-physical-optimizer` | Physical optimization (23+ rules) |
| `sail-plan` | Plan resolution and unified plan types |

### Execution & Runtime (6 crates)
| Crate | Purpose |
|-------|---------|
| `sail-execution` | Distributed execution (Driver, Worker, Tasks) |
| `sail-session` | Session management and context |
| `sail-data-source` | DataSource abstraction and format options |
| `sail-server` | gRPC server infrastructure |
| `sail-flight` | Apache Arrow Flight protocol support |
| `sail-spark-connect` | Spark Connect protocol implementation |

### Data Format & Lakehouse (5 crates)
| Crate | Purpose |
|-------|---------|
| `sail-delta-lake` | Delta Lake table format support |
| `sail-iceberg` | Apache Iceberg table format support |
| `sail-plan-lakehouse` | Lakehouse-specific physical planning |
| `sail-object-store` | Cloud storage abstraction |
| `sail-cache` | Caching layer for catalog operations |

### Catalog Management (8 crates)
| Crate | Purpose |
|-------|---------|
| `sail-catalog` | Base catalog interface and caching |
| `sail-catalog-memory` | In-memory catalog (testing) |
| `sail-catalog-system` | System tables catalog |
| `sail-catalog-iceberg` | Iceberg REST Catalog |
| `sail-catalog-unity` | Databricks Unity Catalog |
| `sail-catalog-onelake` | Microsoft OneLake |
| `sail-catalog-glue` | AWS Glue Catalog |
| `sail-catalog-hms` | Hive Metastore (Thrift) |

### Functions & Bindings (5 crates)
| Crate | Purpose |
|-------|---------|
| `sail-function` | Scalar, aggregate, window, table functions |
| `sail-python-udf` | Python UDF/UDAF/UDWF/UDTF execution |
| `sail-cli` | Command-line interface |
| `sail-python` | PyO3 Python bindings |
| `sail-gold-test` | Test framework utilities |

---

## Query Planning Pipeline

The planning pipeline transforms SQL strings or Spark relations into optimized execution plans:

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 1: PARSING (sail-sql-parser)                                           │
├──────────────────────────────────────────────────────────────────────────────┤
│ SQL String: "SELECT col1 FROM table1 WHERE col1 > 5"                         │
│      │                                                                       │
│      ▼                                                                       │
│ Lexer: create_lexer() → Tokens                                               │
│      │                                                                       │
│      ▼                                                                       │
│ Parser: create_parser() → AST (unresolved, syntactic)                        │
│                                                                              │
│ Key AST Types:                                                               │
│ • Statement { Query | DDL }                                                  │
│ • Query { with_clause, body, modifiers }                                     │
│ • Expr { Atom, BinaryOp, FunctionCall, Literal, ... }                        │
│ • DataType, Literal, etc.                                                    │
└──────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 2: ANALYSIS (sail-sql-analyzer)                                        │
├──────────────────────────────────────────────────────────────────────────────┤
│ AST → spec::Plan (semantic intermediate representation)                      │
│                                                                              │
│ Conversion Functions:                                                        │
│ • from_ast_statement(Statement) → spec::Plan                                 │
│ • from_ast_query(Query) → spec::QueryPlan                                    │
│ • from_ast_expression(Expr) → spec::Expr                                     │
│                                                                              │
│ Key Spec Types (sail_common::spec):                                          │
│ • Plan { Query(QueryPlan) | Command(CommandPlan) }                           │
│ • QueryNode: 40+ variants (Read, Project, Filter, Join, Aggregate, ...)      │
│ • Expr: Literal, Attribute, Function, Cast, Alias, ...                       │
└──────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 3: RESOLUTION (sail-plan resolver)                                     │
├──────────────────────────────────────────────────────────────────────────────┤
│ spec::Plan → DataFusion LogicalPlan (semantically resolved)                  │
│                                                                              │
│ Key Operations:                                                              │
│ • Catalog lookup for tables/views                                            │
│ • Schema resolution for column references                                    │
│ • Function signature validation                                              │
│ • Expression type coercion                                                   │
│                                                                              │
│ Entry Point: PlanResolver::resolve_named_plan(plan)                          │
│ • resolve_query_plan() → dispatches on QueryNode variant                     │
│ • resolve_named_expression() → spec::Expr → DataFusion Expr                  │
└──────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 4: LOGICAL OPTIMIZATION (sail-logical-optimizer + DataFusion)          │
├──────────────────────────────────────────────────────────────────────────────┤
│ LogicalPlan → Optimized LogicalPlan                                          │
│                                                                              │
│ Custom Rules:                                                                │
│ • DecorrelateLateralProjection (LATERAL correlated subqueries)               │
│                                                                              │
│ DataFusion Built-in Rules:                                                   │
│ • Expression simplification                                                  │
│ • Predicate pushdown                                                         │
│ • Projection pushdown                                                        │
│ • Join ordering                                                              │
│ • Constant folding                                                           │
└──────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 5: PHYSICAL PLANNING & OPTIMIZATION (sail-physical-optimizer)          │
├──────────────────────────────────────────────────────────────────────────────┤
│ Optimized LogicalPlan → ExecutionPlan                                        │
│                                                                              │
│ 23+ Optimization Rules:                                                      │
│ 1. OutputRequirements (add mode)     13. TopKAggregation                     │
│ 2. AggregateStatistics               14. LimitPushPastWindows                │
│ 3. JoinReorder (cost-based DP)       15. LimitPushdown                       │
│ 4. JoinSelection                     16. ProjectionPushdown                  │
│ 5. LimitedDistinctAggregation        17. PushdownSort                        │
│ 6. FilterPushdown                    18. EnsureCooperative                   │
│ 7. EnforceDistribution               19. FilterPushdown (post)               │
│ 8. CombinePartialFinalAggregate      20. RewriteExplicitRepartition (custom) │
│ 9. EnforceSorting                    21. RewriteCollectLeftHashJoin (custom) │
│ 10. OptimizeAggregateOrder           22. EnforceBarrierPartitioning (custom) │
│ 11. ProjectionPushdown               23. SanityCheckPlan                     │
│ 12. OutputRequirements (remove)                                              │
│                                                                              │
│ Output: Arc<dyn ExecutionPlan> with concrete algorithms                      │
└──────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 6: CUSTOM PLAN OPERATORS (sail-logical-plan, sail-physical-plan)       │
├──────────────────────────────────────────────────────────────────────────────┤
│ Sail-specific operators for Spark compatibility:                             │
│                                                                              │
│ • Barrier          - Synchronization for coordinated writes                  │
│ • FileWrite        - Custom file writing                                     │
│ • FileDelete       - File deletion operations                                │
│ • MapPartitions    - User-defined partition mapping                          │
│ • MonotonicId      - Monotonic ID generation                                 │
│ • Range            - Range generation                                        │
│ • Repartition      - Custom partitioning                                     │
│ • SchemaPivot      - Schema transformation                                   │
│ • ShowString       - String representation                                   │
│ • SparkPartitionId - Partition ID generation                                 │
│ • Streaming        - Filter, limit, collector, source adapter                │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Execution Layer

### Local Mode

In local mode, Sail runs as a single process with multi-threaded partition processing:

```
┌─────────────────────────────────────────────────────────────────┐
│                    LOCAL MODE ARCHITECTURE                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Client Request                                                  │
│       │                                                          │
│       ▼                                                          │
│  SessionContext (with LocalJobRunner)                            │
│       │                                                          │
│       ▼                                                          │
│  LocalJobRunner::execute()                                       │
│       │                                                          │
│       ▼                                                          │
│  DataFusion::execute_stream()                                    │
│       │                                                          │
│       ├───────┬───────┬───────┬───────┐                         │
│       ▼       ▼       ▼       ▼       ▼                         │
│   Partition Partition Partition ... Partition                    │
│      0         1         2             N                         │
│       │       │       │               │                          │
│       └───────┴───────┴───────┴───────┘                         │
│                       │                                          │
│                       ▼                                          │
│              Arrow RecordBatch Stream                            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Cluster Mode

In cluster mode, Sail forms a distributed system with driver and workers:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CLUSTER MODE ARCHITECTURE                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                         CONTROL PLANE (gRPC)                          │   │
│  │                                                                        │   │
│  │   ┌─────────────┐         ┌─────────────┐         ┌─────────────┐    │   │
│  │   │   Driver    │◄───────►│   Worker 1  │         │   Worker N  │    │   │
│  │   │   Actor     │         │    Actor    │   ...   │    Actor    │    │   │
│  │   │             │◄────────┴─────────────┴─────────►              │    │   │
│  │   │ • JobScheduler        │             │         │             │    │   │
│  │   │ • TaskAssigner        │ • TaskRunner│         │ • TaskRunner│    │   │
│  │   │ • WorkerPool          │ • StreamMgr │         │ • StreamMgr │    │   │
│  │   │ • StreamManager       │             │         │             │    │   │
│  │   └─────────────┘         └──────┬──────┘         └──────┬──────┘    │   │
│  │                                  │                       │            │   │
│  └──────────────────────────────────┼───────────────────────┼────────────┘   │
│                                     │                       │                │
│  ┌──────────────────────────────────┼───────────────────────┼────────────┐   │
│  │                         DATA PLANE (Arrow Flight)        │            │   │
│  │                                  │                       │            │   │
│  │                                  ▼                       ▼            │   │
│  │                           ┌─────────────────────────────────┐         │   │
│  │                           │     Shuffle Data Exchange       │         │   │
│  │                           │   (Arrow columnar batches)      │         │   │
│  │                           └─────────────────────────────────┘         │   │
│  │                                                                        │   │
│  └────────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Execution Flow (Cluster Mode)

```
1. Client submits ExecutePlanRequest via Spark Connect
       │
       ▼
2. SessionContext with ClusterJobRunner receives request
       │
       ▼
3. resolve_and_execute_plan() creates optimized ExecutionPlan
       │
       ▼
4. ClusterJobRunner sends DriverEvent::ExecuteJob to Driver Actor
       │
       ▼
5. Driver Actor:
   ├── JobScheduler converts plan to job graph with stages
   ├── TaskAssigner assigns tasks to workers based on locality
   └── Sends RunTask events to Worker Actors
       │
       ▼
6. Worker Actors:
   ├── Deserialize task definitions
   ├── Execute physical plan partitions using DataFusion
   ├── Exchange shuffle data with peers via Arrow Flight
   └── Report TaskStatus back to Driver
       │
       ▼
7. Driver aggregates results via StreamManager
       │
       ▼
8. Results streamed back to client as ExecutePlanResponseStream
```

### Task Status Lifecycle

```
Pending ──► Running ──► Complete
                   └──► Failed
                   └──► Cancelled
```

---

## Catalog Layer

### Catalog Provider Abstraction

The `CatalogProvider` trait defines the interface for all catalog backends:

```rust
#[async_trait]
pub trait CatalogProvider: Send + Sync {
    // Database operations
    async fn create_database(&self, options: CreateDatabaseOptions) -> Result<()>;
    async fn get_database(&self, namespace: &Namespace) -> Result<Option<DatabaseStatus>>;
    async fn list_databases(&self, parent: Option<&Namespace>) -> Result<Vec<DatabaseStatus>>;
    async fn drop_database(&self, options: DropDatabaseOptions) -> Result<()>;

    // Table operations
    async fn create_table(&self, options: CreateTableOptions) -> Result<()>;
    async fn get_table(&self, namespace: &Namespace, name: &str) -> Result<Option<TableStatus>>;
    async fn list_tables(&self, namespace: &Namespace) -> Result<Vec<TableStatus>>;
    async fn drop_table(&self, options: DropTableOptions) -> Result<()>;
    async fn alter_table(&self, options: AlterTableOptions) -> Result<()>;

    // View operations (similar pattern)
    async fn create_view(&self, options: CreateViewOptions) -> Result<()>;
    async fn get_view(&self, namespace: &Namespace, name: &str) -> Result<Option<TableStatus>>;
    async fn list_views(&self, namespace: &Namespace) -> Result<Vec<TableStatus>>;
    async fn drop_view(&self, options: DropViewOptions) -> Result<()>;
}
```

### Catalog Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CATALOG MANAGER                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│  • Registered catalog providers                                              │
│  • Default catalog/database context                                          │
│  • Name resolution logic                                                     │
│  • Temporary views (session-scoped)                                          │
│  • CatalogObjectTracker (functions, plans)                                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          │                         │                         │
          ▼                         ▼                         ▼
┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
│  CachingCatalog     │  │  CachingCatalog     │  │  CachingCatalog     │
│  Provider           │  │  Provider           │  │  Provider           │
│  (decorator)        │  │  (decorator)        │  │  (decorator)        │
└─────────┬───────────┘  └─────────┬───────────┘  └─────────┬───────────┘
          │                         │                         │
          ▼                         ▼                         ▼
┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
│  RuntimeAware       │  │  RuntimeAware       │  │  RuntimeAware       │
│  CatalogProvider    │  │  CatalogProvider    │  │  CatalogProvider    │
│  (decorator)        │  │  (decorator)        │  │  (decorator)        │
└─────────┬───────────┘  └─────────┬───────────┘  └─────────┬───────────┘
          │                         │                         │
          ▼                         ▼                         ▼
┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
│  GlueCatalog        │  │  UnityCatalog       │  │  HMSCatalog         │
│  Provider           │  │  Provider           │  │  Provider           │
│                     │  │                     │  │                     │
│  (AWS SDK)          │  │  (REST API)         │  │  (Thrift)           │
└─────────────────────┘  └─────────────────────┘  └─────────────────────┘
```

### Supported Catalog Backends

| Catalog | Protocol | Authentication | Features |
|---------|----------|----------------|----------|
| **Memory** | In-process | None | Testing, development |
| **AWS Glue** | AWS SDK | IAM, STS | Multi-region, VPC endpoints |
| **Unity Catalog** | REST | OAuth2 | Databricks integration |
| **Hive Metastore** | Thrift | Kerberos/SASL | Legacy Hive compatibility |
| **Iceberg REST** | REST | Bearer token | Iceberg spec compliance |
| **OneLake** | REST | Azure AD | Microsoft Fabric |

### Caching Layer

```rust
pub struct CachingCatalogProvider<P: CatalogProvider> {
    inner: Arc<P>,
    database_cache: Option<Cache<Option<Namespace>, Vec<DatabaseStatus>>>,
    table_cache: Option<Cache<Namespace, Vec<TableStatus>>>,
    view_cache: Option<Cache<Namespace, Vec<TableStatus>>>,
}
```

Cache types:
- **None**: No caching
- **Global**: Shared across sessions
- **Session**: Per-session with configurable size/TTL

---

## Table Format Layer

### TableFormat Trait

The `TableFormat` trait abstracts table format implementations:

```rust
#[async_trait]
pub trait TableFormat: Send + Sync {
    /// Create a table source for reading
    async fn create_source(&self, info: &SourceInfo) -> Result<Arc<dyn TableSource>>;

    /// Create a writer execution plan
    async fn create_writer(&self, info: &SinkInfo) -> Result<Arc<dyn ExecutionPlan>>;

    /// Create a row-level writer (DELETE/UPDATE/MERGE)
    async fn create_row_level_writer(&self, info: &RowLevelWriteInfo)
        -> Result<Arc<dyn ExecutionPlan>>;

    /// Infer schema without creating full source
    async fn infer_schema(&self, info: &SourceInfo) -> Result<SchemaRef>;

    /// Alter table properties
    async fn alter_table_properties(&self, ...) -> Result<()>;

    /// Get merge strategy (CoW vs MoR)
    fn merge_strategy(&self) -> Option<MergeStrategy>;
}
```

### Supported Table Formats

| Format | Read | Write | Row-Level Ops | Time Travel |
|--------|------|-------|---------------|-------------|
| **Delta Lake** | ✅ | ✅ | ✅ (CoW & MoR) | ✅ |
| **Iceberg** | ✅ | ✅ | ✅ | ✅ |
| **Parquet** | ✅ | ✅ | ❌ | ❌ |
| **CSV** | ✅ | ✅ | ❌ | ❌ |
| **JSON** | ✅ | ✅ | ❌ | ❌ |
| **Avro** | ✅ | ✅ | ❌ | ❌ |

### Data Flow: Read Path

```
1. Catalog.get_table() → TableStatus (format, location, metadata)
       │
       ▼
2. TableFormat.create_source(SourceInfo)
       │
       ├── Delta: Load snapshot, build DeltaTableSource
       └── Iceberg: Load metadata, build IcebergTableSource
       │
       ▼
3. Logical Planning
       │
       ├── Filter pushdown
       ├── Projection pushdown
       └── Partition pruning
       │
       ▼
4. Physical Planning
       │
       ├── Delta: DeltaScanExec with file pruning
       └── Iceberg: IcebergTableProvider with manifest pruning
       │
       ▼
5. Execution
       │
       └── Object store reads → Arrow RecordBatches
```

### Data Flow: Write Path

```
1. TableFormat.create_writer(SinkInfo)
       │
       ▼
2. Partitioning/Bucketing logic
       │
       ▼
3. Format-specific writer executors
       │
       ▼
4. Object store writes
       │
       ▼
5. Metadata commit (Delta log / Iceberg manifest)
```

---

## Storage Layer

### Object Store Registry

```rust
pub struct DynamicObjectStoreRegistry {
    stores: DashMap<String, Arc<dyn ObjectStore>>,
    // URL scheme + authority → ObjectStore instance
}
```

### Supported Storage Backends

| Backend | URL Scheme | Features |
|---------|------------|----------|
| **Local FS** | `file://` | Development, testing |
| **AWS S3** | `s3://`, `s3a://` | IAM, STS, instance profile |
| **Azure Blob** | `abfss://`, `wasbs://` | Managed identity, SAS |
| **GCS** | `gs://` | Service account, ADC |
| **HDFS** | `hdfs://` | Kerberos, WebHDFS |
| **HTTP(S)** | `http://`, `https://` | Public URLs |
| **Hugging Face** | `hf://` | Dataset hub integration |

### Storage Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      DynamicObjectStoreRegistry                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   URL: "s3://bucket/path"                                                    │
│         │                                                                    │
│         ▼                                                                    │
│   Parse scheme + authority → "s3://bucket"                                   │
│         │                                                                    │
│         ▼                                                                    │
│   DashMap lookup (atomic get-or-insert)                                      │
│         │                                                                    │
│         ├── Cache hit: Return existing store                                 │
│         │                                                                    │
│         └── Cache miss: Create new store                                     │
│                   │                                                          │
│                   ▼                                                          │
│         ┌─────────────────────────────────────────────┐                     │
│         │ RuntimeAwareObjectStore (decorator)         │                     │
│         │     └── LoggingObjectStore (decorator)      │                     │
│         │             └── LazyObjectStore (deferred)  │                     │
│         │                     └── S3ObjectStore       │                     │
│         └─────────────────────────────────────────────┘                     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Session Management

### Session Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SESSION LIFECYCLE                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   1. Client connects (Spark Connect / Flight)                                │
│         │                                                                    │
│         ▼                                                                    │
│   2. SessionManager.get_or_create_session_context(session_id, user_id)       │
│         │                                                                    │
│         ├── Session exists: Return existing SessionContext                   │
│         │                                                                    │
│         └── Session not found: Create new session                            │
│                   │                                                          │
│                   ▼                                                          │
│             ServerSessionFactory.create()                                    │
│                   │                                                          │
│                   ├── Create DataFusion SessionContext                       │
│                   ├── Configure catalogs (with caching)                      │
│                   ├── Create JobRunner (Local or Cluster)                    │
│                   ├── Register functions                                     │
│                   └── Schedule idle timeout probe                            │
│                                                                              │
│   3. Session active: Handle queries, commands                                │
│         │                                                                    │
│         ▼                                                                    │
│   4. Session idle timeout or explicit close                                  │
│         │                                                                    │
│         ▼                                                                    │
│   5. SessionManager.delete_session()                                         │
│         │                                                                    │
│         ├── Stop JobRunner                                                   │
│         ├── Collect job history                                              │
│         └── Update state to Deleted                                          │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Session States

```rust
enum ServerSessionState {
    Running { context: SessionContext },
    Deleting,
    Deleted { history: Arc<SessionHistory> },
    Failed,
}
```

---

## Key Design Patterns

### 1. Actor-Based Concurrency

Session management, driver, and workers use the actor model for thread-safe concurrent processing:

```rust
pub trait Actor {
    type Message;
    type Options;

    fn name() -> &'static str;
    fn new(options: Self::Options) -> Self;
    async fn start(&mut self, ctx: &mut ActorContext<Self>);
    fn receive(&mut self, ctx: &mut ActorContext<Self>, message: Self::Message) -> ActorAction;
}
```

### 2. Decorator Pattern

Catalog and storage layers use decorators for cross-cutting concerns:

```
CatalogProvider
    └── CachingCatalogProvider (caching)
            └── RuntimeAwareCatalogProvider (runtime isolation)
                    └── GlueCatalogProvider (implementation)
```

### 3. Streaming Results

All query results are streamed via async channels:

```rust
impl Stream for ExecutePlanResponseStream {
    type Item = Result<ExecutePlanResponse, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<Option<Self::Item>>;
}
```

### 4. Lazy Execution

Query plans are resolved and optimized lazily as clients read results.

### 5. Control Plane / Data Plane Separation

- **Control plane**: Internal gRPC protocol for driver-worker coordination
- **Data plane**: Arrow Flight for high-performance data exchange

---

## Extension Points

### Adding a New Catalog Backend

1. Implement `CatalogProvider` trait
2. Optionally wrap with `RuntimeAwareCatalogProvider` for blocking operations
3. Optionally wrap with `CachingCatalogProvider` for performance
4. Register with `CatalogManager`

### Adding a New Table Format

1. Implement `TableFormat` trait
2. Provide `create_source()` → `TableSource` for reads
3. Provide `create_writer()` → `ExecutionPlan` for writes
4. Register with `TableFormatRegistry`

### Adding a New Storage Backend

1. Add factory function to `DynamicObjectStoreRegistry`
2. Implement `object_store::ObjectStore` trait
3. Handle URL scheme + authority in registry lookup

### Adding Row-Level Operations

1. Override `create_row_level_writer()` in `TableFormat`
2. Implement `MergeCapableSource` for file path tracking
3. Provide strategy via `merge_strategy()` method

---

## Key File References

| Component | Key Files |
|-----------|-----------|
| **SQL Parser** | `crates/sail-sql-parser/src/parser.rs`, `lexer.rs`, `ast/` |
| **SQL Analyzer** | `crates/sail-sql-analyzer/src/statement.rs`, `query.rs` |
| **Plan Resolver** | `crates/sail-plan/src/resolver/` (40+ modules) |
| **Physical Optimizer** | `crates/sail-physical-optimizer/src/join_reorder/` |
| **Spark Connect Server** | `crates/sail-spark-connect/src/server.rs`, `entrypoint.rs` |
| **Session Manager** | `crates/sail-session/src/session_manager/` |
| **Driver Actor** | `crates/sail-execution/src/driver/actor/` |
| **Worker Actor** | `crates/sail-execution/src/worker/actor/` |
| **Catalog Provider** | `crates/sail-catalog/src/provider/mod.rs` |
| **Delta Lake** | `crates/sail-delta-lake/src/table_format.rs` |
| **Iceberg** | `crates/sail-iceberg/src/table_format.rs` |
| **Object Store** | `crates/sail-object-store/src/registry.rs` |

---

## Further Reading

- [Query Planning](./query-planning/) - Detailed explanation of plan optimization
- [Benchmark Results](../introduction/benchmark-results/) - Performance comparisons
- [Configuration](../guide/configuration/) - Configuration options
- [Deployment](../guide/deployment/) - Docker and Kubernetes deployment
