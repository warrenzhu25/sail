---
title: Architecture Deep Dive
rank: 3
---

# Sail Architecture Deep Dive

This document explains Sail's architecture through the code paths that matter
most when evaluating, debugging, or extending the system. It focuses on the key
files that define the request path, session setup, planning pipeline, execution
runtime, catalog layer, table format layer, and storage integration.

Sail is a Rust implementation of a Spark-compatible query engine. The public
surface accepts Spark Connect requests, SQL strings, Arrow Flight SQL requests,
and command-line entry points. Internally those requests are normalized into
Sail's shared plan model, resolved into DataFusion logical plans, planned into
DataFusion physical plans plus Sail extension operators, and executed either in a
single local process or across a driver and worker cluster.

## Reading Guide

This deep dive is organized by architectural layer. Use the table below to
navigate to the section most relevant to your task:

| If you want to... | Start with |
|-------------------|------------|
| Understand how requests enter Sail | [Request Entry Points](#request-entry-points) |
| Debug session or runtime configuration | [Session And Runtime Setup](#session-and-runtime-setup) |
| Understand plan representation | [Plan Model And SQL Conversion](#plan-model-and-sql-conversion) |
| Debug query planning or resolution | [Logical And Physical Planning](#logical-and-physical-planning) |
| Debug local or distributed execution | [Execution](#execution) |
| Add or debug a catalog backend | [Catalogs](#catalogs) |
| Add or debug a table format | [Data Sources And Table Formats](#data-sources-and-table-formats) |
| Debug object storage access | [Object Storage](#object-storage) |
| Add or debug functions | [Functions And Python Execution](#functions-and-python-execution) |

For a higher-level overview of Sail's architecture without code details, see
[Architecture Overview](./architecture/).

## Request Entry Points

### `crates/sail-cli/src/main.rs`

This is the native binary entry point. It loads `CliConfig`, decides whether the
current process should run as the Sail CLI or as an embedded Python interpreter,
and then delegates to `sail_cli::runner::main`.

The embedded Python behavior is important because PySpark and Python UDF support
can launch child Python processes. Sail sets `CliConfigEnv::RUN_PYTHON` before
initializing Python so child processes see the Sail binary as a Python-capable
executable. When that environment variable is already present, `main.rs` calls
`run_python_interpreter`, builds Python-compatible wide-character arguments, and
hands control to `Py_Main`. This keeps process spawning compatible with Python
libraries that rely on `sys.executable`.

The file also installs the optional `mimalloc` global allocator when the feature
is enabled. Apart from that allocator choice, it intentionally keeps startup
logic thin: configuration, Python bootstrap, and CLI delegation live here, while
server and worker behavior lives in the library crates.

### `crates/sail-spark-connect/src/lib.rs`

This file defines the top-level Spark Connect crate structure and exposes the
generated protobuf modules. The `spark::connect` module is generated through
`tonic::include_proto!`, and the file descriptor set is embedded so the gRPC
server can expose Spark Connect service metadata.

The rest of the crate is organized around protocol conversion, service handlers,
session handling, execution streaming, and schema conversion. `lib.rs` is the
place to start when tracing which modules own Spark wire compatibility, but the
request logic itself is in the `service` modules.

### `crates/sail-spark-connect/src/service/mod.rs`

The service module groups Spark Connect service handlers by responsibility:
artifact management, configuration management, plan analysis, and plan
execution. It re-exports those handler modules for the server implementation.

This file does not contain business logic directly. Its role is architectural:
it keeps Spark Connect's RPC surface split into focused handlers instead of a
single large service file. When adding a new Spark Connect operation, first
identify whether it is an analysis request, execution request, artifact request,
or configuration request, then add the implementation to the matching module.

### `crates/sail-spark-connect/src/service/plan_executor.rs`

Reference code excerpt:

```rust
pub struct ExecutePlanResponseStream {
    session_id: String,
    operation_id: String,
    inner: ExecutorOutputStream,
}

impl ExecutePlanResponseStream {
    pub fn new(session_id: String, operation_id: String, inner: ExecutorOutputStream) -> Self {
        Self {
            session_id,
            operation_id,
            inner,
        }
    }
}

impl Stream for ExecutePlanResponseStream {
    type Item = Result<ExecutePlanResponse, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<ExecutePlanResponse, Status>>> {
        self.inner.as_mut().poll_next(cx).map(|poll| {
            poll.map(|item| {
                let mut response = ExecutePlanResponse::default();
                response.session_id.clone_from(&self.session_id);
                response.server_side_session_id.clone_from(&self.session_id);
                response.operation_id.clone_from(&self.operation_id.clone());
                response.response_id = item.id;
                match item.batch {
                    ExecutorBatch::ArrowBatch(batch) => {
                        response.response_type = Some(ResponseType::ArrowBatch(batch));
                    }
                    ExecutorBatch::SqlCommandResult(result) => {
                        response.response_type = Some(ResponseType::SqlCommandResult(*result));
                    }
                    ExecutorBatch::WriteStreamOperationStartResult(result) => {
                        response.response_type =
                            Some(ResponseType::WriteStreamOperationStartResult(*result));
                    }
                    ExecutorBatch::StreamingQueryCommandResult(result) => {
                        response.response_type =
                            Some(ResponseType::StreamingQueryCommandResult(*result));
                    }
                    ExecutorBatch::StreamingQueryManagerCommandResult(result) => {
                        response.response_type =
                            Some(ResponseType::StreamingQueryManagerCommandResult(*result));
                    }
                    ExecutorBatch::CheckpointCommandResult(result) => {
                        response.response_type =
                            Some(ResponseType::CheckpointCommandResult(*result));
                    }
                    ExecutorBatch::Schema(schema) => {
                        response.schema = Some(*schema);
                    }
                    ExecutorBatch::Complete => {
                        response.response_type =
                            Some(ResponseType::ResultComplete(ResultComplete::default()));
                    }
                }
                debug!("{response:?}");
                Ok(response)
            })
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

enum ExecutePlanMode {
    Lazy,
    EagerSilent,
}

async fn handle_execute_plan(
    ctx: &SessionContext,
    plan: spec::Plan,
    metadata: ExecutorMetadata,
    mode: ExecutePlanMode,
) -> SparkResult<ExecutePlanResponseStream> {
    let span = Span::root("handle_execute_plan", SpanContext::random());
    let spark = ctx.extension::<SparkSession>()?;
    let service = ctx.extension::<JobService>()?;
    let operation_id = metadata.operation_id.clone();
    let (plan, _) = resolve_and_execute_plan(ctx, spark.plan_config()?, plan).await?;
    let stream = {
        let span = Span::enter_with_parent("JobRunner::execute", &span);
        service.runner().execute(ctx, plan).in_span(span).await?
    };
    let rx = match mode {
        ExecutePlanMode::Lazy => {
            let _guard = span.set_local_parent();
            let executor = Executor::new(
                metadata,
                stream,
                spark.options().execution_heartbeat_interval,
            );
            let rx = executor.start()?;
            spark.add_executor(executor)?;
            rx
        }
        ExecutePlanMode::EagerSilent => {
            let _ = read_stream(stream).in_span(span).await?;
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            if metadata.reattachable {
                tx.send(ExecutorOutput::complete()).await?;
            }
            ReceiverStream::new(rx)
        }
    };
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        operation_id,
        Box::pin(rx),
    ))
}

pub(crate) async fn handle_execute_relation(
    ctx: &SessionContext,
    relation: Relation,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = relation.try_into()?;
    handle_execute_plan(ctx, plan, metadata, ExecutePlanMode::Lazy).await
}

pub(crate) async fn handle_execute_write_operation(
    ctx: &SessionContext,
    write: WriteOperation,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = spec::Plan::Command(spec::CommandPlan::new(spec::CommandNode::Write(
        write.try_into()?,
    )));
    handle_execute_plan(ctx, plan, metadata, ExecutePlanMode::EagerSilent).await
}
```

Detailed logic:

This is the main Spark Connect execution path. It receives Spark Connect
relations and commands, converts them into `sail_common::spec::Plan`, resolves
and prepares the physical plan, executes it through the session's `JobService`,
and converts results back into Spark Connect response messages.

The central helper is `handle_execute_plan`. It looks up the `SparkSession` and
`JobService` extensions from the `SessionContext`, calls
`resolve_and_execute_plan`, and then invokes `service.runner().execute(ctx,
plan)`. The result is a `SendableRecordBatchStream` produced by the configured
job runner. For normal query execution, the stream is wrapped in an `Executor`
that emits Spark Connect response batches lazily as the client reads. For command
execution, the file often uses eager silent mode: it drains the stream to make
the command happen, then returns an empty or complete response depending on
reattachable operation metadata.

**Session extensions** are Sail-specific objects attached to DataFusion's
`SessionContext` using DataFusion's extension mechanism. When code calls
`ctx.extension::<SparkSession>()`, it retrieves the `SparkSession` object that
was registered during session creation. Sail uses this pattern for several
session-scoped dependencies: `SparkSession` holds Spark-specific metadata and
executors, `JobService` provides the job runner for plan execution,
`CatalogManager` handles catalog operations, and `TableFormatRegistry` provides
table format implementations. This approach keeps Sail's extensions decoupled
from DataFusion's core types while still making them accessible throughout the
request path.

**Resolution** in `resolve_and_execute_plan` refers to the process of converting
Sail's intermediate `spec::Plan` representation into a DataFusion `LogicalPlan`
and then into a physical `ExecutionPlan`. This involves resolving table names
against catalogs, resolving column names against schemas, looking up functions
in the function registry, applying type coercion, and running DataFusion's
optimizer passes. The result is a fully resolved, optimized physical plan ready
for execution.

`ExecutePlanResponseStream` adapts Sail executor output into
`ExecutePlanResponse`. Its `poll_next` implementation maps each internal batch
variant to the correct Spark Connect response type: Arrow batches, SQL command
results, streaming query results, checkpoint results, schema messages, and final
completion messages. This is the final protocol boundary before data reaches a
Spark Connect client.

**Why wrap internal batches?** This wrapper exists because Sail's internal
execution produces a stream of generic `ExecutorBatch` values, but Spark Connect
clients expect `ExecutePlanResponse` messages with session IDs, operation IDs,
and response IDs attached. The wrapper performs this protocol conversion at the
edge of the system rather than threading Spark Connect concerns through the
entire execution layer. This keeps the core execution engine protocol-agnostic:
the same internal batch stream could be adapted for Flight SQL or other protocols
with a different wrapper.

The individual handler functions choose which Sail plan to build. For example,
`handle_execute_relation` converts a Spark relation into a query plan,
`handle_execute_write_operation` wraps write commands in
`spec::CommandNode::Write`, and `handle_execute_merge_into_table_command`
converts Spark's merge command into Sail's command representation. SQL command
handling is slightly special: when a SQL string represents a command rather than
a query, Sail executes the command and converts its output into a local relation
so Spark's SQL command semantics remain compatible.

### `crates/sail-spark-connect/src/service/plan_analyzer.rs`

Reference code:

```rust
async fn analyze_schema(ctx: &SessionContext, plan: sc::Plan) -> SparkResult<sc::DataType> {
    let spark = ctx.extension::<SparkSession>()?;
    let resolver = PlanResolver::new(ctx, spark.plan_config()?);
    let NamedPlan { plan, fields } = resolver
        .resolve_named_plan(spec::Plan::Query(plan.try_into()?))
        .await?;
    let schema = if let Some(fields) = fields {
        rename_schema(plan.schema().inner(), fields.as_slice())?
    } else {
        plan.schema().inner().clone()
    };
    to_spark_schema(schema)
}

pub(crate) async fn handle_analyze_schema(
    ctx: &SessionContext,
    request: SchemaRequest,
) -> SparkResult<SchemaResponse> {
    let SchemaRequest { plan } = request;
    let plan = plan.required("plan")?;
    let schema = analyze_schema(ctx, plan).await?;
    Ok(SchemaResponse {
        schema: Some(schema),
    })
}

pub(crate) async fn handle_analyze_explain(
    ctx: &SessionContext,
    request: ExplainRequest,
) -> SparkResult<ExplainResponse> {
    let spark = ctx.extension::<SparkSession>()?;
    let ExplainRequest { plan, explain_mode } = request;
    let plan = plan.required("plan")?;
    let explain_mode = ExplainMode::try_from(explain_mode)?;
    let spec_mode = explain_mode.try_into()?;
    let options = ExplainOptions::from_mode(spec_mode);
    let explain = explain_string(
        ctx,
        spark.plan_config()?,
        spec::Plan::Query(plan.try_into()?),
        options,
    )
    .await?;
    Ok(ExplainResponse {
        explain_string: explain.output,
    })
}

pub(crate) async fn handle_analyze_ddl_parse(
    ctx: &SessionContext,
    request: DdlParseRequest,
) -> SparkResult<DdlParseResponse> {
    let data_type = parse_spark_data_type(request.ddl_string.as_str())?;
    let spark = ctx.extension::<SparkSession>()?;
    let resolver = PlanResolver::new(ctx, spark.plan_config()?);
    let data_type = resolver.resolve_data_type_for_plan(&data_type)?;
    Ok(DdlParseResponse {
        parsed: Some(data_type.try_into()?),
    })
}
```

Detailed logic:

This file handles Spark Connect analysis requests. These requests ask for
metadata about a plan rather than executing the plan for user data. Examples are
schema analysis, explain strings, tree strings, local/streaming checks, DDL
parsing, Spark version reporting, and storage level metadata.

The key pattern is to convert the Spark Connect plan into `spec::Plan`, create a
`PlanResolver`, and resolve the plan far enough to answer the metadata question.
`analyze_schema` resolves a query plan into a `NamedPlan`, restores user-facing
field names with `rename_schema`, and converts the DataFusion schema back into
Spark's schema protobuf shape. `handle_analyze_explain` delegates to
`sail_plan::explain::explain_string`, which runs the normal planning path in an
explain mode. `handle_analyze_ddl_parse` parses Spark DDL data types and asks
the plan resolver to map them to DataFusion types.

**Why does `NamedPlan` separate the plan from field names?** During resolution,
Sail often renames columns internally to avoid conflicts and enable correct
reference tracking. For example, a column named `id` in user SQL might become
`__sail_resolved_id_42` internally. The `NamedPlan.fields` vector preserves the
original user-facing names so they can be restored when returning results to
clients. This separation keeps the internal plan consistent for optimization
while ensuring the output schema matches what users expect.

The file also documents current compatibility gaps through explicit TODO
handlers. Persist and unpersist currently behave as no-ops with warnings,
whereas input-files, same-semantics, and semantic-hash analysis still return
TODO errors.

### `crates/sail-flight/src/lib.rs`

This file is the Arrow Flight SQL server entry point. The `serve` function
creates a Flight session manager, constructs `SailFlightSqlService`, wraps it in
Arrow Flight's `FlightServiceServer`, and serves it through the shared
`sail_server::ServerBuilder`.

Flight SQL shares the broader Sail session and planning infrastructure with
Spark Connect, but it has its own protocol service and session module. This
separation lets Sail expose both Spark-compatible and Arrow-native entry points
without duplicating the query engine.

## Session And Runtime Setup

### `crates/sail-session/src/lib.rs`

The session crate root exposes the modules that assemble a usable DataFusion
session for Sail. It groups catalog registration, format registration,
observability, optimizer configuration, physical planning, runtime setup,
session factories, and session manager actors.

This file is intentionally declarative. When tracing how a session gains a
catalog manager, job runner, custom query planner, or runtime environment, follow
the modules listed here rather than expecting the crate root to wire everything
directly.

### `crates/sail-session/src/session_factory/mod.rs`

Reference code:

```rust
pub trait SessionFactory<I>: Send {
    /// Create a DataFusion [`SessionContext`].
    /// This method takes `&mut self` so that the factory can maintain internal state if needed.
    /// This method takes an opaque parameter of type `I` for session-specific information.
    fn create(&mut self, info: I) -> Result<SessionContext>;
}
```

Detailed logic:

`SessionFactory` is the abstraction for creating DataFusion `SessionContext`
instances. It is generic over an opaque info type so server sessions and worker
sessions can receive different creation inputs while sharing the same trait.

**The opaque `I` type parameter** allows different factory implementations to
receive different creation inputs without coupling the trait to specific data
structures. For server sessions, `I` might include the session ID, user
configuration, and Spark session metadata. For worker sessions, `I` might include
the worker ID, driver connection info, and task-specific settings. This design
keeps the factory trait reusable across different session creation contexts
without requiring a common input structure.

The trait takes `&mut self` because factories maintain state such as reusable
runtime caches or worker-specific setup. It returns a DataFusion
`SessionContext`, which becomes the container for Sail session extensions:
catalog manager, table format registry, job service, Spark session metadata,
and planning configuration. The module re-exports `ServerSessionFactory` and
`WorkerSessionFactory`, which are the concrete creation paths for client-facing
sessions and worker-side execution sessions.

### `crates/sail-session/src/session_factory/server.rs`

The server session factory builds the `SessionContext` used by client-facing
requests. Its main job is to assemble all session-scoped dependencies in the
right order: runtime environment, catalog manager, registered table formats,
functions, query planner, optimizer settings, and job runner.

This file is where configuration becomes runtime behavior. App configuration
decides whether execution is local or clustered, which catalogs are installed,
how table formats are registered, what session options DataFusion sees, and
which extensions are attached to the context. If a request handler later calls
`ctx.extension::<T>()`, the corresponding extension was usually installed by
this factory or a helper it calls.

Server sessions also differ from worker sessions because they own user-visible
state such as temporary views, Spark session metadata, and job history. This is
the correct layer for adding a session-wide feature that must exist before any
query is analyzed or executed.

### `crates/sail-session/src/session_factory/worker.rs`

The worker session factory creates `SessionContext` instances for remote task
execution. A worker context needs the runtime, object store access, physical
planning support, table formats, and functions required to run assigned
partitions, but it should not behave like a full client session.

**Server sessions own:**

- User-visible state (temporary views, session variables)
- Spark session metadata (session ID, configuration)
- Job history and executor tracking
- Job submission through the job runner
- Catalog manager with full CRUD operations
- Plan resolution from `spec::Plan` to physical plans

**Worker sessions own:**

- Runtime environment for task execution
- Object store access for reading/writing data
- Physical planning support for deserializing plan fragments
- Table format implementations for scan execution
- Function registry for expression evaluation
- No job submission capability (receives pre-planned tasks)

The important distinction is ownership. Server sessions own user-facing
session state and job submission. Worker sessions provide the execution
environment needed by task runners after the driver has already scheduled work.
When debugging cluster-only behavior, check whether the relevant extension or
configuration is installed in both the server and worker factory paths.

### `crates/sail-session/src/runtime.rs`

Reference code:

```rust
pub struct RuntimeEnvFactory {
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    global_file_listing_cache: Option<Arc<dyn ListFilesCache>>,
    global_file_statistics_cache: Option<Arc<dyn FileStatisticsCache>>,
    global_file_metadata_cache: Option<Arc<MokaFileMetadataCache>>,
}

impl RuntimeEnvFactory {
    pub fn new(config: Arc<AppConfig>, runtime: RuntimeHandle) -> Self {
        Self {
            config,
            runtime,
            global_file_listing_cache: None,
            global_file_statistics_cache: None,
            global_file_metadata_cache: None,
        }
    }

    pub fn create<M>(&mut self, mutator: M) -> Result<Arc<RuntimeEnv>>
    where
        M: FnOnce(RuntimeEnvBuilder) -> Result<RuntimeEnvBuilder>,
    {
        let registry = DynamicObjectStoreRegistry::new(self.runtime.clone());
        let cache_config = CacheManagerConfig::default()
            .with_files_statistics_cache(Some(self.create_file_statistics_cache()))
            .with_list_files_cache(Some(self.create_file_listing_cache()))
            .with_file_metadata_cache(Some(self.create_file_metadata_cache()));
        let builder = RuntimeEnvBuilder::default()
            .with_object_store_registry(Arc::new(registry))
            .with_cache_manager(cache_config)
            .with_memory_pool(self.create_memory_pool())
            .with_disk_manager_builder(self.create_disk_manager_builder());
        let builder = mutator(builder)?;
        Ok(Arc::new(builder.build()?))
    }

    fn create_memory_pool(&self) -> Arc<dyn MemoryPool> {
        match self.config.runtime.memory_pool {
            MemoryPoolConfig::Unbounded => Arc::new(UnboundedMemoryPool::default()),
            MemoryPoolConfig::Greedy(GreedyMemoryPoolConfig { max_size }) => {
                Arc::new(GreedyMemoryPool::new(max_size))
            }
            MemoryPoolConfig::Fair(FairMemoryPoolConfig { max_size }) => {
                Arc::new(FairSpillPool::new(max_size))
            }
        }
    }

    fn create_disk_manager_builder(&self) -> DiskManagerBuilder {
        let max_size = self.config.runtime.temporary_files.max_size;
        let paths = self.config.runtime.temporary_files.paths.as_slice();

        let mut builder = DiskManager::builder();
        builder.set_max_temp_directory_size(max_size as u64);
        if max_size == 0 {
            builder.set_mode(DiskManagerMode::Disabled);
        } else if paths.is_empty() {
            builder.set_mode(DiskManagerMode::OsTmpDirectory);
        } else {
            let paths = paths.iter().map(|x| x.into()).collect();
            builder.set_mode(DiskManagerMode::Directories(paths));
        }
        builder
    }
}
```

Detailed logic:

`RuntimeEnvFactory` creates DataFusion `RuntimeEnv` values from Sail
configuration. The runtime environment controls object store resolution, file
metadata caches, file listing caches, file statistics caches, memory pools, and
temporary disk management.

The `create` method constructs a `DynamicObjectStoreRegistry`, a
`CacheManagerConfig`, a memory pool, and a disk manager builder. It accepts a
mutator callback so callers can make final adjustments before the
`RuntimeEnvBuilder` is built. This allows session factories to share the common
runtime policy while still customizing the environment for a specific session
kind.

**The mutator callback pattern** (`FnOnce(RuntimeEnvBuilder) -> Result<RuntimeEnvBuilder>`)
allows callers to inject session-specific customizations without modifying the
factory. For example, a server session factory might add session-specific object
store credentials, while a worker session factory might configure different
memory limits. The factory handles the common setup (registry, caches, pools),
and the mutator applies final adjustments. This pattern avoids proliferating
factory variants for every combination of customizations.

Memory behavior is selected from `AppConfig.runtime.memory_pool`. Sail can use
DataFusion's unbounded pool, greedy bounded pool, or fair spill pool. Temporary
file behavior is selected from configured paths and maximum size: a max size of
zero disables the disk manager, no paths use the OS temporary directory, and
configured paths become explicit spill directories.

The cache helpers implement Sail's cache scoping rules. A cache can be disabled,
global across sessions, or session-scoped. Global caches are stored inside the
factory and reused, while session caches are created fresh for each runtime
environment. This matters for file-heavy workloads because listing, metadata,
and statistics reuse can significantly change planning and scan cost.

**Global vs session-scoped cache tradeoffs:**

- **Global caches** reduce redundant I/O when multiple sessions access the same
  files. They improve performance for shared datasets but require careful
  invalidation when underlying data changes. Memory usage accumulates across all
  sessions.
- **Session-scoped caches** isolate sessions from each other. Cache entries are
  always fresh within a session's lifetime, but repeated queries across sessions
  re-fetch the same metadata. Memory is released when sessions end.
- **Disabled caches** ensure every access fetches fresh data. This is correct
  for rapidly changing data but incurs significant I/O overhead for large file
  sets.

The choice depends on workload characteristics: long-running sessions with
stable data benefit from global caches, while short sessions with frequently
updated data may prefer session-scoped or disabled caches.

### `crates/sail-session/src/session_manager/*`

The session manager owns session lifecycle. It tracks active sessions, creates
new sessions through a session factory, returns existing contexts for repeated
requests, handles deletion, and records history when sessions stop.

The actor implementation serializes lifecycle transitions. That avoids races
between concurrent requests that try to create, use, delete, or observe the same
session. The session state model distinguishes running sessions from deleting,
deleted, and failed sessions, which lets server code report meaningful behavior
for requests that arrive during shutdown or after idle timeout cleanup.

When adding new per-session cleanup behavior, the session manager is the place
to ensure it happens exactly once. When adding new per-session runtime state, the
session factory usually creates it and the manager controls its lifetime.

## Plan Model And SQL Conversion

### `crates/sail-sql-parser/src/lib.rs`

Reference code:

```rust
#[macro_use]
mod keywords {
    include!(concat!(env!("OUT_DIR"), "/keywords.rs"));
}

pub mod ast;
mod combinator;
pub mod common;
mod container;
pub mod lexer;
pub mod location;
pub mod options;
pub mod parser;
pub mod span;
pub mod string;
pub mod token;
pub mod tree;
mod utils;
```

Detailed logic:

The SQL parser crate root defines the parser module layout and includes generated
keyword definitions. Keywords are generated at build time and included before
other modules so parser modules can use the keyword macros.

The crate separates syntax concerns into AST definitions, lexer, parser,
tokens, spans, strings, and tree utilities. The output of this crate is a
syntactic AST: it captures the structure of SQL text, but it does not resolve
catalog objects, validate functions, or produce DataFusion plans.

### `crates/sail-sql-analyzer/src/parser.rs`

Reference code:

```rust
macro_rules! parse {
    ($input:ident, $parser:ident $(,)?) => {{
        let options = ParserOptions::default();
        let length = $input.len();
        let lexer = create_lexer::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(&options);
        let tokens = lexer
            .parse($input)
            .into_result()
            .map_err(SqlError::parser)?;
        let tokens = tokens
            .as_slice()
            .map((length..length).into(), map_parser_input);
        let parser = $parser::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(&options);
        parser.parse(tokens).into_result().map_err(SqlError::parser)
    }};
}

pub fn parse_data_type(s: &str) -> SqlResult<DataType> {
    parse!(s, create_data_type_parser)
}

pub fn parse_expression(s: &str) -> SqlResult<Expr> {
    parse!(s, create_expression_parser)
}

pub fn parse_statements(s: &str) -> SqlResult<Vec<Statement>> {
    parse!(s, create_parser)
}

pub fn parse_one_statement(s: &str) -> SqlResult<Statement> {
    let mut plan = parse_statements(s)?;
    match (plan.pop(), plan.is_empty()) {
        (Some(x), true) => Ok(x),
        _ => Err(SqlError::invalid("expected one statement")),
    }
}
```

Detailed logic:

This file provides the public parsing helpers used by higher layers. It uses
`sail_sql_parser::lexer::create_lexer` to tokenize SQL input, then feeds those
tokens into the appropriate Chumsky parser from `sail_sql_parser::parser`.

The `parse!` macro is the shared implementation for SQL-backed parsers. It
creates default parser options, lexes the input, maps token spans into the shape
expected by Chumsky, invokes a parser factory, and converts parser failures into
`SqlError`. Public helpers such as `parse_statements`, `parse_one_statement`,
`parse_expression`, `parse_data_type`, and `parse_named_expression` are thin
wrappers around that macro.

The file also includes simple literal parsers for date, timestamp, time, and
interval values. These use `parse_simple!` because they parse direct literal
strings rather than full tokenized SQL statements. Tests at the bottom verify
that parsing and unparsing preserve the expected normalized tree text.

### `crates/sail-sql-analyzer/src/statement.rs`

Reference code excerpt:

```rust
/// Converts a parsed SQL AST statement into a spec plan (either a query or a command).
pub fn from_ast_statement(statement: Statement) -> SqlResult<spec::Plan> {
    match statement {
        Statement::Query(query) => {
            let plan = from_ast_query(query)?;
            Ok(spec::Plan::Query(plan))
        }
        Statement::SetCatalog {
            set: _,
            catalog: _,
            name,
        } => {
            let name = match name {
                Either::Left(x) => x.value,
                Either::Right(x) => from_ast_string(x)?,
            };
            let node = spec::CommandNode::SetCurrentCatalog {
                catalog: name.into(),
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::UseDatabase {
            r#use: _,
            database: _,
            name,
        } => {
            let node = spec::CommandNode::SetCurrentDatabase {
                database: from_ast_object_name(name)?,
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        Statement::CreateDatabase {
            create: _,
            database: _,
            name,
            if_not_exists,
            clauses,
        } => {
            let CreateDatabaseClauses {
                comment,
                location,
                properties,
            } = clauses.try_into()?;
            let node = spec::CommandNode::CreateDatabase {
                database: from_ast_object_name(name)?,
                definition: spec::DatabaseDefinition {
                    if_not_exists: if_not_exists.is_some(),
                    comment: comment.map(from_ast_string).transpose()?,
                    location: location.map(from_ast_string).transpose()?,
                    properties: properties
                        .map(from_ast_property_list)
                        .transpose()?
                        .unwrap_or_default(),
                },
            };
            Ok(spec::Plan::Command(spec::CommandPlan::new(node)))
        }
        // Additional SQL statement variants continue here in the source file.
        // They follow the same pattern: convert AST data into spec command or query nodes.
        // ...
    }
}
```

Detailed logic:

`statement.rs` converts top-level SQL AST statements into Sail's shared
semantic plan model. Query statements become `spec::Plan::Query`, while DDL,
DML, and utility statements become `spec::Plan::Command`.

This file is the bridge between SQL syntax and Sail's protocol-independent plan
representation. Spark Connect requests and SQL strings can both become
`spec::Plan`, which means downstream planning does not need to care whether a
plan originated from SQL text or Spark protobuf relations.

When adding support for a new SQL statement, this is usually where the top-level
dispatch belongs. The implementation should delegate expression, query, and data
type details to the corresponding analyzer modules instead of embedding all
conversion logic in the statement layer.

### `crates/sail-sql-analyzer/src/query.rs`

Reference code:

```rust
pub(crate) fn from_ast_query(query: Query) -> SqlResult<spec::QueryPlan> {
    let Query {
        with,
        body,
        modifiers,
    } = query;

    let plan = from_ast_query_body(*body)?;

    let QueryModifiers {
        sort_by,
        order_by,
        cluster_by,
        distribute_by,
        offset,
        limit,
        window: _,
    } = modifiers.try_into()?;

    if cluster_by.is_some() {
        return Err(SqlError::todo("CLUSTER BY"));
    }

    if distribute_by.is_some() {
        return Err(SqlError::todo("DISTRIBUTE BY"));
    }

    let plan = if let Some(items) = sort_by {
        let sort_by = items
            .into_iter()
            .map(from_ast_order_by)
            .collect::<SqlResult<_>>()?;
        spec::QueryPlan::new(spec::QueryNode::Sort {
            input: Box::new(plan),
            order: sort_by,
            is_global: false,
        })
    } else {
        plan
    };

    let plan = if let Some(items) = order_by {
        let order_by = items
            .into_iter()
            .map(from_ast_order_by)
            .collect::<SqlResult<_>>()?;
        spec::QueryPlan::new(spec::QueryNode::Sort {
            input: Box::new(plan),
            order: order_by,
            is_global: true,
        })
    } else {
        plan
    };

    let limit = match limit {
        None => None,
        Some(LimitValue::All(_)) => None,
        Some(LimitValue::Value(value)) => Some(*value),
    };

    let plan = match (offset, limit) {
        (None, None) => plan,
        (offset, limit) => {
            let offset = offset.map(from_ast_expression).transpose()?;
            let limit = limit.map(from_ast_expression).transpose()?;
            spec::QueryPlan::new(spec::QueryNode::Limit {
                input: Box::new(plan),
                skip: offset,
                limit,
            })
        }
    };

    if let Some(WithClause {
        with: _,
        recursive,
        ctes,
    }) = with
    {
        let ctes = from_ast_with(ctes)?;
        Ok(spec::QueryPlan::new(spec::QueryNode::WithCtes {
            input: Box::new(plan),
            recursive: recursive.is_some(),
            ctes,
        }))
    } else {
        Ok(plan)
    }
}
```

Detailed logic:

`query.rs` converts SQL query AST nodes into `spec::QueryPlan` and
`spec::QueryNode` values. It handles the relational shape of SQL: SELECT lists,
FROM items, joins, filters, grouping, ordering, limits, set operations, CTEs,
and related query constructs.

The output is still not a DataFusion logical plan. It is a Sail intermediate
representation that keeps query intent independent from the final execution
engine. That separation lets Spark Connect relation conversion and SQL analysis
share the same downstream resolver.

The most important design point in this file is preserving unresolved intent.
Column names, function names, catalog references, and some type choices are
represented in `spec` form and resolved later by `sail-plan`, where a
`SessionContext` and catalog manager are available.

**What "preserving unresolved intent" means in practice:**

- A column reference like `t.id` becomes `spec::UnresolvedAttribute { name: ["t", "id"] }`
  rather than a resolved column with known type and position
- A function call like `COUNT(*)` becomes `spec::UnresolvedFunction { function_name: "count", ... }`
  rather than a bound aggregate function
- A table reference like `catalog.db.table` becomes `spec::ObjectName` rather
  than a resolved table scan
- Expressions keep their syntactic form (e.g., `BETWEEN` stays as a between
  expression rather than being lowered to `AND` comparisons)

This preservation is important because resolution requires context that the SQL
analyzer does not have: the current default catalog, registered functions, table
schemas, and Spark-specific configuration. By keeping these unresolved, the
analyzer remains pure and testable, and resolution errors surface in one place.

### `crates/sail-sql-analyzer/src/expression.rs`

Reference code excerpt:

```rust
pub fn from_ast_expression(expr: Expr) -> SqlResult<spec::Expr> {
    match expr {
        Expr::Atom(atom) => from_ast_atom_expression(atom),
        Expr::UnaryOperator(op, expr) => {
            Ok(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare(from_ast_unary_operator(op)?),
                arguments: vec![from_ast_expression(*expr)?],
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                is_internal: None,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            }))
        }
        Expr::BinaryOperator(left, op, right) => {
            let op = from_ast_binary_operator(op)?;
            Ok(spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name: spec::ObjectName::bare(op),
                arguments: vec![from_ast_expression(*left)?, from_ast_expression(*right)?],
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                is_internal: None,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            }))
        }
        Expr::Wildcard(expr, _, _) => {
            let expr = from_ast_expression(*expr)?;
            match expr {
                spec::Expr::UnresolvedAttribute {
                    name,
                    plan_id: None,
                    is_metadata_column: false,
                } => Ok(spec::Expr::UnresolvedStar {
                    target: Some(name),
                    plan_id: None,
                    wildcard_options: Default::default(),
                }),
                _ => Err(SqlError::invalid("wildcard qualifier")),
            }
        }
        Expr::Cast(expr, _, data_type) => {
            let expr = from_ast_expression(*expr)?;
            Ok(spec::Expr::Cast {
                expr: Box::new(expr),
                cast_to_type: from_ast_data_type(data_type)?,
                rename: false,
                is_try: false,
            })
        }
        // Additional expression variants continue here in the source file.
        // They convert SQL expression AST nodes into unresolved spec::Expr nodes.
        // ...
    }
}
```

Detailed logic:

`expression.rs` converts SQL expression AST nodes into `spec::Expr`. It covers
literals, attributes, aliases, function calls, casts, operators, predicates,
sort expressions, lambdas, subqueries, and other expression-level constructs.

Expression conversion intentionally stays close to the semantic shape of the
input. It records what the user asked for but does not perform final function
lookup or type coercion. Those operations require session configuration,
registered functions, and DataFusion planning context, so they belong in the
plan resolver.

This file is commonly involved when adding SQL compatibility for a Spark
expression. The matching DataFrame or Spark Connect expression path should also
produce the same `spec::Expr` shape so behavior stays consistent across entry
points.

### `crates/sail-common/src/spec/*`

The `spec` module is Sail's shared intermediate plan model. It defines the
common data types, literals, expressions, query plans, and command plans used
between protocol parsing and DataFusion planning.

`spec::Plan` is the top-level enum. Query plans contain `QueryNode` variants for
relational operations such as reads, projects, filters, joins, aggregates,
limits, repartitions, local relations, streaming operations, pandas operations,
and statistics operations. Command plans contain catalog commands, writes,
explain commands, function registration, merge, delete, and other non-query
operations.

This module is important because it is the compatibility contract between
frontends and the resolver. Spark Connect conversion, SQL analysis, and internal
commands should produce `spec` values. `sail-plan` consumes those values and is
responsible for resolving them against the active session.

**Why does Sail use the `spec` intermediate model?**

The `spec` model exists for five key architectural reasons:

1. **Protocol independence**: Both Spark Connect requests and SQL strings
   convert to `spec::Plan`. This means the resolver only needs one implementation
   path regardless of input protocol. Adding a new protocol (like Arrow Flight
   SQL queries) only requires implementing the conversion to `spec`, not
   duplicating resolver logic.

2. **Deferred resolution**: The `spec` model captures user intent without
   requiring a live session. Table names remain as `spec::ObjectName`, column
   references remain as `spec::UnresolvedAttribute`, and function calls remain as
   `spec::UnresolvedFunction`. This separation allows parsing and semantic
   analysis to happen before catalog lookup, which simplifies error handling and
   enables analysis caching.

3. **Serialization and inspection**: `spec` types are designed to be
   serializable and inspectable. This makes debugging easier because you can
   print or log the `spec::Plan` before resolution. It also enables test fixtures
   that operate on `spec` plans without needing a full session context.

4. **Normalization**: Different SQL dialects and Spark Connect representations
   can express the same operation differently. The `spec` model normalizes these
   variations into a canonical form. For example, both `WHERE x > 5` and a
   DataFrame filter call become the same `spec::QueryNode::Filter`. This reduces
   the number of cases the resolver must handle.

5. **Spark-specific extensibility**: The `spec` model can represent
   Spark-specific constructs that DataFusion's native types do not support, such
   as pandas UDFs, Spark streaming operations, or Spark-specific aggregate
   semantics. These are represented in `spec` and converted to Sail extension
   nodes during resolution.

**What stays unresolved in `spec`?**

- **Table names**: `spec::ObjectName` holds the multi-part name (catalog,
  database, table) without catalog lookup
- **Column references**: `spec::UnresolvedAttribute` holds the column name
  without schema validation
- **Function calls**: `spec::UnresolvedFunction` holds the function name without
  registry lookup
- **Data types**: Some types remain in Spark DDL form until resolution maps them
  to Arrow types
- **Subquery references**: Subqueries are represented structurally without
  correlation analysis

## Logical And Physical Planning

### `crates/sail-plan/src/resolver/mod.rs`

Reference code:

```rust
pub struct PlanResolver<'a> {
    ctx: &'a SessionContext,
    config: Arc<PlanConfig>,
}

impl<'a> PlanResolver<'a> {
    pub fn new(ctx: &'a SessionContext, config: Arc<PlanConfig>) -> Self {
        Self { ctx, config }
    }
}
```

Detailed logic:

`PlanResolver` owns conversion from Sail's `spec` model into DataFusion logical
plans. It holds a borrowed DataFusion `SessionContext` and a shared `PlanConfig`.
The session gives access to catalog state, registered functions, table formats,
and DataFusion configuration. The config carries Spark/Sail planning behavior
that affects resolution.

The resolver is split into modules for commands, constraints, data types,
expressions, functions, literals, queries, schemas, state, and tree rewrites.
That split mirrors the fact that resolution is not a single pass over one kind
of object. It must resolve table references, function calls, column references,
types, hidden fields, generated fields, catalog commands, writes, and extension
nodes.

**`PlanResolverState` role:** The state object tracks context accumulated during
resolution. Key responsibilities include:

- **Field mapping**: Maps internal resolved field names to user-facing names,
  enabling correct schema restoration
- **Outer query schema**: Tracks schemas from outer queries for correlated
  subquery resolution
- **Aggregate state**: Tracks which expressions are inside aggregate contexts,
  affecting column reference validation
- **CTE definitions**: Stores Common Table Expression definitions for reference
  during plan resolution
- **Subquery references**: Tracks subquery correlation for proper scoping
- **Hidden field tracking**: Records which fields are internal (hidden from
  users) vs user-visible

### `crates/sail-plan/src/resolver/plan.rs`

Reference code:

```rust
#[derive(Debug)]
pub struct NamedPlan {
    pub plan: LogicalPlan,
    /// The user-facing fields for query plan,
    /// or `None` for a non-query plan (e.g. a DDL statement).
    pub fields: Option<Vec<String>>,
}

impl PlanResolver<'_> {
    /// Resolves a plan into a named plan.
    pub async fn resolve_named_plan(&self, plan: spec::Plan) -> PlanResult<NamedPlan> {
        let mut state = PlanResolverState::new();
        match plan {
            spec::Plan::Query(query) => {
                let plan = self.resolve_query_plan(query, &mut state).await?;
                let fields = Some(Self::get_field_names(plan.schema(), &state)?);
                Ok(NamedPlan { plan, fields })
            }
            spec::Plan::Command(command) => {
                let plan = self.resolve_command_plan(command, &mut state).await?;
                Ok(NamedPlan { plan, fields: None })
            }
        }
    }
}
```

Detailed logic:

`resolve_named_plan` is the top-level resolver entry point. It creates a fresh
`PlanResolverState`, dispatches on `spec::Plan::Query` versus
`spec::Plan::Command`, and returns a `NamedPlan`.

For query plans, it calls `resolve_query_plan`, then computes user-facing field
names from the resolver state. For command plans, it calls
`resolve_command_plan` and returns no query field list. The distinction matters
because Sail often uses internal resolved field names during planning, then
restores Spark-compatible names at protocol boundaries.

`NamedPlan` therefore carries two pieces of information: the DataFusion
`LogicalPlan` that can be optimized and executed, and the optional field names
that should be exposed to clients. Analysis handlers use those names to return
Spark-compatible schemas.

### `crates/sail-plan/src/resolver/query/mod.rs`

Reference code:

```rust
impl PlanResolver<'_> {
    /// Resolve query plan.
    /// No hidden fields are kept in the resolved plan.
    #[async_recursion]
    pub(super) async fn resolve_query_plan(
        &self,
        plan: spec::QueryPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let plan = self
            .resolve_query_plan_with_hidden_fields(plan, state)
            .await?;
        self.remove_hidden_fields(plan, state)
    }

    /// Resolve query plan.
    /// The resolved plan may contain hidden fields.
    /// If the hidden fields cannot be handled,
    /// [`Self::resolve_query_plan`] should be used instead,
    #[async_recursion]
    async fn resolve_query_plan_with_hidden_fields(
        &self,
        plan: spec::QueryPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::QueryNode;

        let plan_id = plan.plan_id;
        let plan = match plan.node {
            QueryNode::Read {
                read_type,
                is_streaming: _,
            } => match read_type {
                spec::ReadType::NamedTable(table) => {
                    self.resolve_query_read_named_table(*table, state).await?
                }
                spec::ReadType::Udtf(udtf) => self.resolve_query_read_udtf(*udtf, state).await?,
                spec::ReadType::DataSource(source) => {
                    self.resolve_query_read_data_source(*source, state).await?
                }
                spec::ReadType::DynamicTable(table) => {
                    self.resolve_query_read_dynamic_table(*table, state).await?
                }
            },
            QueryNode::Project { input, expressions } => {
                self.resolve_query_project(input.map(|x| *x), expressions, state)
                    .await?
            }
            QueryNode::Filter { input, condition } => {
                self.resolve_query_filter(*input, condition, state).await?
            }
            QueryNode::Join(join) => self.resolve_query_join(join, state).await?,
            QueryNode::SetOperation(op) => self.resolve_query_set_operation(op, state).await?,
            QueryNode::Sort {
                input,
                order,
                is_global,
            } => {
                self.resolve_query_sort(*input, order, is_global, state)
                    .await?
            }
            QueryNode::Limit { input, skip, limit } => {
                self.resolve_query_limit(*input, skip, limit, state).await?
            }
            QueryNode::Aggregate(aggregate) => {
                self.resolve_query_aggregate(aggregate, state).await?
            }
            // Additional QueryNode variants continue here in the source file.
            // Each arm delegates to a focused resolver such as pivoting,
            // repartition, UDF, UDTF, statistics, NA handling, or lateral joins.
            // ...
        };
        self.verify_query_plan(&plan, state)?;
        self.register_schema_with_plan_id(&plan, plan_id, state)?;
        Ok(plan)
    }

    fn remove_hidden_fields(
        &self,
        plan: LogicalPlan,
        state: &PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut columns = vec![];
        let mut has_hidden_columns = false;
        for column in plan.schema().columns() {
            let info = state.get_field_info(column.name())?;
            if info.is_hidden() {
                has_hidden_columns = true;
            } else {
                columns.push(column);
            }
        }
        if has_hidden_columns {
            let plan = LogicalPlanBuilder::new(plan)
                .project(columns.into_iter().map(Expr::Column))?
                .build()?;
            Ok(plan)
        } else {
            Ok(plan)
        }
    }
}
```

Detailed logic:

This file is the main query-node dispatch table. `resolve_query_plan` removes
hidden fields after resolving, while `resolve_query_plan_with_hidden_fields`
recursively matches each `spec::QueryNode` variant and delegates to focused
modules such as `read`, `project`, `filter`, `join`, `aggregate`, `sort`,
`limit`, `repartition`, `pivoting`, `udf`, `udtf`, and streaming-related
modules.

After each query node is resolved, the file verifies that all fields in the
resulting DataFusion schema are known to the resolver state. This catches
resolver bugs where a new field was added without registering its resolved name.
It also registers plan IDs with resolved fields when the input `spec::QueryPlan`
has a plan ID. That mapping supports later expression resolution against Spark
plan identifiers.

Hidden field handling is a central piece of Spark compatibility. Some planning
steps need internal columns to express operations such as grouping, generators,
or metadata tracking. `remove_hidden_fields` projects them away from the final
user-visible plan unless a caller intentionally uses the hidden-field-aware path.

**What are hidden fields and why are they needed?**

Hidden fields are internal columns that Sail adds to plans during resolution but
that should not appear in user-facing output. They serve three main purposes:

1. **Outer join filtering**: When resolving outer joins, Sail may need to track
   which side of the join produced a row. Hidden columns carry this information
   through the plan without exposing it to users.

2. **Generator output tracking**: Operations like `EXPLODE` or `LATERAL VIEW`
   produce multiple output rows from single input rows. Hidden columns track
   generator state and position information needed for correct semantics.

3. **Metadata columns**: Some operations need row-level metadata (file paths,
   row indices, partition values) for correct execution. These are added as
   hidden columns during resolution and used by physical operators.

The pattern is: add hidden columns during resolution when internal tracking is
needed, propagate them through the plan, use them in physical execution, and
strip them from the final output with `remove_hidden_fields`.

### `crates/sail-plan/src/resolver/expression/*`

The expression resolver modules convert `spec::Expr` into DataFusion
`datafusion_expr::Expr`. They resolve attributes against the current plan state,
map functions to registered scalar, aggregate, window, or table functions,
perform casts, handle predicates, expand wildcards, process lambdas, and build
window expressions.

This is where unresolved names become concrete DataFusion expressions. It is
also where Spark-specific expression semantics are normalized before DataFusion
optimization sees the plan. If a query resolves to the wrong column or function,
these modules and `PlanResolverState` are usually the first places to inspect.

### `crates/sail-plan/src/resolver/command/*`

The command resolver modules convert `spec::CommandPlan` into executable
DataFusion logical plans, often by creating Sail logical extension nodes. They
cover catalog operations, inserts, writes, streaming writes, delete, merge,
explain, show commands, functions, and variables.

Commands differ from queries because many of them are executed for side effects:
creating catalog objects, writing files, altering table properties, registering
functions, or starting streams. The resolver still returns a logical plan so the
normal planning and job runner path can execute the command and return a
compatible result stream.

### `crates/sail-session/src/planner.rs`

Reference code:

```rust
#[derive(Debug)]
pub struct ExtensionQueryPlanner {}

#[async_trait]
impl QueryPlanner for ExtensionQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let rewriters: Vec<Box<dyn LogicalRewriter>> = vec![];
        let mut logical_plan = logical_plan.clone();
        for rewriter in rewriters {
            logical_plan = rewriter.rewrite(logical_plan)?.data
        }
        let mut extension_planners = new_lakehouse_extension_planners();
        extension_planners.push(Arc::new(SystemTablePhysicalPlanner));
        extension_planners.push(Arc::new(ExtensionPhysicalPlanner));
        let planner = DefaultPhysicalPlanner::with_extension_planners(extension_planners);
        planner
            .create_physical_plan(&logical_plan, session_state)
            .await
    }
}

pub struct ExtensionPhysicalPlanner;

#[async_trait]
impl ExtensionPlanner for ExtensionPhysicalPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> datafusion_common::Result<Option<Arc<dyn ExecutionPlan>>> {
        let plan: Arc<dyn ExecutionPlan> = if let Some(node) =
            node.as_any().downcast_ref::<RangeNode>()
        {
            let schema = UserDefinedLogicalNode::schema(node).inner().clone();
            let projection = (0..schema.fields().len()).collect();
            Arc::new(RangeExec::try_new(
                node.range().clone(),
                node.num_partitions(),
                schema,
                projection,
            )?)
        } else if let Some(node) = node.as_any().downcast_ref::<ShowStringNode>() {
            let [input] = physical_inputs else {
                return internal_err!("ShowStringExec requires exactly one physical input");
            };
            Arc::new(ShowStringExec::new(
                input.clone(),
                node.names().to_vec(),
                node.limit(),
                node.format().clone(),
                UserDefinedLogicalNode::schema(node).inner().clone(),
            ))
        } else if let Some(node) = node.as_any().downcast_ref::<FileWriteNode>() {
            let [logical_input] = logical_inputs else {
                return internal_err!("FileWriteNode requires exactly one logical input");
            };
            let [physical_input] = physical_inputs else {
                return internal_err!("FileWriteNode requires exactly one physical input");
            };
            create_file_write_physical_plan(
                session_state,
                planner,
                logical_input,
                physical_input.clone(),
                node.options().clone(),
            )
            .await?
        } else if let Some(node) = node.as_any().downcast_ref::<CatalogCommandNode>() {
            let schema = node.schema().inner().clone();
            Arc::new(CatalogCommandExec::new(node.command().clone(), schema))
        } else {
            return internal_err!("unsupported logical extension node: {:?}", node);
        };
        Ok(Some(plan))
    }
}

pub fn new_query_planner() -> Arc<dyn QueryPlanner + Send + Sync> {
    Arc::new(ExtensionQueryPlanner {})
}
```

Detailed logic:

This file plugs Sail's custom logical nodes into DataFusion physical planning.
`new_query_planner` returns an `ExtensionQueryPlanner`, which DataFusion uses
when it needs to create a physical plan for a logical plan.

`ExtensionQueryPlanner::create_physical_plan` prepares extension planners and
then delegates to DataFusion's `DefaultPhysicalPlanner`. Sail registers
lakehouse extension planners, the system table planner, and
`ExtensionPhysicalPlanner`. This means DataFusion handles standard relational
operators, while Sail handles logical extension nodes that DataFusion does not
know about.

**Extension planner chain order matters:**

1. **Lakehouse planners** (`new_lakehouse_extension_planners()`) run first.
   These handle Delta Lake and Iceberg-specific logical nodes like row-level
   write operations. They must run first because lakehouse operations often need
   format-specific planning that the generic planner cannot provide.

2. **System table planner** (`SystemTablePhysicalPlanner`) runs next. It handles
   system catalog tables that expose Sail runtime metadata. These are separate
   from external data sources.

3. **Generic extension planner** (`ExtensionPhysicalPlanner`) runs last as the
   catch-all for Sail's other extension nodes (range, show-string, map-partitions,
   catalog commands, etc.).

Each planner returns `Some(plan)` if it handles the node, or `None` to pass to
the next planner. If no planner handles a node, DataFusion raises an error.

`ExtensionPhysicalPlanner::plan_extension` is the main mapping from Sail logical
extension nodes to physical execution nodes. Examples include `RangeNode` to
`RangeExec`, `ShowStringNode` to `ShowStringExec`, `MapPartitionsNode` to
`MapPartitionsExec`, `MonotonicIdNode` to `MonotonicIdExec`,
`SparkPartitionIdNode` to `SparkPartitionIdExec`, file write and delete nodes to
format-aware physical plans, streaming nodes to streaming exec operators,
catalog command nodes to `CatalogCommandExec`, and barrier nodes to
`BarrierExec`.

This file is one of the most important integration points in Sail. If a new
logical extension node is added under `sail-logical-plan`, it needs a physical
planning case here or in another registered extension planner. Otherwise
DataFusion will not know how to execute it.

### `crates/sail-logical-plan/src/lib.rs`

The logical plan crate defines Sail-specific DataFusion logical extension nodes.
These nodes represent Spark-compatible operations that are not standard
DataFusion logical operators or that require Sail-specific execution behavior.

Examples include range generation, show-string output, map-partitions execution,
monotonic ID generation, Spark partition ID generation, explicit repartitioning,
file writes, file deletes, schema pivoting, barriers, merge planning, and
streaming source/filter/limit/collector nodes. Each node carries the logical
metadata needed by the physical planner to build the corresponding execution
operator.

**Why does Sail need its own extension nodes?**

Sail cannot rely solely on DataFusion's built-in operators for several reasons:

- **Spark-specific semantics**: Operations like `RANGE` with specific partition
  counts, `monotonically_increasing_id()`, and `spark_partition_id()` have
  Spark-defined behavior that differs from or doesn't exist in DataFusion.

- **Format-aware writes**: File writes need to know about partitioning schemes,
  bucketing, sort requirements, and format-specific commit semantics. A generic
  "insert" operator cannot capture this.

- **Python UDF execution**: `MapPartitionsNode` represents Python UDF execution
  with Arrow batch semantics, which requires custom execution behavior.

- **Streaming operations**: Spark Structured Streaming has specific source,
  filter, and collector semantics that don't map to DataFusion's batch model.

- **Catalog commands**: Operations like CREATE TABLE, ALTER TABLE, and DROP
  TABLE need to execute Sail-specific catalog logic, not just return data.

By defining these as extension nodes, Sail can participate in DataFusion's
optimization framework while still controlling execution behavior.

### `crates/sail-physical-plan/src/lib.rs`

The physical plan crate defines Sail-specific `ExecutionPlan` implementations.
These are the physical counterparts to many logical extension nodes. They run
inside DataFusion's execution engine but implement Spark/Sail behavior that is
not available as a built-in DataFusion operator.

The crate includes physical operators for catalog commands, file writes, file
deletes, map partitions, range generation, repartitioning, schema pivoting,
show-string formatting, monotonic IDs, Spark partition IDs, barriers, and
streaming operations. When debugging runtime behavior for one of those
operations, inspect the logical node, its mapping in `sail-session/src/planner.rs`,
and then the matching physical exec implementation.

### `crates/sail-logical-optimizer/src/lib.rs`

This crate contains Sail logical optimizer rules. DataFusion still provides the
main logical optimization framework, but Sail can add custom rewrites where Spark
compatibility or Sail extension nodes require behavior outside DataFusion's
defaults.

The logical optimizer is intentionally smaller than the resolver and physical
planner. Most semantic lowering happens before this point, and many performance
rules happen in DataFusion or the physical optimizer.

### `crates/sail-physical-optimizer/src/lib.rs`

This crate configures physical optimization rules applied after DataFusion
physical planning. It combines DataFusion rules with Sail-specific rules for
distributed execution and Spark-compatible physical behavior.

The physical optimizer is where execution-plan shape is adjusted for runtime
requirements such as repartitioning, distribution, sorting, join strategy,
projection pushdown, limit pushdown, aggregate optimization, and Sail-specific
cluster constraints. If a query produces a valid logical plan but poor or
incorrect physical execution behavior, this crate is a key place to inspect.

### `crates/sail-physical-optimizer/src/join_reorder/*`

The join reorder module contains cost-based join reordering logic. It evaluates
join graphs and selects a physical join order that should reduce execution cost.

This module matters for complex SQL workloads because Spark compatibility is not
only about syntax; query plans must also be optimized well enough to be
practical. Join reordering is one of the places where Sail influences physical
plan quality beyond direct semantic translation.

## Execution

### `crates/sail-execution/src/lib.rs`

The execution crate root defines the distributed execution subsystem. It exposes
driver, worker, task, stream, job graph, job runner, worker manager, RPC, and
codec modules. It also re-exports `run_worker`, which is the worker process
entry point used by the CLI/server layer.

The key architectural split is between job submission and task execution.
Client-facing sessions submit physical plans through a `JobRunner`. Local
execution runs the plan in the same process. Cluster execution sends the plan to
a driver actor, which decomposes it into stages and tasks and coordinates worker
actors.

### `crates/sail-execution/src/job_runner.rs`

Reference code:

```rust
pub struct LocalJobRunner {
    next_job_id: AtomicU64,
    stopped: AtomicBool,
}

impl LocalJobRunner {
    pub fn new() -> Self {
        Self {
            next_job_id: AtomicU64::new(1),
            stopped: AtomicBool::new(false),
        }
    }
}

#[tonic::async_trait]
impl JobRunner for LocalJobRunner {
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        if self.stopped.load(Ordering::Relaxed) {
            return internal_err!("job runner is stopped");
        }
        let job_id = self.next_job_id.fetch_add(1, Ordering::Relaxed);
        let options = TracingExecOptions {
            metrics: global_metrics(),
            job_id: Some(job_id),
            stage: None,
            attempt: None,
            operator_id: None,
        };
        let plan = trace_execution_plan(plan, options)?;
        Ok(execute_stream(plan, ctx.task_ctx())?)
    }

    async fn stop(&self, history: oneshot::Sender<JobRunnerHistory>) {
        self.stopped.store(true, Ordering::Relaxed);
        let _ = history.send(JobRunnerHistory {
            jobs: vec![],
            stages: vec![],
            tasks: vec![],
            workers: vec![],
        });
    }
}

pub struct ClusterJobRunner {
    driver: ActorHandle<DriverActor>,
}

impl ClusterJobRunner {
    pub fn new(system: &mut ActorSystem, options: DriverOptions) -> Self {
        let driver = system.spawn(options);
        Self { driver }
    }
}

#[tonic::async_trait]
impl JobRunner for ClusterJobRunner {
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let (tx, rx) = oneshot::channel();
        self.driver
            .send(DriverEvent::ExecuteJob {
                plan,
                context: ctx.task_ctx(),
                result: tx,
            })
            .await
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        rx.await
            .map_err(|e| internal_datafusion_err!("failed to create job stream: {e}"))?
            .map_err(|e| internal_datafusion_err!("{e}"))
    }

    async fn stop(&self, history: oneshot::Sender<JobRunnerHistory>) {
        let _ = self
            .driver
            .send(DriverEvent::Shutdown {
                history: Some(history),
            })
            .await;
    }
}
```

Detailed logic:

`LocalJobRunner` and `ClusterJobRunner` implement the shared
`sail_common_datafusion::session::job::JobRunner` trait. This trait is the
boundary between planning and execution.

`LocalJobRunner::execute` checks whether the runner has stopped, allocates a job
ID, wraps the physical plan with telemetry using `trace_execution_plan`, and
calls DataFusion's `execute_stream(plan, ctx.task_ctx())`. The result is a
record batch stream produced in the same process using DataFusion's normal task
context.

**What does `trace_execution_plan` do?** This function wraps the physical plan
tree with instrumentation nodes that collect execution metrics. Each operator in
the plan gets wrapped with tracing that records job ID, stage (if applicable),
attempt number, and operator ID. These metrics flow to Sail's telemetry system,
enabling observability into execution time, row counts, and resource usage at
the operator level. The instrumentation is lightweight and designed to have
minimal overhead on execution.

`ClusterJobRunner::execute` sends `DriverEvent::ExecuteJob` to the driver actor
with the physical plan and DataFusion task context. It waits on a oneshot
channel for the driver to return a stream. From the caller's perspective, local
and cluster execution both return `SendableRecordBatchStream`; the difference is
hidden behind the job runner.

**How cluster execution hides distribution from callers:** The `JobRunner` trait
provides a uniform interface: give it a physical plan, get back a record batch
stream. Protocol handlers like `handle_execute_plan` don't need to know whether
execution is local or distributed. The cluster job runner internally coordinates
with the driver to:

1. Decompose the plan into stages at shuffle boundaries
2. Schedule tasks across workers
3. Manage intermediate data exchange between stages
4. Collect final results into a stream

The caller just sees the final stream, as if the query ran locally. This
abstraction allows Sail to switch between local and cluster mode based on
configuration without changing request handling code.

Both runners implement `stop`. Local stop flips an atomic flag and returns empty
history. Cluster stop asks the driver actor to shut down and optionally return
job history. This makes the session manager able to stop either execution mode
through the same job service abstraction.

### `crates/sail-execution/src/driver/actor/*`

The driver actor is the control-plane coordinator for cluster execution. It
receives events such as execute-job, observe-state, and shutdown. For a job
execution event, it coordinates job graph construction, scheduling, task
assignment, stream management, worker pool interaction, and result stream
creation.

The actor structure keeps driver state mutation serialized. That is important
because the driver tracks job lifecycle, stage lifecycle, task attempts, worker
availability, output streams, and shutdown state. Handling these through actor
messages avoids scattered locking and makes state transitions easier to reason
about.

**Why use the actor model for driver and workers?**

1. **No scattered locking**: All state mutations happen in response to messages.
   The actor processes one message at a time, eliminating races without requiring
   fine-grained locks throughout the codebase.

2. **Clear message protocol**: The set of events an actor handles (e.g.,
   `ExecuteJob`, `TaskComplete`, `WorkerRegistered`, `Shutdown`) forms an
   explicit protocol. This makes the system's behavior easier to understand and
   test.

3. **Spawned tasks with isolation**: Long-running operations (like waiting for
   workers or streaming results) can spawn background tasks that communicate
   back via messages. The actor remains responsive to other events.

4. **Graceful shutdown**: The actor can process a shutdown message, clean up
   in-flight work, report final state, and exit cleanly. This is harder to
   coordinate with scattered async code and shared state.

### `crates/sail-execution/src/driver/job_scheduler/*`

The job scheduler converts a physical plan into an executable job structure. It
uses the job graph representation to identify stages, dependencies, and
execution topology. Scheduler state records which jobs and stages are pending,
running, complete, failed, or waiting on dependencies.

This module is responsible for deciding when work is ready. It does not run
tasks itself; instead, it determines the next schedulable units and cooperates
with the task assigner and worker pool.

**Job graph vs job scheduler: the distinction**

- **Job graph** (`job_graph/*`) is a **data structure**. It represents the static
  decomposition of a physical plan into stages and their dependencies. Think of
  it as the "what": which stages exist, which depend on which, and what each
  stage's input/output requirements are.

- **Job scheduler** (`job_scheduler/*`) is a **state machine**. It tracks the
  dynamic execution progress through the job graph. Think of it as the "when":
  which stages are ready to run, which are blocked on dependencies, which have
  completed or failed.

The job graph is built once from the physical plan. The scheduler continuously
updates state as tasks complete, dependencies resolve, and workers report
status. This separation allows the same job graph structure to support
different scheduling policies.

### `crates/sail-execution/src/driver/task_assigner/*`

The task assigner maps schedulable tasks to available workers. It considers
worker state and task scheduling metadata, then emits work assignments that
worker actors can execute.

This module is the place to inspect when work is valid but not landing on
workers as expected. It owns the policy bridge between abstract scheduled tasks
and concrete worker placement.

### `crates/sail-execution/src/driver/worker_pool/*`

The worker pool tracks worker availability and worker lifecycle from the
driver's perspective. It observes worker registration, worker state changes, and
shutdown behavior, and gives the scheduler/assigner the current view of cluster
capacity.

Worker pool state is separate from task state. A task can be ready but blocked
if no suitable worker is available. Conversely, a worker can be available while
no stage has schedulable work. Keeping those concerns separate makes cluster
execution easier to debug.

### `crates/sail-execution/src/worker/actor/*`

The worker actor receives task execution events from the driver, runs the
assigned physical plan partitions through task runners, manages peer and stream
state, and reports task status back to the driver.

Like the driver, the worker uses an actor to serialize state transitions. Worker
state includes running tasks, peer connectivity, local streams, and shutdown
behavior. The worker actor is the cluster-side counterpart to the local
DataFusion execution path: instead of executing an entire job stream in one
process, it executes assigned partitions and cooperates with other workers for
shuffle and stream exchange.

### `crates/sail-execution/src/task_runner/*`

Task runners execute concrete task definitions on workers. They receive the
physical plan fragment, configure execution context, run the partition work, and
monitor completion or failure.

This layer is lower-level than the worker actor. The actor owns message handling
and worker state; the task runner owns the actual execution of a task once the
worker has accepted it.

### `crates/sail-execution/src/job_graph/*`

The job graph module represents the stage and dependency structure derived from
a physical plan. It is the execution planner's view of how a distributed job
should be decomposed.

Shuffle boundaries and stage inputs are important here. They determine which
parts of a plan can run independently, which outputs must be materialized or
streamed to other stages, and how downstream tasks discover their inputs.

### `crates/sail-execution/src/stream*`

The stream modules implement the data exchange layer used by distributed
execution. They cover stream representation, stream reading and writing, stream
access, stream management, and stream services.

The driver uses stream management to assemble job outputs for the client.
Workers use stream services to exchange partition data. This separation lets the
control plane schedule and observe work while the data plane moves Arrow record
batches between execution components.

### `crates/sail-execution/src/worker_manager/*`

Worker managers create and manage worker capacity. The local manager supports
workers on the local machine, while the Kubernetes manager supports cluster
deployment. Options modules define the configuration needed by each manager.

This layer sits below the driver. The driver needs workers; the worker manager
decides how those workers are provisioned and connected in a particular
deployment mode.

## Catalogs

### `crates/sail-catalog/src/provider/mod.rs`

Reference code:

```rust
#[async_trait::async_trait]
pub trait CatalogProvider: Send + Sync {
    fn get_name(&self) -> &str;

    async fn create_database(
        &self,
        database: &Namespace,
        options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus>;

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus>;

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>>;

    async fn drop_database(
        &self,
        database: &Namespace,
        options: DropDatabaseOptions,
    ) -> CatalogResult<()>;

    async fn create_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus>;

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus>;

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>>;

    async fn drop_table(
        &self,
        database: &Namespace,
        table: &str,
        options: DropTableOptions,
    ) -> CatalogResult<()>;

    async fn alter_table(
        &self,
        database: &Namespace,
        table: &str,
        options: AlterTableOptions,
    ) -> CatalogResult<()>;

    async fn create_view(
        &self,
        database: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<TableStatus>;

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus>;

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>>;

    async fn drop_view(
        &self,
        database: &Namespace,
        view: &str,
        options: DropViewOptions,
    ) -> CatalogResult<()>;
}
```

Detailed logic:

`CatalogProvider` is the trait every catalog backend implements. It defines
database, table, and view operations: create, get, list, drop, and alter where
appropriate. It also exposes `get_name` so a provider can report the name under
which it is registered in the session.

The trait uses `Namespace` for databases because Sail supports multi-level
database names. Table and view methods receive the namespace plus the object
name. This design keeps catalog backends independent from SQL or Spark naming
syntax; name parsing and default catalog/database behavior are handled by the
catalog manager.

Catalog backends include memory, system, Glue, Hive Metastore, Iceberg REST,
Unity Catalog, and OneLake. Each backend adapts its external API into this
common provider contract.

### `crates/sail-catalog/src/manager/mod.rs`

Reference code:

```rust
pub struct CatalogManager {
    state: Arc<Mutex<CatalogManagerState>>,
    pub(super) temporary_views: TemporaryViewManager,
    pub(super) tracker: CatalogObjectTracker,
}

pub(super) struct CatalogManagerState {
    pub(super) catalogs: HashMap<Arc<str>, Arc<dyn CatalogProvider>>,
    pub(super) functions: HashMap<Arc<str>, datafusion_expr::ScalarUDF>,
    pub(super) default_catalog: Arc<str>,
    pub(super) default_database: Namespace,
    pub(super) global_temporary_database: Namespace,
}

impl CatalogManagerState {
    pub fn resolve_database_reference<T: AsRef<str>>(
        &self,
        reference: &[T],
    ) -> CatalogResult<(Arc<str>, Namespace)> {
        match reference {
            [] => Err(CatalogError::InvalidArgument(
                "empty database reference".to_string(),
            )),
            [head, tail @ ..] if self.catalogs.contains_key(head.as_ref()) => {
                let catalog = head.as_ref().into();
                let database = tail.try_into()?;
                Ok((catalog, database))
            }
            x => {
                let catalog = self.default_catalog.clone();
                let database = x.try_into()?;
                Ok((catalog, database))
            }
        }
    }

    pub fn resolve_object_reference<T: AsRef<str>>(
        &self,
        reference: &[T],
    ) -> CatalogResult<(Arc<str>, Namespace, Arc<str>)> {
        match reference {
            [] => Err(CatalogError::InvalidArgument(
                "empty object reference".to_string(),
            )),
            [name] => {
                let table = name.as_ref().into();
                let catalog = self.default_catalog.clone();
                let database = self.default_database.clone();
                Ok((catalog, database, table))
            }
            [x @ .., last] => {
                let table = last.as_ref().into();
                let (catalog, database) = self.resolve_database_reference(x)?;
                Ok((catalog, database, table))
            }
        }
    }

    pub fn get_catalog(&self, catalog: &str) -> CatalogResult<Arc<dyn CatalogProvider>> {
        let Some(provider) = self.catalogs.get(catalog) else {
            return Err(CatalogError::NotFound(
                CatalogObject::Catalog,
                catalog.to_string(),
            ));
        };
        Ok(Arc::clone(provider))
    }
}
```

Detailed logic:

`CatalogManager` is the session-level catalog coordinator. It stores registered
catalog providers, scalar functions, the default catalog, the default database,
the global temporary database name, temporary views, and a tracker for temporary
objects such as functions and logical plans.

The most important logic is name resolution. `resolve_database_reference`,
`resolve_optional_database_reference`, and `resolve_object_reference` decide how
to interpret one-part, two-part, and multi-part names using the current default
catalog and database. For example, a one-part table name resolves into the
default catalog and default database, while a longer reference can explicitly
name a catalog if its first part matches a registered catalog.

The manager also tracks objects that cannot be represented directly in external
catalogs. `track_function` and `track_logical_plan` assign internal IDs to
runtime objects so plans can reference them later. This is useful for Spark
Connect flows where functions or relations are registered out of band and then
used by subsequent plans.

### `crates/sail-catalog/src/provider/cache.rs`

The caching provider decorates another `CatalogProvider` and caches catalog
metadata such as database lists, table lists, and view lists according to
configuration. It does not replace the backend provider; it wraps one to reduce
repeated remote catalog calls.

This pattern keeps cache policy separate from catalog protocol implementation.
Glue, HMS, Unity, and other providers can focus on their external APIs, while
the caching layer handles reuse and invalidation behavior common to all
providers.

### `crates/sail-catalog/src/provider/runtime.rs`

The runtime-aware provider decorates catalog providers that need controlled
runtime behavior. Catalog clients may perform blocking or async operations that
should run in a specific runtime context. This wrapper lets Sail isolate that
concern from the provider's catalog semantics.

Together with the caching provider, this is part of Sail's decorator pattern for
catalogs: session code can compose runtime handling and caching around the
actual backend implementation.

**The catalog decorator pattern explained:**

Sail uses the decorator pattern to layer cross-cutting concerns onto catalog
providers without modifying the providers themselves:

```
┌─────────────────────────────────────┐
│  RuntimeAwareCatalogProvider        │  ← Handles async runtime context
│  ┌───────────────────────────────┐  │
│  │  CachingCatalogProvider       │  │  ← Caches metadata to reduce I/O
│  │  ┌─────────────────────────┐  │  │
│  │  │  GlueCatalogProvider    │  │  │  ← Actual backend implementation
│  │  └─────────────────────────┘  │  │
│  └───────────────────────────────┘  │
└─────────────────────────────────────┘
```

**Why decorators instead of built-in features?**

- **Separation of concerns**: The Glue provider focuses only on Glue API
  translation. It doesn't need to know about caching TTLs or Tokio runtimes.
- **Composability**: Not all providers need all decorators. A memory catalog
  doesn't need caching. A synchronous provider doesn't need runtime wrapping.
- **Testability**: Each layer can be tested independently. You can test caching
  behavior with a mock provider, or test runtime handling without a real catalog.
- **Configuration flexibility**: Session setup can choose which decorators to
  apply based on configuration, without changing provider implementations.

### `crates/sail-catalog-memory/src/lib.rs`

The memory catalog is an in-process catalog implementation. It is primarily
useful for testing, local development, and sessions that do not need an external
metastore.

Because it implements the same `CatalogProvider` trait as remote catalogs, it
exercises the same resolver and catalog manager paths. This makes it valuable
for tests that need catalog behavior without external infrastructure.

### `crates/sail-catalog-system/src/lib.rs`

The system catalog exposes system tables. These tables are planned through the
system table physical planner registered in `sail-session/src/planner.rs`.

System tables are different from external data tables because their data comes
from Sail runtime state rather than files in object storage. The catalog
abstraction still lets them appear as tables to SQL and Spark clients.

### External catalog crates

The external catalog crates adapt specific metastore APIs to `CatalogProvider`.
`sail-catalog-glue` uses AWS Glue, `sail-catalog-hms` uses Hive Metastore and
Thrift, `sail-catalog-iceberg` uses the Iceberg REST catalog, `sail-catalog-unity`
uses Unity Catalog, and `sail-catalog-onelake` targets Microsoft OneLake.

These crates should not own general name resolution or plan semantics. They
translate provider trait calls into backend-specific requests and map backend
responses into Sail's common catalog status types.

## Data Sources And Table Formats

### `crates/sail-common-datafusion/src/datasource.rs`

Reference code:

```rust
#[derive(Debug, Clone)]
pub struct SourceInfo {
    pub paths: Vec<String>,
    pub schema: Option<Schema>,
    pub constraints: Constraints,
    pub partition_by: Vec<String>,
    pub bucket_by: Option<BucketBy>,
    pub sort_order: Vec<Sort>,
    pub options: Vec<OptionLayer>,
}

#[derive(Debug, Clone)]
pub struct SinkInfo {
    pub input: Arc<dyn ExecutionPlan>,
    pub mode: PhysicalSinkMode,
    pub partition_by: Vec<CatalogPartitionField>,
    pub bucket_by: Option<BucketBy>,
    pub sort_order: Option<LexRequirement>,
    pub options: Vec<OptionLayer>,
    pub logical_schema: Option<datafusion_common::DFSchemaRef>,
}

#[derive(Debug, Clone)]
pub struct RowLevelWriteInfo {
    pub command: RowLevelCommand,
    pub target: RowLevelTargetInfo,
    pub condition: Option<ExprWithSource>,
    pub expanded_input: Option<Arc<dyn ExecutionPlan>>,
    pub touched_file_plan: Option<Arc<dyn ExecutionPlan>>,
    pub deletion_vector_plan: Option<Arc<dyn ExecutionPlan>>,
    pub with_schema_evolution: bool,
    pub operation_override: Option<OperationOverride>,
    pub merge_strategy: MergeStrategy,
}

#[async_trait]
pub trait TableFormat: Send + Sync {
    fn name(&self) -> &str;

    async fn create_source(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>>;

    async fn infer_schema(&self, ctx: &dyn Session, info: SourceInfo) -> Result<SchemaRef> {
        Ok(self.create_source(ctx, info).await?.schema())
    }

    async fn create_writer(
        &self,
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    async fn create_row_level_writer(
        &self,
        ctx: &dyn Session,
        info: RowLevelWriteInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let _ = (ctx, info);
        not_impl_err!(
            "Row-level operations are not yet implemented for {} format",
            self.name()
        )
    }

    fn merge_strategy(&self) -> MergeStrategy {
        MergeStrategy::Eager
    }
}

#[derive(Default)]
pub struct TableFormatRegistry {
    formats: RwLock<HashMap<String, Arc<dyn TableFormat>>>,
}

impl TableFormatRegistry {
    pub fn register(&self, format: Arc<dyn TableFormat>) -> Result<()> {
        let mut formats = self
            .formats
            .write()
            .map_err(|_| plan_datafusion_err!("table format registry poisoned"))?;
        formats.insert(format.name().to_lowercase(), format);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Result<Arc<dyn TableFormat>> {
        let formats = self
            .formats
            .read()
            .map_err(|_| plan_datafusion_err!("table format registry poisoned"))?;
        formats
            .get(&name.to_lowercase())
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("No table format found for: {name}"))
    }
}
```

Detailed logic:

This file defines the shared DataFusion-facing data source and table format
interfaces. `SourceInfo` describes reads: paths, optional schema, constraints,
partitioning, bucketing, sort order, and layered options. `SinkInfo` describes
writes: input execution plan, sink mode, partitioning, bucketing, sort
requirements, options, and optional logical schema.

`OptionLayer` records where options came from and how they override each other.
Table properties, operation options, table location, and time travel settings
are represented as separate layers. This lets format implementations preserve
option precedence instead of flattening everything too early.

**`OptionLayer` precedence design:**

Options come from multiple sources with different priorities. `OptionLayer`
preserves this structure:

1. **Table properties** (lowest priority): Stored in catalog metadata, apply to
   all operations on the table
2. **Operation options**: Specified in the query (e.g., `OPTIONS (key = value)`),
   override table properties for this operation
3. **Table location**: The base path for the table, which may come from catalog
   or be specified directly
4. **Time travel**: Version or timestamp constraints that affect which snapshot
   to read

**Why preserve structure instead of flattening early?** Different format
implementations may need to know where an option came from. For example, a
format might treat a table-level compression setting differently from a
query-level override. By keeping layers separate, format code can make these
distinctions when needed.

Row-level operations use additional shared metadata. `RowLevelWriteInfo`
describes DELETE, UPDATE, and MERGE execution, including the target table,
condition, expanded input plans, touched-file plans, deletion-vector plans,
schema evolution flag, operation metadata overrides, and merge materialization
strategy. Internal columns such as `__sail_file_path`,
`__sail_file_row_index`, and `__sail_operation_type` carry row-level write
metadata through physical plans.

`TableFormat` is the main extension trait for file and lakehouse formats. It
requires a format name, read source creation, writer creation, and optionally
row-level writer creation, table property alteration, column type alteration,
and merge strategy. `TableFormatRegistry` stores registered implementations by
lowercase format name and is installed as a session extension.

### `crates/sail-data-source/src/lib.rs`

The data source crate root exposes format support, listing helpers, options,
URL resolution, and shared utilities. It is the home for built-in file and
stream data source implementations that are not full lakehouse table engines.

This crate works with the `TableFormat` abstraction from
`sail-common-datafusion`. The planner can ask the registry for a format, pass in
`SourceInfo` or `SinkInfo`, and receive a DataFusion table source or execution
plan without hard-coding every format in the resolver.

### `crates/sail-data-source/src/formats/mod.rs`

This module lists built-in data source format modules: Arrow, Avro, binary,
console, CSV, Delta, Iceberg, JSON, Parquet, Python, rate, socket, and text.

Some modules represent file formats, some represent streaming sources or sinks,
and some bridge to lakehouse crates. The module is intentionally a registry
surface: adding a built-in format means adding a format module and ensuring the
session format registration path can install it into `TableFormatRegistry`.

### `crates/sail-data-source/src/listing/mod.rs`

The listing module supports file discovery for path-based sources. It works with
the object store registry and DataFusion runtime caches to resolve input paths,
list files, and feed file metadata into scan planning.

This layer is important for file formats such as Parquet, CSV, JSON, Avro, text,
and binary. Lakehouse formats often have their own metadata logs or manifests,
but plain file formats need listing logic to determine which files belong to a
scan.

**Why lakehouse formats (Delta, Iceberg) differ from file formats (Parquet, CSV):**

Lakehouse formats are not just "Parquet with extra features." They have
fundamentally different requirements:

- **Metadata logs**: Lakehouse formats maintain transaction logs or metadata
  files that track which data files are part of the table. Reading requires
  parsing these logs, not listing directories.

- **ACID semantics**: Writes must be atomic and isolated. This requires
  coordinating file creation with log commits, handling concurrent writers, and
  supporting rollback on failure.

- **Schema evolution**: Lakehouse formats track schema changes over time and can
  read old data with new schemas (or vice versa). File formats have no built-in
  schema versioning.

- **Row-level operations**: DELETE, UPDATE, and MERGE require tracking which
  rows changed, potentially using deletion vectors or copy-on-write rewrites.
  File formats only support append or full-file replacement.

This is why Delta and Iceberg have dedicated crates (`sail-delta-lake`,
`sail-iceberg`) with datasource, operations, and kernel modules, rather than
being simple format plugins.

### `crates/sail-data-source/src/options/*`

The options modules define typed parsing for data source options. This avoids
spreading ad hoc string parsing across every format implementation.

Format implementations should use these typed options where possible, then fall
back to opaque option maps only for compatibility paths that have not yet moved
to typed configuration.

### `crates/sail-delta-lake/src/lib.rs`

The Delta Lake crate root exposes Delta-specific datasource, table, transaction,
log, schema, conversion, deletion vector, logical, physical, and operation
modules. It integrates Delta tables with Sail's shared table format and planning
interfaces.

Delta support is not just a scan format. It must read Delta logs, build
snapshots, enforce Delta schema and protocol rules, write data files, commit
transaction log entries, support table property changes, and implement
row-level operations such as DELETE, UPDATE, and MERGE where supported.

### `crates/sail-delta-lake/src/datasource/mod.rs`

The Delta datasource module creates DataFusion table sources for Delta reads. It
loads table metadata, interprets options such as location and time travel, and
constructs sources that can participate in projection pushdown, filter pushdown,
partition pruning, and file-level scan planning.

This module is part of the read path. If a query reads a Delta table from a
catalog or direct path, the planner eventually uses the Delta table format to
create a source through this module.

### `crates/sail-delta-lake/src/operations/*`

The operations modules implement Delta write and table mutation behavior. They
cover write planning, commit behavior, and row-level operation support.

For Delta, writes are more than object store file creation. A successful write
must also create a valid Delta transaction log commit. Row-level operations must
decide which files are rewritten or which deletion-vector artifacts are
produced, and they must commit metadata that readers can interpret later.

### `crates/sail-delta-lake/src/kernel/*`

The kernel modules handle Delta snapshot and transaction mechanics. Snapshot
logic determines the table state for a given version or timestamp. Transaction
logic prepares and commits changes while respecting Delta protocol expectations.

This is the layer to inspect when table state, time travel, or commit behavior
looks wrong. The datasource and operations layers depend on kernel state being
accurate.

### `crates/sail-iceberg/src/lib.rs`

The Iceberg crate root exposes Iceberg datasource, table, catalog spec,
manifest, metadata, partition, snapshot, physical, logical, and operation
modules. Like Delta, Iceberg is a lakehouse table implementation rather than a
simple file format.

Iceberg support must interpret table metadata, snapshots, manifests, partition
specs, schema evolution, and write/commit behavior. Sail integrates that through
the shared table format interfaces and lakehouse extension planners.

### `crates/sail-iceberg/src/datasource/mod.rs`

The Iceberg datasource module creates read sources for Iceberg tables. It uses
Iceberg table metadata and manifests to determine which data files belong to a
scan and how schemas, partitions, and snapshots should be interpreted.

This module is the Iceberg read-path counterpart to Delta's datasource module.
It is involved when a query reads an Iceberg table through a catalog or path.

### `crates/sail-iceberg/src/operations/*`

The Iceberg operations modules implement writes and table modifications. They
create data files, write or update Iceberg metadata, and integrate physical
write execution with Iceberg commit semantics.

When debugging write correctness for Iceberg, inspect the operation modules
together with physical plan commit modules and metadata spec modules. The
operation must produce both correct data files and correct Iceberg metadata.

### `crates/sail-plan-lakehouse/src/lib.rs`

This crate provides lakehouse-specific extension planners. It is registered by
`sail-session/src/planner.rs` before Sail's generic extension planner.

Lakehouse planning needs format-aware behavior for operations such as row-level
writes and table commits. Keeping that logic in a lakehouse planning crate
prevents the generic session planner from becoming the owner of all Delta and
Iceberg details.

## Object Storage

### `crates/sail-object-store/src/lib.rs`

The object store crate root exposes dynamic object store registration and store
layers. Sail uses this crate through DataFusion's object store registry
interface, allowing scans and writes to resolve URLs such as local files, S3,
Azure, GCS, HTTP, HDFS, and other supported schemes.

The important architectural idea is dynamic resolution. Sail does not require
every object store to be pre-registered manually for every bucket or authority.
Instead, the runtime environment installs a registry that can create stores on
demand from the URL being accessed.

### `crates/sail-object-store/src/layers/mod.rs`

Store layers wrap object store implementations with cross-cutting behavior.
Examples include runtime-aware access, logging, lazy initialization, or other
decorators that should apply consistently across backend stores.

This mirrors the catalog decorator pattern. Backend-specific object stores focus
on storage protocol behavior, while layers handle concerns that are common
across protocols.

### Dynamic object store registry

The dynamic registry is created in `RuntimeEnvFactory` and installed into
DataFusion's `RuntimeEnv`. When DataFusion needs to read or write a URL, the
registry parses the URL scheme and authority, checks whether a matching store
already exists, and creates one if necessary.

This design is especially important for multi-bucket and multi-cloud workloads.
A single session can discover stores lazily as plans reference new paths, while
the registry still caches stores so repeated access to the same authority does
not rebuild clients unnecessarily.

**How dynamic registration works:**

1. **URL parsing**: When a plan references a path like `s3://bucket-name/path/file.parquet`,
   the registry extracts the scheme (`s3`) and authority (`bucket-name`).

2. **Cache lookup**: The registry checks if a store for this scheme+authority
   combination already exists. If so, it returns the cached store.

3. **Lazy creation**: If no store exists, the registry creates one using
   configuration (credentials, endpoints, retry policies) and caches it for
   future use.

4. **Why this matters for multi-cloud/multi-bucket**: A single query might read
   from `s3://analytics-bucket/...` and write to `gs://archive-bucket/...`. The
   registry handles both transparently, creating appropriate clients for each
   cloud provider. Without dynamic registration, users would need to
   pre-configure every bucket they might access.

## Functions And Python Execution

### `crates/sail-function/src/lib.rs`

The function crate defines Sail's built-in scalar, aggregate, window, and table
functions. Submodules group scalar functions by domain such as strings,
datetime, arrays, maps, predicates, math, JSON, CSV, XML, URLs, variants, and
geospatial behavior.

Function registration is part of session setup. Expression resolution then maps
unresolved function names from SQL or Spark Connect into the registered
DataFusion function objects. When adding Spark-compatible function behavior, add
or update the function implementation here and ensure the resolver can find it
under the expected name.

**Function registration flow:**

- **Built-in functions**: Defined in `sail-function` crate, registered during
  session setup. The session factory calls registration helpers that add scalar,
  aggregate, and window functions to DataFusion's function registry. These are
  available immediately when the session starts.

- **Python UDFs**: Registered dynamically via Spark Connect or DataFrame API.
  When a Python UDF is registered, it's stored in `CatalogManager`. During
  resolution, `spec::UnresolvedFunction` nodes referencing UDFs are converted to
  `MapPartitionsNode` logical nodes, which become `MapPartitionsExec` physical
  operators that invoke Python.

- **SQL-defined functions**: Functions created via `CREATE FUNCTION` are stored
  in the catalog and resolved like table references. They may wrap expressions
  or reference external implementations.

### `crates/sail-python-udf/src/lib.rs`

The Python UDF crate supports Python user-defined functions and related Python
execution behavior. Its modules handle Python integration, cereal-based data
exchange, and UDF execution.

This crate is used when Spark plans contain Python UDF, UDAF, UDWF, or UDTF
behavior. It bridges between Arrow/DataFusion execution and Python callable
semantics, which is why the CLI entry point also has special embedded Python
handling.

### `crates/sail-python/src/lib.rs`

The Python bindings crate exposes Sail functionality to Python through PyO3. It
contains Spark and Flight binding modules so Python users can interact with Sail
through familiar client APIs.

This crate is separate from Python UDF execution. `sail-python` is about Python
bindings and client-facing integration; `sail-python-udf` is about executing
Python functions inside query plans.

## Telemetry And Server Infrastructure

### `crates/sail-server/src/lib.rs`

The server crate provides shared gRPC server infrastructure and actor support.
Spark Connect, Flight SQL, drivers, workers, and session managers use this
infrastructure instead of each service building its own server and actor
runtime patterns.

The most visible concept is `ServerBuilder`, which lets protocol crates add
services and then serve them with common options. The actor system types support
the driver, worker, and session manager components that need serialized message
handling.

### `crates/sail-telemetry/src/lib.rs`

The telemetry crate integrates tracing and metrics into execution. Job runners
use helpers such as `trace_execution_plan` and global metrics to annotate
physical plans with job, stage, attempt, and operator metadata.

Telemetry is attached close to execution so it can observe the actual physical
plan being run. This helps connect user-level jobs to DataFusion operator
metrics and distributed execution state.

## How A Query Moves Through Sail

A Spark Connect query starts in `plan_executor.rs` as a Spark protobuf relation.
The relation is converted into `spec::Plan`. `handle_execute_plan` resolves the
plan through `sail-plan`, producing a DataFusion physical plan through the
session planner and optimizers. The session's `JobService` chooses the local or
cluster job runner. The job runner returns a stream of Arrow record batches, and
`ExecutePlanResponseStream` converts those batches into Spark Connect responses.

**Detailed 8-step query flow:**

| Step | Component | Action |
|------|-----------|--------|
| 1. **gRPC reception** | `plan_executor.rs` | Receive Spark Connect `ExecutePlanRequest` |
| 2. **Protocol conversion** | Spark Connect converters | Convert protobuf `Relation` → `spec::Plan` |
| 3. **Resolution** | `PlanResolver` | Resolve tables, columns, functions → DataFusion `LogicalPlan` |
| 4. **Optimization** | DataFusion optimizer | Apply logical optimization rules |
| 5. **Physical planning** | `ExtensionQueryPlanner` | Convert logical plan → `ExecutionPlan` with Sail extensions |
| 6. **Schema renaming** | `NamedPlan` | Restore user-facing field names from internal names |
| 7. **Execution** | `JobRunner` | Execute locally or send to driver for cluster execution |
| 8. **Response streaming** | `ExecutePlanResponseStream` | Convert Arrow batches → Spark Connect responses |

A SQL query follows the same downstream path after parsing and analysis. The SQL
text is tokenized and parsed by `sail-sql-parser`, converted into `spec::Plan`
by `sail-sql-analyzer`, resolved by `sail-plan`, physically planned by the
session planner, and executed by the job runner.

Catalog reads enter the path when the resolver sees a named table. The
`CatalogManager` resolves the name to a catalog provider, namespace, and object
name. The provider returns table metadata. The resolver and table format
registry use that metadata to create a DataFusion table source or write plan.

Lakehouse reads and writes add format-specific work. Delta and Iceberg table
formats interpret table logs or metadata, build scan sources, create write
plans, and commit metadata changes. Row-level operations carry additional
metadata columns and operation state through physical plans so format writers
can rewrite or mark affected rows correctly.

Cluster execution adds the driver and worker layers. The cluster job runner
sends the physical plan to the driver actor. The driver builds a job graph,
schedules stages, assigns tasks to workers, and manages output streams. Workers
execute task fragments and exchange stream data as needed. The driver returns a
record batch stream to the same protocol layer that local execution uses.

## Extension Points

### Adding a catalog backend

Implement `CatalogProvider` for the backend, map external database/table/view
metadata into Sail status types, add configuration and factory wiring in the
session catalog setup, and decide whether the provider should be wrapped with
runtime-aware and caching decorators.

The provider should not implement Sail name resolution itself. It should accept
the namespace and object names passed by `CatalogManager`.

### Adding a table format

Implement `TableFormat`, including `name`, `create_source`, and `create_writer`.
Add `create_row_level_writer`, `merge_strategy`, table property alteration, or
column type alteration only when the format supports those operations. Register
the format in the session format setup so planners can find it by name.

The format implementation should consume `SourceInfo`, `SinkInfo`, and
`OptionLayer` rather than inventing a separate option precedence model.

### Adding a logical extension operator

Define the logical node in `sail-logical-plan`, add resolver logic that emits
the node from the relevant `spec` plan, add physical planning support in
`sail-session/src/planner.rs` or another registered extension planner, and
implement the execution operator in `sail-physical-plan`.

The resolver must register any new output fields with `PlanResolverState`.
Otherwise `resolve_query_plan` will fail verification because the resulting
schema contains fields the resolver does not know about.

### Adding execution behavior for cluster mode

Decide whether the change belongs in job graph planning, job scheduling, task
assignment, worker pool management, worker actor handling, task running, or
stream exchange. Local execution changes usually belong near DataFusion physical
operators or `LocalJobRunner`, while distributed-only behavior belongs in the
driver, worker, or stream modules.

Keep the `JobRunner` boundary intact when possible. Protocol handlers should not
need to know whether a plan is running locally or in a cluster.

## Debugging Guide By Symptom

| Symptom | Where to look |
|---------|---------------|
| SQL fails to parse | `sail-sql-analyzer/src/parser.rs`, `sail-sql-parser` modules |
| SQL parses but wrong semantic plan | `sail-sql-analyzer/src/statement.rs`, `query.rs`, `expression.rs` |
| Spark Connect converts incorrectly | Protobuf converters in `sail-spark-connect`, compare `spec::Plan` |
| Table/column not found | `CatalogManager` resolution, catalog provider, `PlanResolverState` |
| Table found but scans wrong data | Table format datasource module, object store registry |
| Logical plan valid but physical fails | `sail-session/src/planner.rs` extension planner cases |
| Physical plan valid but local exec fails | `sail-physical-plan` operator, `LocalJobRunner` |
| Fails only in cluster mode | Driver actor, job scheduler, task assigner, worker actor, streams |
| Wrong schema returned to client | `NamedPlan` field names, hidden field removal, `plan_analyzer.rs` |

**Additional debugging tips:**

- **Enable EXPLAIN output**: Use `EXPLAIN` or `EXPLAIN ANALYZE` to see the
  logical and physical plans. This shows where the plan diverges from
  expectations.

- **Check resolver state**: If column resolution fails, the issue is often in
  `PlanResolverState`. Check that field names are registered correctly and that
  plan IDs match when using Spark Connect plan references.

- **Inspect `spec::Plan`**: Since `spec` types are serializable, you can log
  or print the intermediate plan before resolution. This helps isolate whether
  the issue is in protocol conversion or resolution.

- **Compare SQL vs Spark Connect**: If an operation works via SQL but not Spark
  Connect (or vice versa), compare the `spec::Plan` produced by each path. They
  should be equivalent.

- **Cluster-specific debugging checklist**:
  - Is the physical plan serializable? (All operators must support serde)
  - Are workers receiving tasks? (Check driver actor logs)
  - Are streams connecting between workers? (Check worker actor logs)
  - Is the job graph correct? (Inspect stage dependencies and shuffle boundaries)

## Further Reading

- [Query Planning](./query-planning/) for more detail on plan optimization.
- [Benchmark Results](../introduction/benchmark-results/) for performance
  comparisons.
- [Configuration](../guide/configuration/) for runtime and deployment options.
- [Deployment](../guide/deployment/) for Docker and Kubernetes deployment.
