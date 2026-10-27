//! Tests for stopping the sessions when the server shuts down.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::SessionConfig;
use futures::StreamExt;
use sail_common::config::{AppConfig, ExecutionMode};
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::JobService;
use sail_server::actor::ActorSystem;
use sail_session::session_factory::{
    ServerSessionFactory, ServerSessionInfo, ServerSessionMutator, SessionFactory,
};
use sail_session::session_manager::{SessionManager, SessionManagerOptions};

struct NoopMutator;

impl ServerSessionMutator for NoopMutator {
    fn mutate_config(
        &self,
        config: SessionConfig,
        _info: &ServerSessionInfo,
    ) -> Result<SessionConfig> {
        Ok(config)
    }

    fn mutate_state(
        &self,
        builder: SessionStateBuilder,
        _info: &ServerSessionInfo,
    ) -> Result<SessionStateBuilder> {
        Ok(builder)
    }

    fn mutate_runtime_env(
        &self,
        builder: RuntimeEnvBuilder,
        _info: &ServerSessionInfo,
    ) -> Result<RuntimeEnvBuilder> {
        Ok(builder)
    }
}

/// Builds a two-stage plan that hash-repartitions many rows.
fn build_plan() -> Result<Arc<dyn ExecutionPlan>> {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let rows = 1024;
    let data = (0..4)
        .map(|p| {
            (0..8)
                .map(|b| {
                    let start = (p * 8 + b) * rows;
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(Int32Array::from_iter_values(start..start + rows))],
                    )
                })
                .collect::<std::result::Result<Vec<_>, _>>()
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let source = MemorySourceConfig::try_new_exec(&data, schema, None)?;
    let shuffle: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        source,
        Partitioning::Hash(vec![Arc::new(Column::new("x", 0))], 8),
    )?);
    Ok(Arc::new(CoalescePartitionsExec::new(shuffle)))
}

fn count_files(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| {
            let path = e.path();
            if path.is_dir() {
                count_files(&path)
            } else {
                1
            }
        })
        .sum()
}

/// A job is still running when the server shuts down (its output has not been
/// fully consumed). Stopping the sessions stops the driver, which removes the
/// shuffle files of the job.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_stop_sessions_removes_shuffle_files() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize Python for Python data source registration when creating sessions.
    pyo3::Python::initialize();
    let root: PathBuf = std::env::temp_dir().join(format!(
        "sail-stop-sessions-{}-{:016x}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos() as u64
    ));
    let mut config = AppConfig::load()?;
    config.mode = ExecutionMode::LocalCluster;
    config.cluster.worker_initial_count = 1;
    config.cluster.worker_max_count = 1;
    config.cluster.shuffle_mode = "blocking".to_string();
    config.cluster.shuffle_dir = root.to_string_lossy().to_string();
    let config = Arc::new(config);
    let handle = tokio::runtime::Handle::current();
    let runtime = RuntimeHandle::new(handle.clone(), handle);
    let system = Arc::new(Mutex::new(ActorSystem::new()));
    let factory = {
        let config = config.clone();
        let runtime = runtime.clone();
        let system = system.clone();
        Box::new(move || {
            Box::new(ServerSessionFactory::new(
                config.clone(),
                runtime.clone(),
                system.clone(),
                Box::new(NoopMutator),
            )) as Box<dyn SessionFactory<ServerSessionInfo>>
        })
    };
    let manager = SessionManager::try_new(SessionManagerOptions::new(runtime, system, factory))?;
    let context = manager
        .get_or_create_session_context("s1".to_string(), "u1".to_string())
        .await?;
    let service = context.extension::<JobService>()?;
    let mut stream = service.runner().execute(&context, build_plan()?).await?;
    // Read one batch so that the map stage has completed and written shuffle files,
    // but leave the job running.
    assert!(stream.next().await.transpose()?.is_some());
    assert!(count_files(&root) > 0, "no shuffle files were written");

    manager.stop_sessions().await?;
    assert_eq!(count_files(&root), 0, "shuffle files are left behind");
    drop(stream);
    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}
