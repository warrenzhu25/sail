//! End-to-end tests for the `local-process-cluster` mode, where each worker runs as a
//! separate `sail worker` OS process launched from the Sail CLI binary.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use sail_common::config::{AppConfig, FixedRetryStrategy, RetryStrategy};
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::session::job::{JobRunner, JobRunnerHistory};
use sail_execution::driver::DriverOptions;
use sail_execution::job_runner::ClusterJobRunner;
use sail_execution::worker_manager::{ProcessWorkerManager, ProcessWorkerManagerOptions};
use sail_server::actor::ActorSystem;
use tokio::sync::oneshot;

fn test_config(shuffle_root: &Path) -> AppConfig {
    #[expect(clippy::unwrap_used)]
    let mut config = AppConfig::load().unwrap();
    config.cluster.worker_initial_count = 2;
    config.cluster.worker_max_count = 2;
    config.cluster.worker_heartbeat_interval_secs = 1;
    config.cluster.worker_heartbeat_timeout_secs = 5;
    config.cluster.shuffle_mode = "blocking".to_string();
    config.cluster.shuffle_dir = shuffle_root.to_string_lossy().to_string();
    config
}

fn temp_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "sail-{name}-{}-{:016x}",
        std::process::id(),
        rand_suffix()
    ))
}

fn rand_suffix() -> u64 {
    use std::hash::{BuildHasher, RandomState};
    RandomState::new().hash_one(std::time::SystemTime::now())
}

fn worker_manager() -> Arc<ProcessWorkerManager> {
    Arc::new(ProcessWorkerManager::new(ProcessWorkerManagerOptions {
        program: PathBuf::from(env!("CARGO_BIN_EXE_sail")),
        stop_grace_period: Duration::from_secs(10),
    }))
}

/// Builds a two-stage plan that hash-repartitions the values `0..partitions * batches * rows`.
fn build_plan(partitions: i32, batches: i32, rows: i32) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let mut data = vec![];
    for p in 0..partitions {
        let mut items = vec![];
        for b in 0..batches {
            let start = (p * batches + b) * rows;
            items.push(RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(start..start + rows))],
            )?);
        }
        data.push(items);
    }
    let source = MemorySourceConfig::try_new_exec(&data, schema, None)?;
    let shuffle: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
        source,
        Partitioning::Hash(vec![Arc::new(Column::new("x", 0))], 8),
    )?);
    Ok(Arc::new(CoalescePartitionsExec::new(shuffle)))
}

async fn collect_values(
    runner: &ClusterJobRunner,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Vec<i32>> {
    let session = SessionContext::new();
    let mut stream = runner.execute(&session, plan).await?;
    let mut values = vec![];
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        #[expect(clippy::unwrap_used)]
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        values.extend(col.values().iter().copied());
    }
    values.sort_unstable();
    Ok(values)
}

async fn stop(runner: ClusterJobRunner, mut system: ActorSystem) -> JobRunnerHistory {
    let (tx, rx) = oneshot::channel();
    runner.stop(tx).await;
    #[expect(clippy::unwrap_used)]
    let history = rx.await.unwrap();
    system.join().await;
    history
}

/// Returns the shuffle directories of the worker processes under the driver shuffle directory.
fn worker_dirs(driver_dir: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(driver_dir)
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.file_name().to_string_lossy().starts_with("worker_"))
                .map(|e| e.path())
                .collect()
        })
        .unwrap_or_default()
}

fn runtime() -> RuntimeHandle {
    let handle = tokio::runtime::Handle::current();
    RuntimeHandle::new(handle.clone(), handle)
}

/// Runs a job with blocking shuffle on worker processes, and verifies that each worker
/// process uses its own shuffle directory under the driver shuffle directory,
/// and that all worker processes and shuffle files are gone after the driver stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_process_cluster_blocking_shuffle() -> Result<()> {
    let root = temp_root("process-cluster");
    let mut config = test_config(&root);
    // Use few task slots so that both workers run map tasks and write shuffle files.
    config.cluster.worker_task_slots = 2;
    let manager = worker_manager();
    let options = DriverOptions::new(&config, runtime(), manager.clone());
    let driver_dir = options.shuffle_dir.clone();
    let mut system = ActorSystem::new();
    let runner = ClusterJobRunner::new(&mut system, options);

    let values = collect_values(&runner, build_plan(8, 4, 1024)?).await?;
    assert_eq!(values, (0..8 * 4 * 1024).collect::<Vec<_>>());

    let pids = manager.worker_pids().await;
    assert_eq!(pids.len(), 2, "expected two worker processes: {pids:?}");
    let dirs = worker_dirs(&driver_dir);
    assert_eq!(dirs.len(), 2, "expected two worker shuffle directories");
    for pid in pids.values() {
        assert!(
            dirs.iter().any(|d| d
                .file_name()
                .is_some_and(|n| n.to_string_lossy().contains(&format!("-{pid}-")))),
            "missing shuffle directory for worker process {pid}: {dirs:?}"
        );
    }

    let history = stop(runner, system).await;
    assert!(!history.tasks.is_empty());
    assert!(manager.worker_pids().await.is_empty());
    for pid in pids.values() {
        assert!(
            !Path::new(&format!("/proc/{pid}")).exists(),
            "worker process {pid} is still running"
        );
    }
    assert!(
        !driver_dir.exists(),
        "driver shuffle directory is not removed"
    );
    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

/// Kills a worker process with `SIGKILL` while the job is running, and verifies that
/// the driver detects the worker exit, recovers via task and stage retry,
/// and returns the complete result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_process_cluster_worker_killed() -> Result<()> {
    let root = temp_root("process-cluster-kill");
    let mut config = test_config(&root);
    config.cluster.worker_task_slots = 1;
    // Use a long heartbeat timeout so that the job can only finish in time if
    // the driver is notified of the worker process exit.
    config.cluster.worker_heartbeat_timeout_secs = 600;
    config.cluster.task_stream_creation_timeout_secs = 1;
    config.cluster.task_max_attempts = 1;
    config.cluster.adaptive_shuffle_enabled = false;
    config.cluster.rpc_retry_strategy = RetryStrategy::Fixed(FixedRetryStrategy {
        max_count: 1,
        delay_secs: 0,
    });
    let manager = worker_manager();
    let options = DriverOptions::new(&config, runtime(), manager.clone());
    let driver_dir = options.shuffle_dir.clone();
    let mut system = ActorSystem::new();
    let runner = ClusterJobRunner::new(&mut system, options);

    const PARTITIONS: i32 = 8;
    const BATCHES: i32 = 8;
    const ROWS: i32 = 32 * 1024;
    let plan = build_plan(PARTITIONS, BATCHES, ROWS)?;

    // Kill the first worker process that starts writing shuffle files.
    let m = manager.clone();
    let dir = driver_dir.clone();
    let killer = tokio::spawn(async move {
        for _ in 0..200_000 {
            for (_, pid) in m.worker_pids().await {
                let writing = worker_dirs(&dir).iter().any(|d| {
                    d.to_string_lossy().contains(&format!("-{pid}-"))
                        && std::fs::read_dir(d).is_ok_and(|mut x| x.next().is_some())
                });
                if writing {
                    let _ = std::process::Command::new("kill")
                        .args(["-9", &pid.to_string()])
                        .status();
                    return Some(pid);
                }
            }
            tokio::time::sleep(Duration::from_micros(100)).await;
        }
        None
    });

    #[expect(clippy::unwrap_used)]
    let values = tokio::time::timeout(Duration::from_secs(120), collect_values(&runner, plan))
        .await
        .unwrap()?;
    #[expect(clippy::unwrap_used)]
    let killed = killer.await.unwrap();
    assert!(killed.is_some(), "no worker process was killed");
    assert_eq!(values, (0..PARTITIONS * BATCHES * ROWS).collect::<Vec<_>>());

    let history = stop(runner, system).await;
    assert!(
        history.tasks.iter().any(|t| t.attempt > 0),
        "expected at least one task retry after the worker process was killed"
    );
    assert!(manager.worker_pids().await.is_empty());
    assert!(
        !driver_dir.exists(),
        "driver shuffle directory is not removed"
    );
    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}
