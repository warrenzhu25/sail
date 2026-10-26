use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use datafusion::common::{internal_datafusion_err, internal_err, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;
use sail_common_datafusion::session::job::{JobRunner, JobRunnerHistory};
use sail_common_datafusion::system::observable::{JobRunnerObserver, Observer, StateObservable};
use sail_server::actor::{ActorHandle, ActorSystem};
use sail_telemetry::telemetry::global_metrics;
use sail_telemetry::{trace_execution_plan, TracingExecOptions};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot;

use crate::driver::{DriverActor, DriverEvent, DriverOptions};

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

impl Default for LocalJobRunner {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl StateObservable<JobRunnerObserver> for LocalJobRunner {
    async fn observe(&self, observer: JobRunnerObserver) {
        observer.nothing()
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
impl StateObservable<JobRunnerObserver> for ClusterJobRunner {
    async fn observe(&self, observer: JobRunnerObserver) {
        let result = self
            .driver
            .send(DriverEvent::ObserveState { observer })
            .await;
        if let Err(SendError(DriverEvent::ObserveState { observer })) = result {
            observer.fail(internal_datafusion_err!(
                "failed to observe state for cluster job runner"
            ));
        }
    }
}

#[tonic::async_trait]
impl JobRunner for ClusterJobRunner {
    /// Executes a plan on the cluster. This is where the cool stuff happens.
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

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use std::sync::Arc;
    use std::time::Duration;

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{ExecutionPlan, Partitioning};
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;
    use sail_common::config::AppConfig;
    use sail_common::runtime::RuntimeHandle;
    use sail_common_datafusion::session::job::JobRunner;
    use sail_server::actor::ActorSystem;
    use tokio::sync::oneshot;

    use super::ClusterJobRunner;
    use crate::driver::DriverOptions;
    use crate::worker_manager::LocalWorkerManager;

    /// Runs a multi-stage job with blocking (disk) shuffle, kills one of the workers after
    /// the map stage completes (wiping that worker's shuffle files), and verifies that the
    /// driver detects the fetch failure, recomputes the lost map partitions on the surviving
    /// worker, and returns the complete result without failing the job.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_stage_retry_on_worker_loss() -> datafusion::common::Result<()> {
        let mut config = AppConfig::load().unwrap();
        config.cluster.worker_initial_count = 2;
        config.cluster.worker_max_count = 2;
        config.cluster.worker_task_slots = 1;
        config.cluster.worker_heartbeat_interval_secs = 1;
        config.cluster.worker_heartbeat_timeout_secs = 1;
        config.cluster.task_stream_creation_timeout_secs = 1;
        config.cluster.task_max_attempts = 1;
        config.cluster.shuffle_mode = "blocking".to_string();
        config.cluster.adaptive_shuffle_enabled = false;
        config.cluster.rpc_retry_strategy =
            sail_common::config::RetryStrategy::Fixed(sail_common::config::FixedRetryStrategy {
                max_count: 1,
                delay_secs: 0,
            });

        let handle = tokio::runtime::Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let session = SessionContext::new();
        let worker_manager = Arc::new(LocalWorkerManager::new(runtime.clone(), session.clone()));
        let options = DriverOptions::new(&config, runtime, worker_manager.clone());
        let mut system = ActorSystem::new();
        let runner = ClusterJobRunner::new(&mut system, options);

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let mut partitions = vec![];
        // Use enough data per partition so that the map stage takes long enough for the
        // worker to be stopped while the job is still running, even under parallel test load.
        const PARTITIONS: i32 = 8;
        const BATCHES: i32 = 8;
        const ROWS: i32 = 32 * 1024;
        for p in 0..PARTITIONS {
            let mut batches = vec![];
            for b in 0..BATCHES {
                let start = (p * BATCHES + b) * ROWS;
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int32Array::from_iter_values(start..start + ROWS))],
                )?;
                batches.push(batch);
            }
            partitions.push(batches);
        }
        let source = MemorySourceConfig::try_new_exec(&partitions, schema, None)?;
        let shuffle1: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            source,
            Partitioning::Hash(vec![Arc::new(Column::new("x", 0))], 8),
        )?);
        let shuffle2: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(shuffle1));

        let mut stream = runner.execute(&session, shuffle2).await?;

        // Wait until worker 1 has started writing stage 0 shuffle files, then stop it
        // immediately so that stage 0 / stage 1 must recover via stage retry.
        let wm = worker_manager.clone();
        let killer = tokio::spawn(async move {
            let dir = std::env::temp_dir()
                .join("sail")
                .join("shuffle")
                .join("worker_1")
                .join("1");
            let _ = std::fs::remove_dir_all(&dir);
            for _ in 0..100_000 {
                if dir.exists() {
                    let _ = wm.stop_worker(1.into()).await;
                    break;
                }
                tokio::time::sleep(Duration::from_micros(50)).await;
            }
        });

        let mut values = vec![];
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            values.extend(col.values().iter().copied());
        }
        let _ = killer.await;

        values.sort_unstable();
        assert_eq!(values, (0..PARTITIONS * BATCHES * ROWS).collect::<Vec<_>>());

        let (tx, rx) = oneshot::channel();
        runner.stop(tx).await;
        let history = rx.await.unwrap();
        system.join().await;

        // At least one task in stage 0 or stage 1 must have been retried (attempt > 0).
        assert!(
            history.tasks.iter().any(|t| t.attempt > 0),
            "expected at least one task retry after worker loss, count: {}",
            history.tasks.len()
        );
        Ok(())
    }
}
