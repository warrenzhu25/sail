mod env;
mod kubernetes;
mod local;
mod options;
mod process;

pub(crate) use options::WorkerLaunchOptions;

use crate::error::ExecutionResult;
use crate::id::WorkerId;

#[tonic::async_trait]
pub trait WorkerManager: Send + Sync + 'static {
    /// Launch a worker.
    async fn launch_worker(
        &self,
        id: WorkerId,
        options: WorkerLaunchOptions,
    ) -> ExecutionResult<()>;

    /// Stop a single worker on a best-effort basis.
    /// The driver may have attempted to send a shutdown event to the worker
    /// at this point, so the worker manager should give the worker a chance to
    /// exit gracefully before forcefully terminating it.
    /// This is also called for workers that are considered lost, which may still be
    /// running but unresponsive.
    /// The default implementation does nothing and relies on the worker to exit on its own.
    async fn stop_worker(&self, _id: WorkerId) -> ExecutionResult<()> {
        Ok(())
    }

    /// Stop all workers on a best-effort basis.
    /// The driver have attempted to sent shutdown events to all workers
    /// at this point, but it is unknown whether the events have been received.
    /// The worker manager is supposed to wait for the termination of all workers
    /// before returning from this method.
    /// Note that the driver process may terminate abnormally before this method finishes.
    /// In such cases, the external systems should be responsible for cleaning up the workers
    /// (e.g. via the cascading deletion mechanism in Kubernetes).
    async fn stop(&self) -> ExecutionResult<()>;
}

pub use kubernetes::{KubernetesWorkerManager, KubernetesWorkerManagerOptions};
pub use local::LocalWorkerManager;
pub use process::{ProcessWorkerManager, ProcessWorkerManagerOptions};
