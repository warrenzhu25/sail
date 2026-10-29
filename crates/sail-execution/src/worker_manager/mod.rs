mod env;
mod kubernetes;
mod local;
mod options;
mod process;

use futures::future::BoxFuture;
pub(crate) use options::WorkerLaunchOptions;
use sail_common::actor::ActorSystem;

use crate::error::ExecutionResult;
use crate::id::WorkerId;

#[tonic::async_trait]
pub trait WorkerManager: Send + Sync + 'static {
    /// Start launching a worker.
    ///
    /// Local workers are spawned into `system` immediately. The returned task launches an
    /// external worker when necessary.
    fn launch_worker(
        &self,
        system: &mut ActorSystem,
        id: WorkerId,
        options: WorkerLaunchOptions,
    ) -> BoxFuture<'static, ExecutionResult<()>>;

    /// Stop all workers on a best-effort basis.
    /// The driver have attempted to sent shutdown events to all workers
    /// at this point, but it is unknown whether the events have been received.
    /// The worker manager is supposed to wait for the termination of all workers
    /// before returning from this method.
    /// Note that the driver process may terminate abnormally before this method finishes.
    /// In such cases, the external systems should be responsible for cleaning up the workers
    /// (e.g. via the cascading deletion mechanism in Kubernetes).
    async fn stop(&self) -> ExecutionResult<()>;

    /// Returns a future that completes when a launched worker exits, with a description
    /// of how the worker exited. This allows the driver to react to worker loss immediately,
    /// instead of waiting for the worker heartbeat timeout.
    ///
    /// The future is created when the worker is launched, and it is polled only after the
    /// launch has succeeded. The future resolves to `None` if the worker manager cannot
    /// observe the exit of workers (the default) or if the worker is unknown.
    fn wait_for_worker_exit(&self, _id: WorkerId) -> BoxFuture<'static, Option<String>> {
        Box::pin(std::future::ready(None))
    }
}

pub use kubernetes::{KubernetesWorkerManager, KubernetesWorkerManagerOptions};
pub use local::LocalWorkerManager;
pub use process::{ProcessWorkerManager, ProcessWorkerManagerOptions};
