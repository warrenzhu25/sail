use std::collections::HashMap;

use datafusion::prelude::SessionContext;
use sail_common::runtime::RuntimeHandle;
use sail_server::actor::{ActorHandle, ActorSystem};
use tokio::sync::Mutex;

use crate::error::ExecutionResult;
use crate::id::WorkerId;
use crate::worker::{WorkerActor, WorkerOptions};
use crate::worker_manager::{WorkerLaunchOptions, WorkerManager};

struct LocalWorkerManagerState {
    system: ActorSystem,
    workers: HashMap<WorkerId, ActorHandle<WorkerActor>>,
}

impl LocalWorkerManagerState {
    fn new() -> Self {
        Self {
            system: ActorSystem::new(),
            workers: HashMap::new(),
        }
    }
}

pub struct LocalWorkerManager {
    state: Mutex<LocalWorkerManagerState>,
    runtime: RuntimeHandle,
    session: SessionContext,
}

impl LocalWorkerManager {
    pub fn new(runtime: RuntimeHandle, session: SessionContext) -> Self {
        Self {
            state: Mutex::new(LocalWorkerManagerState::new()),
            runtime,
            session,
        }
    }

    /// Stops a single worker in the local cluster.
    #[cfg(test)]
    pub async fn stop_worker(&self, id: WorkerId) -> ExecutionResult<()> {
        use crate::worker::WorkerEvent;
        let handle = {
            let mut state = self.state.lock().await;
            state.workers.remove(&id)
        };
        if let Some(handle) = handle {
            let _ = handle.send(WorkerEvent::Shutdown).await;
        }
        Ok(())
    }

    #[cfg(test)]
    pub async fn worker_ids(&self) -> Vec<WorkerId> {
        let state = self.state.lock().await;
        state.workers.keys().copied().collect()
    }
}

#[tonic::async_trait]
impl WorkerManager for LocalWorkerManager {
    async fn launch_worker(
        &self,
        id: WorkerId,
        options: WorkerLaunchOptions,
    ) -> ExecutionResult<()> {
        let options = WorkerOptions::local(id, options, self.runtime.clone(), self.session.clone());
        let mut state = self.state.lock().await;
        let handle = state.system.spawn(options);
        state.workers.insert(id, handle);
        Ok(())
    }

    async fn stop(&self) -> ExecutionResult<()> {
        let mut state = self.state.lock().await;
        state.system.join().await;
        Ok(())
    }
}
