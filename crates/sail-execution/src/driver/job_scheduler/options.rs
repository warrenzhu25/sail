//! A dedicated module for job scheduler options to ensure readonly access.

use std::time::Duration;

use crate::driver::DriverOptions;
use crate::driver::actor::AdaptiveCoalesceOptions;
use crate::shuffle::ShuffleBackendKind;

#[readonly::make]
pub struct JobSchedulerOptions {
    pub session_id: String,
    pub task_launch_timeout: Duration,
    pub task_max_attempts: usize,
    pub shuffle_backend: ShuffleBackendKind,
    pub adaptive_coalesce: Option<AdaptiveCoalesceOptions>,
}

impl From<&DriverOptions> for JobSchedulerOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            session_id: options.session_id.clone(),
            task_launch_timeout: options.task_launch_timeout,
            task_max_attempts: options.task_max_attempts,
            shuffle_backend: options.shuffle_backend.clone(),
            adaptive_coalesce: options.adaptive_coalesce,
        }
    }
}
