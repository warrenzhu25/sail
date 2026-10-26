mod core;
mod monitor;

use std::collections::HashMap;

use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use sail_common_datafusion::error::CommonErrorCause;
use tokio::sync::oneshot;

use crate::driver::{DriverEvent, TaskStatus};
use crate::id::TaskKey;
use crate::task::stats::TaskShuffleStats;
use crate::worker::WorkerEvent;

pub struct TaskRunner {
    signals: HashMap<TaskKey, oneshot::Sender<()>>,
    codec: Box<dyn PhysicalExtensionCodec>,
}

pub trait TaskRunnerMessage {
    fn report_task_status(
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
        shuffle_stats: Option<TaskShuffleStats>,
    ) -> Self;
}

impl TaskRunnerMessage for DriverEvent {
    fn report_task_status(
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
        shuffle_stats: Option<TaskShuffleStats>,
    ) -> Self {
        DriverEvent::UpdateTask {
            key,
            status,
            message,
            cause,
            shuffle_stats,
            sequence: None,
        }
    }
}

impl TaskRunnerMessage for WorkerEvent {
    fn report_task_status(
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
        shuffle_stats: Option<TaskShuffleStats>,
    ) -> Self {
        WorkerEvent::ReportTaskStatus {
            key,
            status,
            message,
            cause,
            shuffle_stats,
        }
    }
}
