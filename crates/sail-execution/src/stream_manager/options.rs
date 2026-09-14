use std::path::PathBuf;
use std::time::Duration;

use crate::driver::DriverOptions;
use crate::worker::WorkerOptions;

#[readonly::make]
#[derive(Debug, Clone)]
pub struct StreamManagerOptions {
    pub task_stream_buffer: usize,
    pub task_stream_creation_timeout: Duration,
    pub shuffle_dir: PathBuf,
}

impl Default for StreamManagerOptions {
    fn default() -> Self {
        Self {
            task_stream_buffer: 16,
            task_stream_creation_timeout: Duration::from_secs(60),
            shuffle_dir: PathBuf::from(std::env::temp_dir()).join("sail").join("shuffle"),
        }
    }
}

impl From<&DriverOptions> for StreamManagerOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            task_stream_buffer: options.task_stream_buffer,
            task_stream_creation_timeout: options.task_stream_creation_timeout,
            shuffle_dir: PathBuf::from(std::env::temp_dir()).join("sail").join("shuffle"),
        }
    }
}

impl From<&WorkerOptions> for StreamManagerOptions {
    fn from(options: &WorkerOptions) -> Self {
        Self {
            task_stream_buffer: options.task_stream_buffer,
            task_stream_creation_timeout: options.task_stream_creation_timeout,
            shuffle_dir: PathBuf::from(std::env::temp_dir()).join("sail").join("shuffle"),
        }
    }
}
