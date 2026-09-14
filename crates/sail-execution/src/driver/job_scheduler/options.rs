//! A dedicated module for job scheduler options to ensure readonly access.

use std::path::PathBuf;
use std::time::Duration;

use crate::driver::DriverOptions;
use crate::job_graph::OutputMode;

#[readonly::make]
#[derive(Debug, Clone)]
pub struct JobSchedulerOptions {
    pub task_launch_timeout: Duration,
    pub task_max_attempts: usize,
    pub shuffle_mode: OutputMode,
    pub shuffle_dir: PathBuf,
    pub adaptive_enabled: bool,
    pub target_partition_size: u64,
    pub min_partition_size: u64,
    pub skew_factor: f64,
    pub min_skew_threshold: u64,
}

impl Default for JobSchedulerOptions {
    fn default() -> Self {
        Self {
            task_launch_timeout: Duration::from_secs(60),
            task_max_attempts: 3,
            shuffle_mode: OutputMode::Pipelined,
            shuffle_dir: std::env::temp_dir().join("sail").join("shuffle"),
            adaptive_enabled: true,
            target_partition_size: 64 * 1024 * 1024,
            min_partition_size: 1024 * 1024,
            skew_factor: 5.0,
            min_skew_threshold: 128 * 1024 * 1024,
        }
    }
}

#[expect(dead_code)]
impl JobSchedulerOptions {
    pub fn with_shuffle_mode(mut self, shuffle_mode: OutputMode) -> Self {
        self.shuffle_mode = shuffle_mode;
        self
    }

    pub fn with_shuffle_dir(mut self, shuffle_dir: PathBuf) -> Self {
        self.shuffle_dir = shuffle_dir;
        self
    }

    pub fn with_adaptive_enabled(mut self, adaptive_enabled: bool) -> Self {
        self.adaptive_enabled = adaptive_enabled;
        self
    }

    pub fn with_target_partition_size(mut self, target_partition_size: u64) -> Self {
        self.target_partition_size = target_partition_size;
        self
    }

    pub fn with_min_partition_size(mut self, min_partition_size: u64) -> Self {
        self.min_partition_size = min_partition_size;
        self
    }

    pub fn with_skew_factor(mut self, skew_factor: f64) -> Self {
        self.skew_factor = skew_factor;
        self
    }

    pub fn with_min_skew_threshold(mut self, min_skew_threshold: u64) -> Self {
        self.min_skew_threshold = min_skew_threshold;
        self
    }
}

impl From<&DriverOptions> for JobSchedulerOptions {
    fn from(options: &DriverOptions) -> Self {
        Self {
            task_launch_timeout: options.task_launch_timeout,
            task_max_attempts: options.task_max_attempts,
            shuffle_mode: OutputMode::Pipelined,
            shuffle_dir: std::env::temp_dir().join("sail").join("shuffle"),
            adaptive_enabled: true,
            target_partition_size: 64 * 1024 * 1024,
            min_partition_size: 1024 * 1024,
            skew_factor: 5.0,
            min_skew_threshold: 128 * 1024 * 1024,
        }
    }
}
