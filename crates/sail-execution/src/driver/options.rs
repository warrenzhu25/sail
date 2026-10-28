use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_server::RetryStrategy;

use crate::stream_manager::shuffle_file::ShuffleCompression;
use crate::worker_manager::WorkerManager;

#[readonly::make]
pub struct DriverOptions {
    pub enable_tls: bool,
    pub driver_listen_host: String,
    pub driver_listen_port: u16,
    pub driver_external_host: String,
    pub driver_external_port: u16,
    pub worker_initial_count: usize,
    pub worker_max_count: usize,
    pub worker_task_slots: usize,
    pub worker_max_idle_time: Duration,
    pub worker_heartbeat_interval: Duration,
    pub worker_heartbeat_timeout: Duration,
    pub worker_launch_timeout: Duration,
    pub task_launch_timeout: Duration,
    pub task_stream_buffer: usize,
    pub task_stream_creation_timeout: Duration,
    pub task_max_attempts: usize,
    pub shuffle_mode: String,
    /// The shuffle directory of this driver instance.
    /// Local workers use subdirectories of this directory.
    pub shuffle_dir: PathBuf,
    /// The size of buffered shuffle output of a map task before it is spilled to disk.
    pub shuffle_spill_threshold: usize,
    /// Whether to sync the shuffle files to stable storage.
    pub shuffle_fsync: bool,
    pub shuffle_compression: ShuffleCompression,
    pub adaptive_shuffle_enabled: bool,
    pub adaptive_skew_join_enabled: bool,
    pub adaptive_broadcast_join_threshold_bytes: u64,
    pub adaptive_shuffle_target_partition_bytes: u64,
    pub rpc_retry_strategy: RetryStrategy,
    pub runtime: RuntimeHandle,
    pub worker_manager: Arc<dyn WorkerManager>,
}

impl DriverOptions {
    pub fn new(
        config: &AppConfig,
        runtime: RuntimeHandle,
        worker_manager: Arc<dyn WorkerManager>,
    ) -> Self {
        Self {
            enable_tls: config.cluster.enable_tls,
            driver_listen_host: config.cluster.driver_listen_host.clone(),
            driver_listen_port: config.cluster.driver_listen_port,
            driver_external_host: config.cluster.driver_external_host.clone(),
            driver_external_port: config.cluster.driver_external_port,
            worker_initial_count: config.cluster.worker_initial_count,
            worker_max_count: config.cluster.worker_max_count,
            worker_task_slots: config.cluster.worker_task_slots,
            worker_max_idle_time: Duration::from_secs(config.cluster.worker_max_idle_time_secs),
            worker_heartbeat_interval: Duration::from_secs(
                config.cluster.worker_heartbeat_interval_secs,
            ),
            worker_heartbeat_timeout: Duration::from_secs(
                config.cluster.worker_heartbeat_timeout_secs,
            ),
            worker_launch_timeout: Duration::from_secs(config.cluster.worker_launch_timeout_secs),
            rpc_retry_strategy: (&config.cluster.rpc_retry_strategy).into(),
            task_launch_timeout: Duration::from_secs(config.cluster.task_launch_timeout_secs),
            task_stream_buffer: config.cluster.task_stream_buffer,
            task_stream_creation_timeout: Duration::from_secs(
                config.cluster.task_stream_creation_timeout_secs,
            ),
            task_max_attempts: config.cluster.task_max_attempts,
            shuffle_mode: config.cluster.shuffle_mode.clone(),
            // Each driver instance gets a unique shuffle directory, so that multiple drivers
            // in the same process (e.g. one per session) or on the same host never share
            // (or delete) each other's shuffle files.
            shuffle_dir: shuffle_root_dir(&config.cluster.shuffle_dir)
                .join(crate::driver::shuffle_dir::driver_shuffle_dir_name()),
            shuffle_spill_threshold: config.cluster.shuffle_spill_threshold_bytes,
            shuffle_fsync: config.cluster.shuffle_fsync,
            shuffle_compression: ShuffleCompression::from_config(
                &config.cluster.shuffle_compression,
            ),
            adaptive_shuffle_enabled: config.cluster.adaptive_shuffle_enabled,
            adaptive_skew_join_enabled: config.cluster.adaptive_skew_join_enabled,
            adaptive_broadcast_join_threshold_bytes: config
                .cluster
                .adaptive_broadcast_join_threshold_bytes,
            adaptive_shuffle_target_partition_bytes: config
                .cluster
                .adaptive_shuffle_target_partition_bytes,
            runtime,
            worker_manager,
        }
    }
}

/// Returns the root directory for shuffle files given the configured value.
pub(crate) fn shuffle_root_dir(configured: &str) -> PathBuf {
    if configured.is_empty() {
        std::env::temp_dir().join("sail").join("shuffle")
    } else {
        PathBuf::from(configured)
    }
}
