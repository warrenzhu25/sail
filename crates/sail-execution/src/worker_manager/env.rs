use std::env;

use fastrace::collector::SpanContext;
use sail_common::config::{ClusterConfigEnv, ExecutionConfigEnv};
use sail_common::telemetry::ContextPropagationEnv;
use sail_common::utils::retry::RetryStrategy;

use crate::id::WorkerId;
use crate::shuffle::ShuffleBackendKind;
use crate::worker_manager::WorkerLaunchOptions;

/// The settings for a standalone worker (the `sail worker` command) that depend on
/// how the worker is launched, rather than on the driver configuration.
pub(crate) struct WorkerEnvOptions<'a> {
    /// The host on which the worker listens.
    pub listen_host: &'a str,
    /// The host for other workers and the driver to connect to the worker.
    /// If the value is `None`, the external host is not set by this module,
    /// and the worker manager is responsible for setting it.
    pub external_host: Option<&'a str>,
    /// Whether to force the worker to listen on a random port assigned by the OS,
    /// which is needed when multiple workers run on the same host.
    /// The worker reports the actual port to the driver.
    pub random_port: bool,
}

/// Builds the environment variables to configure a standalone worker.
/// This is shared by all worker managers that launch the `sail worker` command,
/// so that workers are configured in the same way no matter where they run.
pub(crate) fn build_worker_env(
    id: WorkerId,
    options: WorkerLaunchOptions,
    env_options: WorkerEnvOptions<'_>,
) -> Vec<(String, String)> {
    let WorkerLaunchOptions {
        enable_tls,
        batch_size,
        driver_id,
        session_id,
        driver_external_host,
        driver_external_port,
        worker_heartbeat_interval,
        task_stream_buffer,
        task_stream_creation_timeout,
        rpc_retry_strategy,
        shuffle_backend,
    } = options;
    let WorkerEnvOptions {
        listen_host,
        external_host,
        random_port,
    } = env_options;
    let w3c_traceparent = SpanContext::current_local_parent().map(|x| x.encode_w3c_traceparent());

    let mut env = vec![
        (
            "RUST_LOG".to_string(),
            env::var("RUST_LOG").unwrap_or("info".to_string()),
        ),
        (
            ClusterConfigEnv::ENABLE_TLS.to_string(),
            enable_tls.to_string(),
        ),
        (
            ExecutionConfigEnv::BATCH_SIZE.to_string(),
            batch_size.to_string(),
        ),
        (
            ClusterConfigEnv::DRIVER_EXTERNAL_HOST.to_string(),
            driver_external_host,
        ),
        (
            ClusterConfigEnv::DRIVER_EXTERNAL_PORT.to_string(),
            driver_external_port.to_string(),
        ),
        (
            ClusterConfigEnv::DRIVER_ID.to_string(),
            u64::from(driver_id).to_string(),
        ),
        (ClusterConfigEnv::SESSION_ID.to_string(), session_id),
        (
            ClusterConfigEnv::WORKER_ID.to_string(),
            u64::from(id).to_string(),
        ),
        (
            ClusterConfigEnv::WORKER_LISTEN_HOST.to_string(),
            listen_host.to_string(),
        ),
        (
            ClusterConfigEnv::WORKER_HEARTBEAT_INTERVAL_SECS.to_string(),
            worker_heartbeat_interval.as_secs().to_string(),
        ),
        (
            ClusterConfigEnv::TASK_STREAM_BUFFER.to_string(),
            task_stream_buffer.to_string(),
        ),
        (
            ClusterConfigEnv::TASK_STREAM_CREATION_TIMEOUT_SECS.to_string(),
            task_stream_creation_timeout.as_secs().to_string(),
        ),
        (
            ClusterConfigEnv::RPC_RETRY_STRATEGY.to_string(),
            format_retry_strategy(&rpc_retry_strategy),
        ),
        (
            ClusterConfigEnv::SHUFFLE_BACKEND__TYPE.to_string(),
            match &shuffle_backend {
                ShuffleBackendKind::Flight { .. } => "flight",
                ShuffleBackendKind::Storage { .. } => "storage",
                ShuffleBackendKind::Celeborn { .. } => "celeborn",
            }
            .to_string(),
        ),
    ];
    if random_port {
        env.push((
            ClusterConfigEnv::WORKER_LISTEN_PORT.to_string(),
            "0".to_string(),
        ));
        env.push((
            ClusterConfigEnv::WORKER_EXTERNAL_PORT.to_string(),
            "0".to_string(),
        ));
    }
    if let Some(host) = external_host {
        env.push((
            ClusterConfigEnv::WORKER_EXTERNAL_HOST.to_string(),
            host.to_string(),
        ));
    }
    match &shuffle_backend {
        ShuffleBackendKind::Flight {
            compression,
            connection_count,
            initial_stream_window_size,
            initial_connection_window_size,
        } => {
            env.extend([
                (
                    ClusterConfigEnv::SHUFFLE_BACKEND__FLIGHT__COMPRESSION.to_string(),
                    compression.to_string(),
                ),
                (
                    ClusterConfigEnv::SHUFFLE_BACKEND__FLIGHT__CONNECTION_COUNT.to_string(),
                    connection_count.to_string(),
                ),
                (
                    ClusterConfigEnv::SHUFFLE_BACKEND__FLIGHT__INITIAL_STREAM_WINDOW_SIZE
                        .to_string(),
                    initial_stream_window_size.unwrap_or(0).to_string(),
                ),
                (
                    ClusterConfigEnv::SHUFFLE_BACKEND__FLIGHT__INITIAL_CONNECTION_WINDOW_SIZE
                        .to_string(),
                    initial_connection_window_size.unwrap_or(0).to_string(),
                ),
            ]);
        }
        ShuffleBackendKind::Storage {
            path,
            max_file_size,
            compression,
        } => {
            if let Some(path) = path {
                env.push((
                    ClusterConfigEnv::SHUFFLE_BACKEND__STORAGE__PATH.to_string(),
                    path.clone(),
                ));
            }
            env.extend([
                (
                    ClusterConfigEnv::SHUFFLE_BACKEND__STORAGE__MAX_FILE_SIZE.to_string(),
                    max_file_size.to_string(),
                ),
                (
                    ClusterConfigEnv::SHUFFLE_BACKEND__STORAGE__COMPRESSION.to_string(),
                    compression.to_string(),
                ),
            ]);
        }
        ShuffleBackendKind::Celeborn {
            compression,
            heartbeat_interval_secs,
            partition_split_threshold,
            partition_split_mode,
            ..
        } => {
            env.extend([
                (
                    ClusterConfigEnv::SHUFFLE_BACKEND__CELEBORN__MASTER_ENDPOINTS.to_string(),
                    shuffle_backend.celeborn_master_endpoints_string(),
                ),
                (
                    ClusterConfigEnv::SHUFFLE_BACKEND__CELEBORN__COMPRESSION.to_string(),
                    compression.to_string(),
                ),
                (
                    ClusterConfigEnv::SHUFFLE_BACKEND__CELEBORN__HEARTBEAT_INTERVAL_SECS
                        .to_string(),
                    heartbeat_interval_secs.to_string(),
                ),
                (
                    ClusterConfigEnv::SHUFFLE_BACKEND__CELEBORN__ENDPOINT_OVERRIDES.to_string(),
                    shuffle_backend.celeborn_endpoint_overrides_string(),
                ),
                (
                    ClusterConfigEnv::SHUFFLE_BACKEND__CELEBORN__PARTITION_SPLIT_THRESHOLD
                        .to_string(),
                    partition_split_threshold.to_string(),
                ),
                (
                    ClusterConfigEnv::SHUFFLE_BACKEND__CELEBORN__PARTITION_SPLIT_MODE.to_string(),
                    partition_split_mode.to_string(),
                ),
            ]);
        }
    }
    if let Some(traceparent) = w3c_traceparent {
        env.push((ContextPropagationEnv::TRACEPARENT.to_string(), traceparent));
    }
    env
}

/// Formats the retry strategy as an inline TOML table for the environment variable.
/// There is no guarantee that serializing a Rust data structure produces an inline table,
/// so we have to construct the nested TOML value manually.
fn format_retry_strategy(strategy: &RetryStrategy) -> String {
    match strategy {
        RetryStrategy::ExponentialBackoff {
            max_count,
            initial_delay,
            max_delay,
            factor,
        } => format!(
            "{{ exponential_backoff = {{ max_count = {}, initial_delay_secs = {}, max_delay_secs = {}, factor = {} }} }}",
            max_count,
            initial_delay.as_secs(),
            max_delay.as_secs(),
            factor,
        ),
        RetryStrategy::Fixed { max_count, delay } => format!(
            "{{ fixed = {{ max_count = {}, delay_secs = {} }} }}",
            max_count,
            delay.as_secs()
        ),
    }
}
