use std::env;

use fastrace::collector::SpanContext;
use sail_common::config::ClusterConfigEnv;
use sail_server::RetryStrategy;
use sail_telemetry::common::ContextPropagationEnv;

use crate::id::WorkerId;
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
    /// The shuffle directory under which the worker creates its own
    /// shuffle subdirectory. If the value is `None`, the worker uses
    /// the shuffle directory in its own configuration.
    pub shuffle_dir: Option<&'a str>,
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
        driver_external_host,
        driver_external_port,
        worker_heartbeat_interval,
        task_stream_buffer,
        task_stream_creation_timeout,
        rpc_retry_strategy,
        // The shuffle directory of the driver is not accessible to remote workers.
        // The worker manager decides the worker shuffle directory instead.
        shuffle_dir: _,
        shuffle_spill_threshold,
    } = options;
    let WorkerEnvOptions {
        listen_host,
        external_host,
        random_port,
        shuffle_dir,
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
            ClusterConfigEnv::DRIVER_EXTERNAL_HOST.to_string(),
            driver_external_host,
        ),
        (
            ClusterConfigEnv::DRIVER_EXTERNAL_PORT.to_string(),
            driver_external_port.to_string(),
        ),
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
            ClusterConfigEnv::SHUFFLE_SPILL_THRESHOLD_BYTES.to_string(),
            shuffle_spill_threshold.to_string(),
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
    if let Some(dir) = shuffle_dir {
        env.push((ClusterConfigEnv::SHUFFLE_DIR.to_string(), dir.to_string()));
    }
    if let Some(traceparent) = w3c_traceparent {
        env.push((ContextPropagationEnv::TRACEPARENT.to_string(), traceparent));
    }
    env
}

/// Formats the retry strategy as an inline TOML table.
/// There is no guarantee that serializing a Rust data structure produces an inline table,
/// so we have to construct the nested TOML value manually.
fn format_retry_strategy(strategy: &RetryStrategy) -> String {
    match strategy {
        RetryStrategy::ExponentialBackoff {
            max_count,
            initial_delay,
            max_delay,
            factor,
        } => {
            format!(
                "{{ exponential_backoff = {{ max_count = {}, initial_delay_secs = {}, max_delay_secs = {}, factor = {} }} }}",
                max_count,
                initial_delay.as_secs(),
                max_delay.as_secs(),
                factor,
            )
        }
        RetryStrategy::Fixed { max_count, delay } => {
            format!(
                "{{ fixed = {{ max_count = {}, delay_secs = {} }} }}",
                max_count,
                delay.as_secs()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::Duration;

    use super::*;

    fn launch_options() -> WorkerLaunchOptions {
        WorkerLaunchOptions {
            enable_tls: false,
            driver_external_host: "10.0.0.1".to_string(),
            driver_external_port: 7000,
            worker_heartbeat_interval: Duration::from_secs(5),
            task_stream_buffer: 16,
            task_stream_creation_timeout: Duration::from_secs(60),
            rpc_retry_strategy: RetryStrategy::Fixed {
                max_count: 3,
                delay: Duration::from_secs(2),
            },
            shuffle_dir: PathBuf::from("/driver/shuffle"),
            shuffle_spill_threshold: 1024,
        }
    }

    #[test]
    fn test_build_worker_env() {
        let env = build_worker_env(
            WorkerId::from(42),
            launch_options(),
            WorkerEnvOptions {
                listen_host: "127.0.0.1",
                external_host: Some("127.0.0.1"),
                random_port: true,
                shuffle_dir: Some("/data/shuffle"),
            },
        )
        .into_iter()
        .collect::<HashMap<_, _>>();
        let get = |k: &str| env.get(k).map(|x| x.as_str());
        assert_eq!(get(ClusterConfigEnv::WORKER_ID), Some("42"));
        assert_eq!(
            get(ClusterConfigEnv::DRIVER_EXTERNAL_HOST),
            Some("10.0.0.1")
        );
        assert_eq!(get(ClusterConfigEnv::DRIVER_EXTERNAL_PORT), Some("7000"));
        assert_eq!(get(ClusterConfigEnv::WORKER_LISTEN_HOST), Some("127.0.0.1"));
        assert_eq!(get(ClusterConfigEnv::WORKER_LISTEN_PORT), Some("0"));
        assert_eq!(
            get(ClusterConfigEnv::WORKER_EXTERNAL_HOST),
            Some("127.0.0.1")
        );
        assert_eq!(get(ClusterConfigEnv::WORKER_EXTERNAL_PORT), Some("0"));
        assert_eq!(get(ClusterConfigEnv::SHUFFLE_DIR), Some("/data/shuffle"));
        assert_eq!(
            get(ClusterConfigEnv::SHUFFLE_SPILL_THRESHOLD_BYTES),
            Some("1024")
        );
        assert_eq!(
            get(ClusterConfigEnv::RPC_RETRY_STRATEGY),
            Some("{ fixed = { max_count = 3, delay_secs = 2 } }")
        );
    }

    #[test]
    fn test_build_worker_env_without_optional_values() {
        let env = build_worker_env(
            WorkerId::from(1),
            launch_options(),
            WorkerEnvOptions {
                listen_host: "0.0.0.0",
                external_host: None,
                random_port: false,
                shuffle_dir: None,
            },
        );
        for key in [
            ClusterConfigEnv::SHUFFLE_DIR,
            ClusterConfigEnv::WORKER_EXTERNAL_HOST,
            ClusterConfigEnv::WORKER_LISTEN_PORT,
            ClusterConfigEnv::WORKER_EXTERNAL_PORT,
        ] {
            assert!(!env.iter().any(|(k, _)| k == key), "unexpected {key}");
        }
    }
}
