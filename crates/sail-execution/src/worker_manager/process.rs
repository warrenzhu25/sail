use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use log::{error, info, warn};
use sail_common::config::CliConfigEnv;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{oneshot, watch, Mutex};
use tokio::task::JoinHandle;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::WorkerId;
use crate::worker_manager::env::{build_worker_env, WorkerEnvOptions};
use crate::worker_manager::{WorkerLaunchOptions, WorkerManager};

#[derive(Debug, Clone)]
pub struct ProcessWorkerManagerOptions {
    /// The path of the `sail` executable used to launch the workers.
    pub program: PathBuf,
    /// The time to wait for a worker process to exit on its own after the worker
    /// has been asked to stop, before the process is killed.
    pub stop_grace_period: Duration,
}

impl ProcessWorkerManagerOptions {
    /// Creates options that launch workers using the current executable.
    pub fn from_current_exe() -> ExecutionResult<Self> {
        let program = std::env::current_exe().map_err(|e| {
            ExecutionError::InternalError(format!("failed to get the current executable: {e}"))
        })?;
        Ok(Self {
            program,
            stop_grace_period: Duration::from_secs(10),
        })
    }
}

/// A handle to a worker process.
struct ProcessWorker {
    pid: Option<u32>,
    /// The sender to request the supervisor to stop the process.
    /// The value is the grace period before the process is killed.
    stop: Option<oneshot::Sender<Duration>>,
    /// The supervisor task that waits for the process to exit.
    supervisor: JoinHandle<()>,
    /// The receiver for the description of how the process exited.
    exit: watch::Receiver<Option<String>>,
}

/// A worker manager that launches each worker as a separate `sail worker` OS process
/// on the local host. The workers are configured via environment variables
/// in the same way as Kubernetes worker pods.
///
/// The worker processes inherit the environment of the driver process,
/// so that settings (e.g. runtime, execution, and Parquet configuration) are shared,
/// while the cluster settings specific to each worker are overridden.
/// Each worker creates its shuffle directory under the shuffle directory of the driver,
/// so that the shuffle files of all workers are removed when the driver stops,
/// even if some worker processes have been killed.
///
/// On Linux, the worker processes are killed when the driver process exits
/// (even abnormally) via the parent death signal.
pub struct ProcessWorkerManager {
    options: ProcessWorkerManagerOptions,
    workers: Mutex<HashMap<WorkerId, ProcessWorker>>,
}

impl ProcessWorkerManager {
    pub fn new(options: ProcessWorkerManagerOptions) -> Self {
        Self {
            options,
            workers: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the process IDs of the worker processes that have not exited.
    pub async fn worker_pids(&self) -> HashMap<WorkerId, u32> {
        let workers = self.workers.lock().await;
        workers
            .iter()
            .filter(|(_, w)| !w.supervisor.is_finished())
            .filter_map(|(id, w)| w.pid.map(|pid| (*id, pid)))
            .collect()
    }

    fn build_command(&self, id: WorkerId, options: WorkerLaunchOptions) -> Command {
        let shuffle_dir = options.shuffle_dir.to_string_lossy().to_string();
        let env = build_worker_env(
            id,
            options,
            WorkerEnvOptions {
                listen_host: "127.0.0.1",
                external_host: Some("127.0.0.1"),
                random_port: true,
                shuffle_dir: Some(&shuffle_dir),
            },
        );
        let mut command = Command::new(&self.options.program);
        command
            .arg("worker")
            // The Sail CLI sets this variable for its own child processes to behave as
            // Python interpreters, which must not apply to the worker processes.
            .env_remove(CliConfigEnv::RUN_PYTHON)
            .envs(env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        set_parent_death_signal(&mut command);
        command
    }
}

#[cfg(target_os = "linux")]
fn set_parent_death_signal(command: &mut Command) {
    let parent = std::process::id() as libc::pid_t;
    // SAFETY: The closure only calls async-signal-safe functions.
    unsafe {
        command.pre_exec(move || {
            // Note that the signal is delivered when the *thread* that spawned the child exits.
            // Worker processes are spawned from long-lived async runtime threads.
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            // The parent may have exited before the signal was set up.
            if libc::getppid() != parent {
                return Err(std::io::Error::from_raw_os_error(libc::ESRCH));
            }
            Ok(())
        });
    }
}

#[cfg(not(target_os = "linux"))]
fn set_parent_death_signal(_command: &mut Command) {}

/// Forwards the output of the worker process to the standard error of the driver process,
/// with each line prefixed by the worker ID.
fn forward_output(id: WorkerId, reader: impl AsyncRead + Unpin + Send + 'static) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => eprintln!("[worker {id}] {line}"),
                Ok(None) => break,
                Err(e) => {
                    warn!("failed to read output of worker {id}: {e}");
                    break;
                }
            }
        }
    });
}

/// Removes the shuffle directories of a worker process that has exited.
/// The worker may not have cleaned up the directory if it was killed.
fn remove_worker_shuffle_dirs(shuffle_dir: &Path, id: WorkerId, pid: u32) {
    let prefix = format!("worker_{id}-{pid}-");
    let Ok(entries) = std::fs::read_dir(shuffle_dir) else {
        return;
    };
    for entry in entries.flatten() {
        if entry.file_name().to_string_lossy().starts_with(&prefix) {
            if let Err(e) = std::fs::remove_dir_all(entry.path()) {
                warn!(
                    "failed to remove shuffle directory {} of worker {id}: {e}",
                    entry.path().display()
                );
            }
        }
    }
}

async fn kill_and_wait(id: WorkerId, child: &mut Child) -> std::io::Result<ExitStatus> {
    warn!("killing worker {id} process");
    if let Err(e) = child.start_kill() {
        warn!("failed to kill worker {id} process: {e}");
    }
    child.wait().await
}

async fn supervise(
    id: WorkerId,
    mut child: Child,
    mut stop: oneshot::Receiver<Duration>,
    exit: watch::Sender<Option<String>>,
    shuffle_dir: PathBuf,
) {
    let pid = child.id();
    let status = tokio::select! {
        status = child.wait() => status,
        grace = &mut stop => {
            // If the sender is dropped, the worker manager is gone, so we kill
            // the process immediately.
            let grace = grace.unwrap_or(Duration::ZERO);
            match tokio::time::timeout(grace, child.wait()).await {
                Ok(status) => status,
                Err(_) => kill_and_wait(id, &mut child).await,
            }
        }
    };
    let description = match status {
        Ok(status) if status.success() => {
            info!("worker {id} process exited");
            format!("process exited with {status}")
        }
        Ok(status) => {
            warn!("worker {id} process exited with {status}");
            format!("process exited with {status}")
        }
        Err(e) => {
            error!("failed to wait for worker {id} process: {e}");
            format!("failed to wait for process: {e}")
        }
    };
    if let Some(pid) = pid {
        // Removing the directories may block for a long time if there are large files.
        let _ = tokio::task::spawn_blocking(move || {
            remove_worker_shuffle_dirs(&shuffle_dir, id, pid);
        })
        .await;
    }
    let _ = exit.send(Some(description));
}

#[tonic::async_trait]
impl WorkerManager for ProcessWorkerManager {
    async fn launch_worker(
        &self,
        id: WorkerId,
        options: WorkerLaunchOptions,
    ) -> ExecutionResult<()> {
        let shuffle_dir = options.shuffle_dir.clone();
        let mut command = self.build_command(id, options);
        let mut workers = self.workers.lock().await;
        // Forget the workers whose processes have exited.
        workers.retain(|_, w| !w.supervisor.is_finished());
        if workers.contains_key(&id) {
            return Err(ExecutionError::InternalError(format!(
                "worker {id} is already running"
            )));
        }
        let mut child = command.spawn().map_err(|e| {
            ExecutionError::InternalError(format!(
                "failed to launch worker {id} process using {}: {e}",
                self.options.program.display()
            ))
        })?;
        let pid = child.id();
        info!("launched worker {id} process (pid {pid:?})");
        if let Some(stdout) = child.stdout.take() {
            forward_output(id, stdout);
        }
        if let Some(stderr) = child.stderr.take() {
            forward_output(id, stderr);
        }
        let (tx, rx) = oneshot::channel();
        let (exit_tx, exit_rx) = watch::channel(None);
        let supervisor = tokio::spawn(supervise(id, child, rx, exit_tx, shuffle_dir));
        workers.insert(
            id,
            ProcessWorker {
                pid,
                stop: Some(tx),
                supervisor,
                exit: exit_rx,
            },
        );
        Ok(())
    }

    async fn wait_for_worker_exit(&self, id: WorkerId) -> Option<String> {
        let mut exit = {
            let workers = self.workers.lock().await;
            workers.get(&id)?.exit.clone()
        };
        let result = match exit.wait_for(|x| x.is_some()).await {
            Ok(x) => x.clone(),
            // The supervisor is gone without reporting the exit (e.g. it was aborted).
            Err(_) => Some("the worker process supervisor has stopped".to_string()),
        };
        result
    }

    async fn stop_worker(&self, id: WorkerId) -> ExecutionResult<()> {
        let mut workers = self.workers.lock().await;
        if let Some(stop) = workers.get_mut(&id).and_then(|w| w.stop.take()) {
            let _ = stop.send(self.options.stop_grace_period);
        }
        Ok(())
    }

    async fn stop(&self) -> ExecutionResult<()> {
        let workers = {
            let mut workers = self.workers.lock().await;
            std::mem::take(&mut *workers)
        };
        let mut supervisors = Vec::with_capacity(workers.len());
        for (id, mut worker) in workers {
            if let Some(stop) = worker.stop.take() {
                let _ = stop.send(self.options.stop_grace_period);
            }
            supervisors.push((id, worker.supervisor));
        }
        for (id, supervisor) in supervisors {
            if let Err(e) = supervisor.await {
                error!("failed to join the supervisor of worker {id} process: {e}");
            }
        }
        Ok(())
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    use sail_server::RetryStrategy;

    use super::*;

    /// A fake worker program that creates a worker shuffle directory in the same way as
    /// the real worker, records its environment, and then waits until it is killed.
    const FAKE_WORKER: &str = r#"#!/bin/sh
dir="$SAIL_CLUSTER__SHUFFLE_DIR/worker_${SAIL_CLUSTER__WORKER_ID}-$$-0000000000000000"
mkdir -p "$dir"
echo "$1" > "$dir/args"
env > "$SAIL_CLUSTER__SHUFFLE_DIR/env_${SAIL_CLUSTER__WORKER_ID}.tmp"
mv "$SAIL_CLUSTER__SHUFFLE_DIR/env_${SAIL_CLUSTER__WORKER_ID}.tmp" "$SAIL_CLUSTER__SHUFFLE_DIR/env_${SAIL_CLUSTER__WORKER_ID}"
echo "fake worker started"
exec sleep 1000
"#;

    fn launch_options(shuffle_dir: PathBuf) -> WorkerLaunchOptions {
        WorkerLaunchOptions {
            enable_tls: false,
            driver_external_host: "127.0.0.1".to_string(),
            driver_external_port: 12345,
            worker_heartbeat_interval: Duration::from_secs(1),
            task_stream_buffer: 16,
            task_stream_creation_timeout: Duration::from_secs(1),
            rpc_retry_strategy: RetryStrategy::Fixed {
                max_count: 1,
                delay: Duration::from_secs(0),
            },
            shuffle_dir,
        }
    }

    fn worker_dirs(dir: &Path, id: u64) -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .flatten()
                    .filter(|e| {
                        e.file_name()
                            .to_string_lossy()
                            .starts_with(&format!("worker_{id}-"))
                    })
                    .map(|e| e.path())
                    .collect()
            })
            .unwrap_or_default()
    }

    async fn wait_for(mut condition: impl FnMut() -> bool) -> bool {
        for _ in 0..500 {
            if condition() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    fn is_process_alive(pid: u32) -> bool {
        // A zombie process is considered dead.
        std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .map(|s| {
                s.rsplit_once(')')
                    .is_some_and(|(_, rest)| !rest.trim_start().starts_with('Z'))
            })
            .unwrap_or(false)
    }

    #[tokio::test]
    #[expect(clippy::unwrap_used)]
    async fn test_process_worker_lifecycle() {
        let root = std::env::temp_dir().join(format!(
            "sail-process-worker-test-{}-{:016x}",
            std::process::id(),
            rand::random::<u64>()
        ));
        let shuffle_dir = root.join("shuffle");
        std::fs::create_dir_all(&shuffle_dir).unwrap();
        let program = root.join("fake-worker.sh");
        std::fs::write(&program, FAKE_WORKER).unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();

        let manager = ProcessWorkerManager::new(ProcessWorkerManagerOptions {
            program,
            stop_grace_period: Duration::from_millis(100),
        });
        for id in [1u64, 2] {
            manager
                .launch_worker(id.into(), launch_options(shuffle_dir.clone()))
                .await
                .unwrap();
        }
        assert!(
            manager
                .launch_worker(1.into(), launch_options(shuffle_dir.clone()))
                .await
                .is_err(),
            "launching a running worker again should fail"
        );
        for id in [1, 2] {
            let env_file = shuffle_dir.join(format!("env_{id}"));
            assert!(
                wait_for(|| env_file.exists()).await,
                "worker {id} not started"
            );
            let env = std::fs::read_to_string(&env_file).unwrap();
            for expected in [
                format!("SAIL_CLUSTER__WORKER_ID={id}"),
                "SAIL_CLUSTER__DRIVER_EXTERNAL_PORT=12345".to_string(),
                "SAIL_CLUSTER__WORKER_LISTEN_HOST=127.0.0.1".to_string(),
                "SAIL_CLUSTER__WORKER_LISTEN_PORT=0".to_string(),
                "SAIL_CLUSTER__WORKER_EXTERNAL_HOST=127.0.0.1".to_string(),
                format!("SAIL_CLUSTER__SHUFFLE_DIR={}", shuffle_dir.display()),
            ] {
                assert!(env.lines().any(|x| x == expected), "missing {expected}");
            }
            let dirs = worker_dirs(&shuffle_dir, id);
            assert_eq!(dirs.len(), 1);
            let args = std::fs::read_to_string(dirs[0].join("args")).unwrap();
            assert_eq!(args.trim(), "worker");
        }
        let pids = manager.worker_pids().await;
        assert_eq!(pids.len(), 2);
        let pid1 = pids[&1.into()];
        let pid2 = pids[&2.into()];
        assert!(is_process_alive(pid1) && is_process_alive(pid2));

        // The fake worker does not exit on its own, so it is killed after the grace period,
        // and its shuffle directory is removed.
        manager.stop_worker(1.into()).await.unwrap();
        assert!(wait_for(|| worker_dirs(&shuffle_dir, 1).is_empty()).await);
        assert!(!is_process_alive(pid1));
        assert!(is_process_alive(pid2));

        // The exit is published after the shuffle directory is removed.
        let exit = manager.wait_for_worker_exit(1.into()).await;
        assert!(exit.as_deref().is_some_and(|x| x.contains("SIGKILL")));
        for _ in 0..100 {
            if manager.worker_pids().await.len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(manager.worker_pids().await.len(), 1);

        // A worker process that exits unexpectedly is detected and cleaned up.
        // SAFETY: Sending a signal to a child process is safe.
        assert_eq!(unsafe { libc::kill(pid2 as libc::pid_t, libc::SIGKILL) }, 0);
        let exit = tokio::time::timeout(
            Duration::from_secs(5),
            manager.wait_for_worker_exit(2.into()),
        )
        .await
        .unwrap();
        assert!(
            exit.as_deref().is_some_and(|x| x.contains("SIGKILL")),
            "{exit:?}"
        );
        assert!(worker_dirs(&shuffle_dir, 2).is_empty());

        // A new worker can be launched after the previous workers have exited.
        manager
            .launch_worker(3.into(), launch_options(shuffle_dir.clone()))
            .await
            .unwrap();
        let env_file = shuffle_dir.join("env_3");
        assert!(wait_for(|| env_file.exists()).await, "worker 3 not started");
        let pid3 = manager.worker_pids().await[&3.into()];
        manager.stop().await.unwrap();
        assert!(!is_process_alive(pid3));
        assert!(worker_dirs(&shuffle_dir, 3).is_empty());
        assert!(manager.worker_pids().await.is_empty());
        assert!(manager.wait_for_worker_exit(3.into()).await.is_none());

        std::fs::remove_dir_all(&root).unwrap();
    }

    #[tokio::test]
    async fn test_process_worker_launch_failure() {
        let manager = ProcessWorkerManager::new(ProcessWorkerManagerOptions {
            program: PathBuf::from("/nonexistent/sail"),
            stop_grace_period: Duration::from_millis(100),
        });
        let result = manager
            .launch_worker(1.into(), launch_options(std::env::temp_dir()))
            .await;
        assert!(result.is_err());
        assert!(manager.stop().await.is_ok());
    }
}
