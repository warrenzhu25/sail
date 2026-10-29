use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use log::{error, info, warn};
use sail_common::actor::ActorSystem;
use sail_common::config::CliConfigEnv;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, oneshot, watch};
use tokio::task::JoinHandle;

use crate::error::{ExecutionError, ExecutionResult};
use crate::id::WorkerId;
use crate::worker_manager::env::{WorkerEnvOptions, build_worker_env};
use crate::worker_manager::{WorkerLaunchOptions, WorkerManager};

#[derive(Debug, Clone)]
pub struct ProcessWorkerManagerOptions {
    /// The path of the program used to launch the workers (the `sail` executable,
    /// or the Python interpreter for PySail).
    pub program: PathBuf,
    /// The arguments passed to the program before the `worker` command
    /// (e.g. `["-m", "pysail"]` when the program is a Python interpreter).
    pub program_args: Vec<String>,
    /// The time to wait for a worker process to exit on its own after the worker
    /// has been asked to stop, before the process is killed.
    pub stop_grace_period: Duration,
}

impl ProcessWorkerManagerOptions {
    /// Creates options that launch workers using the Sail CLI command if it has been set
    /// (e.g. when Sail runs inside a Python interpreter via PySail),
    /// or the current executable otherwise.
    pub fn from_current_exe() -> ExecutionResult<Self> {
        let (program, program_args) = match sail_common::config::cli_command() {
            Some([program, args @ ..]) => (PathBuf::from(program), args.to_vec()),
            _ => {
                let program = std::env::current_exe().map_err(|e| {
                    ExecutionError::InternalError(format!(
                        "failed to get the current executable: {e}"
                    ))
                })?;
                (program, vec![])
            }
        };
        Ok(Self {
            program,
            program_args,
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

type ProcessWorkers = Arc<Mutex<HashMap<WorkerId, ProcessWorker>>>;

/// A worker manager that launches each worker as a separate `sail worker` OS process
/// on the local host. The workers are configured via environment variables
/// in the same way as Kubernetes worker pods.
///
/// The worker processes inherit the environment of the driver process,
/// so that settings (e.g. runtime, execution, and Parquet configuration) are shared,
/// while the cluster settings specific to each worker are overridden.
///
/// On Linux, the worker processes are killed when the driver process exits
/// (even abnormally) via the parent death signal.
pub struct ProcessWorkerManager {
    options: Arc<ProcessWorkerManagerOptions>,
    workers: ProcessWorkers,
}

impl ProcessWorkerManager {
    pub fn new(options: ProcessWorkerManagerOptions) -> Self {
        Self {
            options: Arc::new(options),
            workers: Arc::new(Mutex::new(HashMap::new())),
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

    /// Waits for a launched worker process to exit, and returns a description of how
    /// the process exited. Returns `None` if the worker is unknown.
    pub async fn wait_for_worker_exit(&self, id: WorkerId) -> Option<String> {
        let mut exit = {
            let workers = self.workers.lock().await;
            workers.get(&id)?.exit.clone()
        };
        match exit.wait_for(|x| x.is_some()).await {
            Ok(x) => x.clone(),
            // The supervisor is gone without reporting the exit (e.g. it was aborted).
            Err(_) => Some("the worker process supervisor has stopped".to_string()),
        }
    }

    /// Stops a single worker process. The process is given a grace period to exit
    /// on its own (e.g. after the driver has asked the worker to shut down),
    /// before it is killed.
    pub async fn stop_worker(&self, id: WorkerId) {
        let mut workers = self.workers.lock().await;
        if let Some(stop) = workers.get_mut(&id).and_then(|w| w.stop.take()) {
            let _ = stop.send(self.options.stop_grace_period);
        }
    }

    fn build_command(&self, id: WorkerId, options: WorkerLaunchOptions) -> Command {
        let env = build_worker_env(
            id,
            options,
            WorkerEnvOptions {
                listen_host: "127.0.0.1",
                external_host: Some("127.0.0.1"),
                random_port: true,
            },
        );
        let mut command = Command::new(&self.options.program);
        command
            .args(&self.options.program_args)
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
            // Note that the signal is delivered when the *thread* that spawned the child exits,
            // so worker processes are spawned from a dedicated thread that lives as long as
            // the process (see `spawn_on_launcher_thread`).
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

/// A request to spawn a worker process on the launcher thread.
type SpawnRequest = (
    Command,
    tokio::runtime::Handle,
    std::sync::mpsc::Sender<std::io::Result<Child>>,
);

/// Spawns the command on a dedicated thread that lives as long as the process.
///
/// The parent death signal of the child process is tied to the thread that spawned
/// the child. Async runtime threads are not suitable since a worker thread can be
/// converted into a blocking thread (e.g. via `block_in_place`), and blocking threads
/// exit when they are idle, which would kill the worker processes unexpectedly.
fn spawn_on_launcher_thread(command: Command) -> std::io::Result<Child> {
    static LAUNCHER: std::sync::OnceLock<std::sync::Mutex<std::sync::mpsc::Sender<SpawnRequest>>> =
        std::sync::OnceLock::new();
    let launcher = LAUNCHER.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel::<SpawnRequest>();
        let spawned = std::thread::Builder::new()
            .name("sail-worker-launcher".to_string())
            .spawn(move || {
                for (mut command, handle, result) in rx {
                    // The child process must be registered with the async runtime
                    // of the caller so that it can be awaited there.
                    let _guard = handle.enter();
                    let _ = result.send(command.spawn());
                }
            });
        if let Err(e) = spawned {
            error!("failed to start the worker launcher thread: {e}");
        }
        std::sync::Mutex::new(tx)
    });
    let (tx, rx) = std::sync::mpsc::channel();
    launcher
        .lock()
        .map_err(|_| std::io::Error::other("worker launcher lock is poisoned"))?
        .send((command, tokio::runtime::Handle::current(), tx))
        .map_err(|_| std::io::Error::other("worker launcher thread is not running"))?;
    rx.recv()
        .map_err(|_| std::io::Error::other("worker launcher thread is not running"))?
}

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
) {
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
    let _ = exit.send(Some(description));
}

#[tonic::async_trait]
impl WorkerManager for ProcessWorkerManager {
    fn launch_worker(
        &self,
        _system: &mut ActorSystem,
        id: WorkerId,
        options: WorkerLaunchOptions,
    ) -> BoxFuture<'static, ExecutionResult<()>> {
        let command = self.build_command(id, options);
        let program = self.options.program.clone();
        let workers = Arc::clone(&self.workers);
        Box::pin(async move {
            let mut workers = workers.lock().await;
            // Forget the workers whose processes have exited.
            workers.retain(|_, w| !w.supervisor.is_finished());
            if workers.contains_key(&id) {
                return Err(ExecutionError::InternalError(format!(
                    "worker {id} is already running"
                )));
            }
            // Spawning a process is fast, so it is fine to wait for the launcher thread here.
            let mut child = spawn_on_launcher_thread(command).map_err(|e| {
                ExecutionError::InternalError(format!(
                    "failed to launch worker {id} process using {}: {e}",
                    program.display()
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
            let supervisor = tokio::spawn(supervise(id, child, rx, exit_tx));
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
        })
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
    #![expect(clippy::unwrap_used)]

    use std::path::Path;

    use sail_common::utils::retry::RetryStrategy;

    use super::*;
    use crate::id::DriverId;
    use crate::shuffle::ShuffleBackendKind;

    /// A fake worker program run via `/bin/sh <script> <dir> worker`. It records its
    /// arguments and environment in `<dir>`, and then waits until it is killed.
    const FAKE_WORKER: &str = r#"
dir="$1"
id="$SAIL_CLUSTER__WORKER_ID"
echo "$2" > "$dir/args_$id"
env > "$dir/env_$id.tmp"
mv "$dir/env_$id.tmp" "$dir/env_$id"
echo "fake worker started"
exec sleep 1000
"#;

    struct Fixture {
        root: PathBuf,
        manager: ProcessWorkerManager,
    }

    impl Fixture {
        fn new(name: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "sail-process-worker-{name}-{}-{:016x}",
                std::process::id(),
                rand::random::<u64>()
            ));
            std::fs::create_dir_all(&root).unwrap();
            let script = root.join("fake-worker.sh");
            std::fs::write(&script, FAKE_WORKER).unwrap();
            let manager = ProcessWorkerManager::new(ProcessWorkerManagerOptions {
                program: PathBuf::from("/bin/sh"),
                program_args: vec![
                    script.to_string_lossy().to_string(),
                    root.to_string_lossy().to_string(),
                ],
                stop_grace_period: Duration::from_millis(100),
            });
            Self { root, manager }
        }

        async fn launch(&self, id: u64) -> ExecutionResult<()> {
            let mut system = ActorSystem::new();
            self.manager
                .launch_worker(&mut system, id.into(), launch_options())
                .await
        }

        fn file(&self, name: &str) -> PathBuf {
            self.root.join(name)
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn launch_options() -> WorkerLaunchOptions {
        WorkerLaunchOptions {
            enable_tls: false,
            batch_size: 8192,
            session_id: "session-1".to_string(),
            driver_id: DriverId::from(7),
            driver_external_host: "127.0.0.1".to_string(),
            driver_external_port: 12345,
            worker_heartbeat_interval: Duration::from_secs(1),
            task_stream_buffer: 16,
            task_stream_creation_timeout: Duration::from_secs(1),
            rpc_retry_strategy: RetryStrategy::Fixed {
                max_count: 1,
                delay: Duration::from_secs(0),
            },
            shuffle_backend: ShuffleBackendKind::Storage {
                path: Some("file:///tmp/shuffle".to_string()),
                max_file_size: 1024,
                compression: crate::shuffle::ShuffleCompression::Lz4,
            },
        }
    }

    async fn wait_for(condition: impl Fn() -> bool) -> bool {
        for _ in 0..500 {
            if condition() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    fn is_process_alive(pid: u32) -> bool {
        Path::new(&format!("/proc/{pid}")).exists()
            && !std::fs::read_to_string(format!("/proc/{pid}/stat"))
                .unwrap_or_default()
                .contains(") Z ")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_process_worker_lifecycle() {
        let fixture = Fixture::new("lifecycle");
        for id in [1u64, 2] {
            fixture.launch(id).await.unwrap();
        }
        assert!(
            fixture.launch(1).await.is_err(),
            "launching a running worker again should fail"
        );
        for id in [1, 2] {
            let env_file = fixture.file(&format!("env_{id}"));
            assert!(
                wait_for(|| env_file.exists()).await,
                "worker {id} not started"
            );
            let env = std::fs::read_to_string(&env_file).unwrap();
            for expected in [
                format!("SAIL_CLUSTER__WORKER_ID={id}"),
                "SAIL_CLUSTER__DRIVER_EXTERNAL_PORT=12345".to_string(),
                "SAIL_CLUSTER__DRIVER_ID=7".to_string(),
                "SAIL_CLUSTER__SESSION_ID=session-1".to_string(),
                "SAIL_CLUSTER__WORKER_LISTEN_HOST=127.0.0.1".to_string(),
                "SAIL_CLUSTER__WORKER_LISTEN_PORT=0".to_string(),
                "SAIL_CLUSTER__WORKER_EXTERNAL_HOST=127.0.0.1".to_string(),
                "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE=storage".to_string(),
                "SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__PATH=file:///tmp/shuffle".to_string(),
            ] {
                assert!(env.lines().any(|x| x == expected), "missing {expected}");
            }
            assert!(!env.contains(CliConfigEnv::RUN_PYTHON));
            let args = std::fs::read_to_string(fixture.file(&format!("args_{id}"))).unwrap();
            assert_eq!(args.trim(), "worker");
        }
        let pids = fixture.manager.worker_pids().await;
        assert_eq!(pids.len(), 2);
        let (pid1, pid2) = (pids[&1.into()], pids[&2.into()]);
        assert!(is_process_alive(pid1) && is_process_alive(pid2));

        // The fake worker does not exit on its own, so it is killed after the grace period,
        // and the exit is observed.
        fixture.manager.stop_worker(1.into()).await;
        let exit = tokio::time::timeout(
            Duration::from_secs(10),
            fixture.manager.wait_for_worker_exit(1.into()),
        )
        .await
        .unwrap();
        assert!(exit.is_some_and(|x| x.contains("signal")));
        assert!(wait_for(|| !is_process_alive(pid1)).await);
        assert!(is_process_alive(pid2));

        // A worker killed externally is observed as exited.
        // SAFETY: Sending a signal to a child process.
        unsafe {
            libc::kill(pid2 as libc::pid_t, libc::SIGKILL);
        }
        let exit = tokio::time::timeout(
            Duration::from_secs(10),
            fixture.manager.wait_for_worker_exit(2.into()),
        )
        .await
        .unwrap();
        assert!(exit.is_some());

        // A worker ID can be reused after the previous process has exited.
        fixture.launch(1).await.unwrap();
        fixture.manager.stop().await.unwrap();
        assert!(fixture.manager.worker_pids().await.is_empty());
        assert!(
            fixture
                .manager
                .wait_for_worker_exit(1.into())
                .await
                .is_none()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_process_worker_launch_failure() {
        let manager = ProcessWorkerManager::new(ProcessWorkerManagerOptions {
            program: PathBuf::from("/nonexistent/sail"),
            program_args: vec![],
            stop_grace_period: Duration::from_millis(100),
        });
        let mut system = ActorSystem::new();
        let result = manager
            .launch_worker(&mut system, 1.into(), launch_options())
            .await;
        assert!(result.is_err());
        assert!(manager.worker_pids().await.is_empty());
    }

    /// The worker process must survive the exit of the thread that requested the launch,
    /// since the parent death signal is tied to the spawning thread.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_worker_process_survives_exit_of_requesting_thread() {
        let mut command = Command::new("sleep");
        command.arg("30").kill_on_drop(true);
        set_parent_death_signal(&mut command);
        let handle = tokio::runtime::Handle::current();
        // Request the launch from a short-lived thread, similar to a blocking thread
        // that exits when it is idle.
        let mut child = std::thread::spawn(move || {
            let _guard = handle.enter();
            spawn_on_launcher_thread(command)
        })
        .join()
        .unwrap()
        .unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            child.try_wait().unwrap().is_none(),
            "the worker process was killed when the requesting thread exited"
        );
        child.kill().await.unwrap();
    }
}
