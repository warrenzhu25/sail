/// Waits for a signal that requests the server to shut down gracefully.
///
/// This handles `SIGINT` (e.g. Ctrl-C) on all platforms, and also `SIGTERM` on Unix,
/// which is the signal sent by process managers such as Kubernetes when stopping
/// the process. Handling `SIGTERM` allows the server to stop the workers and clean up
/// resources (e.g. shuffle files) before exiting.
pub(crate) async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
