pub mod error;
mod metrics;
pub mod service;
pub mod session;
mod state;

use std::future::Future;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_server::{ServerBuilder, ServerBuilderOptions};
use service::SailFlightSqlService;
use session::create_flight_session_manager;
use tokio::net::TcpListener;

pub async fn serve<F>(
    listener: TcpListener,
    signal: F,
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()>,
{
    let session_manager = create_flight_session_manager(config, runtime)?;
    let service = SailFlightSqlService::new(session_manager.clone());
    let flight_service = FlightServiceServer::new(service);

    let builder = ServerBuilder::new("flight_sql", ServerBuilderOptions::default())
        .add_service(flight_service, None)
        .await;

    let result = builder.serve(listener, signal).await;
    // The server error is not `Send`, so it is converted to a string
    // before awaiting below.
    let result = result.map_err(|e| e.to_string());
    // Stop the sessions so that running jobs are canceled and their resources
    // (e.g. workers and shuffle files) are released before the process exits.
    if let Err(e) = session_manager.stop_sessions().await {
        log::warn!("failed to stop sessions: {e}");
    }
    result.map_err(Into::into)
}
