mod actor;
mod client;
mod event;
pub(super) mod job_scheduler;
mod options;
pub(super) mod output;
mod server;
pub(crate) mod shuffle_dir;
mod task_assigner;
pub(super) mod worker_pool;

#[expect(clippy::allow_attributes)]
mod gen {
    tonic::include_proto!("sail.driver");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("sail_driver_descriptor");
}

pub(crate) use actor::DriverActor;
pub(crate) use client::DriverClientSet;
pub(crate) use event::{DriverEvent, TaskStatus};
pub(crate) use gen::driver_service_client::DriverServiceClient;
pub(crate) use options::shuffle_root_dir;
pub use options::DriverOptions;
