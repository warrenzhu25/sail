pub mod definition;
pub mod scheduling;
pub mod stats;

pub mod gen {
    tonic::include_proto!("sail.task");
}
