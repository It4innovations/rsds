pub mod client;
pub mod comm;
pub mod core;
pub mod notifications;
pub mod protocol;
pub mod reactor;
pub mod rpc;
pub mod task;
pub mod worker;

#[derive(Debug, Copy, Clone)]
pub enum WorkerType {
    Dask,
    Rsds,
}
