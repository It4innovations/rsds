pub use crate::error::DsError;

#[macro_use]
pub mod protocol;

mod client;
pub mod comm;
mod common;
pub mod connection;
pub mod core;
mod error;
mod notifications;
pub mod reactor;
pub mod scheduler;
mod task;
mod util;
mod worker;

#[cfg(test)]
mod test_util;

pub type Result<T> = std::result::Result<T, DsError>;
