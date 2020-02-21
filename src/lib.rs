pub use crate::error::DsError;

#[macro_use]
mod trace;

mod client;
pub mod comm;
mod common;
pub mod core;
mod error;
pub mod protocol;
pub mod reactor;
pub mod scheduler;
mod task;
mod util;
mod worker;

#[cfg(test)]
mod test_util;

pub type Result<T> = std::result::Result<T, DsError>;
