pub use crate::error::DsError;

#[macro_use]
mod trace;
#[macro_use]
mod util;

pub mod comm;
mod common;
mod error;
pub mod protocol;
pub mod scheduler;
pub mod server;

#[cfg(test)]
mod test_util;

pub type Result<T> = std::result::Result<T, DsError>;
