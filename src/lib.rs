#[macro_use]
pub mod trace;
#[macro_use]
mod util;

pub mod comm;
mod common;
mod error;
pub mod scheduler;
pub mod server;
pub mod worker;

pub use util::setup_interrupt;

#[cfg(test)]
mod test_util;

pub type Error = error::DsError;
pub type Result<T> = std::result::Result<T, Error>;
