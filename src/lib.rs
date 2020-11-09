pub use util::{setup_interrupt, setup_logging};

#[macro_use]
pub mod trace;
#[macro_use]
mod util;

mod common;
mod error;
pub mod scheduler;
pub mod server;
pub mod worker;

#[cfg(test)]
mod test_util;

pub type Error = error::DsError;
pub type Result<T> = std::result::Result<T, Error>;
