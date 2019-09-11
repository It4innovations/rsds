#[macro_use]
extern crate quick_error;

mod client;
mod common;
pub mod connection;
mod core;
mod daskcodec;
mod error;
mod messages;
pub mod prelude;
pub mod scheduler;
mod task;
mod worker;

pub use crate::error::DsError;
pub type Result<T> = std::result::Result<T, DsError>;
