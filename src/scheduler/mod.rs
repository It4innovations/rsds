pub use protocol::{FromSchedulerMessage, ToSchedulerMessage};

use crate::scheduler::protocol::SchedulerRegistration;
use tokio::sync::mpsc::UnboundedSender;

mod comm;
mod graph;
pub mod protocol;
mod task;
mod utils;
mod worker;

mod blevel;
mod random;
mod workstealing;

pub use blevel::BlevelScheduler;
pub use comm::{drive_scheduler, observe_scheduler, prepare_scheduler_comm, SchedulerComm};
pub use protocol::{TaskAssignment, TaskId, WorkerId};
pub use random::RandomScheduler;
pub use workstealing::WorkstealingScheduler;

pub type SchedulerSender = UnboundedSender<FromSchedulerMessage>;

pub trait Scheduler {
    fn identify(&self) -> SchedulerRegistration;

    /// Returns true if the scheduler requires someone to invoke `schedule` sometime in the future.
    fn handle_messages(&mut self, messages: Vec<ToSchedulerMessage>) -> bool;
    fn schedule(&mut self) -> Vec<TaskAssignment>;
}
