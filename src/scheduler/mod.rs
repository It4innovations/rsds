pub use protocol::{FromSchedulerMessage, ToSchedulerMessage};

use crate::scheduler::protocol::SchedulerRegistration;
use tokio::sync::mpsc::UnboundedSender;

mod comm;
pub mod protocol;
mod random;
mod task;
mod worker;
mod workstealing;

pub use comm::{observe_scheduler, prepare_scheduler_comm, scheduler_driver, SchedulerComm};
pub use protocol::{TaskAssignment, TaskId, WorkerId};
pub use random::RandomScheduler;
pub use workstealing::WorkstealingScheduler;

pub type SchedulerSender = UnboundedSender<FromSchedulerMessage>;

pub trait Scheduler {
    fn identify(&self) -> SchedulerRegistration;
    fn update(&mut self, messages: Vec<ToSchedulerMessage>, sender: &mut SchedulerSender);
}
