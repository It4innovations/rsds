use crate::scheduler::{FromSchedulerMessage, ToSchedulerMessage};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

mod basic;
mod network;

/// Communication channels used by the scheduler to receive events and send assignments.
pub struct SchedulerComm {
    pub(crate) recv: UnboundedReceiver<ToSchedulerMessage>,
    pub(crate) send: UnboundedSender<FromSchedulerMessage>,
}
pub use basic::BasicScheduler;
pub use network::NetworkScheduler;

pub fn prepare_scheduler_comm() -> (
    SchedulerComm,
    UnboundedSender<ToSchedulerMessage>,
    UnboundedReceiver<FromSchedulerMessage>,
) {
    let (up_sender, up_receiver) = unbounded_channel::<ToSchedulerMessage>();
    let (down_sender, down_receiver) = unbounded_channel::<FromSchedulerMessage>();

    (
        SchedulerComm {
            recv: up_receiver,
            send: down_sender,
        },
        up_sender,
        down_receiver,
    )
}
