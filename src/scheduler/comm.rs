use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::scheduler::{FromSchedulerMessage, ToSchedulerMessage};

/// Communication channels used by the scheduler to receive events and send assignments.
pub struct SchedulerComm {
    pub(crate) recv: UnboundedReceiver<Vec<ToSchedulerMessage>>,
    send: UnboundedSender<FromSchedulerMessage>,
}

impl SchedulerComm {
    pub fn send(&mut self, message: FromSchedulerMessage) {
        self.send
            .send(message)
            .expect("Couldn't send scheduler message")
    }
}

pub fn prepare_scheduler_comm() -> (
    SchedulerComm,
    UnboundedSender<Vec<ToSchedulerMessage>>,
    UnboundedReceiver<FromSchedulerMessage>,
) {
    let (up_sender, up_receiver) = unbounded_channel::<Vec<ToSchedulerMessage>>();
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
