use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::comm::CommRef;
use crate::core::CoreRef;
use crate::scheduler::{FromSchedulerMessage, ToSchedulerMessage};
use crate::DsError;
use futures::StreamExt;

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

pub async fn observe_scheduler(
    core_ref: CoreRef,
    comm_ref: CommRef,
    mut receiver: UnboundedReceiver<FromSchedulerMessage>,
) -> crate::Result<()> {
    log::debug!("Starting scheduler");

    match receiver.next().await {
        Some(crate::scheduler::FromSchedulerMessage::Register(r)) => {
            log::debug!("Scheduler registered: {:?}", r)
        }
        None => {
            return Err(DsError::SchedulerError(
                "Scheduler closed connection without registration".to_owned(),
            ))
        }
        _ => {
            return Err(DsError::SchedulerError(
                "First message of scheduler has to be registration".to_owned(),
            ))
        }
    }

    while let Some(msg) = receiver.next().await {
        match msg {
            FromSchedulerMessage::TaskAssignments(assignments) => {
                let mut core = core_ref.get_mut();
                let mut notifications = Default::default();
                core.process_assignments(assignments, &mut notifications);
                comm_ref.get_mut().notify(&mut core, notifications).unwrap();
            }
            FromSchedulerMessage::Register(_) => {
                return Err(DsError::SchedulerError(
                    "Double registration of scheduler".to_owned(),
                ));
            }
        }
    }

    Ok(())
}
