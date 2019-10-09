use futures::StreamExt;

use crate::scheduler::{FromSchedulerMessage, SchedulerComm};
use crate::scheduler::schedproto::SchedulerRegistration;
use crate::scheduler::state::State;

pub struct BasicScheduler;

impl BasicScheduler {
    pub async fn start(self, mut comm: SchedulerComm) -> crate::Result<()> {
        log::debug!("Scheduler initialized");
        let mut state = State::new();

        comm.send
            .try_send(FromSchedulerMessage::Register(SchedulerRegistration {
                protocol_version: 0,
                scheduler_name: "test_scheduler".into(),
                scheduler_version: "0.0".into(),
                reassigning: false,
            }))
            .expect("Send failed");

        while let Some(msgs) = comm.recv.next().await {
            state.update(msgs, &mut comm.send);
        }
        log::debug!("Scheduler closed");
        Ok(())
    }
}
