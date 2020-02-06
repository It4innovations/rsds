use crate::scheduler::comm::SchedulerComm;
use crate::scheduler::schedproto::SchedulerRegistration;
use crate::scheduler::FromSchedulerMessage;
use futures::StreamExt;

pub struct Scheduler;

impl Scheduler {
    pub fn new() -> Self {
        Self
    }

    pub async fn start(self, mut comm: SchedulerComm) -> crate::Result<()> {
        log::debug!("Random scheduler initialized");

        comm.send
            .send(FromSchedulerMessage::Register(SchedulerRegistration {
                protocol_version: 0,
                scheduler_name: "random-scheduler".into(),
                scheduler_version: "0.0".into(),
                reassigning: false,
            }))
            .expect("Scheduler start send failed");

        while let Some(msgs) = comm.recv.next().await {
            // TODO
        }
        log::debug!("Scheduler closed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {}
