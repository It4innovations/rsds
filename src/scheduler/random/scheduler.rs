use crate::scheduler::comm::SchedulerComm;
use crate::scheduler::schedproto::{SchedulerRegistration, TaskAssignment, WorkerId, TaskId};
use crate::scheduler::{FromSchedulerMessage, ToSchedulerMessage};
use futures::StreamExt;
use rand::prelude::ThreadRng;
use rand::seq::SliceRandom;

pub struct Scheduler {
    workers: Vec<WorkerId>,
    pending_tasks: Vec<TaskId>,
    rng: ThreadRng,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            workers: Default::default(),
            pending_tasks: Default::default(),
            rng: Default::default(),
        }
    }

    pub async fn start(mut self, mut comm: SchedulerComm) -> crate::Result<()> {
        log::debug!("Random scheduler initialized");

        comm.send(FromSchedulerMessage::Register(SchedulerRegistration {
            protocol_version: 0,
            scheduler_name: "random-scheduler".into(),
            scheduler_version: "0.0".into(),
        }));

        while let Some(msgs) = comm.recv.next().await {
            self.update(msgs, &mut comm);
        }
        log::debug!("Scheduler closed");
        Ok(())
    }

    fn update(&mut self, messages: Vec<ToSchedulerMessage>, comm: &mut SchedulerComm) {
        let mut assignments = vec![];
        for message in messages {
            match message {
                ToSchedulerMessage::NewTask(task) => {
                    match self
                        .workers
                        .choose(&mut self.rng) {
                        Some(&worker) => {
                            assignments.push(TaskAssignment {
                                task: task.id,
                                worker,
                                priority: 0,
                            })
                        }
                        None => self.pending_tasks.push(task.id)
                    }
                }
                ToSchedulerMessage::NewWorker(worker) => {
                    self.workers.push(worker.id);
                    if !self.pending_tasks.is_empty() {
                        for task in self.pending_tasks.drain(..) {
                            assignments.push(TaskAssignment {
                                task,
                                worker: worker.id,
                                priority: 0,
                            });
                        }
                    }
                }
                ToSchedulerMessage::TaskStealResponse(_) => {
                    panic!("Random scheduler received steal response")
                }
                _ => { /* Ignore */ }
            }
        }

        if !assignments.is_empty() {
            comm.send(FromSchedulerMessage::TaskAssignments(assignments));
        }
    }
}

#[cfg(test)]
mod tests {}
