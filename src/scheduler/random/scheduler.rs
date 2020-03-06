use crate::scheduler::protocol::{SchedulerRegistration, TaskAssignment, TaskId, WorkerId};
use crate::scheduler::{FromSchedulerMessage, Scheduler, SchedulerSender, ToSchedulerMessage};
use rand::prelude::ThreadRng;
use rand::seq::SliceRandom;

#[derive(Default, Debug)]
pub struct RandomScheduler {
    workers: Vec<WorkerId>,
    pending_tasks: Vec<TaskId>,
    rng: ThreadRng,
}

impl RandomScheduler {
    fn handle_messages(&mut self, messages: Vec<ToSchedulerMessage>, sender: &mut SchedulerSender) {
        let mut assignments = vec![];
        for message in messages {
            match message {
                ToSchedulerMessage::NewTask(task) => match self.workers.choose(&mut self.rng) {
                    Some(&worker) => assignments.push(TaskAssignment {
                        task: task.id,
                        worker,
                        priority: 0,
                    }),
                    None => self.pending_tasks.push(task.id),
                },
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
            sender
                .send(FromSchedulerMessage::TaskAssignments(assignments))
                .expect("Couldn't send scheduler message");
        }
    }
}

impl Scheduler for RandomScheduler {
    fn identify(&self) -> SchedulerRegistration {
        SchedulerRegistration {
            protocol_version: 0,
            scheduler_name: "random-scheduler".into(),
            scheduler_version: "0.0".into(),
        }
    }

    #[inline]
    fn update(&mut self, messages: Vec<ToSchedulerMessage>, sender: &mut SchedulerSender) {
        self.handle_messages(messages, sender)
    }
}
