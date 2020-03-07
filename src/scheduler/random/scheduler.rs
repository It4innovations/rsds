use crate::scheduler::protocol::{TaskAssignment, TaskId, WorkerId, SchedulerRegistration};
use crate::scheduler::{FromSchedulerMessage, SchedulerSender, ToSchedulerMessage, Scheduler};
use rand::prelude::ThreadRng;
use rand::seq::SliceRandom;

#[derive(Default, Debug)]
pub struct RandomScheduler {
    workers: Vec<WorkerId>,
    pending_tasks: Vec<TaskId>,
    assignments: Vec<TaskAssignment>,
    rng: ThreadRng,
}

impl Scheduler for RandomScheduler {
    fn identify(&self) -> SchedulerRegistration {
        SchedulerRegistration {
            protocol_version: 0,
            scheduler_name: "random-scheduler".into(),
            scheduler_version: "0.0".into(),
        }
    }

    fn handle_messages(&mut self, messages: Vec<ToSchedulerMessage>) -> bool {
        for message in messages {
            match message {
                ToSchedulerMessage::NewTask(task) => match self.workers.choose(&mut self.rng) {
                    Some(&worker) => self.assignments.push(TaskAssignment {
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
                            self.assignments.push(TaskAssignment {
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
        !self.assignments.is_empty()
    }

    fn schedule(&mut self, sender: &mut SchedulerSender) {
        if !self.assignments.is_empty() {
            sender
                .send(FromSchedulerMessage::TaskAssignments(std::mem::take(&mut self.assignments)))
                .expect("Couldn't send scheduler message");
        }
    }
}
