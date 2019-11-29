use std::collections::HashMap;

use tokio::sync::mpsc::UnboundedSender;

use crate::scheduler::{ToSchedulerMessage, FromSchedulerMessage};
use crate::scheduler::schedproto::{WorkerId, TaskAssignment, SchedulerRegistration};
use crate::scheduler::interface::SchedulerComm;
use futures::StreamExt;

pub struct Worker {
    pub id: WorkerId,
    pub ncpus: u32,
    pub free_cpus: i32,
}

pub struct Scheduler {
    network_bandwidth: f32,
    workers: HashMap<WorkerId, Worker>,
    //tasks: HashMap<TaskId, TaskRef>,

    _tmp_hack: Vec<WorkerId>,
}

impl Scheduler {
    pub fn new() -> Self {
        Scheduler {
            workers: Default::default(),
            //tasks: Default::default(),
            network_bandwidth: 100.0, // Guess better default
            _tmp_hack: Vec::new(),
        }
    }

    pub async fn start(mut self, mut comm: SchedulerComm) -> crate::Result<()> {
            log::debug!("Scheduler initialized");

            comm.send
                .try_send(FromSchedulerMessage::Register(SchedulerRegistration {
                    protocol_version: 0,
                    scheduler_name: "test_scheduler".into(),
                    scheduler_version: "0.0".into(),
                    reassigning: false,
                }))
                .expect("Send failed");

            while let Some(msgs) = comm.recv.next().await {
                self.update(msgs, &mut comm.send);
            }
            log::debug!("Scheduler closed");
            Ok(())
    }

    pub fn update(&mut self, messages: Vec<ToSchedulerMessage>, sender: &mut UnboundedSender<FromSchedulerMessage>) {
        for message in messages {
            match message {
                ToSchedulerMessage::TaskUpdate(_) => { /* TODO */ }
                ToSchedulerMessage::NewTask(ti) => {
                    self._tmp_hack.push(ti.id);
                }
                ToSchedulerMessage::NewWorker(wi) => {
                    assert!(self
                        .workers
                        .insert(
                            wi.id,
                            Worker {
                                id: wi.id,
                                ncpus: wi.ncpus,
                                free_cpus: wi.ncpus as i32,
                            },
                        )
                        .is_none());
                }
                ToSchedulerMessage::NetworkBandwidth(nb) => {
                    self.network_bandwidth = nb;
                }
            }
        }

        // HACK, random scheduler
        if !self.workers.is_empty() {
            use rand::seq::SliceRandom;
            let mut result = Vec::new();
            let mut rng = rand::thread_rng();
            let ws: Vec<WorkerId> = self.workers.values().map(|w| w.id).collect();
            // TMP HACK
            for task_id in &self._tmp_hack {
                result.push(TaskAssignment {
                    task: *task_id,
                    worker: *ws.choose(&mut rng).unwrap(),
                    priority: 0,
                });
            }
            self._tmp_hack.clear();

            sender
                .try_send(FromSchedulerMessage::TaskAssignments(result))
                .unwrap();
        }
    }
}
