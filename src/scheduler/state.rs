use std::collections::HashMap;

use tokio::sync::mpsc::UnboundedSender;

use super::schedproto::*;

pub struct Worker {
    pub id: WorkerId,
    pub ncpus: u32,
    pub free_cpus: i32,
}

pub struct State {
    network_bandwidth: f32,
    workers: HashMap<WorkerId, Worker>,

    _tmp_hack: Vec<WorkerId>,
}

impl State {
    pub fn new() -> Self {
        State {
            workers: Default::default(),
            network_bandwidth: 100.0, // Guess better default
            _tmp_hack: Vec::new(),
        }
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
