use super::schedproto::*;
use std::collections::HashMap;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub struct Worker {
    pub id: WorkerId,
    pub ncpus: u32,
    pub free_cpus: i32,
}

pub struct State {
    network_bandwidth: f32,
    workers: HashMap<WorkerId, Worker>,
}

impl State {
    pub fn new() -> Self {
        State {
            workers: Default::default(),
            network_bandwidth: 100.0, // Guess better default
        }
    }

    pub fn update(&mut self, update: Update, sender: &UnboundedSender<FromSchedulerMessage>) {
        for wi in update.new_workers {
            assert!(self
                .workers
                .insert(
                    wi.id,
                    Worker {
                        id: wi.id,
                        ncpus: wi.ncpus,
                        free_cpus: wi.ncpus as i32,
                    }
                )
                .is_none());
        }
        if let Some(nb) = update.network_bandwidth {
            self.network_bandwidth = nb;
        }

        for task_info in &update.new_tasks {}
    }
}
