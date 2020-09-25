use std::cmp::Reverse;

use bytes::Bytes;
use hashbrown::HashMap;
use tokio::sync::mpsc::UnboundedSender;

use crate::common::WrappedRcRefCell;
use crate::server::protocol::Priority;
use crate::worker::subworker::{SubworkerId, SubworkerRef};
use crate::worker::task::TaskRef;
use crate::worker::data::DataObjectRef;
use crate::server::protocol::key::DaskKey;

pub type WorkerStateRef = WrappedRcRefCell<WorkerState>;

pub struct WorkerState {
    pub sender: UnboundedSender<Bytes>,
    pub ncpus: u32,
    pub listen_address: String,
    pub subworkers: HashMap<SubworkerId, SubworkerRef>,
    pub free_subworkers: Vec<SubworkerRef>,
    pub task_queue: priority_queue::PriorityQueue<TaskRef, Reverse<(Priority, Priority)>>,
    pub data_objects: HashMap<DaskKey, DataObjectRef>,
}


impl WorkerState {
    pub fn set_subworkers(&mut self, subworkers: Vec<SubworkerRef>) {
        assert!(self.subworkers.is_empty() && self.free_subworkers.is_empty());
        self.free_subworkers = subworkers.clone();
        self.subworkers = subworkers
            .iter()
            .map(|s| {
                let id = s.get().id;
                (id, s.clone())
            })
            .collect();
    }

    pub fn add_data_object(&mut self, data_ref: DataObjectRef) {
        let key = data_ref.get().key.clone();
        self.data_objects.insert(key, data_ref);
    }

    pub fn send_message_to_server(&self, data: Vec<u8>) {
        self.sender.send(data.into()).unwrap();
    }
}

impl WorkerStateRef {
    pub fn new(
        sender: UnboundedSender<Bytes>,
        ncpus: u32,
        listen_address: String,
    ) -> Self {
        Self::wrap(WorkerState {
            sender,
            ncpus,
            listen_address,
            subworkers: Default::default(),
            free_subworkers: Default::default(),
            task_queue: Default::default(),
            data_objects: Default::default(),
        })
    }
}
