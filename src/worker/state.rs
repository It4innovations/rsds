use crate::common::{Set, WrappedRcRefCell};
use crate::server::protocol::key::DaskKey;
use crate::server::protocol::dasktransport::DaskPacket;
use tokio::sync::mpsc::UnboundedSender;
use bytes::Bytes;
use crate::worker::subworker::{SubworkerRef, SubworkerId};
use hashbrown::{HashSet, HashMap};
use crate::server::protocol::Priority;
use crate::worker::task::TaskRef;
use std::cmp::Reverse;

pub type WorkerStateRef = WrappedRcRefCell<WorkerState>;

pub struct WorkerState {
    pub sender: UnboundedSender<Bytes>,
    pub ncpus: u32,
    pub listen_address: String,
    pub subworkers: HashMap<SubworkerId, SubworkerRef>,
    pub free_subworkers: Vec<SubworkerRef>,

    pub task_queue: priority_queue::PriorityQueue<TaskRef, Reverse<(Priority, Priority)>>,
}

impl WorkerState {
    /*pub fn send(&self, packet: DaskPacket) {
        self.sender.send(packet).unwrap();
    }*/

}

impl WorkerStateRef {
    pub fn new(sender: UnboundedSender<Bytes>, ncpus: u32, listen_address: String, subworkers: &[SubworkerRef]) -> Self {
        let sw : HashMap<SubworkerId, SubworkerRef> = subworkers.iter().map(|s| {
            let id = s.get().id;
            (id, s.clone())
        }).collect();
        Self::wrap(WorkerState {
            sender,
            ncpus,
            listen_address,
            subworkers: sw,
            free_subworkers: subworkers.to_vec(),
            task_queue: Default::default(),
        })
    }

}
